import asyncio
import uuid
import logging
from contextlib import asynccontextmanager
from typing import Optional

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# ============================================================
# CONFIG
# ============================================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("nba-chain")

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "nba_chain",
    "user": "postgres",
    "password": "Helia16",
}

TURN_DURATION = 15  # secondes par tour


def get_conn():
    return psycopg2.connect(**DB_CONFIG, cursor_factory=psycopg2.extras.RealDictCursor)


# ============================================================
# ÉTAT EN MÉMOIRE — Matchmaking & Parties
# ============================================================

# File d'attente : liste de tuples (WebSocket, player_name)
waiting_queue: list[tuple[WebSocket, str]] = []

# Parties actives : { room_id -> GameState }
active_games: dict[str, dict] = {}


# ============================================================
# APP
# ============================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("NBA Chain API démarrée ✅")
    yield
    logger.info("NBA Chain API arrêtée")

app = FastAPI(title="NBA Chain API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# ENDPOINT 1 — Autocomplete joueurs
# GET /search?q=lebron
# ============================================================

@app.get("/search")
def search_players(q: str = Query(..., min_length=2)):
    """
    Retourne les joueurs dont le nom contient 'q'.
    Utilisé pendant que le joueur tape sa réponse.
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, name
        FROM players
        WHERE name ILIKE %s
        ORDER BY name
        LIMIT 10
        """,
        (f"%{q}%",),
    )
    results = cur.fetchall()
    cur.close()
    conn.close()
    return {"players": [dict(r) for r in results]}


# ============================================================
# ENDPOINT 2 — Vérification (hors WebSocket, optionnel)
# POST /verify
# Body: { "current_player_id": 2544, "proposed_player_id": 203076 }
# ============================================================

class VerifyRequest(BaseModel):
    current_player_id: int
    proposed_player_id: int


@app.post("/verify")
def verify(req: VerifyRequest):
    """
    Vérifie si deux joueurs ont joué ensemble.
    Retourne valid + l'équipe et la saison commune.
    """
    a = min(req.current_player_id, req.proposed_player_id)
    b = max(req.current_player_id, req.proposed_player_id)

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT pt.season, t.name AS team
        FROM played_together pt
        JOIN teams t ON t.id = pt.team_id
        WHERE pt.player_a = %s AND pt.player_b = %s
        ORDER BY pt.season DESC
        LIMIT 1
        """,
        (a, b),
    )
    result = cur.fetchone()
    cur.close()
    conn.close()

    if result:
        return {"valid": True, "season": result["season"], "team": result["team"]}
    return {"valid": False}


# ============================================================
# ENDPOINT 3 — Détail d'un joueur
# GET /player/2544
# ============================================================

@app.get("/player/{player_id}")
def get_player(player_id: int):
    """
    Retourne les infos d'un joueur : nom + toutes ses équipes.
    """
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT id, name FROM players WHERE id = %s", (player_id,))
    player = cur.fetchone()

    if not player:
        return {"error": "Joueur introuvable"}

    cur.execute(
        """
        SELECT t.name AS team, s.season
        FROM stints s
        JOIN teams t ON t.id = s.team_id
        WHERE s.player_id = %s
        ORDER BY s.season DESC
        """,
        (player_id,),
    )
    career = cur.fetchall()
    cur.close()
    conn.close()

    return {"id": player["id"], "name": player["name"], "career": [dict(c) for c in career]}


# ============================================================
# HELPERS — Logique de partie
# ============================================================

def db_verify_link(player_a_id: int, player_b_id: int) -> Optional[dict]:
    """Retourne la connexion (team + saison) si les deux joueurs ont joué ensemble, None sinon."""
    a = min(player_a_id, player_b_id)
    b = max(player_a_id, player_b_id)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT pt.season, t.name AS team
        FROM played_together pt
        JOIN teams t ON t.id = pt.team_id
        WHERE pt.player_a = %s AND pt.player_b = %s
        ORDER BY pt.season DESC
        LIMIT 1
        """,
        (a, b),
    )
    result = cur.fetchone()
    cur.close()
    conn.close()
    return dict(result) if result else None


def db_get_player_name(player_id: int) -> Optional[str]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT name FROM players WHERE id = %s", (player_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row["name"] if row else None


async def broadcast(game: dict, message: dict):
    """Envoie un message JSON aux deux joueurs de la room."""
    for ws, _ in game["players"]:
        try:
            await ws.send_json(message)
        except Exception:
            pass


async def send_to(ws: WebSocket, message: dict):
    try:
        await ws.send_json(message)
    except Exception:
        pass


# ============================================================
# TIMER SERVEUR — 15 secondes par tour
# ============================================================

async def run_turn_timer(room_id: str, turn_index: int):
    """
    Attend TURN_DURATION secondes. Si le tour n'a pas changé,
    le joueur actif est déclaré perdant (timeout).
    """
    await asyncio.sleep(TURN_DURATION)

    game = active_games.get(room_id)
    if not game:
        return
    if game["current_turn"] != turn_index:
        return  # Le tour a déjà avancé, réponse fournie à temps

    loser_idx = turn_index % 2
    winner_idx = 1 - loser_idx
    loser_ws, loser_name = game["players"][loser_idx]
    winner_ws, winner_name = game["players"][winner_idx]

    logger.info(f"[{room_id}] Timeout — {loser_name} a perdu")

    await send_to(loser_ws, {"event": "game_over", "result": "lose", "reason": "timeout"})
    await send_to(winner_ws, {"event": "game_over", "result": "win", "reason": "opponent_timeout"})

    del active_games[room_id]


def start_turn_timer(room_id: str, turn_index: int):
    asyncio.create_task(run_turn_timer(room_id, turn_index))


# ============================================================
# WEBSOCKET — /ws/{player_name}
# ============================================================
#
# Flux de messages (JSON) :
#
# Serveur → Client :
#   { "event": "waiting" }
#   { "event": "game_start", "your_turn": bool, "opponent": str,
#     "first_player": { "id": int, "name": str } }
#   { "event": "your_turn", "current_player": { "id": int, "name": str } }
#   { "event": "opponent_played", "player": { "id": int, "name": str },
#     "link": { "team": str, "season": str } }
#   { "event": "invalid_answer", "reason": str }
#   { "event": "game_over", "result": "win"|"lose", "reason": str }
#
# Client → Serveur :
#   { "action": "play", "player_id": int }
#
# ============================================================

@app.websocket("/ws/{player_name}")
async def websocket_endpoint(websocket: WebSocket, player_name: str):
    await websocket.accept()
    logger.info(f"Connexion WebSocket : {player_name}")

    # ── Matchmaking ──────────────────────────────────────────
    if waiting_queue:
        opponent_ws, opponent_name = waiting_queue.pop(0)
        room_id = str(uuid.uuid4())

        # Joueur de départ tiré aléatoirement
        import random
        players = [(websocket, player_name), (opponent_ws, opponent_name)]
        random.shuffle(players)

        # Joueur NBA de départ : LeBron James (id stable dans la nba_api)
        first_player_id = 2544
        first_player_name = db_get_player_name(first_player_id) or "LeBron James"

        game = {
            "room_id": room_id,
            "players": players,           # [(ws, name), (ws, name)]
            "current_turn": 0,            # index du joueur actif (alterne 0/1)
            "current_player_id": first_player_id,
            "used_player_ids": {first_player_id},
        }
        active_games[room_id] = game

        logger.info(f"[{room_id}] Partie créée : {players[0][1]} vs {players[1][1]}")

        # Notifier les deux joueurs
        for idx, (ws, name) in enumerate(players):
            await send_to(ws, {
                "event": "game_start",
                "room_id": room_id,
                "your_name": name,
                "opponent": players[1 - idx][1],
                "your_turn": idx == 0,
                "first_player": {"id": first_player_id, "name": first_player_name},
            })

        # Lancer le timer pour le premier joueur
        start_turn_timer(room_id, 0)

    else:
        # Pas d'adversaire disponible : entrer en file d'attente
        waiting_queue.append((websocket, player_name))
        await send_to(websocket, {"event": "waiting"})
        logger.info(f"{player_name} en attente d'adversaire")

    # ── Boucle de réception des messages ─────────────────────
    try:
        while True:
            data = await websocket.receive_json()

            if data.get("action") != "play":
                continue

            # Trouver la room de ce joueur
            room_id = None
            player_idx = None
            for rid, g in active_games.items():
                for idx, (ws, _) in enumerate(g["players"]):
                    if ws is websocket:
                        room_id = rid
                        player_idx = idx
                        break
                if room_id:
                    break

            if room_id is None:
                await send_to(websocket, {"event": "error", "message": "Aucune partie en cours"})
                continue

            game = active_games[room_id]

            # ── Vérifier que c'est bien le tour de ce joueur ──
            if game["current_turn"] % 2 != player_idx:
                await send_to(websocket, {
                    "event": "invalid_answer",
                    "reason": "Ce n'est pas ton tour",
                })
                continue

            proposed_id = data.get("player_id")
            if not proposed_id:
                await send_to(websocket, {"event": "invalid_answer", "reason": "player_id manquant"})
                continue

            # ── Vérifier que le joueur n'a pas déjà été utilisé ──
            if proposed_id in game["used_player_ids"]:
                await send_to(websocket, {
                    "event": "invalid_answer",
                    "reason": "Ce joueur a déjà été utilisé dans cette partie",
                })
                continue

            # ── Vérifier le lien avec le joueur courant ──────────
            link = db_verify_link(game["current_player_id"], proposed_id)
            if not link:
                await send_to(websocket, {
                    "event": "invalid_answer",
                    "reason": "Ces deux joueurs n'ont jamais évolué ensemble",
                })
                continue

            # ── Réponse valide ────────────────────────────────────
            proposed_name = db_get_player_name(proposed_id) or str(proposed_id)
            game["used_player_ids"].add(proposed_id)
            game["current_player_id"] = proposed_id
            game["current_turn"] += 1

            logger.info(
                f"[{room_id}] Tour {game['current_turn']} : "
                f"{game['players'][player_idx][1]} joue {proposed_name} "
                f"(via {link['team']}, {link['season']})"
            )

            # Notifier les deux joueurs du coup joué
            await broadcast(game, {
                "event": "opponent_played",
                "played_by": game["players"][player_idx][1],
                "player": {"id": proposed_id, "name": proposed_name},
                "link": link,
            })

            # Notifier le joueur suivant que c'est son tour
            next_idx = game["current_turn"] % 2
            next_ws = game["players"][next_idx][0]
            await send_to(next_ws, {
                "event": "your_turn",
                "current_player": {"id": proposed_id, "name": proposed_name},
            })

            # Lancer le timer pour le prochain tour
            start_turn_timer(room_id, game["current_turn"])

    except WebSocketDisconnect:
        logger.info(f"Déconnexion WebSocket : {player_name}")

        # Retirer de la file d'attente si le joueur n'avait pas d'adversaire
        waiting_queue[:] = [(ws, name) for ws, name in waiting_queue if ws is not websocket]

        # Si une partie était en cours, l'adversaire gagne par forfait
        for room_id, game in list(active_games.items()):
            for idx, (ws, _) in enumerate(game["players"]):
                if ws is websocket:
                    opponent_ws = game["players"][1 - idx][0]
                    opponent_name = game["players"][1 - idx][1]
                    await send_to(opponent_ws, {
                        "event": "game_over",
                        "result": "win",
                        "reason": "opponent_disconnected",
                    })
                    logger.info(f"[{room_id}] {player_name} s'est déconnecté — {opponent_name} gagne")
                    del active_games[room_id]
                    break


# ============================================================
# ENDPOINT 4 — Santé / debug
# GET /status
# ============================================================

@app.get("/status")
def status():
    return {
        "waiting_players": len(waiting_queue),
        "active_games": len(active_games),
        "games": [
            {
                "room_id": g["room_id"],
                "players": [name for _, name in g["players"]],
                "turn": g["current_turn"],
                "current_player_id": g["current_player_id"],
            }
            for g in active_games.values()
        ],
    }


# ============================================================
# SERVIR LE FRONTEND (optionnel — décommenter en prod)
# Place index.html dans un dossier /static
# ============================================================
# app.mount("/", StaticFiles(directory="static", html=True), name="static")
