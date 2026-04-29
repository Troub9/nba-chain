import asyncio
import uuid
import random
import logging
import os
from contextlib import asynccontextmanager

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("nba-chain")

# ============================================================
# CONFIG BASE DE DONNEES
# ============================================================

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "dbname":   os.getenv("DB_NAME", "nba_chain"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "Helia16"),
}

TURN_DURATION = 15  # secondes par tour


def get_conn():
    return psycopg2.connect(**DB_CONFIG, cursor_factory=psycopg2.extras.RealDictCursor)


# ============================================================
# ETAT EN MEMOIRE
# ============================================================

# File d'attente : liste de tuples (WebSocket, player_name)
waiting_queue: list = []

# Parties actives : { room_id -> dict }
active_games: dict = {}


# ============================================================
# APP FASTAPI
# ============================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("NBA Chain API demarree")
    yield
    logger.info("NBA Chain API arretee")

app = FastAPI(title="NBA Chain API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# ENDPOINT 1 — Autocomplete
# GET /search?q=lebron
# ============================================================

@app.get("/search")
def search_players(q: str = Query(..., min_length=2)):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, name FROM players WHERE name ILIKE %s ORDER BY name LIMIT 10",
        (f"%{q}%",),
    )
    results = cur.fetchall()
    cur.close()
    conn.close()
    return {"players": [dict(r) for r in results]}


# ============================================================
# ENDPOINT 2 — Verification manuelle (optionnel, hors WebSocket)
# POST /verify
# Body: { "current_player_id": 2544, "proposed_player_id": 203076 }
# ============================================================

class VerifyRequest(BaseModel):
    current_player_id: int
    proposed_player_id: int


@app.post("/verify")
def verify(req: VerifyRequest):
    result = db_verify_link(req.current_player_id, req.proposed_player_id)
    if result:
        return {"valid": True, "season": result["season"], "team": result["team"]}
    return {"valid": False}


# ============================================================
# ENDPOINT 3 — Detail d'un joueur
# GET /player/2544
# ============================================================

@app.get("/player/{player_id}")
def get_player(player_id: int):
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
# ENDPOINT 4 — Status / debug
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
# HELPERS BASE DE DONNEES
# ============================================================

def db_verify_link(player_a_id: int, player_b_id: int):
    """Retourne {team, season} si les deux joueurs ont joue ensemble, None sinon."""
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


def db_get_player(player_id: int):
    """Retourne {id, name} pour un joueur, None si introuvable."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM players WHERE id = %s", (player_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return dict(row) if row else None


def db_random_starter():
    """Retourne un joueur NBA aleatoire depuis la base pour demarrer la partie."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM players ORDER BY RANDOM() LIMIT 1")
    row = cur.fetchone()
    cur.close()
    conn.close()
    return dict(row) if row else {"id": 2544, "name": "LeBron James"}


# ============================================================
# TIMER SERVEUR
# ============================================================

async def run_turn_timer(room_id: str, turn_index: int):
    """
    Attend TURN_DURATION secondes.
    Si le tour n'a pas change, le joueur actif est declare perdant.
    """
    await asyncio.sleep(TURN_DURATION)

    game = active_games.get(room_id)
    if not game:
        return
    if game["current_turn"] != turn_index:
        return  # Le joueur a repondu a temps

    loser_idx  = turn_index % 2
    winner_idx = 1 - loser_idx
    loser_ws,  loser_name  = game["players"][loser_idx]
    winner_ws, winner_name = game["players"][winner_idx]

    logger.info("[%s] Timeout — %s a perdu", room_id, loser_name)

    await safe_send(loser_ws,  {"event": "game_over", "result": "lose",  "reason": "timeout"})
    await safe_send(winner_ws, {"event": "game_over", "result": "win",   "reason": "opponent_timeout"})

    active_games.pop(room_id, None)


def start_turn_timer(room_id: str, turn_index: int):
    asyncio.create_task(run_turn_timer(room_id, turn_index))


# ============================================================
# HELPERS WEBSOCKET
# ============================================================

async def safe_send(ws: WebSocket, message: dict):
    try:
        await ws.send_json(message)
    except Exception:
        pass


async def broadcast(game: dict, message: dict):
    for ws, _ in game["players"]:
        await safe_send(ws, message)


def find_game_by_ws(websocket: WebSocket):
    """Retourne (room_id, player_index) pour la WebSocket donnee, ou (None, None)."""
    for room_id, game in active_games.items():
        for idx, (ws, _) in enumerate(game["players"]):
            if ws is websocket:
                return room_id, idx
    return None, None


# ============================================================
# WEBSOCKET — /ws/{player_name}
#
# Evenements serveur -> client :
#   waiting          — en attente d'adversaire
#   game_start       — partie trouvee : { opponent, your_turn, first_player }
#   opponent_played  — coup valide joue : { played_by, player, link }
#   your_turn        — c'est ton tour : { current_player }
#   invalid_answer   — coup refuse : { reason }
#   game_over        — fin : { result: win|lose, reason }
#
# Evenements client -> serveur :
#   { "action": "play", "player_id": int }
# ============================================================

@app.websocket("/ws/{player_name}")
async def websocket_endpoint(websocket: WebSocket, player_name: str):
    await websocket.accept()
    logger.info("Connexion : %s", player_name)

    # ── Matchmaking ──────────────────────────────────────────
    if waiting_queue:
        opponent_ws, opponent_name = waiting_queue.pop(0)
        room_id = str(uuid.uuid4())

        # Ordre aleatoire : qui commence
        players = [(websocket, player_name), (opponent_ws, opponent_name)]
        random.shuffle(players)

        # Joueur NBA de depart
        starter = db_random_starter()

        game = {
            "room_id":          room_id,
            "players":          players,
            "current_turn":     0,
            "current_player_id": starter["id"],
            "used_player_ids":  {starter["id"]},
        }
        active_games[room_id] = game

        logger.info("[%s] Partie : %s vs %s | Starter : %s", room_id, players[0][1], players[1][1], starter["name"])

        for idx, (ws, name) in enumerate(players):
            await safe_send(ws, {
                "event":        "game_start",
                "room_id":      room_id,
                "your_name":    name,
                "opponent":     players[1 - idx][1],
                "your_turn":    idx == 0,
                "first_player": starter,
            })

        start_turn_timer(room_id, 0)

    else:
        waiting_queue.append((websocket, player_name))
        await safe_send(websocket, {"event": "waiting"})
        logger.info("%s en attente", player_name)

    # ── Boucle de reception ───────────────────────────────────
    try:
        while True:
            data = await websocket.receive_json()

            if data.get("action") != "play":
                continue

            room_id, player_idx = find_game_by_ws(websocket)
            if room_id is None:
                await safe_send(websocket, {"event": "error", "message": "Aucune partie en cours"})
                continue

            game = active_games[room_id]

            # Verifier que c'est bien le tour de ce joueur
            if game["current_turn"] % 2 != player_idx:
                await safe_send(websocket, {"event": "invalid_answer", "reason": "Ce n'est pas ton tour"})
                continue

            proposed_id = data.get("player_id")
            if not proposed_id:
                await safe_send(websocket, {"event": "invalid_answer", "reason": "player_id manquant"})
                continue

            # Joueur deja utilise dans cette partie ?
            if proposed_id in game["used_player_ids"]:
                await safe_send(websocket, {"event": "invalid_answer", "reason": "Ce joueur a deja ete utilise"})
                continue

            # Lien avec le joueur courant ?
            link = db_verify_link(game["current_player_id"], proposed_id)
            if not link:
                await safe_send(websocket, {"event": "invalid_answer", "reason": "Ces deux joueurs n'ont jamais evolue ensemble"})
                continue

            # Reponse valide
            player_info = db_get_player(proposed_id)
            proposed_name = player_info["name"] if player_info else str(proposed_id)

            game["used_player_ids"].add(proposed_id)
            game["current_player_id"] = proposed_id
            game["current_turn"] += 1

            logger.info("[%s] Tour %d : %s joue %s (%s %s)",
                room_id, game["current_turn"],
                game["players"][player_idx][1],
                proposed_name, link["team"], link["season"])

            # Notifier les deux joueurs
            await broadcast(game, {
                "event":     "opponent_played",
                "played_by": game["players"][player_idx][1],
                "player":    {"id": proposed_id, "name": proposed_name},
                "link":      link,
            })

            # Demander son coup au joueur suivant
            next_idx = game["current_turn"] % 2
            next_ws  = game["players"][next_idx][0]
            await safe_send(next_ws, {
                "event":          "your_turn",
                "current_player": {"id": proposed_id, "name": proposed_name},
            })

            start_turn_timer(room_id, game["current_turn"])

    except WebSocketDisconnect:
        logger.info("Deconnexion : %s", player_name)

        # Retirer de la file d'attente si pas encore en partie
        waiting_queue[:] = [(ws, n) for ws, n in waiting_queue if ws is not websocket]

        # Si une partie etait en cours, l'adversaire gagne par forfait
        room_id, player_idx = find_game_by_ws(websocket)
        if room_id:
            game = active_games[room_id]
            opponent_ws   = game["players"][1 - player_idx][0]
            opponent_name = game["players"][1 - player_idx][1]
            await safe_send(opponent_ws, {
                "event":  "game_over",
                "result": "win",
                "reason": "opponent_disconnected",
            })
            logger.info("[%s] %s deconnecte — %s gagne", room_id, player_name, opponent_name)
            active_games.pop(room_id, None)


# ============================================================
# SERVIR LE FRONTEND (static/)
# Place index.html dans le dossier static/
# ============================================================

app.mount("/", StaticFiles(directory="static", html=True), name="static")
