import asyncio
import uuid
import random
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from urllib.parse import urlparse

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from passlib.context import CryptContext
from jose import JWTError, jwt

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("scoriax")

# ============================================================
# CONFIG
# ============================================================

DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL:
    r = urlparse(DATABASE_URL)
    DB_CONFIG = {
        "host":     r.hostname,
        "port":     r.port or 5432,
        "dbname":   r.path.lstrip("/"),
        "user":     r.username,
        "password": r.password,
    }
else:
    DB_CONFIG = {
        "host":     "localhost",
        "port":     5432,
        "dbname":   "nba_chain",
        "user":     "postgres",
        "password": os.getenv("DB_PASSWORD", "Helia16"),
    }

SECRET_KEY        = os.getenv("SECRET_KEY", "scoriax-dev-secret-change-in-prod")
ALGORITHM         = "HS256"
TOKEN_EXPIRE_HOURS = 72
TURN_DURATION     = 15

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def get_conn():
    return psycopg2.connect(**DB_CONFIG, cursor_factory=psycopg2.extras.RealDictCursor)


# ============================================================
# ETAT EN MEMOIRE
# ============================================================

waiting_queue: list = []
matchmaking_lock: asyncio.Lock = None
active_games: dict = {}


# ============================================================
# APP
# ============================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    global matchmaking_lock
    matchmaking_lock = asyncio.Lock()
    logger.info("Scoriax API demarree")
    yield
    logger.info("Scoriax API arretee")


app = FastAPI(title="Scoriax API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# ENDPOINT — Frontend
# ============================================================

@app.get("/", response_class=HTMLResponse)
def root():
    with open("static/index.html", encoding="utf-8") as f:
        return HTMLResponse(f.read())


# ============================================================
# AUTH HELPERS
# ============================================================

def hash_password(password: str) -> str:
    return pwd_context.hash(password)

def verify_password(password: str, hashed: str) -> bool:
    return pwd_context.verify(password, hashed)

def create_token(username: str) -> str:
    expire = datetime.utcnow() + timedelta(hours=TOKEN_EXPIRE_HOURS)
    return jwt.encode({"sub": username, "exp": expire}, SECRET_KEY, algorithm=ALGORITHM)

def decode_token(token: str):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload.get("sub")
    except JWTError:
        return None


# ============================================================
# ELO HELPER
# ============================================================

def compute_elo_change(winner_elo: int, loser_elo: int, k: int = 32) -> int:
    expected = 1 / (1 + 10 ** ((loser_elo - winner_elo) / 400))
    return round(k * (1 - expected))


# ============================================================
# ENDPOINTS AUTH
# ============================================================

class RegisterRequest(BaseModel):
    username: str
    password: str

class LoginRequest(BaseModel):
    username: str
    password: str


@app.post("/register")
def register(req: RegisterRequest):
    if len(req.username) < 3 or len(req.username) > 20:
        raise HTTPException(400, "Le pseudo doit faire entre 3 et 20 caracteres")
    if len(req.password) < 6:
        raise HTTPException(400, "Le mot de passe doit faire au moins 6 caracteres")
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO users (username, password_hash) VALUES (%s, %s) RETURNING id, username, elo",
            (req.username, hash_password(req.password))
        )
        user = dict(cur.fetchone())
        conn.commit()
        return {"token": create_token(user["username"]), "username": user["username"], "elo": user["elo"]}
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        raise HTTPException(409, "Ce pseudo est deja pris")
    finally:
        cur.close()
        conn.close()


@app.post("/login")
def login(req: LoginRequest):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE username = %s", (req.username,))
    user = cur.fetchone()
    cur.close()
    conn.close()
    if not user or not verify_password(req.password, user["password_hash"]):
        raise HTTPException(401, "Pseudo ou mot de passe incorrect")
    return {"token": create_token(user["username"]), "username": user["username"], "elo": user["elo"]}


@app.get("/me")
def me(token: str = Query(...)):
    username = decode_token(token)
    if not username:
        raise HTTPException(401, "Token invalide")
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT username, elo, wins, losses FROM users WHERE username = %s", (username,))
    user = cur.fetchone()
    cur.close()
    conn.close()
    if not user:
        raise HTTPException(404, "Utilisateur introuvable")
    return dict(user)


# ============================================================
# ENDPOINT — Leaderboard
# ============================================================

@app.get("/leaderboard")
def leaderboard():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT username, elo, wins, losses,
               RANK() OVER (ORDER BY elo DESC) AS rank
        FROM users
        ORDER BY elo DESC
        LIMIT 10
    """)
    rows = [dict(r) for r in cur.fetchall()]
    cur.close()
    conn.close()
    return {"leaderboard": rows}


# ============================================================
# ENDPOINT 1 — Autocomplete
# GET /search?q=lebron
# ============================================================

@app.get("/search")
def search_players(q: str = Query(..., min_length=2)):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, name FROM players WHERE name ILIKE %s ORDER BY is_featured DESC NULLS LAST, name LIMIT 10",
        (f"%{q}%",),
    )
    results = cur.fetchall()
    cur.close()
    conn.close()
    return {"players": [dict(r) for r in results]}


# ============================================================
# ENDPOINT 2 — Verification manuelle
# POST /verify
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
# ENDPOINT 3 — Detail joueur
# GET /player/{player_id}
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
# ENDPOINT 4 — Hints apres defaite
# GET /hints/{player_id}?exclude=1,2,3
# ============================================================

@app.get("/hints/{player_id}")
def get_hints(player_id: int, exclude: str = Query(default="")):
    exclude_ids = [int(x) for x in exclude.split(",") if x.strip().isdigit()]
    exclude_ids.append(player_id)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT p.id, p.name, t.name AS team, pt.season
        FROM played_together pt
        JOIN players p ON p.id = CASE
            WHEN pt.player_a = %s THEN pt.player_b
            ELSE pt.player_a
        END
        JOIN teams t ON t.id = pt.team_id
        WHERE (pt.player_a = %s OR pt.player_b = %s)
          AND p.id != ALL(%s)
        ORDER BY RANDOM()
        LIMIT 3
    """, (player_id, player_id, player_id, exclude_ids))
    hints = [dict(r) for r in cur.fetchall()]
    cur.close()
    conn.close()
    return {"hints": hints}


# ============================================================
# ENDPOINT 5 — Status debug
# GET /status
# ============================================================

@app.get("/status")
def status():
    return {
        "waiting_players": len(waiting_queue),
        "active_games":    len(active_games),
        "games": [
            {
                "room_id":           g["room_id"],
                "players":           [name for _, name in g["players"]],
                "turn":              g["current_turn"],
                "current_player_id": g["current_player_id"],
            }
            for g in active_games.values()
        ],
    }


# ============================================================
# HELPERS DB
# ============================================================

def db_verify_link(player_a_id: int, player_b_id: int):
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
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM players WHERE id = %s", (player_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return dict(row) if row else None


def db_random_starter():
    """Pioche un joueur featured (Top 75 + notables) comme joueur de depart"""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM players WHERE is_featured = TRUE ORDER BY RANDOM() LIMIT 1")
    row = cur.fetchone()
    cur.close()
    conn.close()
    # Fallback si is_featured pas encore rempli
    if not row:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT id, name FROM players ORDER BY RANDOM() LIMIT 1")
        row = cur.fetchone()
        cur.close()
        conn.close()
    return dict(row) if row else {"id": 2544, "name": "LeBron James"}


async def save_game_result(winner_name: str, loser_name: str, chain: list, reason: str):
    """Sauvegarde le resultat et met a jour les ELOs"""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, elo FROM users WHERE username = %s", (winner_name,))
    winner = cur.fetchone()
    cur.execute("SELECT id, elo FROM users WHERE username = %s", (loser_name,))
    loser = cur.fetchone()

    if not winner or not loser:
        cur.close(); conn.close()
        return None

    change = compute_elo_change(winner["elo"], loser["elo"])

    cur.execute("UPDATE users SET elo = elo + %s, wins = wins + 1 WHERE id = %s", (change, winner["id"]))
    cur.execute("UPDATE users SET elo = GREATEST(100, elo - %s), losses = losses + 1 WHERE id = %s", (change, loser["id"]))
    cur.execute("""
        INSERT INTO game_history (winner_id, loser_id, winner_elo_before, loser_elo_before, elo_change, chain, reason)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (winner["id"], loser["id"], winner["elo"], loser["elo"], change, psycopg2.extras.Json(chain), reason))
    conn.commit()
    cur.close()
    conn.close()
    return change


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
    for room_id, game in active_games.items():
        for idx, (ws, _) in enumerate(game["players"]):
            if ws is websocket:
                return room_id, idx
    return None, None


# ============================================================
# TIMER SERVEUR
# ============================================================

async def run_turn_timer(room_id: str, turn_index: int):
    await asyncio.sleep(TURN_DURATION)
    game = active_games.get(room_id)
    if not game or game["current_turn"] != turn_index:
        return

    loser_idx  = turn_index % 2
    winner_idx = 1 - loser_idx
    loser_ws,  loser_name  = game["players"][loser_idx]
    winner_ws, winner_name = game["players"][winner_idx]

    logger.info("[%s] Timeout — %s a perdu", room_id, loser_name)

    elo_change = await save_game_result(winner_name, loser_name, game["chain"], "timeout")

    # Recuperer nouveaux ELOs
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT elo FROM users WHERE username = %s", (winner_name,))
    w = cur.fetchone()
    cur.execute("SELECT elo FROM users WHERE username = %s", (loser_name,))
    l = cur.fetchone()
    cur.close(); conn.close()

    await safe_send(loser_ws, {
        "event": "game_over", "result": "lose", "reason": "timeout",
        "current_player_id": game["current_player_id"],
        "used_ids": list(game["used_player_ids"]),
        "elo_change": f"-{elo_change}" if elo_change else None,
        "new_elo": l["elo"] if l else None,
        "chain": game["chain"],
    })
    await safe_send(winner_ws, {
        "event": "game_over", "result": "win", "reason": "opponent_timeout",
        "elo_change": f"+{elo_change}" if elo_change else None,
        "new_elo": w["elo"] if w else None,
        "chain": game["chain"],
    })
    active_games.pop(room_id, None)


def start_turn_timer(room_id: str, turn_index: int):
    asyncio.create_task(run_turn_timer(room_id, turn_index))


# ============================================================
# WEBSOCKET — /ws/{player_name}
# ============================================================

@app.websocket("/ws/{player_name}")
async def websocket_endpoint(websocket: WebSocket, player_name: str):
    await websocket.accept()
    logger.info("Connexion : %s", player_name)

    do_start      = False
    opponent_ws   = None
    opponent_name = None

    async with matchmaking_lock:
        if waiting_queue:
            opponent_ws, opponent_name = waiting_queue.pop(0)
            do_start = True
        else:
            waiting_queue.append((websocket, player_name))

    if do_start:
        room_id = str(uuid.uuid4())
        players = [(websocket, player_name), (opponent_ws, opponent_name)]
        random.shuffle(players)
        starter = db_random_starter()

        game = {
            "room_id":           room_id,
            "players":           players,
            "current_turn":      0,
            "current_player_id": starter["id"],
            "used_player_ids":   {starter["id"]},
            "chain":             [{"id": starter["id"], "name": starter["name"]}],
        }
        active_games[room_id] = game

        logger.info("[%s] Partie : %s vs %s | Starter : %s",
                    room_id, players[0][1], players[1][1], starter["name"])

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
        await safe_send(websocket, {"event": "waiting"})
        logger.info("%s en attente d'adversaire", player_name)

    # ── Boucle de reception ───────────────────────────────────
    try:
        while True:
            data = await websocket.receive_json()
            action = data.get("action")

            # Anti-triche : forfait (changement d'onglet)
            if action == "forfeit":
                room_id, player_idx = find_game_by_ws(websocket)
                if room_id:
                    game = active_games[room_id]
                    winner_ws, winner_name = game["players"][1 - player_idx]
                    loser_name = game["players"][player_idx][1]
                    elo_change = await save_game_result(winner_name, loser_name, game["chain"], "forfeit")

                    conn = get_conn()
                    cur = conn.cursor()
                    cur.execute("SELECT elo FROM users WHERE username = %s", (winner_name,))
                    w = cur.fetchone()
                    cur.execute("SELECT elo FROM users WHERE username = %s", (loser_name,))
                    l = cur.fetchone()
                    cur.close(); conn.close()

                    await safe_send(websocket, {
                        "event": "game_over", "result": "lose", "reason": "forfeit",
                        "current_player_id": game["current_player_id"],
                        "used_ids": list(game["used_player_ids"]),
                        "elo_change": f"-{elo_change}" if elo_change else None,
                        "new_elo": l["elo"] if l else None,
                        "chain": game["chain"],
                    })
                    await safe_send(winner_ws, {
                        "event": "game_over", "result": "win", "reason": "opponent_forfeit",
                        "elo_change": f"+{elo_change}" if elo_change else None,
                        "new_elo": w["elo"] if w else None,
                        "chain": game["chain"],
                    })
                    active_games.pop(room_id, None)
                break

            if action != "play":
                continue

            room_id, player_idx = find_game_by_ws(websocket)
            if room_id is None:
                await safe_send(websocket, {"event": "error", "message": "Aucune partie en cours"})
                continue

            game = active_games[room_id]

            if game["current_turn"] % 2 != player_idx:
                await safe_send(websocket, {"event": "invalid_answer", "reason": "Ce n'est pas ton tour"})
                continue

            proposed_id = data.get("player_id")
            if not proposed_id:
                await safe_send(websocket, {"event": "invalid_answer", "reason": "player_id manquant"})
                continue

            if proposed_id in game["used_player_ids"]:
                await safe_send(websocket, {"event": "invalid_answer", "reason": "Ce joueur a deja ete utilise"})
                continue

            link = db_verify_link(game["current_player_id"], proposed_id)
            if not link:
                await safe_send(websocket, {"event": "invalid_answer", "reason": "Ces deux joueurs n'ont jamais evolue ensemble"})
                continue

            # Reponse valide
            player_info   = db_get_player(proposed_id)
            proposed_name = player_info["name"] if player_info else str(proposed_id)

            game["used_player_ids"].add(proposed_id)
            game["current_player_id"] = proposed_id
            game["current_turn"]     += 1
            game["chain"].append({"id": proposed_id, "name": proposed_name})

            logger.info("[%s] Tour %d : %s joue %s (%s %s)",
                        room_id, game["current_turn"],
                        game["players"][player_idx][1],
                        proposed_name, link["team"], link["season"])

            await broadcast(game, {
                "event":     "opponent_played",
                "played_by": game["players"][player_idx][1],
                "player":    {"id": proposed_id, "name": proposed_name},
                "link":      link,
            })

            next_idx = game["current_turn"] % 2
            await safe_send(game["players"][next_idx][0], {
                "event":          "your_turn",
                "current_player": {"id": proposed_id, "name": proposed_name},
            })

            start_turn_timer(room_id, game["current_turn"])

    except WebSocketDisconnect:
        logger.info("Deconnexion : %s", player_name)
        waiting_queue[:] = [(ws, n) for ws, n in waiting_queue if ws is not websocket]

        room_id, player_idx = find_game_by_ws(websocket)
        if room_id:
            game       = active_games[room_id]
            opp_ws,  opp_name  = game["players"][1 - player_idx]
            loser_name = game["players"][player_idx][1]
            elo_change = await save_game_result(opp_name, loser_name, game["chain"], "disconnect")

            conn = get_conn()
            cur = conn.cursor()
            cur.execute("SELECT elo FROM users WHERE username = %s", (opp_name,))
            w = cur.fetchone()
            cur.close(); conn.close()

            await safe_send(opp_ws, {
                "event": "game_over", "result": "win", "reason": "opponent_disconnected",
                "elo_change": f"+{elo_change}" if elo_change else None,
                "new_elo": w["elo"] if w else None,
                "chain": game["chain"],
            })
            logger.info("[%s] %s deconnecte — %s gagne", room_id, loser_name, opp_name)
            active_games.pop(room_id, None)
