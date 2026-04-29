from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import psycopg2
import psycopg2.extras

# --- Config DB (même mot de passe que le script 03) ---
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "nba_chain",
    "user":     "postgres",
    "password": "Helia16",
}

app = FastAPI(title="NBA Chain API")

# Autoriser les requêtes depuis le navigateur (utile pour le frontend plus tard)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_conn():
    return psycopg2.connect(**DB_CONFIG, cursor_factory=psycopg2.extras.RealDictCursor)


# ============================================================
# ENDPOINT 1 — Autocomplete
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
    cur.execute("""
        SELECT id, name
        FROM players
        WHERE name ILIKE %s
        ORDER BY name
        LIMIT 10
    """, (f"%{q}%",))
    results = cur.fetchall()
    cur.close()
    conn.close()
    return {"players": [dict(r) for r in results]}


# ============================================================
# ENDPOINT 2 — Vérification (le cœur du jeu)
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
    cur.execute("""
        SELECT pt.season, t.name AS team
        FROM played_together pt
        JOIN teams t ON t.id = pt.team_id
        WHERE pt.player_a = %s AND pt.player_b = %s
        ORDER BY pt.season DESC
        LIMIT 1
    """, (a, b))
    result = cur.fetchone()
    cur.close()
    conn.close()

    if result:
        return {
            "valid": True,
            "season": result["season"],
            "team":   result["team"],
        }
    else:
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

    # Infos de base
    cur.execute("SELECT id, name FROM players WHERE id = %s", (player_id,))
    player = cur.fetchone()

    if not player:
        return {"error": "Joueur introuvable"}

    # Carrière
    cur.execute("""
        SELECT t.name AS team, s.season
        FROM stints s
        JOIN teams t ON t.id = s.team_id
        WHERE s.player_id = %s
        ORDER BY s.season DESC
    """, (player_id,))
    career = cur.fetchall()

    cur.close()
    conn.close()

    return {
        "id":     player["id"],
        "name":   player["name"],
        "career": [dict(c) for c in career],
    }
