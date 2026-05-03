"""
SCRIPT 01 — Marquer les joueurs "featured" (Top 75 NBA + notables récents)
Lance ce script UNE FOIS sur ta base Railway ou locale.

Usage:
  pip install psycopg2-binary python-dotenv
  DATABASE_URL=postgresql://... python 01_mark_featured_players.py
  # ou en local sans variable d'env (utilise les valeurs par défaut)
"""

import os
import psycopg2
from urllib.parse import urlparse

# --- Connexion (même logique que main.py) ---
DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL:
    r = urlparse(DATABASE_URL)
    DB_CONFIG = {
        "host": r.hostname, "port": r.port,
        "dbname": r.path[1:], "user": r.username, "password": r.password,
        "sslmode": "require"
    }
else:
    DB_CONFIG = {
        "host": "localhost", "port": 5432,
        "dbname": "nba_chain", "user": "postgres",
        "password": os.getenv("DB_PASSWORD", ""),
    }

# -------------------------------------------------------------------
# Liste officielle NBA 75th Anniversary Team (2021)
# + joueurs notables post-2000 (MVP, champions, 5x All-Star+)
# -------------------------------------------------------------------
FEATURED_NAMES = [
    # === NBA 75th Anniversary Official List ===
    "Kareem Abdul-Jabbar", "Ray Allen", "Giannis Antetokounmpo",
    "Carmelo Anthony", "Nate Archibald", "Paul Arizin",
    "Charles Barkley", "Rick Barry", "Elgin Baylor", "Dave Bing",
    "Larry Bird", "Kobe Bryant", "Wilt Chamberlain", "Bob Cousy",
    "Dave Cowens", "Billy Cunningham", "Stephen Curry", "Anthony Davis",
    "Dave DeBusschere", "Clyde Drexler", "Tim Duncan", "Kevin Durant",
    "Julius Erving", "Patrick Ewing", "Walt Frazier", "Kevin Garnett",
    "George Gervin", "Hal Greer", "James Harden", "John Havlicek",
    "Elvin Hayes", "Allen Iverson", "LeBron James", "Dennis Johnson",
    "Magic Johnson", "Sam Jones", "Michael Jordan", "Jason Kidd",
    "Bob Lanier", "Jerry Lucas", "Karl Malone", "Moses Malone",
    "Pete Maravich", "Bob McAdoo", "Kevin McHale", "George Mikan",
    "Reggie Miller", "Earl Monroe", "Steve Nash", "Dirk Nowitzki",
    "Hakeem Olajuwon", "Shaquille O'Neal", "Robert Parish",
    "Gary Payton", "Bob Pettit", "Scottie Pippen", "Willis Reed",
    "Oscar Robertson", "David Robinson", "Dennis Rodman",
    "Bill Russell", "Dolph Schayes", "Bill Sharman", "John Stockton",
    "Isiah Thomas", "Nate Thurmond", "Wes Unseld", "Dwyane Wade",
    "Bill Walton", "Jerry West", "Paul Westphal", "Lenny Wilkens",
    "Dominique Wilkins", "James Worthy",

    # === Notables récents (post-2000, critères : MVP / champion / 5x All-Star+) ===
    "Nikola Jokic",       # 3x MVP
    "Joel Embiid",        # MVP 2023
    "Luka Doncic",        # superstar génération actuelle
    "Jayson Tatum",       # champion 2024
    "Jaylen Brown",       # champion 2024, MVP Finals
    "Damian Lillard",     # 7x All-Star
    "Paul George",        # 9x All-Star
    "Kawhi Leonard",      # 2x champion, 2x Finals MVP
    "Russell Westbrook",  # MVP 2017, triple-double record
    "Chris Paul",         # 12x All-Star
    "Blake Griffin",      # 6x All-Star
    "Dwight Howard",      # 8x All-Star, 3x DPOY
    "Tony Parker",        # 6x All-Star, Finals MVP 2007
    "Manu Ginobili",      # 2x champion, Hall of Fame
    "Tracy McGrady",      # 2x scoring champion
    "Vince Carter",       # dunk contest legend
    "Paul Pierce",        # champion 2008, Finals MVP
    "Grant Hill",         # 7x All-Star
    "Yao Ming",           # 8x All-Star
    "Pau Gasol",          # 6x All-Star, 2x champion
    "LaMarcus Aldridge",  # 7x All-Star
    "Marc Gasol",         # champion 2019, DPOY
    "Jimmy Butler",       # 6x All-Star
    "Kyrie Irving",       # champion 2016, 7x All-Star
    "Devin Booker",       # champion 2021 ? non, mais 3x All-Star
    "Ja Morant",          # MIP, 2x All-Star
    "Zion Williamson",    # #1 pick, phénomène
    "Trae Young",         # 2x All-Star
    "Bam Adebayo",        # 3x All-Star
    "Khris Middleton",    # champion 2021
    "Jrue Holiday",       # champion 2021, champion 2024
    "Tyrese Haliburton",  # All-Star 2024
    "Shai Gilgeous-Alexander", # MVP candidat 2024
    "Victor Wembanyama",  # #1 pick 2023, ROTY
    "Anthony Edwards",    # All-Star, Team USA
    "Donovan Mitchell",   # 3x All-Star
    "De'Aaron Fox",       # All-Star 2024
]

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # 1. Ajouter la colonne si elle n'existe pas
    cur.execute("""
        ALTER TABLE players
        ADD COLUMN IF NOT EXISTS is_featured BOOLEAN DEFAULT FALSE;
    """)
    conn.commit()
    print("✅ Colonne is_featured ajoutée (ou déjà existante)")

    # 2. Reset tous les joueurs à FALSE
    cur.execute("UPDATE players SET is_featured = FALSE;")

    # 3. Marquer les joueurs featured
    found = 0
    not_found = []

    for name in FEATURED_NAMES:
        cur.execute(
            "UPDATE players SET is_featured = TRUE WHERE name ILIKE %s RETURNING id, name;",
            (name,)
        )
        rows = cur.fetchall()
        if rows:
            found += len(rows)
            for row in rows:
                print(f"  ✓ {row[1]} (id={row[0]})")
        else:
            not_found.append(name)

    conn.commit()

    print(f"\n✅ {found} joueurs marqués comme featured")

    if not_found:
        print(f"\n⚠️  {len(not_found)} joueurs non trouvés en base (noms à vérifier) :")
        for name in not_found:
            # Chercher des noms proches pour aider
            cur.execute(
                "SELECT name FROM players WHERE name ILIKE %s LIMIT 3;",
                (f"%{name.split()[0]}%",)
            )
            suggestions = [r[0] for r in cur.fetchall()]
            sugg_str = f" → suggestions: {', '.join(suggestions)}" if suggestions else ""
            print(f"  ✗ {name}{sugg_str}")

    # 4. Vérification finale
    cur.execute("SELECT COUNT(*) FROM players WHERE is_featured = TRUE;")
    total = cur.fetchone()[0]
    print(f"\n🎯 Total featured en base : {total} joueurs")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
