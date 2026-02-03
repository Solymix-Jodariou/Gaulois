import os
import sqlite3
from datetime import datetime

import discord
from discord import app_commands
from discord.ext import commands

TOKEN = os.getenv("DISCORD_TOKEN")
DB_PATH = "leaderboard.db"
CLAN_TAG = "[GAL]"

intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)


def get_db():
    return sqlite3.connect(DB_PATH)


def init_db():
    try:
        with get_db() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS players (
                    discord_id TEXT PRIMARY KEY,
                    pseudo TEXT NOT NULL,
                    player_id TEXT NOT NULL,
                    wins_ffa INTEGER DEFAULT 0,
                    losses_ffa INTEGER DEFAULT 0,
                    wins_team INTEGER DEFAULT 0,
                    losses_team INTEGER DEFAULT 0,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
    except Exception as exc:
        print(f"❌ Erreur DB: {exc}")


def is_pseudo_valid(pseudo: str) -> bool:
    return "#" not in pseudo


def calculate_ratio(wins_ffa, losses_ffa, wins_team, losses_team):
    wins = wins_ffa + wins_team
    losses = losses_ffa + losses_team
    return wins / (losses + 1)


def fetch_player_stats(player_id: str):
    """
    Placeholder de récupération de stats.
    À remplacer par une source externe plus tard.
    """
    # TODO: implémenter la récupération réelle des stats
    # Pour l'instant, on retourne des valeurs par défaut.
    return {
        "wins_ffa": 0,
        "losses_ffa": 0,
        "wins_team": 0,
        "losses_team": 0,
    }


@bot.event
async def on_ready():
    init_db()
    try:
        await bot.tree.sync()
    except Exception as exc:
        print(f"❌ Erreur sync commandes: {exc}")
    print(f"✅ Bot connecté : {bot.user}")


@bot.tree.command(name="register", description="Enregistre un joueur dans le leaderboard.")
@app_commands.describe(pseudo="Pseudo sans tag Discord (#)", player_id="ID Player OpenFront")
async def register(interaction: discord.Interaction, pseudo: str, player_id: str):
    if not is_pseudo_valid(pseudo):
        await interaction.response.send_message(
            "❌ Le pseudo ne doit pas contenir de tag Discord (#).",
            ephemeral=True,
        )
        return

    try:
        stats = fetch_player_stats(player_id)
        if not stats:
            await interaction.response.send_message(
                "❌ Erreur : Impossible de récupérer les stats du joueur.",
                ephemeral=True,
            )
            return
    except Exception:
        await interaction.response.send_message(
            "❌ Erreur : Impossible de récupérer les stats du joueur.",
            ephemeral=True,
        )
        return

    try:
        with get_db() as conn:
            conn.execute(
                """
                INSERT INTO players (
                    discord_id, pseudo, player_id,
                    wins_ffa, losses_ffa, wins_team, losses_team, last_updated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(discord_id) DO UPDATE SET
                    pseudo = excluded.pseudo,
                    player_id = excluded.player_id,
                    wins_ffa = excluded.wins_ffa,
                    losses_ffa = excluded.losses_ffa,
                    wins_team = excluded.wins_team,
                    losses_team = excluded.losses_team,
                    last_updated = excluded.last_updated
                """,
                (
                    str(interaction.user.id),
                    pseudo,
                    player_id,
                    stats["wins_ffa"],
                    stats["losses_ffa"],
                    stats["wins_team"],
                    stats["losses_team"],
                    datetime.utcnow().isoformat(),
                ),
            )
    except Exception as exc:
        await interaction.response.send_message(
            f"❌ Erreur : base de données inaccessible. ({exc})",
            ephemeral=True,
        )
        return

    await interaction.response.send_message(
        f"✅ {pseudo} a été enregistré avec l'ID {player_id} !"
    )


@bot.tree.command(name="setleaderboard", description="Affiche le leaderboard [GAL].")
async def setleaderboard(interaction: discord.Interaction):
    try:
        with get_db() as conn:
            rows = conn.execute(
                """
                SELECT pseudo, wins_ffa, losses_ffa, wins_team, losses_team
                FROM players
                WHERE pseudo LIKE ?
                """,
                (f"%{CLAN_TAG}%",),
            ).fetchall()
    except Exception as exc:
        await interaction.response.send_message(
            f"❌ Erreur : base de données inaccessible. ({exc})",
            ephemeral=True,
        )
        return

    if not rows:
        await interaction.response.send_message(
            "❌ Aucun joueur avec le tag [GAL] trouvé.",
            ephemeral=True,
        )
        return

    players = []
    for pseudo, wins_ffa, losses_ffa, wins_team, losses_team in rows:
        ratio = calculate_ratio(wins_ffa, losses_ffa, wins_team, losses_team)
        total_wins = wins_ffa + wins_team
        players.append(
            {
                "pseudo": pseudo,
                "wins_ffa": wins_ffa,
                "losses_ffa": losses_ffa,
                "wins_team": wins_team,
                "losses_team": losses_team,
                "ratio": ratio,
                "total_wins": total_wins,
            }
        )

    players.sort(key=lambda p: (p["ratio"], p["total_wins"]), reverse=True)
    top = players[:30]

    embed = discord.Embed(
        title="🏆 Leaderboard [GAL] - Top 30",
        color=discord.Color.orange(),
    )

    for i, p in enumerate(top, 1):
        embed.add_field(
            name=f"#{i} {p['pseudo']}",
            value=(
                f"Ratio: `{p['ratio']:.2f}`\n"
                f"FFA: `{p['wins_ffa']}W / {p['losses_ffa']}L`\n"
                f"ÉQUIPE: `{p['wins_team']}W / {p['losses_team']}L`"
            ),
            inline=False,
        )

    embed.set_footer(text=f"Mis à jour le {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")

    await interaction.response.send_message(embed=embed)


if __name__ == "__main__":
    if not TOKEN:
        raise ValueError("DISCORD_TOKEN manquant.")
    bot.run(TOKEN)
import discord
from discord.ext import commands
import aiohttp
import aiomysql
import json
from datetime import datetime
from urllib.parse import urlparse

# Configuration
import os
TOKEN = os.getenv('DISCORD_TOKEN')  # prend la variable d'environnement
TAG_CLAN = 'GAL'
API_BASE = 'https://api.openfront.io'
OPENFRONT_API_KEY = os.getenv('OPENFRONT_API_KEY')
MAX_GAMES_DEFAULT = 10
MAX_GAMES_CAP = 30

db_pool = None
db_initialized = False

# Vérification du token
if not TOKEN:
    raise ValueError("❌ DISCORD_TOKEN n'est pas défini. Veuillez configurer la variable d'environnement sur Railway.")

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents)

# Base de données simple (pseudo discord -> pseudo openfront)
registered_users = {}
# Base de données simple (discord -> player_id openfront)
registered_player_ids = {}

def get_db_config():
    """Récupère la config MySQL depuis Railway."""
    mysql_url = os.getenv('MYSQL_URL') or os.getenv('DATABASE_URL')
    if mysql_url:
        parsed = urlparse(mysql_url)
        return {
            "host": parsed.hostname,
            "port": parsed.port or 3306,
            "user": parsed.username,
            "password": parsed.password,
            "db": parsed.path.lstrip('/'),
        }

    host = os.getenv('MYSQLHOST')
    user = os.getenv('MYSQLUSER')
    password = os.getenv('MYSQLPASSWORD')
    db = os.getenv('MYSQLDATABASE')
    port = int(os.getenv('MYSQLPORT') or 3306)

    if host and user and db:
        return {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "db": db,
        }
    return None

async def init_db():
    """Initialise le pool MySQL et crée les tables."""
    global db_pool, db_initialized
    if db_initialized:
        return
    db_initialized = True

    config = get_db_config()
    if not config:
        print("⚠️ MySQL non configuré (variables Railway manquantes)")
        return

    db_pool = await aiomysql.create_pool(
        host=config["host"],
        port=config["port"],
        user=config["user"],
        password=config["password"],
        db=config["db"],
        autocommit=True,
        minsize=1,
        maxsize=5,
    )

    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                CREATE TABLE IF NOT EXISTS discord_users (
                    discord_id BIGINT PRIMARY KEY,
                    openfront_username VARCHAR(64),
                    player_id VARCHAR(64),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                )
                """
            )
            await cur.execute(
                """
                CREATE TABLE IF NOT EXISTS player_stats_cache (
                    player_id VARCHAR(64) PRIMARY KEY,
                    data JSON NOT NULL,
                    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

async def db_upsert_user(discord_id, openfront_username=None, player_id=None):
    """Insère ou met à jour un utilisateur."""
    if not db_pool:
        return
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO discord_users (discord_id, openfront_username, player_id)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    openfront_username = COALESCE(VALUES(openfront_username), openfront_username),
                    player_id = COALESCE(VALUES(player_id), player_id)
                """,
                (discord_id, openfront_username, player_id),
            )

async def db_get_user(discord_id):
    """Récupère un utilisateur."""
    if not db_pool:
        return None
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT openfront_username, player_id FROM discord_users WHERE discord_id = %s",
                (discord_id,),
            )
            row = await cur.fetchone()
            if not row:
                return None
            return {"openfront_username": row[0], "player_id": row[1]}

async def db_delete_user(discord_id):
    """Supprime un utilisateur."""
    if not db_pool:
        return
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "DELETE FROM discord_users WHERE discord_id = %s",
                (discord_id,),
            )

async def db_save_player_stats(player_id, data):
    """Enregistre les stats d'un joueur en cache."""
    if not db_pool:
        return
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO player_stats_cache (player_id, data)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE
                    data = VALUES(data),
                    fetched_at = CURRENT_TIMESTAMP
                """,
                (player_id, json.dumps(data)),
            )

async def db_get_player_stats(player_id):
    """Récupère le cache des stats d'un joueur."""
    if not db_pool:
        return None
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT data, fetched_at FROM player_stats_cache WHERE player_id = %s",
                (player_id,),
            )
            row = await cur.fetchone()
            if not row:
                return None
            return {"data": row[0], "fetched_at": row[1]}

# ==================== FONCTIONS API ====================

def build_api_headers():
    """Construit les headers pour l'API (si clé fournie)."""
    if not OPENFRONT_API_KEY:
        return {}
    # Compatibilité : certaines APIs utilisent Authorization, d'autres X-API-Key
    return {
        "Authorization": f"Bearer {OPENFRONT_API_KEY}",
        "X-API-Key": OPENFRONT_API_KEY,
    }

def format_api_error(error: str) -> str:
    if not error:
        return ""
    if "401" in error:
        return "Accès refusé (401). L'API semble privée : configure `OPENFRONT_API_KEY`."
    if "404" in error:
        return "Endpoint introuvable (404). L'API a peut-être changé."
    return f"Erreur API : {error}"

async def get_leaderboard():
    """Récupère le leaderboard complet"""
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(
                f'{API_BASE}/leaderboard',
                headers=build_api_headers(),
                timeout=10,
            ) as resp:
                if resp.status == 200:
                    return await resp.json(), None
                text = await resp.text()
                return None, f"HTTP {resp.status}: {text[:200]}"
        except Exception as e:
            print(f"❌ Erreur API leaderboard: {e}")
            return None, str(e)

async def get_recent_games():
    """Récupère une liste de parties récentes (ids)."""
    endpoints_to_try = [
        "games",
        "games/recent",
        "matches",
    ]
    async with aiohttp.ClientSession() as session:
        best_error = None
        for endpoint in endpoints_to_try:
            try:
                async with session.get(
                    f'{API_BASE}/{endpoint}',
                    headers=build_api_headers(),
                    timeout=10,
                ) as resp:
                    if resp.status == 200:
                        return await resp.json(), None
                    text = await resp.text()
                    last_error = f"HTTP {resp.status}: {text[:200]}"
                    # Si l'API est privée, inutile de tester plus loin
                    if resp.status in (401, 403):
                        return None, last_error
                    best_error = best_error or last_error
            except Exception as e:
                last_error = str(e)
                best_error = best_error or last_error
        return None, best_error or "Erreur inconnue"

async def get_game_data(game_id):
    """Récupère les données d'une partie"""
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(
                f'{API_BASE}/game/{game_id}',
                headers=build_api_headers(),
                timeout=10,
            ) as resp:
                if resp.status == 200:
                    return await resp.json(), None
                text = await resp.text()
                return None, f"HTTP {resp.status}: {text[:200]}"
        except Exception as e:
            print(f"❌ Erreur API game: {e}")
            return None, str(e)

async def get_player_data(player_id):
    """Récupère les données d'un joueur via différents endpoints possibles."""
    endpoints_to_try = [
        f"player/{player_id}",
        f"players/{player_id}",
        f"profile/{player_id}",
        f"user/{player_id}",
        f"users/{player_id}",
        f"account/{player_id}",
    ]
    async with aiohttp.ClientSession() as session:
        best_error = None
        for endpoint in endpoints_to_try:
            try:
                async with session.get(
                    f'{API_BASE}/{endpoint}',
                    headers=build_api_headers(),
                    timeout=10,
                ) as resp:
                    if resp.status == 200:
                        return await resp.json(), None
                    text = await resp.text()
                    last_error = f"HTTP {resp.status}: {text[:200]}"
                    if resp.status in (401, 403):
                        return None, last_error
                    best_error = best_error or last_error
            except Exception as e:
                best_error = best_error or str(e)
        return None, best_error or "Erreur inconnue"

def get_clan_stats(leaderboard_data, clan_tag):
    """Extrait les stats d'un clan du leaderboard"""
    if not leaderboard_data or 'clans' not in leaderboard_data:
        return None
    
    for clan in leaderboard_data['clans']:
        if clan['clanTag'].upper() == clan_tag.upper():
            return clan
    return None

# ==================== HELPERS ====================

def is_tagged_user(username: str, tag: str) -> bool:
    """Détecte le tag clan dans un pseudo (ex: [GAL] ou GAL <pseudo>)."""
    if not username:
        return False
    upper_name = username.upper()
    upper_tag = tag.upper()
    return f'[{upper_tag}]' in upper_name or upper_name.startswith(f'{upper_tag} ')

def extract_game_id(item):
    """Extrait un identifiant de partie d'un objet API."""
    if isinstance(item, str):
        return item
    if not isinstance(item, dict):
        return None
    for key in ("gameId", "game_id", "id", "_id"):
        value = item.get(key)
        if value:
            return value
    return None

def get_metric_key(players):
    """Trouve une stat numérique utilisable pour trier un leaderboard."""
    metric_keys = ("score", "kills", "wins", "points", "territory", "land", "power")
    for key in metric_keys:
        if any(isinstance(p.get(key), (int, float)) for p in players):
            return key
    return None

# ==================== COMMANDES ====================

@bot.event
async def on_ready():
    await init_db()
    print(f'✅ Bot connecté : {bot.user.name}')
    print(f'🎯 Tag suivi : {TAG_CLAN}')
    print(f'📡 API : {API_BASE}')

@bot.command(name='help_bot')
async def help_command(ctx):
    """Affiche l'aide"""
    embed = discord.Embed(
        title="🤖 Commandes du Bot Openfront",
        description=f"Bot de statistiques pour Openfront.io",
        color=discord.Color.blue()
    )
    
    embed.add_field(
        name="!register <pseudo>",
        value="Enregistre ton pseudo Openfront.io",
        inline=False
    )
    embed.add_field(
        name="!register_id <player_id>",
        value="Enregistre ton Player ID OpenFront",
        inline=False
    )
    embed.add_field(
        name="!myid",
        value="Affiche ton Player ID enregistré",
        inline=False
    )
    embed.add_field(
        name="!stats_id [player_id]",
        value="Affiche les stats d'un Player ID",
        inline=False
    )
    embed.add_field(
        name="!stats_me",
        value="Affiche tes stats (via Player ID enregistré)",
        inline=False
    )
    embed.add_field(
        name="!unregister",
        value="Supprime ton pseudo enregistré",
        inline=False
    )
    embed.add_field(
        name="!myinfo",
        value="Affiche ton pseudo enregistré",
        inline=False
    )
    embed.add_field(
        name="!stats_gal",
        value="Affiche les stats globales du clan GAL",
        inline=False
    )
    embed.add_field(
        name="!leaderboard_clans [max_games] [top]",
        value="Scanne des parties récentes et affiche le top [GAL]",
        inline=False
    )
    embed.add_field(
        name="!game <game_id>",
        value="Affiche les infos d'une partie",
        inline=False
    )
    
    embed.set_footer(text=f"Tag recherché : {TAG_CLAN}")
    await ctx.send(embed=embed)

@bot.command(name='register')
async def register(ctx, pseudo: str = None):
    """Enregistre le pseudo Openfront d'un joueur"""
    if not pseudo:
        await ctx.send("❌ Usage : `!register <pseudo_openfront>`")
        return
    
    registered_users[str(ctx.author.id)] = pseudo
    await db_upsert_user(ctx.author.id, openfront_username=pseudo)
    await ctx.send(f"✅ {ctx.author.mention} enregistré avec le pseudo **{pseudo}**")

@bot.command(name='register_id')
async def register_id(ctx, player_id: str = None):
    """Enregistre le Player ID OpenFront"""
    if not player_id:
        await ctx.send("❌ Usage : `!register_id <player_id>`")
        return

    registered_player_ids[str(ctx.author.id)] = player_id
    await db_upsert_user(ctx.author.id, player_id=player_id)
    await ctx.send(f"✅ {ctx.author.mention} enregistré avec le Player ID **{player_id}**")

@bot.command(name='unregister')
async def unregister(ctx):
    """Supprime l'enregistrement"""
    user_id = str(ctx.author.id)
    had_any = False
    if user_id in registered_users:
        del registered_users[user_id]
        had_any = True
    if user_id in registered_player_ids:
        del registered_player_ids[user_id]
        had_any = True
    row = await db_get_user(ctx.author.id)
    if row:
        had_any = True
    await db_delete_user(ctx.author.id)
    if had_any:
        await ctx.send("✅ Enregistrement supprimé")
    else:
        await ctx.send("❌ Tu n'es pas enregistré")

@bot.command(name='myid')
async def myid(ctx):
    """Affiche le Player ID enregistré"""
    user_id = str(ctx.author.id)
    player_id = registered_player_ids.get(user_id)
    if not player_id:
        row = await db_get_user(ctx.author.id)
        player_id = row["player_id"] if row else None
        if player_id:
            registered_player_ids[user_id] = player_id
    if player_id:
        await ctx.send(f"🆔 Ton Player ID OpenFront : **{player_id}**")
        return
    await ctx.send("❌ Tu n'as pas de Player ID enregistré. Utilise `!register_id <player_id>`")

@bot.command(name='myinfo')
async def myinfo(ctx):
    """Affiche les infos de l'utilisateur"""
    user_id = str(ctx.author.id)
    pseudo = registered_users.get(user_id)
    if not pseudo:
        row = await db_get_user(ctx.author.id)
        pseudo = row["openfront_username"] if row else None
        if pseudo:
            registered_users[user_id] = pseudo
    if pseudo:
        await ctx.send(f"📋 Ton pseudo Openfront : **{pseudo}**")
        return
    await ctx.send("❌ Tu n'es pas enregistré. Utilise `!register <pseudo>`")

@bot.command(name='stats_id')
async def stats_id(ctx, player_id: str = None):
    """Affiche les stats d'un Player ID"""
    if not player_id:
        player_id = registered_player_ids.get(str(ctx.author.id))
        if not player_id:
            row = await db_get_user(ctx.author.id)
            player_id = row["player_id"] if row else None
            if player_id:
                registered_player_ids[str(ctx.author.id)] = player_id
        if not player_id:
            await ctx.send("❌ Usage : `!stats_id <player_id>` ou enregistre avec `!register_id`")
            return

    await ctx.send(f"🔄 Récupération des stats du Player ID {player_id}...")

    data, error = await get_player_data(player_id)
    if not data:
        cached = await db_get_player_stats(player_id)
        if cached:
            cached_data = cached["data"]
            if not isinstance(cached_data, str):
                cached_data = json.dumps(cached_data, indent=2)
            await ctx.send("⚠️ API inaccessible, voici le dernier cache disponible.")
            await ctx.send(f"```json\n{cached_data[:1900]}\n```")
            return
        await ctx.send(f"❌ Impossible de récupérer les stats. {format_api_error(error)}")
        return

    await db_save_player_stats(player_id, data)

    json_str = json.dumps(data, indent=2)
    if len(json_str) > 1900:
        json_str = json_str[:1900] + "\n...\n(tronqué)"

    await ctx.send(f"```json\n{json_str}\n```")

@bot.command(name='stats_me')
async def stats_me(ctx):
    """Affiche les stats du Player ID enregistré"""
    await stats_id(ctx, None)

@bot.command(name='stats_gal')
async def stats_gal(ctx):
    """Affiche les stats du clan GAL"""
    await ctx.send("🔄 Récupération des stats...")
    
    data, error = await get_leaderboard()
    if not data:
        await ctx.send(f"❌ Impossible de récupérer les données du leaderboard. {format_api_error(error)}")
        return
    
    clan_stats = get_clan_stats(data, TAG_CLAN)
    if not clan_stats:
        await ctx.send(f"❌ Clan **{TAG_CLAN}** non trouvé dans le leaderboard")
        return
    
    embed = discord.Embed(
        title=f"📊 Stats du clan [{TAG_CLAN}]",
        color=discord.Color.gold()
    )
    
    embed.add_field(name="🎮 Parties jouées", value=f"`{clan_stats['games']:,}`", inline=True)
    embed.add_field(name="✅ Victoires", value=f"`{clan_stats['wins']:,}`", inline=True)
    embed.add_field(name="❌ Défaites", value=f"`{clan_stats['losses']:,}`", inline=True)
    
    embed.add_field(name="👥 Sessions joueurs", value=f"`{clan_stats['playerSessions']:,}`", inline=True)
    embed.add_field(name="⚖️ W/L Ratio", value=f"`{clan_stats['weightedWLRatio']:.2f}`", inline=True)
    embed.add_field(name="🏆 Wins pondérés", value=f"`{clan_stats['weightedWins']:.2f}`", inline=True)
    
    # Calcul du winrate
    winrate = (clan_stats['wins'] / clan_stats['games'] * 100) if clan_stats['games'] > 0 else 0
    embed.add_field(name="📈 Winrate", value=f"`{winrate:.1f}%`", inline=True)
    
    period = f"Du {data['start'][:10]} au {data['end'][:10]}"
    embed.set_footer(text=period)
    
    await ctx.send(embed=embed)

@bot.command(name='leaderboard_clans')
async def leaderboard_clans(ctx, max_games: int = MAX_GAMES_DEFAULT, top: int = 10):
    """Scanne les parties récentes et affiche le top GAL"""
    if max_games < 1:
        await ctx.send("❌ Usage : `!leaderboard_clans [max_games] [top]`")
        return

    max_games = min(max_games, MAX_GAMES_CAP)
    await ctx.send(f"🔄 Scan de {max_games} parties récentes pour le tag [{TAG_CLAN}]...")

    recent_data, error = await get_recent_games()
    if not recent_data:
        await ctx.send(f"❌ Impossible de récupérer les parties récentes. {format_api_error(error)}")
        return

    if isinstance(recent_data, dict):
        game_items = recent_data.get('games') or recent_data.get('matches') or recent_data.get('data')
    else:
        game_items = recent_data

    if not game_items:
        await ctx.send("❌ Liste des parties récentes introuvable")
        return

    game_ids = []
    for item in game_items:
        game_id = extract_game_id(item)
        if game_id:
            game_ids.append(game_id)
        if len(game_ids) >= max_games:
            break

    if not game_ids:
        await ctx.send("❌ Aucun game_id trouvé dans les parties récentes")
        return

    leaderboard = {}
    games_with_gal = 0

    for game_id in game_ids:
        data, _error = await get_game_data(game_id)
        if not data:
            continue

        players = None
        if isinstance(data, dict):
            players = data.get('info', {}).get('players') or data.get('players')

        if not players:
            continue

        gal_players = [p for p in players if is_tagged_user(p.get('username', ''), TAG_CLAN)]
        if not gal_players:
            continue

        games_with_gal += 1
        metric_key = get_metric_key(gal_players)

        for player in gal_players:
            username = player.get('username', 'Unknown')
            entry = leaderboard.setdefault(username, {"count": 0, "score": 0})
            entry["count"] += 1
            if metric_key:
                value = player.get(metric_key, 0)
                if isinstance(value, (int, float)):
                    entry["score"] += value

    if games_with_gal == 0 or not leaderboard:
        await ctx.send(f"❌ Aucune partie avec **{TAG_CLAN}** dans les {len(game_ids)} dernières parties")
        return

    leaderboard_sorted = sorted(
        leaderboard.items(),
        key=lambda x: (x[1]["score"], x[1]["count"]),
        reverse=True
    )[:top]

    embed = discord.Embed(
        title=f"🏆 Top {top} Joueurs [{TAG_CLAN}] - {games_with_gal}/{len(game_ids)} parties",
        color=discord.Color.purple()
    )

    description = "```\n"
    description += f"{'#':<3} {'JOUEUR':<20} {'SCORE':<8} {'GAMES':<8}\n"
    description += "-" * 43 + "\n"

    for i, (username, stats) in enumerate(leaderboard_sorted, 1):
        display_name = username[:20]
        description += f"{i:<3} {display_name:<20} {stats['score']:<8.2f} {stats['count']:<8}\n"

    description += "```"
    embed.description = description

    await ctx.send(embed=embed)

@bot.command(name='leaderboard_gal')
async def leaderboard_gal(ctx, max_games: int = MAX_GAMES_DEFAULT, top: int = 10):
    """Alias de leaderboard_clans"""
    await leaderboard_clans(ctx, max_games, top)

@bot.command(name='game')
async def game_info(ctx, game_id: str = None):
    """Affiche les infos d'une partie"""
    if not game_id:
        await ctx.send("❌ Usage : `!game <game_id>`")
        return
    
    await ctx.send(f"🔄 Récupération de la partie {game_id}...")
    
    data, error = await get_game_data(game_id)
    if not data:
        await ctx.send(f"❌ Impossible de récupérer les données de la partie {game_id}. {format_api_error(error)}")
        return
    
    # Afficher le JSON formaté (limité à 2000 caractères)
    json_str = json.dumps(data, indent=2)
    
    if len(json_str) > 1900:
        json_str = json_str[:1900] + "\n...\n(tronqué)"
    
    await ctx.send(f"```json\n{json_str}\n```")
    await ctx.send(f"ID: {game_id}")

@bot.command(name='find_gal_players')
async def find_gal_players(ctx, game_id: str = None):
    """Trouve les joueurs GAL dans une partie"""
    if not game_id:
        await ctx.send("❌ Usage : `!find_gal_players <game_id>`")
        return
    
    data, error = await get_game_data(game_id)
    if not data:
        await ctx.send(f"❌ Données de partie invalides. {format_api_error(error)}")
        return
    if 'info' not in data or 'players' not in data['info']:
        await ctx.send("❌ Données de partie invalides")
        return
    
    gal_players = []
    for player in data['info']['players']:
        username = player.get('username', 'Unknown')
        if is_tagged_user(username, TAG_CLAN):
            gal_players.append(username)
    
    if gal_players:
        players_list = "\n".join(f"• {p}" for p in gal_players)
        await ctx.send(f"✅ Joueurs **{TAG_CLAN}** trouvés :\n{players_list}")
    else:
        await ctx.send(f"❌ Aucun joueur **{TAG_CLAN}** dans cette partie")

# ==================== LANCEMENT ====================

if __name__ == '__main__':
    if not TOKEN:
        print("❌ ERREUR: DISCORD_TOKEN n'est pas défini dans les variables d'environnement")
        print("💡 Configurez la variable DISCORD_TOKEN sur Railway")
        exit(1)
    bot.run(TOKEN)
