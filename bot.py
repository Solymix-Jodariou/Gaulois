import os
import json
import asyncio
import re
from datetime import datetime, timezone, timedelta

import aiohttp
import asyncpg
import discord
from discord import app_commands
from discord.ext import commands

TOKEN = os.getenv("DISCORD_TOKEN") or os.getenv("DISCORD_BOT_TOKEN")
GUILD_ID = os.getenv("DISCORD_GUILD_ID")
DB_URL = (
    os.getenv("DATABASE_URL")
    or os.getenv("POSTGRES_URL")
    or os.getenv("POSTGRESQL_URL")
)

CLAN_TAG = os.getenv("CLAN_TAG", "GAL")
CLAN_DISPLAY = f"[{CLAN_TAG}]"
MERGE_PREFIXES = [
    p.strip().upper()
    for p in os.getenv("LEADERBOARD_MERGE_PREFIXES", "PRINCE,ELP").split(",")
    if p.strip()
]

API_BASE = "https://api.openfront.io/public"
USER_AGENT = "Mozilla/5.0 (GauloisBot/1.1)"
OPENFRONT_API_KEY = os.getenv("OPENFRONT_API_KEY")
ONEV1_LEADERBOARD_URL = os.getenv(
    "OPENFRONT_1V1_LEADERBOARD_URL",
    "https://api.openfront.io/leaderboard/ranked",
)

REFRESH_MINUTES = int(os.getenv("LEADERBOARD_REFRESH_MINUTES", "30"))
RANGE_HOURS = int(os.getenv("LEADERBOARD_RANGE_HOURS", "24"))
MAX_SESSIONS = int(os.getenv("LEADERBOARD_MAX_SESSIONS", "300"))
BACKFILL_START = os.getenv("LEADERBOARD_BACKFILL_START", "2026-01-01T00:00:00Z")
BACKFILL_INTERVAL_MINUTES = int(os.getenv("LEADERBOARD_BACKFILL_INTERVAL_MINUTES", "5"))
MIN_GAMES = int(os.getenv("LEADERBOARD_MIN_GAMES", "10"))
PRIOR_GAMES = int(os.getenv("LEADERBOARD_PRIOR_GAMES", "50"))
ONEV1_BACKFILL_START = os.getenv("LEADERBOARD_1V1_BACKFILL_START", "2026-01-01T00:00:00Z")
ONEV1_BACKFILL_INTERVAL_MINUTES = int(os.getenv("LEADERBOARD_1V1_BACKFILL_INTERVAL_MINUTES", "10"))
ONEV1_MAX_GAMES = int(os.getenv("LEADERBOARD_1V1_MAX_GAMES", "200"))
ONEV1_REFRESH_MINUTES = int(os.getenv("LEADERBOARD_1V1_REFRESH_MINUTES", "60"))
SCORE_RATIO_WEIGHT = float(os.getenv("LEADERBOARD_SCORE_RATIO_WEIGHT", "100"))
SCORE_GAMES_WEIGHT = float(os.getenv("LEADERBOARD_SCORE_GAMES_WEIGHT", "0.1"))
WIN_NOTIFY_CHANNEL_ID = os.getenv("WIN_NOTIFY_CHANNEL_ID")
WIN_NOTIFY_POLL_SECONDS = int(os.getenv("WIN_NOTIFY_POLL_SECONDS", "300"))
WIN_NOTIFY_RANGE_HOURS = int(os.getenv("WIN_NOTIFY_RANGE_HOURS", "6"))

if RANGE_HOURS > 48:
    RANGE_HOURS = 48
if RANGE_HOURS < 1:
    RANGE_HOURS = 1
if REFRESH_MINUTES < 5:
    REFRESH_MINUTES = 5
if MAX_SESSIONS < 50:
    MAX_SESSIONS = 50
if MAX_SESSIONS > 1000:
    MAX_SESSIONS = 1000
if BACKFILL_INTERVAL_MINUTES < 5:
    BACKFILL_INTERVAL_MINUTES = 5
if MIN_GAMES < 1:
    MIN_GAMES = 1
if PRIOR_GAMES < 1:
    PRIOR_GAMES = 1
if ONEV1_BACKFILL_INTERVAL_MINUTES < 5:
    ONEV1_BACKFILL_INTERVAL_MINUTES = 5
if ONEV1_MAX_GAMES < 10:
    ONEV1_MAX_GAMES = 10
if ONEV1_MAX_GAMES > 1000:
    ONEV1_MAX_GAMES = 1000
if ONEV1_REFRESH_MINUTES < 10:
    ONEV1_REFRESH_MINUTES = 10
if WIN_NOTIFY_POLL_SECONDS < 60:
    WIN_NOTIFY_POLL_SECONDS = 60
if WIN_NOTIFY_RANGE_HOURS < 1:
    WIN_NOTIFY_RANGE_HOURS = 1
if WIN_NOTIFY_RANGE_HOURS > 48:
    WIN_NOTIFY_RANGE_HOURS = 48

intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)

pool = None
ONEV1_CACHE = {"items": [], "fetched_at": None}


def calculate_ratio(wins_ffa, losses_ffa, wins_team, losses_team):
    wins = wins_ffa + wins_team
    losses = losses_ffa + losses_team
    total = wins + losses
    if total == 0:
        return 0.0
    return wins / total


def calculate_score(wins, losses, games):
    if games <= 0:
        return 0.0
    ratio = wins / games
    return ratio * SCORE_RATIO_WEIGHT + games * SCORE_GAMES_WEIGHT


def format_local_time(dt: datetime) -> str:
    offset_hours = int(os.getenv("LEADERBOARD_TIMEZONE_OFFSET_HOURS", "1"))
    local_dt = dt + timedelta(hours=offset_hours)
    return local_dt.strftime("%Y-%m-%d %H:%M")


def is_pseudo_valid(pseudo: str) -> bool:
    return "#" not in pseudo


def compute_ffa_stats_from_sessions(sessions):
    wins = losses = 0
    for s in sessions:
        mode = (s.get("gameMode") or "").lower()
        if "free for all" in mode or mode == "ffa":
            if s.get("hasWon"):
                wins += 1
            else:
                losses += 1
    return wins, losses


def build_api_headers():
    headers = {"User-Agent": USER_AGENT}
    if OPENFRONT_API_KEY:
        headers["X-API-Key"] = OPENFRONT_API_KEY
        headers["Authorization"] = f"Bearer {OPENFRONT_API_KEY}"
    return headers


def is_clan_username(username: str) -> bool:
    if not username:
        return False
    upper = username.upper()
    tag = CLAN_TAG.upper()
    return f"[{tag}]" in upper or upper.startswith(f"{tag} ")


def game_mode(info):
    return (info.get("config", {}) or {}).get("gameMode") or ""


def get_winner_client_ids(info):
    winner = info.get("winner")
    if not winner or not isinstance(winner, list) or len(winner) < 3:
        return set()
    winners = winner[2]
    if not isinstance(winners, list):
        return set()
    return set(winners)


def is_1v1_game(info):
    config = info.get("config", {}) or {}
    mode = str(config.get("gameMode") or "").lower()
    player_teams = config.get("playerTeams")
    players = info.get("players") or []
    if len(players) != 2:
        return False
    if "1v1" in mode or "solo" in mode:
        return True
    if "team" in mode:
        return True
    if isinstance(player_teams, int) and player_teams == 1:
        return True
    if isinstance(player_teams, str) and player_teams.lower() in {"solo", "solos", "1v1"}:
        return True
    return False


def extract_gal_players(info):
    names = []
    for p in info.get("players", []):
        username = p.get("username") or ""
        if is_clan_username(username):
            names.append(username)
    return sorted(set(names))


def build_win_embed(info):
    mode = game_mode(info) or "Team"
    start_raw = info.get("start")
    end_raw = info.get("end")
    gal_players = extract_gal_players(info)
    game_id = info.get("gameID") or "?"

    embed = discord.Embed(
        title=f"✅ Victoire {CLAN_DISPLAY}",
        color=discord.Color.green(),
    )
    embed.add_field(name="Mode", value=str(mode), inline=True)
    embed.add_field(name="Game ID", value=str(game_id), inline=True)
    if start_raw:
        try:
            start_dt = datetime.fromisoformat(start_raw.replace("Z", "+00:00"))
            embed.add_field(name="Début", value=format_local_time(start_dt), inline=True)
        except Exception:
            embed.add_field(name="Début", value=str(start_raw), inline=True)
    if end_raw:
        try:
            end_dt = datetime.fromisoformat(end_raw.replace("Z", "+00:00"))
            embed.add_field(name="Fin", value=format_local_time(end_dt), inline=True)
        except Exception:
            embed.add_field(name="Fin", value=str(end_raw), inline=True)

    if gal_players:
        shown = gal_players[:10]
        more = len(gal_players) - len(shown)
        players_text = ", ".join(shown)
        if more > 0:
            players_text += f" (+{more})"
        embed.add_field(name="Joueurs [GAL]", value=players_text, inline=False)

    return embed


async def fetch_player_sessions(player_id: str):
    url = f"{API_BASE}/player/{player_id}/sessions"
    headers = {"User-Agent": USER_AGENT}
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(url, timeout=25) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise RuntimeError(f"HTTP {resp.status}: {text[:200]}")
            return await resp.json()


def normalize_username(raw: str) -> str:
    if not raw:
        return ""
    name = raw.strip()
    tag = re.escape(CLAN_TAG)
    # Remove tag variations anywhere
    name = re.sub(rf"\[{tag}\]", " ", name, flags=re.IGNORECASE)
    name = re.sub(rf"\b{tag}\b", " ", name, flags=re.IGNORECASE)
    # Keep only letters/numbers/spaces, collapse spaces
    name = re.sub(r"[^\w\s]", " ", name, flags=re.UNICODE)
    name = name.replace("_", " ")
    name = re.sub(r"\s+", " ", name).strip()
    # Merge cases like "El p" + "SOR"/"YER" by removing spaces for the key
    return name


def build_display_name(raw: str) -> str:
    base = normalize_username(raw)
    if not base:
        return CLAN_DISPLAY
    return f"{CLAN_DISPLAY} {base}"


def merge_prefix_key(base_name: str):
    if not base_name:
                return None
    upper = base_name.upper()
    upper_no_space = re.sub(r"\s+", "", upper)
    for prefix in MERGE_PREFIXES:
        if upper.startswith(prefix) or upper_no_space.startswith(prefix):
            return prefix
            return None


async def init_db():
    global pool
    if not DB_URL:
        raise ValueError("DATABASE_URL manquant (Postgres).")
    pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=5)
    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS player_stats (
                username TEXT PRIMARY KEY,
                display_name TEXT,
                wins_ffa INTEGER DEFAULT 0,
                losses_ffa INTEGER DEFAULT 0,
                wins_team INTEGER DEFAULT 0,
                losses_team INTEGER DEFAULT 0,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        columns = await conn.fetch(
            "SELECT column_name FROM information_schema.columns WHERE table_name='player_stats'"
        )
        colset = {c["column_name"] for c in columns}
        if "display_name" not in colset:
            await conn.execute("ALTER TABLE player_stats ADD COLUMN display_name TEXT")
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS processed_games (
                game_id TEXT PRIMARY KEY
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS backfill_state (
                id INTEGER PRIMARY KEY,
                cursor TEXT NOT NULL,
                completed BOOLEAN NOT NULL DEFAULT FALSE,
                last_attempt TEXT,
                last_error TEXT,
                last_sessions INTEGER DEFAULT 0,
                last_games_processed INTEGER DEFAULT 0
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS leaderboard_message (
                guild_id BIGINT PRIMARY KEY,
                channel_id BIGINT NOT NULL,
                message_id BIGINT NOT NULL
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS leaderboard_message_ffa (
                guild_id BIGINT PRIMARY KEY,
                channel_id BIGINT NOT NULL,
                message_id BIGINT NOT NULL
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS leaderboard_message_1v1 (
                guild_id BIGINT PRIMARY KEY,
                channel_id BIGINT NOT NULL,
                message_id BIGINT NOT NULL
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS leaderboard_message_1v1_gal (
                guild_id BIGINT PRIMARY KEY,
                channel_id BIGINT NOT NULL,
                message_id BIGINT NOT NULL
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS win_notifications (
                game_id TEXT PRIMARY KEY,
                notified_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ffa_players (
                discord_id BIGINT PRIMARY KEY,
                pseudo TEXT NOT NULL,
                player_id TEXT NOT NULL
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ffa_stats (
                player_id TEXT PRIMARY KEY,
                pseudo TEXT NOT NULL,
                wins_ffa INTEGER DEFAULT 0,
                losses_ffa INTEGER DEFAULT 0,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS player_stats_1v1 (
                username TEXT PRIMARY KEY,
                wins INTEGER DEFAULT 0,
                losses INTEGER DEFAULT 0,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS processed_games_1v1 (
                game_id TEXT PRIMARY KEY
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS backfill_state_1v1 (
                id INTEGER PRIMARY KEY,
                cursor TEXT NOT NULL,
                completed BOOLEAN NOT NULL DEFAULT FALSE,
                last_attempt TEXT,
                last_error TEXT
            )
            """
        )
        await conn.execute(
            """
            INSERT INTO backfill_state_1v1 (id, cursor, completed)
            VALUES (1, $1, FALSE)
            ON CONFLICT (id) DO NOTHING
            """,
            ONEV1_BACKFILL_START,
        )
        # Migrations (Postgres)
        columns = await conn.fetch(
            "SELECT column_name FROM information_schema.columns WHERE table_name='backfill_state'"
        )
        colset = {c["column_name"] for c in columns}
        if "last_sessions" not in colset:
            await conn.execute("ALTER TABLE backfill_state ADD COLUMN last_sessions INTEGER DEFAULT 0")
        if "last_games_processed" not in colset:
            await conn.execute("ALTER TABLE backfill_state ADD COLUMN last_games_processed INTEGER DEFAULT 0")
        await conn.execute(
            """
            INSERT INTO backfill_state (id, cursor, completed)
            VALUES (1, $1, FALSE)
            ON CONFLICT (id) DO NOTHING
            """,
            BACKFILL_START,
        )


async def get_backfill_state():
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT cursor, completed, last_attempt, last_error, last_sessions, last_games_processed
            FROM backfill_state WHERE id = 1
            """
        )
    return row[0], bool(row[1]), row[2], row[3], row[4], row[5]


async def set_backfill_state(
    cursor,
    completed,
    last_attempt=None,
    last_error=None,
    last_sessions=0,
    last_games_processed=0,
):
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO backfill_state (
                id, cursor, completed, last_attempt, last_error, last_sessions, last_games_processed
            )
            VALUES (1, $1, $2, $3, $4, $5, $6)
            ON CONFLICT (id) DO UPDATE SET
                cursor = EXCLUDED.cursor,
                completed = EXCLUDED.completed,
                last_attempt = EXCLUDED.last_attempt,
                last_error = EXCLUDED.last_error,
                last_sessions = EXCLUDED.last_sessions,
                last_games_processed = EXCLUDED.last_games_processed
            """,
            cursor,
            completed,
            last_attempt,
            last_error,
            last_sessions,
            last_games_processed,
        )


async def is_game_processed(game_id: str) -> bool:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT 1 FROM processed_games WHERE game_id = $1",
            game_id,
        )
    return row is not None


async def get_backfill_state_1v1():
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT cursor, completed, last_attempt, last_error
            FROM backfill_state_1v1 WHERE id = 1
            """
        )
    return row[0], bool(row[1]), row[2], row[3]


async def set_backfill_state_1v1(
    cursor,
    completed,
    last_attempt=None,
    last_error=None,
):
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO backfill_state_1v1 (id, cursor, completed, last_attempt, last_error)
            VALUES (1, $1, $2, $3, $4)
            ON CONFLICT (id) DO UPDATE SET
                cursor = EXCLUDED.cursor,
                completed = EXCLUDED.completed,
                last_attempt = EXCLUDED.last_attempt,
                last_error = EXCLUDED.last_error
            """,
            cursor,
            completed,
            last_attempt,
            last_error,
        )


async def mark_game_processed(game_id: str):
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO processed_games (game_id) VALUES ($1) ON CONFLICT DO NOTHING",
            game_id,
        )


async def upsert_player(username_key, display_name, wins_ffa, losses_ffa, wins_team, losses_team):
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO player_stats (
                username, display_name, wins_ffa, losses_ffa, wins_team, losses_team, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT(username) DO UPDATE SET
                display_name = EXCLUDED.display_name,
                wins_ffa = player_stats.wins_ffa + EXCLUDED.wins_ffa,
                losses_ffa = player_stats.losses_ffa + EXCLUDED.losses_ffa,
                wins_team = player_stats.wins_team + EXCLUDED.wins_team,
                losses_team = player_stats.losses_team + EXCLUDED.losses_team,
                updated_at = EXCLUDED.updated_at
            """,
            username_key,
            display_name,
            wins_ffa,
            losses_ffa,
            wins_team,
            losses_team,
            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        )


async def load_leaderboard():
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT username, display_name, wins_ffa, losses_ffa, wins_team, losses_team, updated_at
            FROM player_stats
            """
        )
    aggregated = {}
    last_updated = None
    for row in rows:
        raw_name = row[1] or row[0]
        base = normalize_username(raw_name)
        merged_prefix = merge_prefix_key(base)
        if merged_prefix:
            key = merged_prefix
        else:
            key = re.sub(r"\s+", "", base).upper()
        if not key:
            continue
        entry = aggregated.setdefault(
            key,
            {
                "display_name": f"{CLAN_DISPLAY} {merged_prefix.title()}" if merged_prefix else build_display_name(raw_name),
                "wins_ffa": 0,
                "losses_ffa": 0,
                "wins_team": 0,
                "losses_team": 0,
                "updated_at": None,
            },
        )
        entry["wins_ffa"] += row[2]
        entry["losses_ffa"] += row[3]
        entry["wins_team"] += row[4]
        entry["losses_team"] += row[5]
        if row[6] and (entry["updated_at"] is None or row[6] > entry["updated_at"]):
            entry["updated_at"] = row[6]
        if row[6] and (last_updated is None or row[6] > last_updated):
            last_updated = row[6]

    # Merge keys where one is a prefix of another (helps with small name variants)
    min_prefix = int(os.getenv("LEADERBOARD_MERGE_PREFIX_MIN", "6"))
    max_diff = int(os.getenv("LEADERBOARD_MERGE_MAX_DIFF", "6"))
    if min_prefix < 3:
        min_prefix = 3
    if max_diff < 1:
        max_diff = 1

    keys_sorted = sorted(aggregated.keys(), key=len)
    for base_key in keys_sorted:
        if base_key not in aggregated:
            continue
        if len(base_key) < min_prefix:
            continue
        for other_key in list(aggregated.keys()):
            if other_key == base_key:
                continue
            if other_key.startswith(base_key) and 0 < (len(other_key) - len(base_key)) <= max_diff:
                src = aggregated.pop(other_key)
                dst = aggregated[base_key]
                dst["wins_ffa"] += src["wins_ffa"]
                dst["losses_ffa"] += src["losses_ffa"]
                dst["wins_team"] += src["wins_team"]
                dst["losses_team"] += src["losses_team"]
                if src.get("updated_at") and (dst.get("updated_at") is None or src["updated_at"] > dst["updated_at"]):
                    dst["updated_at"] = src["updated_at"]

    players = []
    for key, entry in aggregated.items():
        ratio = calculate_ratio(
            entry["wins_ffa"], entry["losses_ffa"], entry["wins_team"], entry["losses_team"]
        )
        total_wins = entry["wins_ffa"] + entry["wins_team"]
        total_losses = entry["losses_ffa"] + entry["losses_team"]
        total_games = total_wins + total_losses
        players.append(
            {
                "username": key,
                "display_name": entry["display_name"],
                "wins_ffa": entry["wins_ffa"],
                "losses_ffa": entry["losses_ffa"],
                "wins_team": entry["wins_team"],
                "losses_team": entry["losses_team"],
                "ratio": ratio,
                "total_wins": total_wins,
                "total_games": total_games,
            }
        )
    for p in players:
        p["score"] = calculate_score(
            p["total_wins"], p["total_games"] - p["total_wins"], p["total_games"]
        )

    players.sort(
        key=lambda p: (
            p["total_games"] >= MIN_GAMES,
            p["score"],
            p["total_wins"],
            p["total_games"],
        ),
        reverse=True,
    )
    return players, last_updated


async def get_leaderboard_message(guild_id: int):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT guild_id, channel_id, message_id FROM leaderboard_message WHERE guild_id = $1",
            guild_id,
        )


async def set_leaderboard_message(guild_id: int, channel_id: int, message_id: int):
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO leaderboard_message (guild_id, channel_id, message_id)
            VALUES ($1, $2, $3)
            ON CONFLICT (guild_id) DO UPDATE SET
                channel_id = EXCLUDED.channel_id,
                message_id = EXCLUDED.message_id
            """,
            guild_id,
            channel_id,
            message_id,
        )


async def clear_leaderboard_message(guild_id: int):
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM leaderboard_message WHERE guild_id = $1",
            guild_id,
        )


def get_total_pages(total_items, page_size):
    if total_items <= 0:
        return 1
    return (total_items + page_size - 1) // page_size


async def get_top_players():
    players, last_updated = await load_leaderboard()
    if not players:
        return [], None
    filtered = [p for p in players if p["total_games"] >= MIN_GAMES]
    return filtered[:100], last_updated


async def build_leaderboard_embed(guild, page: int, page_size: int):
    top, last_updated = await get_top_players()
    if not top:
        return None
    
    total_pages = get_total_pages(len(top), page_size)
    page = max(1, min(page, total_pages))
    start = (page - 1) * page_size
    end = start + page_size
    page_items = top[start:end]

    embed = discord.Embed(
        title=f"🏆 Leaderboard {CLAN_DISPLAY} — Page {page}/{total_pages}",
        color=discord.Color.orange(),
    )
    total_wins = sum(p["wins_ffa"] + p["wins_team"] for p in top)
    total_losses = sum(p["losses_ffa"] + p["losses_team"] for p in top)
    total_players = len(top)

    embed.description = (
        f"**Joueurs:** {total_players}  |  "
        f"**Wins:** {total_wins}  |  "
        f"**Losses:** {total_losses}"
    )
    if guild and guild.icon:
        embed.set_thumbnail(url=guild.icon.url)

    name_width = 16

    def truncate_name(name: str) -> str:
        if len(name) <= name_width:
            return name
        return name[: name_width - 3] + "..."

    truncated_counts = {}
    for p in page_items:
        t = truncate_name(p["display_name"])
        truncated_counts[t] = truncated_counts.get(t, 0) + 1

    def format_table_name(player):
        display = player["display_name"]
        name = truncate_name(display)
        if truncated_counts.get(name, 0) > 1:
            suffix = player["username"][-3:]
            base = display[: name_width - 4] if len(display) >= name_width - 3 else display
            name = base[: name_width - 4] + "+" + suffix
        return name

    def format_line(rank, player):
        username = format_table_name(player)
        score = f"{player['score']:.1f}"
        team = f"{player['wins_team']}W/{player['losses_team']}L"
        games = f"{player['total_games']}"
        return f"{rank:<3} {username:<{name_width}} {score:>5}  {team:>7}  {games:>3}"

    header = f"{'#':<3} {'JOUEUR':<{name_width}} {'SCORE':>5} {'TEAM':>7} {'G':>3}"
    sep = "-" * (name_width + 22)
    table = [header, sep]
    for i, p in enumerate(page_items, start + 1):
        table.append(format_line(i, p))
    embed.add_field(name="Classement", value="```\n" + "\n".join(table) + "\n```", inline=False)

    if last_updated:
        try:
            last_dt = datetime.strptime(last_updated, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            next_dt = last_dt + timedelta(minutes=REFRESH_MINUTES)
            footer = f"Mis à jour le {format_local_time(last_dt)} | Prochaine maj {format_local_time(next_dt)}"
        except Exception:
            footer = f"Mis à jour le {last_updated}"
        embed.set_footer(text=footer)

    return embed


async def upsert_ffa_player(discord_id: int, pseudo: str, player_id: str):
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO ffa_players (discord_id, pseudo, player_id)
            VALUES ($1, $2, $3)
            ON CONFLICT (discord_id) DO UPDATE SET
                pseudo = EXCLUDED.pseudo,
                player_id = EXCLUDED.player_id
            """,
            discord_id,
            pseudo,
            player_id,
        )


async def upsert_ffa_stats(player_id: str, pseudo: str, wins: int, losses: int):
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO ffa_stats (player_id, pseudo, wins_ffa, losses_ffa, updated_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (player_id) DO UPDATE SET
                pseudo = EXCLUDED.pseudo,
                wins_ffa = EXCLUDED.wins_ffa,
                losses_ffa = EXCLUDED.losses_ffa,
                updated_at = EXCLUDED.updated_at
            """,
            player_id,
            pseudo,
            wins,
            losses,
            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        )


async def get_ffa_players():
    async with pool.acquire() as conn:
        return await conn.fetch(
            "SELECT discord_id, pseudo, player_id FROM ffa_players"
        )


async def is_game_processed_1v1(game_id: str) -> bool:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT 1 FROM processed_games_1v1 WHERE game_id = $1",
            game_id,
        )
    return row is not None


async def mark_game_processed_1v1(game_id: str):
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO processed_games_1v1 (game_id) VALUES ($1) ON CONFLICT DO NOTHING",
            game_id,
        )


async def is_win_notified(game_id: str) -> bool:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT 1 FROM win_notifications WHERE game_id = $1",
            game_id,
        )
    return row is not None


async def mark_win_notified(game_id: str):
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO win_notifications (game_id) VALUES ($1) ON CONFLICT DO NOTHING",
            game_id,
        )


async def upsert_1v1_stats(username: str, wins: int, losses: int):
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO player_stats_1v1 (username, wins, losses, updated_at)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT(username) DO UPDATE SET
                wins = player_stats_1v1.wins + EXCLUDED.wins,
                losses = player_stats_1v1.losses + EXCLUDED.losses,
                updated_at = EXCLUDED.updated_at
            """,
            username,
            wins,
            losses,
            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        )


async def load_1v1_leaderboard():
    items, fetched_at = await get_official_1v1_leaderboard_cached(100)
    return items, fetched_at


async def load_ffa_leaderboard():
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT pseudo, wins_ffa, losses_ffa, updated_at FROM ffa_stats"
        )
    players = []
    last_updated = None
    for row in rows:
        wins = row[1]
        losses = row[2]
        games = wins + losses
        ratio = (wins / games) if games > 0 else 0.0
        score = calculate_score(wins, losses, games)
        players.append(
            {
                "display_name": row[0],
                "wins": wins,
                "losses": losses,
                "games": games,
                "ratio": ratio,
                "score": score,
            }
        )
        if row[3] and (last_updated is None or row[3] > last_updated):
            last_updated = row[3]
    players = [p for p in players if p["games"] >= MIN_GAMES]
    players.sort(key=lambda p: (p["score"], p["games"]), reverse=True)
    return players[:100], last_updated


async def refresh_ffa_stats():
    players = await get_ffa_players()
    for _discord_id, pseudo, player_id in players:
        try:
            sessions = await fetch_player_sessions(player_id)
            wins, losses = compute_ffa_stats_from_sessions(sessions)
            await upsert_ffa_stats(player_id, pseudo, wins, losses)
        except Exception:
            continue


async def build_leaderboard_ffa_embed(guild, page: int, page_size: int):
    top, last_updated = await load_ffa_leaderboard()
    if not top:
        return None

    total_pages = get_total_pages(len(top), page_size)
    page = max(1, min(page, total_pages))
    start = (page - 1) * page_size
    end = start + page_size
    page_items = top[start:end]
    
    embed = discord.Embed(
        title=f"🎯 Leaderboard FFA {CLAN_DISPLAY} — Page {page}/{total_pages}",
        color=discord.Color.orange(),
    )

    total_wins = sum(p["wins"] for p in top)
    total_losses = sum(p["losses"] for p in top)
    embed.description = f"**Wins:** {total_wins}  |  **Losses:** {total_losses}"
    if guild and guild.icon:
        embed.set_thumbnail(url=guild.icon.url)

    name_width = 16

    def truncate_name(name: str) -> str:
        if len(name) <= name_width:
            return name
        return name[: name_width - 3] + "..."

    header = f"{'#':<3} {'JOUEUR':<{name_width}} {'SCORE':>5} {'W/L':>7} {'G':>3}"
    sep = "-" * (name_width + 22)
    table = [header, sep]
    for i, p in enumerate(page_items, start + 1):
        name = truncate_name(p["display_name"])
        score = f"{p['score']:.1f}"
        wl = f"{p['wins']}W/{p['losses']}L"
        games = f"{p['games']}"
        table.append(f"{i:<3} {name:<{name_width}} {score:>5} {wl:>7} {games:>3}")

    embed.add_field(name="Classement FFA", value="```\n" + "\n".join(table) + "\n```", inline=False)

    if last_updated:
        try:
            last_dt = datetime.strptime(last_updated, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            next_dt = last_dt + timedelta(minutes=REFRESH_MINUTES)
            footer = f"Mis à jour le {format_local_time(last_dt)} | Prochaine maj {format_local_time(next_dt)}"
        except Exception:
            footer = f"Mis à jour le {last_updated}"
        embed.set_footer(text=footer)

    return embed


async def build_leaderboard_1v1_embed(guild, page: int, page_size: int):
    top, last_updated = await load_1v1_leaderboard()
    if not top:
        return None

    total_pages = get_total_pages(len(top), page_size)
    page = max(1, min(page, total_pages))
    start = (page - 1) * page_size
    end = start + page_size
    page_items = top[start:end]

    embed = discord.Embed(
        title=f"🥇 Leaderboard 1v1 OpenFront — Top 100 — Page {page}/{total_pages}",
        color=discord.Color.orange(),
    )

    total_games = sum(p.get("games", 0) for p in top)
    embed.description = f"**Joueurs:** {len(top)}  |  **Games:** {total_games}"
    if guild and guild.icon:
        embed.set_thumbnail(url=guild.icon.url)

    name_width = 16

    def truncate_name(name: str) -> str:
        if len(name) <= name_width:
            return name
        return name[: name_width - 3] + "..."

    truncated_counts = {}
    for p in page_items:
        t = truncate_name(p.get("name") or "Unknown")
        truncated_counts[t] = truncated_counts.get(t, 0) + 1

    def format_table_name(player):
        raw_name = player.get("name") or "Unknown"
        name = truncate_name(raw_name)
        if truncated_counts.get(name, 0) > 1 and len(raw_name) >= 3:
            suffix = raw_name[-3:]
            base = raw_name[: name_width - 4] if len(raw_name) >= name_width - 3 else raw_name
            name = base[: name_width - 4] + "+" + suffix
        if is_clan_username(raw_name):
            if len(name) >= name_width:
                name = name[: name_width - 1]
            name = f"★{name}"
        return name

    def format_line(rank, player):
        username = format_table_name(player)
        elo = player.get("elo")
        elo_text = f"{int(elo)}" if isinstance(elo, (int, float)) else "?"
        games = f"{player.get('games', 0)}"
        ratio_pct = player.get("ratio_pct")
        ratio_text = f"{ratio_pct:.1f}%" if isinstance(ratio_pct, (int, float)) else "?"
        return f"{rank:<3} {username:<{name_width}} {elo_text:>5}  {games:>5}  {ratio_text:>6}"

    header = f"{'#':<3} {'JOUEUR':<{name_width}} {'ELO':>5} {'GAMES':>5} {'RATIO':>6}"
    sep = "-" * (name_width + 24)
    table = [header, sep]
    for i, p in enumerate(page_items, start + 1):
        table.append(format_line(i, p))

    embed.add_field(name="Classement 1v1", value="```\n" + "\n".join(table) + "\n```", inline=False)

    if last_updated:
        try:
            if isinstance(last_updated, datetime):
                last_dt = last_updated
            else:
                last_dt = datetime.strptime(last_updated, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            next_dt = last_dt + timedelta(minutes=ONEV1_REFRESH_MINUTES)
            footer = f"Mis à jour le {format_local_time(last_dt)} | Prochaine maj {format_local_time(next_dt)}"
        except Exception:
            footer = f"Mis à jour le {last_updated}"
        embed.set_footer(text=footer)

    return embed


async def build_leaderboard_1v1_gal_embed(guild):
    top, last_updated = await load_1v1_leaderboard()
    if not top:
        return None

    gal_items = []
    for idx, p in enumerate(top, 1):
        name = p.get("name") or "Unknown"
        if is_clan_username(name):
            item = dict(p)
            item["rank"] = idx
            gal_items.append(item)

    if not gal_items:
        return None

    embed = discord.Embed(
        title=f"🥇 Leaderboard 1v1 {CLAN_DISPLAY} — Top 100",
        color=discord.Color.orange(),
    )
    total_games = sum(p.get("games", 0) for p in gal_items)
    embed.description = f"**Joueurs:** {len(gal_items)}  |  **Games:** {total_games}"
    if guild and guild.icon:
        embed.set_thumbnail(url=guild.icon.url)

    name_width = 16

    def truncate_name(name: str) -> str:
        if len(name) <= name_width:
            return name
        return name[: name_width - 3] + "..."

    truncated_counts = {}
    for p in gal_items:
        t = truncate_name(p.get("name") or "Unknown")
        truncated_counts[t] = truncated_counts.get(t, 0) + 1

    def format_table_name(player):
        raw_name = player.get("name") or "Unknown"
        name = truncate_name(raw_name)
        if truncated_counts.get(name, 0) > 1 and len(raw_name) >= 3:
            suffix = raw_name[-3:]
            base = raw_name[: name_width - 4] if len(raw_name) >= name_width - 3 else raw_name
            name = base[: name_width - 4] + "+" + suffix
        if len(name) >= name_width:
            name = name[: name_width - 1]
        return f"★{name}"

    def format_line(player):
        rank = player["rank"]
        username = format_table_name(player)
        elo = player.get("elo")
        elo_text = f"{int(elo)}" if isinstance(elo, (int, float)) else "?"
        games = f"{player.get('games', 0)}"
        ratio_pct = player.get("ratio_pct")
        ratio_text = f"{ratio_pct:.1f}%" if isinstance(ratio_pct, (int, float)) else "?"
        return f"{rank:<3} {username:<{name_width}} {elo_text:>5}  {games:>5}  {ratio_text:>6}"

    header = f"{'#':<3} {'JOUEUR':<{name_width}} {'ELO':>5} {'GAMES':>5} {'RATIO':>6}"
    sep = "-" * (name_width + 24)
    table = [header, sep]
    for p in gal_items:
        table.append(format_line(p))

    embed.add_field(name="Classement 1v1 [GAL]", value="```\n" + "\n".join(table) + "\n```", inline=False)

    if last_updated:
        try:
            if isinstance(last_updated, datetime):
                last_dt = last_updated
            else:
                last_dt = datetime.strptime(last_updated, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            next_dt = last_dt + timedelta(minutes=ONEV1_REFRESH_MINUTES)
            footer = f"Mis à jour le {format_local_time(last_dt)} | Prochaine maj {format_local_time(next_dt)}"
        except Exception:
            footer = f"Mis à jour le {last_updated}"
        embed.set_footer(text=footer)

    return embed


async def get_leaderboard_message_ffa(guild_id: int):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT guild_id, channel_id, message_id FROM leaderboard_message_ffa WHERE guild_id = $1",
            guild_id,
        )


async def set_leaderboard_message_ffa(guild_id: int, channel_id: int, message_id: int):
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO leaderboard_message_ffa (guild_id, channel_id, message_id)
            VALUES ($1, $2, $3)
            ON CONFLICT (guild_id) DO UPDATE SET
                channel_id = EXCLUDED.channel_id,
                message_id = EXCLUDED.message_id
            """,
            guild_id,
            channel_id,
            message_id,
        )


async def clear_leaderboard_message_ffa(guild_id: int):
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM leaderboard_message_ffa WHERE guild_id = $1",
            guild_id,
        )


async def get_leaderboard_message_1v1(guild_id: int):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT guild_id, channel_id, message_id FROM leaderboard_message_1v1 WHERE guild_id = $1",
            guild_id,
        )


async def set_leaderboard_message_1v1(guild_id: int, channel_id: int, message_id: int):
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO leaderboard_message_1v1 (guild_id, channel_id, message_id)
            VALUES ($1, $2, $3)
            ON CONFLICT (guild_id) DO UPDATE SET
                channel_id = EXCLUDED.channel_id,
                message_id = EXCLUDED.message_id
            """,
            guild_id,
            channel_id,
            message_id,
        )


async def clear_leaderboard_message_1v1(guild_id: int):
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM leaderboard_message_1v1 WHERE guild_id = $1",
            guild_id,
        )


async def get_leaderboard_message_1v1_gal(guild_id: int):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT guild_id, channel_id, message_id FROM leaderboard_message_1v1_gal WHERE guild_id = $1",
            guild_id,
        )


async def set_leaderboard_message_1v1_gal(guild_id: int, channel_id: int, message_id: int):
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO leaderboard_message_1v1_gal (guild_id, channel_id, message_id)
            VALUES ($1, $2, $3)
            ON CONFLICT (guild_id) DO UPDATE SET
                channel_id = EXCLUDED.channel_id,
                message_id = EXCLUDED.message_id
            """,
            guild_id,
            channel_id,
            message_id,
        )


async def clear_leaderboard_message_1v1_gal(guild_id: int):
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM leaderboard_message_1v1_gal WHERE guild_id = $1",
            guild_id,
        )


class LeaderboardView(discord.ui.View):
    def __init__(self, page: int, page_size: int):
        super().__init__(timeout=None)
        self.page = page
        self.page_size = page_size

    async def update(self, interaction: discord.Interaction, page: int):
        embed = await build_leaderboard_embed(interaction.guild, page, self.page_size)
        if not embed:
            await interaction.response.send_message(
                f"No data for {CLAN_DISPLAY}. Wait for refresh.",
                ephemeral=True,
            )
            return
        self.page = page
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="◀", style=discord.ButtonStyle.secondary, custom_id="lb_prev")
    async def prev(self, interaction: discord.Interaction, _button: discord.ui.Button):
        await self.update(interaction, max(1, self.page - 1))

    @discord.ui.button(label="▶", style=discord.ButtonStyle.secondary, custom_id="lb_next")
    async def next(self, interaction: discord.Interaction, _button: discord.ui.Button):
        top, _ = await get_top_players()
        total_pages = get_total_pages(len(top), self.page_size)
        await self.update(interaction, min(total_pages, self.page + 1))


class LeaderboardFfaView(discord.ui.View):
    def __init__(self, page: int, page_size: int):
        super().__init__(timeout=None)
        self.page = page
        self.page_size = page_size

    async def update(self, interaction: discord.Interaction, page: int):
        embed = await build_leaderboard_ffa_embed(interaction.guild, page, self.page_size)
        if not embed:
            await interaction.response.send_message(
                f"No data for FFA {CLAN_DISPLAY}.",
                ephemeral=True,
            )
            return
        self.page = page
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="◀", style=discord.ButtonStyle.secondary, custom_id="ffa_prev")
    async def prev(self, interaction: discord.Interaction, _button: discord.ui.Button):
        await self.update(interaction, max(1, self.page - 1))

    @discord.ui.button(label="▶", style=discord.ButtonStyle.secondary, custom_id="ffa_next")
    async def next(self, interaction: discord.Interaction, _button: discord.ui.Button):
        top, _ = await load_ffa_leaderboard()
        total_pages = get_total_pages(len(top), self.page_size)
        await self.update(interaction, min(total_pages, self.page + 1))


class Leaderboard1v1View(discord.ui.View):
    def __init__(self, page: int, page_size: int):
        super().__init__(timeout=None)
        self.page = page
        self.page_size = page_size

    async def update(self, interaction: discord.Interaction, page: int):
        embed = await build_leaderboard_1v1_embed(interaction.guild, page, self.page_size)
        if not embed:
            await interaction.response.send_message(
                "No data for 1v1.",
                ephemeral=True,
            )
            return
        self.page = page
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="◀", style=discord.ButtonStyle.secondary, custom_id="1v1_prev")
    async def prev(self, interaction: discord.Interaction, _button: discord.ui.Button):
        await self.update(interaction, max(1, self.page - 1))

    @discord.ui.button(label="▶", style=discord.ButtonStyle.secondary, custom_id="1v1_next")
    async def next(self, interaction: discord.Interaction, _button: discord.ui.Button):
        top, _ = await load_1v1_leaderboard()
        total_pages = get_total_pages(len(top), self.page_size)
        await self.update(interaction, min(total_pages, self.page + 1))


async def update_leaderboard_message():
    if not bot.guilds:
        return
    for guild in bot.guilds:
        record = await get_leaderboard_message(guild.id)
        if not record:
            continue
        channel_id = record["channel_id"]
        message_id = record["message_id"]
        try:
            channel = bot.get_channel(channel_id) or await bot.fetch_channel(channel_id)
            message = await channel.fetch_message(message_id)
            embed = await build_leaderboard_embed(guild, 1, 20)
            if embed:
                await message.edit(embed=embed, view=LeaderboardView(1, 20))
        except Exception:
            await clear_leaderboard_message(guild.id)


async def update_leaderboard_message_ffa():
    if not bot.guilds:
        return
    for guild in bot.guilds:
        record = await get_leaderboard_message_ffa(guild.id)
        if not record:
            continue
        channel_id = record["channel_id"]
        message_id = record["message_id"]
        try:
            channel = bot.get_channel(channel_id) or await bot.fetch_channel(channel_id)
            message = await channel.fetch_message(message_id)
            embed = await build_leaderboard_ffa_embed(guild, 1, 20)
            if embed:
                await message.edit(embed=embed, view=LeaderboardFfaView(1, 20))
        except Exception:
            await clear_leaderboard_message_ffa(guild.id)


async def update_leaderboard_message_1v1():
    if not bot.guilds:
        return
    for guild in bot.guilds:
        record = await get_leaderboard_message_1v1(guild.id)
        if not record:
            continue
        channel_id = record["channel_id"]
        message_id = record["message_id"]
        try:
            channel = bot.get_channel(channel_id) or await bot.fetch_channel(channel_id)
            message = await channel.fetch_message(message_id)
            embed = await build_leaderboard_1v1_embed(guild, 1, 20)
            if embed:
                await message.edit(embed=embed, view=Leaderboard1v1View(1, 20))
        except Exception:
            await clear_leaderboard_message_1v1(guild.id)


async def update_leaderboard_message_1v1_gal():
    if not bot.guilds:
        return
    for guild in bot.guilds:
        record = await get_leaderboard_message_1v1_gal(guild.id)
        if not record:
            continue
        channel_id = record["channel_id"]
        message_id = record["message_id"]
        try:
            channel = bot.get_channel(channel_id) or await bot.fetch_channel(channel_id)
            message = await channel.fetch_message(message_id)
            embed = await build_leaderboard_1v1_gal_embed(guild)
            if embed:
                await message.edit(embed=embed)
        except Exception:
            await clear_leaderboard_message_1v1_gal(guild.id)


async def get_progress_stats():
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                COUNT(*) AS players,
                COALESCE(SUM(wins_ffa + wins_team), 0) AS wins_total,
                COALESCE(SUM(losses_ffa + losses_team), 0) AS losses_total
            FROM player_stats
            """
        )
        games_row = await conn.fetchrow("SELECT COUNT(*) FROM processed_games")
    return {
        "players": row[0] if row else 0,
        "wins_total": row[1] if row else 0,
        "losses_total": row[2] if row else 0,
        "games_processed": games_row[0] if games_row else 0,
    }


def compute_next_backfill_eta(last_attempt):
    if not last_attempt:
        return "inconnu"
    try:
        last_dt = datetime.strptime(last_attempt, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except Exception:
        return "inconnu"
    next_dt = last_dt + timedelta(minutes=BACKFILL_INTERVAL_MINUTES)
    now = datetime.now(timezone.utc)
    delta = next_dt - now
    if delta.total_seconds() <= 0:
        return "imminent"
    minutes = int(delta.total_seconds() // 60)
    seconds = int(delta.total_seconds() % 60)
    return f"{minutes}m {seconds}s"


def process_game(info, clan_has_won):
    mode = game_mode(info).lower()
    is_ffa = "free for all" in mode or mode == "ffa"
    is_team = "team" in mode

    for p in info.get("players", []):
        username_raw = p.get("username") or ""
        if not is_clan_username(username_raw):
            continue
        username_base = normalize_username(username_raw)
        if not username_base:
            continue
        username_key = username_base.upper()
        display_name = build_display_name(username_raw)

        if is_ffa:
            if clan_has_won:
                asyncio.create_task(upsert_player(username_key, display_name, 1, 0, 0, 0))
            else:
                asyncio.create_task(upsert_player(username_key, display_name, 0, 1, 0, 0))
        elif is_team:
            if clan_has_won:
                asyncio.create_task(upsert_player(username_key, display_name, 0, 0, 1, 0))
            else:
                asyncio.create_task(upsert_player(username_key, display_name, 0, 0, 0, 1))


async def fetch_clan_sessions(session, start_iso, end_iso):
    url = f"{API_BASE}/clan/{CLAN_TAG}/sessions"
    params = {"start": start_iso, "end": end_iso}
    async with session.get(url, params=params, timeout=25) as resp:
        if resp.status != 200:
            text = await resp.text()
            raise RuntimeError(f"HTTP {resp.status}: {text[:200]}")
        return await resp.json()


async def fetch_game_info(session, game_id):
    url = f"{API_BASE}/game/{game_id}"
    async with session.get(url, params={"turns": "false"}, timeout=25) as resp:
        if resp.status != 200:
            text = await resp.text()
            raise RuntimeError(f"HTTP {resp.status}: {text[:200]}")
        data = await resp.json()
        return data.get("info", {})


async def fetch_games_list(session, start_iso: str, end_iso: str, max_games: int):
    games = []
    offset = 0
    while len(games) < max_games:
        limit = min(1000, max_games - len(games))
        params = {
            "start": start_iso,
            "end": end_iso,
            "type": "Public",
            "limit": str(limit),
            "offset": str(offset),
        }
        url = f"{API_BASE}/games"
        async with session.get(url, params=params, timeout=25) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise RuntimeError(f"HTTP {resp.status}: {text[:200]}")
            batch = await resp.json()
        if not batch:
            break
        games.extend(batch)
        offset += len(batch)
        if len(batch) < limit:
            break
    return games


def _extract_list(payload):
    if isinstance(payload, list):
        return payload
    for key in ("items", "data", "players", "leaderboard", "results"):
        if isinstance(payload, dict) and isinstance(payload.get(key), list):
            return payload[key]
    return []


def _get_first_value(entry, keys, default=None):
    for key in keys:
        if key in entry and entry[key] is not None:
            return entry[key]
    return default


def _normalize_1v1_entry(entry):
    name = _get_first_value(entry, ["username", "player", "name", "displayName", "user"])
    if not name:
        return None
    clan_tag = entry.get("clanTag")
    if clan_tag and f"[{clan_tag}]".upper() not in str(name).upper():
        name = f"[{clan_tag}] {name}"
    elo = _get_first_value(entry, ["elo", "rating", "mmr", "score"])
    wins = _get_first_value(entry, ["wins", "win", "victories"], 0)
    losses = _get_first_value(entry, ["losses", "loss", "defeats"], 0)
    games = _get_first_value(entry, ["games", "matches", "totalGames", "played"])
    if games is None:
        games = (wins or 0) + (losses or 0)
    ratio = _get_first_value(entry, ["winRate", "winrate", "ratio", "winLossRatio"])
    if ratio is None and games:
        ratio = (wins / games) if games > 0 else 0.0
    if isinstance(ratio, (int, float)) and ratio <= 1.0:
        ratio_pct = ratio * 100
    elif isinstance(ratio, (int, float)):
        ratio_pct = float(ratio)
    else:
        ratio_pct = None
    return {
        "name": str(name),
        "elo": elo,
        "games": int(games) if games is not None else 0,
        "wins": int(wins) if wins is not None else 0,
        "losses": int(losses) if losses is not None else 0,
        "ratio_pct": ratio_pct,
    }


async def fetch_official_1v1_leaderboard(limit: int):
    headers = build_api_headers()
    items = []
    page = 1
    async with aiohttp.ClientSession(headers=headers) as session:
        while len(items) < limit:
            params = {"page": str(page)}
            async with session.get(ONEV1_LEADERBOARD_URL, params=params, timeout=25) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    raise RuntimeError(f"HTTP {resp.status}: {text[:200]}")
                payload = await resp.json()
            raw_items = payload.get("1v1") or payload.get("oneVone") or _extract_list(payload)
            if not raw_items:
                break
            for entry in raw_items:
                norm = _normalize_1v1_entry(entry)
                if norm:
                    items.append(norm)
                    if len(items) >= limit:
                        break
            if len(raw_items) < 50:
                break
            page += 1
    return items[:limit]


async def get_official_1v1_leaderboard_cached(limit: int):
    now = datetime.now(timezone.utc)
    cached_at = ONEV1_CACHE.get("fetched_at")
    cached_items = ONEV1_CACHE.get("items") or []
    if cached_items and cached_at:
        age = (now - cached_at).total_seconds()
        if age < ONEV1_REFRESH_MINUTES * 60:
            return cached_items[:limit], cached_at
    items = await fetch_official_1v1_leaderboard(limit)
    ONEV1_CACHE["items"] = items
    ONEV1_CACHE["fetched_at"] = now
    return items[:limit], now


async def refresh_from_range(start_dt, end_dt):
    start_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_iso = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    headers = {"User-Agent": USER_AGENT}
    async with aiohttp.ClientSession(headers=headers) as session:
        sessions = await fetch_clan_sessions(session, start_iso, end_iso)
        sessions = sessions[:MAX_SESSIONS]

        processed_in_step = 0
        for s in sessions:
            game_id = s.get("gameId")
            if not game_id:
                continue
            if await is_game_processed(game_id):
                continue
            try:
                info = await fetch_game_info(session, game_id)
            except Exception:
                continue
            clan_has_won = bool(s.get("hasWon"))
            process_game(info, clan_has_won)
            await mark_game_processed(game_id)
            processed_in_step += 1

        return len(sessions), processed_in_step


async def refresh_1v1_from_range(start_dt, end_dt):
    start_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_iso = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    headers = {"User-Agent": USER_AGENT}
    async with aiohttp.ClientSession(headers=headers) as session:
        games = await fetch_games_list(session, start_iso, end_iso, ONEV1_MAX_GAMES)
        processed_in_step = 0
        for g in games:
            game_id = g.get("game")
            if not game_id:
                continue
            if await is_game_processed_1v1(game_id):
                continue
            try:
                info = await fetch_game_info(session, game_id)
            except Exception:
                continue
            if not is_1v1_game(info):
                await mark_game_processed_1v1(game_id)
                continue
            winners = get_winner_client_ids(info)
            if not winners:
                await mark_game_processed_1v1(game_id)
                continue
            for p in info.get("players", []):
                username_raw = p.get("username") or ""
                username_key = normalize_username(username_raw)
                if not username_key:
                    continue
                if p.get("clientID") in winners:
                    await upsert_1v1_stats(username_key, 1, 0)
                else:
                    await upsert_1v1_stats(username_key, 0, 1)
            await mark_game_processed_1v1(game_id)
            processed_in_step += 1

        return len(games), processed_in_step


async def run_backfill_step():
    cursor, completed, _last_attempt, _last_error, _last_sessions, _last_games = await get_backfill_state()
    if completed:
        return {"status": "done", "cursor": cursor}

    try:
        start_dt = datetime.fromisoformat(cursor.replace("Z", "+00:00"))
    except Exception:
        start_dt = datetime.now(timezone.utc) - timedelta(hours=48)

    end_dt = start_dt + timedelta(hours=48)
    now_dt = datetime.now(timezone.utc)
    if end_dt > now_dt:
        end_dt = now_dt

    last_attempt = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    last_error = None

    try:
        last_sessions, last_games_processed = await refresh_from_range(start_dt, end_dt)
    except Exception as exc:
        last_error = str(exc)[:500]
        await set_backfill_state(cursor, False, last_attempt, last_error, 0, 0)
        print(f"Backfill failed: {exc}")
        return {"status": "error", "cursor": cursor, "error": last_error}

    new_cursor = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    completed = end_dt >= now_dt
    await set_backfill_state(
        new_cursor,
        completed,
        last_attempt,
        last_error,
        last_sessions,
        last_games_processed,
    )
    print(f"Backfill step: {cursor} -> {new_cursor} (done={completed})")
    return {"status": "ok", "cursor": new_cursor, "completed": completed}


async def backfill_loop():
    while True:
        await run_backfill_step()
        await asyncio.sleep(BACKFILL_INTERVAL_MINUTES * 60)


async def live_loop():
    while True:
        try:
            end_dt = datetime.now(timezone.utc)
            start_dt = end_dt - timedelta(hours=RANGE_HOURS)
            await refresh_from_range(start_dt, end_dt)
            print("Live refresh done")
            await update_leaderboard_message()
            await refresh_ffa_stats()
            await update_leaderboard_message_ffa()
        except Exception as exc:
            print(f"Live refresh failed: {exc}")
        await asyncio.sleep(REFRESH_MINUTES * 60)


async def run_backfill_1v1_step():
    cursor, completed, _last_attempt, _last_error = await get_backfill_state_1v1()
    if completed:
        return {"status": "done", "cursor": cursor}

    try:
        start_dt = datetime.fromisoformat(cursor.replace("Z", "+00:00"))
    except Exception:
        start_dt = datetime.now(timezone.utc) - timedelta(hours=48)

    end_dt = start_dt + timedelta(hours=48)
    now_dt = datetime.now(timezone.utc)
    if end_dt > now_dt:
        end_dt = now_dt

    last_attempt = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    last_error = None

    try:
        await refresh_1v1_from_range(start_dt, end_dt)
    except Exception as exc:
        last_error = str(exc)[:500]
        await set_backfill_state_1v1(cursor, False, last_attempt, last_error)
        print(f"Backfill 1v1 failed: {exc}")
        return {"status": "error", "cursor": cursor, "error": last_error}

    new_cursor = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    completed = end_dt >= now_dt
    await set_backfill_state_1v1(
        new_cursor,
        completed,
        last_attempt,
        last_error,
    )
    print(f"Backfill 1v1 step: {cursor} -> {new_cursor} (done={completed})")
    return {"status": "ok", "cursor": new_cursor, "completed": completed}


async def backfill_1v1_loop():
    while True:
        await run_backfill_1v1_step()
        await asyncio.sleep(ONEV1_BACKFILL_INTERVAL_MINUTES * 60)


async def live_1v1_loop():
    while True:
        try:
            end_dt = datetime.now(timezone.utc)
            start_dt = end_dt - timedelta(hours=48)
            await refresh_1v1_from_range(start_dt, end_dt)
            await update_leaderboard_message_1v1()
            await update_leaderboard_message_1v1_gal()
        except Exception as exc:
            print(f"Live 1v1 refresh failed: {exc}")
        await asyncio.sleep(ONEV1_REFRESH_MINUTES * 60)


async def win_notify_loop():
    if not WIN_NOTIFY_CHANNEL_ID:
        return
    bootstrap = True
    while True:
        try:
            channel = bot.get_channel(int(WIN_NOTIFY_CHANNEL_ID)) or await bot.fetch_channel(int(WIN_NOTIFY_CHANNEL_ID))
            end_dt = datetime.now(timezone.utc)
            start_dt = end_dt - timedelta(hours=WIN_NOTIFY_RANGE_HOURS)
            start_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            end_iso = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

            headers = {"User-Agent": USER_AGENT}
            async with aiohttp.ClientSession(headers=headers) as session:
                sessions = await fetch_clan_sessions(session, start_iso, end_iso)
                for s in sessions:
                    if not s.get("hasWon"):
                        continue
                    game_id = s.get("gameId")
                    if not game_id:
                        continue
                    if await is_win_notified(game_id):
                        continue
                    if bootstrap:
                        await mark_win_notified(game_id)
                        continue
                    try:
                        info = await fetch_game_info(session, game_id)
                    except Exception:
                        continue
                    embed = build_win_embed(info)
                    await channel.send(embed=embed)
                    await mark_win_notified(game_id)
        except Exception as exc:
            print(f"Win notify failed: {exc}")
        bootstrap = False
        await asyncio.sleep(WIN_NOTIFY_POLL_SECONDS)


@bot.event
async def on_ready():
    await init_db()
    try:
        if GUILD_ID:
            guild = discord.Object(id=int(GUILD_ID))
            await bot.tree.sync(guild=guild)
            await bot.tree.sync(guild=None)
            print(f"Commands synced for guild {GUILD_ID}")
        else:
            await bot.tree.sync()
            print("Commands synced globally")
    except Exception as exc:
        print(f"Command sync error: {exc}")

    bot.add_view(LeaderboardView(1, 20))
    bot.add_view(LeaderboardFfaView(1, 20))
    bot.add_view(Leaderboard1v1View(1, 20))
    bot.loop.create_task(backfill_loop())
    bot.loop.create_task(live_loop())
    bot.loop.create_task(backfill_1v1_loop())
    bot.loop.create_task(live_1v1_loop())
    if WIN_NOTIFY_CHANNEL_ID:
        bot.loop.create_task(win_notify_loop())
    print(f"Bot connected: {bot.user}")


@bot.tree.command(name="setleaderboard", description="Show the clan leaderboard.")
async def setleaderboard(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message("Commande disponible uniquement sur un serveur.", ephemeral=True)
        return

    record = await get_leaderboard_message(interaction.guild.id)
    if record:
        await interaction.response.send_message(
            "Un leaderboard est déjà actif sur ce serveur. Utilise /removeleaderboard.",
            ephemeral=True,
        )
        return
    
    embed = await build_leaderboard_embed(interaction.guild, 1, 20)
    if not embed:
        await interaction.response.send_message(
            f"No data for {CLAN_DISPLAY}. Wait for refresh.",
            ephemeral=True,
        )
        return
    
    await interaction.response.send_message(embed=embed, view=LeaderboardView(1, 20))
    message = await interaction.original_response()
    await set_leaderboard_message(interaction.guild.id, interaction.channel_id, message.id)


@bot.tree.command(name="register", description="Enregistre un joueur pour le leaderboard FFA.")
@app_commands.describe(pseudo="Pseudo sans tag Discord (#)", player_id="OpenFront player ID")
async def register(interaction: discord.Interaction, pseudo: str, player_id: str):
    if not is_pseudo_valid(pseudo):
        await interaction.response.send_message("Pseudo invalide (pas de #).", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    try:
        await upsert_ffa_player(interaction.user.id, pseudo, player_id)
        sessions = await fetch_player_sessions(player_id)
        wins, losses = compute_ffa_stats_from_sessions(sessions)
        await upsert_ffa_stats(player_id, pseudo, wins, losses)
    except Exception as exc:
        await interaction.followup.send(f"Erreur: {exc}", ephemeral=True)
        return
    await interaction.followup.send(f"✅ {pseudo} enregistré pour le leaderboard FFA.", ephemeral=True)


@bot.tree.command(name="setleaderboardffa", description="Show the FFA leaderboard.")
async def setleaderboardffa(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message("Commande disponible uniquement sur un serveur.", ephemeral=True)
        return

    record = await get_leaderboard_message_ffa(interaction.guild.id)
    if record:
        await interaction.response.send_message(
            "Un leaderboard FFA est déjà actif. Utilise /removeleaderboardffa.",
            ephemeral=True,
        )
        return

    embed = await build_leaderboard_ffa_embed(interaction.guild, 1, 20)
    if not embed:
        await interaction.response.send_message(
            f"Aucune donnée FFA. Enregistre-toi avec /register.",
            ephemeral=True,
        )
        return

    await interaction.response.send_message(embed=embed, view=LeaderboardFfaView(1, 20))
    message = await interaction.original_response()
    await set_leaderboard_message_ffa(interaction.guild.id, interaction.channel_id, message.id)


@bot.tree.command(name="removeleaderboardffa", description="Supprime le leaderboard FFA du serveur.")
async def removeleaderboardffa(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message("Commande disponible uniquement sur un serveur.", ephemeral=True)
        return
    record = await get_leaderboard_message_ffa(interaction.guild.id)
    if not record:
        await interaction.response.send_message("Aucun leaderboard FFA actif.", ephemeral=True)
        return
    try:
        channel = bot.get_channel(record["channel_id"]) or await bot.fetch_channel(record["channel_id"])
        message = await channel.fetch_message(record["message_id"])
        await message.delete()
    except Exception:
        pass
    await clear_leaderboard_message_ffa(interaction.guild.id)
    await interaction.response.send_message("Leaderboard FFA supprimé.", ephemeral=True)


@bot.tree.command(name="setleaderboard1v1", description="Show the 1v1 leaderboard.")
async def setleaderboard1v1(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message("Commande disponible uniquement sur un serveur.", ephemeral=True)
        return

    record = await get_leaderboard_message_1v1(interaction.guild.id)
    if record:
        await interaction.response.send_message(
            "Un leaderboard 1v1 est déjà actif. Utilise /removeleaderboard1v1.",
            ephemeral=True,
        )
        return

    embed = await build_leaderboard_1v1_embed(interaction.guild, 1, 20)
    if not embed:
        await interaction.response.send_message(
            "Aucune donnée 1v1 disponible pour le moment.",
            ephemeral=True,
        )
        return

    await interaction.response.send_message(embed=embed, view=Leaderboard1v1View(1, 20))
    message = await interaction.original_response()
    await set_leaderboard_message_1v1(interaction.guild.id, interaction.channel_id, message.id)


@bot.tree.command(name="removeleaderboard1v1", description="Supprime le leaderboard 1v1 du serveur.")
async def removeleaderboard1v1(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message("Commande disponible uniquement sur un serveur.", ephemeral=True)
        return
    record = await get_leaderboard_message_1v1(interaction.guild.id)
    if not record:
        await interaction.response.send_message("Aucun leaderboard 1v1 actif.", ephemeral=True)
        return
    try:
        channel = bot.get_channel(record["channel_id"]) or await bot.fetch_channel(record["channel_id"])
        message = await channel.fetch_message(record["message_id"])
        await message.delete()
    except Exception:
        pass
    await clear_leaderboard_message_1v1(interaction.guild.id)
    await interaction.response.send_message("Leaderboard 1v1 supprimé.", ephemeral=True)


@bot.tree.command(name="setleaderboard1v1gal", description="Show the 1v1 leaderboard for [GAL] members.")
async def setleaderboard1v1gal(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message("Commande disponible uniquement sur un serveur.", ephemeral=True)
        return

    record = await get_leaderboard_message_1v1_gal(interaction.guild.id)
    if record:
        await interaction.response.send_message(
            "Un leaderboard 1v1 [GAL] est déjà actif. Utilise /removeleaderboard1v1gal.",
            ephemeral=True,
        )
        return

    embed = await build_leaderboard_1v1_gal_embed(interaction.guild)
    if not embed:
        await interaction.response.send_message(
            "Aucune donnée 1v1 [GAL] disponible pour le moment.",
            ephemeral=True,
        )
        return

    await interaction.response.send_message(embed=embed)
    message = await interaction.original_response()
    await set_leaderboard_message_1v1_gal(interaction.guild.id, interaction.channel_id, message.id)


@bot.tree.command(name="removeleaderboard1v1gal", description="Supprime le leaderboard 1v1 [GAL] du serveur.")
async def removeleaderboard1v1gal(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message("Commande disponible uniquement sur un serveur.", ephemeral=True)
        return
    record = await get_leaderboard_message_1v1_gal(interaction.guild.id)
    if not record:
        await interaction.response.send_message("Aucun leaderboard 1v1 [GAL] actif.", ephemeral=True)
        return
    try:
        channel = bot.get_channel(record["channel_id"]) or await bot.fetch_channel(record["channel_id"])
        message = await channel.fetch_message(record["message_id"])
        await message.delete()
    except Exception:
        pass
    await clear_leaderboard_message_1v1_gal(interaction.guild.id)
    await interaction.response.send_message("Leaderboard 1v1 [GAL] supprimé.", ephemeral=True)


@bot.tree.command(name="refresh_leaderboard", description="Force a live refresh.")
async def refresh_leaderboard_cmd(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    try:
        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(hours=RANGE_HOURS)
        await refresh_from_range(start_dt, end_dt)
        await update_leaderboard_message()
        await interaction.followup.send("OK: leaderboard refreshed.", ephemeral=True)
    except Exception as exc:
        await interaction.followup.send(f"Error: {exc}", ephemeral=True)


@bot.tree.command(name="removeleaderboard", description="Supprime le leaderboard du serveur.")
async def removeleaderboard(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message("Commande disponible uniquement sur un serveur.", ephemeral=True)
        return
    record = await get_leaderboard_message(interaction.guild.id)
    if not record:
        await interaction.response.send_message("Aucun leaderboard actif.", ephemeral=True)
        return
    try:
        channel = bot.get_channel(record["channel_id"]) or await bot.fetch_channel(record["channel_id"])
        message = await channel.fetch_message(record["message_id"])
        await message.delete()
    except Exception:
        pass
    await clear_leaderboard_message(interaction.guild.id)
    await interaction.response.send_message("Leaderboard supprimé.", ephemeral=True)


@bot.tree.command(name="backfill_step", description="Force a 48h backfill step.")
async def backfill_step_cmd(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    result = await run_backfill_step()
    await interaction.followup.send(f"{result}", ephemeral=True)


@bot.tree.command(name="debug_api", description="Debug OpenFront API.")
async def debug_api(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    cursor, completed, last_attempt, last_error, last_sessions, last_games = await get_backfill_state()
    msg = (
        f"Tag: {CLAN_TAG}\n"
        f"Backfill cursor: {cursor}\n"
        f"Backfill done: {completed}\n"
        f"Last attempt: {last_attempt}\n"
        f"Last error: {last_error}\n"
        f"Last sessions: {last_sessions}\n"
        f"Last games processed: {last_games}\n"
        f"Range hours: {RANGE_HOURS}\n"
        f"Refresh minutes: {REFRESH_MINUTES}\n"
        f"Backfill interval minutes: {BACKFILL_INTERVAL_MINUTES}"
    )
    await interaction.followup.send(msg, ephemeral=True)


@bot.tree.command(name="stats_progress", description="Affiche la progression du backfill.")
async def stats_progress(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    cursor, completed, last_attempt, last_error, last_sessions, last_games = await get_backfill_state()
    stats = await get_progress_stats()
    eta = compute_next_backfill_eta(last_attempt)
    msg = (
        f"Backfill cursor: {cursor}\n"
        f"Backfill done: {completed}\n"
        f"Last attempt: {last_attempt}\n"
        f"Last error: {last_error}\n"
        f"Derniere tranche sessions: {last_sessions}\n"
        f"Derniere tranche games: {last_games}\n"
        f"Prochaine tranche dans: {eta}\n"
        f"Games traitees: {stats['games_processed']}\n"
        f"Joueurs connus: {stats['players']}\n"
        f"Wins total: {stats['wins_total']}\n"
        f"Losses total: {stats['losses_total']}"
    )
    await interaction.followup.send(msg, ephemeral=True)


@bot.tree.command(name="reset_leaderboard", description="Réinitialise le leaderboard (Postgres).")
async def reset_leaderboard(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    async with pool.acquire() as conn:
        await conn.execute("TRUNCATE TABLE player_stats")
        await conn.execute("TRUNCATE TABLE processed_games")
        await conn.execute(
            """
            INSERT INTO backfill_state (id, cursor, completed, last_attempt, last_error, last_sessions, last_games_processed)
            VALUES (1, $1, FALSE, NULL, NULL, 0, 0)
            ON CONFLICT (id) DO UPDATE SET
                cursor = EXCLUDED.cursor,
                completed = FALSE,
                last_attempt = NULL,
                last_error = NULL,
                last_sessions = 0,
                last_games_processed = 0
            """,
            BACKFILL_START,
        )
    await interaction.followup.send(
        f"OK: leaderboard réinitialisé. Nouveau départ: {BACKFILL_START}",
        ephemeral=True,
    )


if __name__ == "__main__":
    if not TOKEN:
        raise ValueError("DISCORD_TOKEN missing.")
    if not DB_URL:
        raise ValueError("DATABASE_URL missing (Postgres).")
bot.run(TOKEN)

