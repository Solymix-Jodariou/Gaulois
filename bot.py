import os
import json
import asyncio
import re
from io import BytesIO
from typing import Optional
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
OPENFRONT_GAME_URL_TEMPLATE = os.getenv(
    "OPENFRONT_GAME_URL_TEMPLATE",
    "https://openfront.io/#/game/{game_id}",
)
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
WIN_NOTIFY_RANGE_HOURS = int(os.getenv("WIN_NOTIFY_RANGE_HOURS", "24"))
WIN_NOTIFY_EMPTY_COOLDOWN_MINUTES = int(os.getenv("WIN_NOTIFY_EMPTY_COOLDOWN_MINUTES", "60"))
OFM_ROLE_ID = int(os.getenv("OFM_ROLE_ID", "1469695783790968963"))
OFM_MANAGER_ROLE_ID = int(os.getenv("OFM_MANAGER_ROLE_ID", "1469701081759219723"))
OFM_TEAM_ROLE_ID = int(os.getenv("OFM_TEAM_ROLE_ID", "1469701766223368216"))
OFM_LEADER_ROLE_ID = int(os.getenv("OFM_LEADER_ROLE_ID", "0"))
OFM_SUB_ROLE_ID = int(os.getenv("OFM_SUB_ROLE_ID", "0"))
OFM_CATEGORY_ID = int(os.getenv("OFM_CATEGORY_ID", "1469703934514827531"))
OFM_BOARD_CHANNEL_ID = int(os.getenv("OFM_BOARD_CHANNEL_ID", "1469696688804466972"))
OFM_ADMIN_CHANNEL_ID = int(os.getenv("OFM_ADMIN_CHANNEL_ID", "1469711880565162201"))
ADMIN_PANEL_CHANNEL_ID = int(os.getenv("ADMIN_PANEL_CHANNEL_ID", "1469724972246237436"))
MOD_LOG_CHANNEL_ID = int(os.getenv("MOD_LOG_CHANNEL_ID", "1351168832261193791"))
FOUNDER_USER_ID = int(os.getenv("FOUNDER_USER_ID", "1350921590359195699"))
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "1351313848275042385"))
MOD_COMMANDS = [
    "warn",
    "warnlist",
    "clearwarn",
    "mute",
    "kick",
    "ban",
    "unban",
    "case",
    "note",
]

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
if WIN_NOTIFY_EMPTY_COOLDOWN_MINUTES < 1:
    WIN_NOTIFY_EMPTY_COOLDOWN_MINUTES = 1

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


def parse_openfront_time(value) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        ts = int(value)
        if ts > 1_000_000_000_000:
            return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
        if ts > 1_000_000_000:
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        return None
    if isinstance(value, str):
        raw = value.strip()
        if raw.isdigit():
            ts = int(raw)
            if ts > 1_000_000_000_000:
                return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
            if ts > 1_000_000_000:
                return datetime.fromtimestamp(ts, tz=timezone.utc)
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except Exception:
            return None
    return None


def parse_duration_seconds(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    raw = value.strip().lower()
    if raw.isdigit():
        return int(raw)
    match = re.fullmatch(r"(\d+)([smhd])", raw)
    if not match:
        return None
    amount = int(match.group(1))
    unit = match.group(2)
    if unit == "s":
        return amount
    if unit == "m":
        return amount * 60
    if unit == "h":
        return amount * 3600
    if unit == "d":
        return amount * 86400
    return None


def format_duration(seconds: Optional[int]) -> str:
    if not seconds:
        return "Permanent"
    if seconds % 86400 == 0:
        return f"{seconds // 86400}d"
    if seconds % 3600 == 0:
        return f"{seconds // 3600}h"
    if seconds % 60 == 0:
        return f"{seconds // 60}m"
    return f"{seconds}s"


def is_admin_user_id(user_id: int) -> bool:
    return user_id in {FOUNDER_USER_ID, ADMIN_USER_ID}


def is_admin_member(member: discord.Member) -> bool:
    if member.id == member.guild.owner_id:
        return True
    if is_admin_user_id(member.id):
        return True
    role_ids = {role.id for role in member.roles}
    return bool(role_ids.intersection({FOUNDER_USER_ID, ADMIN_USER_ID}))


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


def summarize_ffa_sessions(sessions):
    ffa_sessions = [s for s in sessions if is_ffa_session(s)]
    def session_key(s):
        dt = get_session_time(s)
        return dt or datetime.min.replace(tzinfo=timezone.utc)
    ffa_sessions.sort(key=session_key, reverse=True)
    wins, losses = compute_ffa_stats_from_sessions(ffa_sessions)
    games = wins + losses
    winrate = (wins / games * 100) if games else 0.0
    last10 = ffa_sessions[:10]
    last10_wins = sum(1 for s in last10 if s.get("hasWon"))
    streak = 0
    for s in ffa_sessions:
        if s.get("hasWon"):
            streak += 1
        else:
            break
    return {
        "wins": wins,
        "losses": losses,
        "games": games,
        "winrate": winrate,
        "last10_wins": last10_wins,
        "streak": streak,
    }


def is_ffa_session(session: dict) -> bool:
    mode = (session.get("gameMode") or session.get("mode") or "").lower()
    return "free for all" in mode or mode == "ffa"


def get_session_game_id(session: dict) -> Optional[str]:
    return session.get("gameId") or session.get("game") or session.get("id")


def get_session_time(session: dict) -> Optional[datetime]:
    for key in ("end", "endTime", "start", "startTime", "createdAt"):
        if key in session and session.get(key) is not None:
            return parse_openfront_time(session.get(key))
    return None


def build_api_headers():
    headers = {"User-Agent": USER_AGENT}
    if OPENFRONT_API_KEY:
        headers["X-API-Key"] = OPENFRONT_API_KEY
        headers["Authorization"] = f"Bearer {OPENFRONT_API_KEY}"
    return headers


def get_notify_channel_error(channel) -> Optional[str]:
    if channel is None:
        return "Salon introuvable."
    if not isinstance(channel, (discord.TextChannel, discord.Thread)):
        return "WIN_NOTIFY_CHANNEL_ID ne pointe pas vers un salon texte."
    guild = channel.guild
    if not guild:
        return "Salon sans guild associée."
    perms = channel.permissions_for(guild.me)
    required = []
    if not perms.view_channel:
        required.append("Voir le salon")
    if isinstance(channel, discord.Thread):
        if not perms.send_messages_in_threads:
            required.append("Envoyer des messages dans les fils")
    else:
        if not perms.send_messages:
            required.append("Envoyer des messages")
    if not perms.embed_links:
        required.append("Intégrer des liens")
    if required:
        return "Permissions manquantes: " + ", ".join(required)
    return None


def is_clan_username(username: str) -> bool:
    if not username:
        return False
    upper = username.upper()
    tag = CLAN_TAG.upper()
    return f"[{tag}]" in upper or upper.startswith(f"{tag} ")


def is_clan_player(player: dict) -> bool:
    tag = player.get("clanTag")
    if tag and str(tag).upper() == CLAN_TAG.upper():
        return True
    return is_clan_username(player.get("username") or "")


def game_mode(info):
    return (info.get("config", {}) or {}).get("gameMode") or ""


def get_winner_client_ids(info):
    winner = info.get("winner")
    if not winner or not isinstance(winner, list) or len(winner) < 3:
        return set()
    winners = winner[2]
    if isinstance(winners, list):
        return set(winners)
    # Format observed: ["team", "Purple", "id1", "id2", ...]
    tail = winner[2:]
    if all(isinstance(x, str) for x in tail):
        return set(tail)
    return set()


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
        if is_clan_player(p):
            names.append(username or CLAN_DISPLAY)
    return sorted(set(names))


def clan_won_game(info) -> bool:
    winners = get_winner_client_ids(info)
    if not winners:
        return False
    gal_clients = {
        p.get("clientID")
        for p in info.get("players", [])
        if is_clan_player(p)
    }
    return bool(winners & gal_clients)


def extract_clan_tag_from_player(player: dict) -> Optional[str]:
    tag = player.get("clanTag")
    if tag:
        return f"[{str(tag).upper()}]"
    username = player.get("username") or ""
    match = re.search(r"\[([A-Za-z0-9]+)\]", username)
    if match:
        return f"[{match.group(1).upper()}]"
    return None


def extract_winner_names(info):
    winners = get_winner_client_ids(info)
    names = []
    for p in info.get("players", []):
        if winners and p.get("clientID") not in winners:
            continue
        username = p.get("username") or p.get("name") or p.get("player") or ""
        if username:
            names.append(username)
    if not names:
        names = extract_gal_players(info)
    seen = set()
    ordered = []
    for name in names:
        if name not in seen:
            seen.add(name)
            ordered.append(name)
    return ordered


def extract_opponent_clans(info):
    winners = get_winner_client_ids(info)
    tags = []
    for p in info.get("players", []):
        if winners and p.get("clientID") in winners:
            continue
        tag = extract_clan_tag_from_player(p)
        if not tag or tag.upper() == CLAN_DISPLAY.upper():
            continue
        tags.append(tag.upper())
    return sorted(set(tags))


def build_win_embed(info):
    mode = game_mode(info) or "Team"
    start_raw = info.get("start")
    end_raw = info.get("end")
    game_id = info.get("gameID") or "?"
    winners = extract_winner_names(info)
    opponent_clans = extract_opponent_clans(info)

    game_url = None
    if game_id and game_id != "?":
        try:
            game_url = OPENFRONT_GAME_URL_TEMPLATE.format(game_id=game_id)
        except Exception:
            game_url = None

    embed = discord.Embed(
        title=f"🏆 OpenFront Game {game_id}",
        url=game_url,
        description=f"{CLAN_DISPLAY} vient de gagner une partie !",
        color=discord.Color.orange(),
    )

    winners_by_id = {
        p.get("clientID"): p for p in info.get("players", []) if p.get("clientID")
    }
    winner_ids = get_winner_client_ids(info)
    winner_players = [winners_by_id.get(cid) for cid in winner_ids if winners_by_id.get(cid)]

    if winner_players:
        name_width = 22

        def format_winner_row(player):
            username = player.get("username") or "Unknown"
            name = username if len(username) <= name_width else username[: name_width - 3] + "..."
            marker = "★" if is_clan_player(player) else " "
            return f"{marker} {name}"

        lines = [format_winner_row(p) for p in winner_players[:12]]
        more = len(winner_players) - len(lines)
        if more > 0:
            lines.append(f"... +{more}")
        embed.add_field(
            name="Gagnants (★ = [GAL])",
            value="```\n" + "\n".join(lines) + "\n```",
            inline=True,
        )
    elif winners:
        shown = winners[:12]
        more = len(winners) - len(shown)
        lines = [f"★ {name}" if is_clan_username(name) else f"  {name}" for name in shown]
        if more > 0:
            lines.append(f"... +{more}")
        embed.add_field(
            name="Gagnants (★ = [GAL])",
            value="```\n" + "\n".join(lines) + "\n```",
            inline=True,
        )
    else:
        embed.add_field(name="Gagnants", value=CLAN_DISPLAY, inline=True)

    if opponent_clans:
        clans_text = " ".join(opponent_clans)
        embed.add_field(name="Clans affront�s", value=clans_text, inline=True)
    else:
        embed.add_field(name="Clans affront�s", value="Aucun tag d�tect�", inline=True)

    embed.add_field(name="Mode", value=str(mode), inline=True)

    footer_time = None
    if end_raw:
        end_dt = parse_openfront_time(end_raw)
        if end_dt:
            footer_time = format_local_time(end_dt)
        else:
            footer_time = str(end_raw)
    elif start_raw:
        start_dt = parse_openfront_time(start_raw)
        if start_dt:
            footer_time = format_local_time(start_dt)
        else:
            footer_time = str(start_raw)
    if footer_time:
        embed.add_field(name="Heure victoire", value=footer_time, inline=True)
        embed.set_footer(text=f"Mis � jour le {footer_time}")

    return embed


def build_ffa_win_embed(pseudo: str, player_id: str, session: dict, game_id: str):
    mode = session.get("gameMode") or session.get("mode") or "FFA"
    end_raw = session.get("end") or session.get("endTime")
    start_raw = session.get("start") or session.get("startTime")

    game_url = None
    if game_id:
        try:
            game_url = OPENFRONT_GAME_URL_TEMPLATE.format(game_id=game_id)
        except Exception:
            game_url = None

    display_name = f"★ {pseudo}" if is_clan_username(pseudo) else pseudo
    embed = discord.Embed(
        title=f"🏆 Victoire FFA — {display_name}",
        url=game_url,
        description="Victoire FFA détectée via /register",
        color=discord.Color.orange(),
    )
    embed.add_field(name="Player ID", value=str(player_id), inline=True)
    embed.add_field(name="Mode", value=str(mode), inline=True)

    footer_time = None
    if end_raw:
        end_dt = parse_openfront_time(end_raw)
        if end_dt:
            footer_time = format_local_time(end_dt)
        else:
            footer_time = str(end_raw)
    elif start_raw:
        start_dt = parse_openfront_time(start_raw)
        if start_dt:
            footer_time = format_local_time(start_dt)
        else:
            footer_time = str(start_raw)
    if footer_time:
        embed.add_field(name="Heure victoire", value=footer_time, inline=True)
        embed.set_footer(text=f"Mis à jour le {footer_time}")
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
            CREATE TABLE IF NOT EXISTS ffa_win_notifications (
                player_id TEXT NOT NULL,
                game_id TEXT NOT NULL,
                notified_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (player_id, game_id)
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS win_notify_state (
                id INTEGER PRIMARY KEY,
                last_empty_at TEXT,
                last_scan_at TEXT,
                last_scan_sessions INTEGER DEFAULT 0,
                last_scan_wins INTEGER DEFAULT 0,
                last_scan_sent INTEGER DEFAULT 0,
                last_scan_skipped INTEGER DEFAULT 0,
                last_scan_missing_game_id INTEGER DEFAULT 0,
                last_scan_fetch_errors INTEGER DEFAULT 0,
                last_scan_error TEXT
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ofm_board_message (
                guild_id BIGINT PRIMARY KEY,
                channel_id BIGINT NOT NULL,
                message_id BIGINT NOT NULL
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ofm_participants (
                guild_id BIGINT NOT NULL,
                user_id BIGINT NOT NULL,
                status TEXT NOT NULL,
                team_role_id BIGINT,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (guild_id, user_id)
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ofm_admin_panel_message (
                guild_id BIGINT PRIMARY KEY,
                channel_id BIGINT NOT NULL,
                message_id BIGINT NOT NULL
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ofm_team_name (
                guild_id BIGINT PRIMARY KEY,
                name TEXT NOT NULL
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS mod_admin_panel_message (
                guild_id BIGINT PRIMARY KEY,
                channel_id BIGINT NOT NULL,
                message_id BIGINT NOT NULL
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS mod_warnings (
                id BIGSERIAL PRIMARY KEY,
                guild_id BIGINT NOT NULL,
                user_id BIGINT NOT NULL,
                moderator_id BIGINT NOT NULL,
                reason TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS mod_actions (
                id BIGSERIAL PRIMARY KEY,
                guild_id BIGINT NOT NULL,
                user_id BIGINT NOT NULL,
                moderator_id BIGINT NOT NULL,
                action_type TEXT NOT NULL,
                reason TEXT,
                duration_seconds INTEGER,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS mod_permissions (
                guild_id BIGINT NOT NULL,
                role_id BIGINT NOT NULL,
                command TEXT NOT NULL,
                allowed BOOLEAN NOT NULL DEFAULT TRUE,
                PRIMARY KEY (guild_id, role_id, command)
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS mod_config (
                guild_id BIGINT PRIMARY KEY,
                log_channel_id BIGINT,
                default_mute_seconds INTEGER DEFAULT 3600,
                default_ban_seconds INTEGER DEFAULT 0
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS mod_notes (
                id BIGSERIAL PRIMARY KEY,
                guild_id BIGINT NOT NULL,
                user_id BIGINT NOT NULL,
                moderator_id BIGINT NOT NULL,
                note TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        columns = await conn.fetch(
            "SELECT column_name FROM information_schema.columns WHERE table_name='win_notify_state'"
        )
        colset = {c["column_name"] for c in columns}
        if "last_scan_at" not in colset:
            await conn.execute("ALTER TABLE win_notify_state ADD COLUMN last_scan_at TEXT")
        if "last_scan_sessions" not in colset:
            await conn.execute("ALTER TABLE win_notify_state ADD COLUMN last_scan_sessions INTEGER DEFAULT 0")
        if "last_scan_wins" not in colset:
            await conn.execute("ALTER TABLE win_notify_state ADD COLUMN last_scan_wins INTEGER DEFAULT 0")
        if "last_scan_sent" not in colset:
            await conn.execute("ALTER TABLE win_notify_state ADD COLUMN last_scan_sent INTEGER DEFAULT 0")
        if "last_scan_skipped" not in colset:
            await conn.execute("ALTER TABLE win_notify_state ADD COLUMN last_scan_skipped INTEGER DEFAULT 0")
        if "last_scan_missing_game_id" not in colset:
            await conn.execute("ALTER TABLE win_notify_state ADD COLUMN last_scan_missing_game_id INTEGER DEFAULT 0")
        if "last_scan_fetch_errors" not in colset:
            await conn.execute("ALTER TABLE win_notify_state ADD COLUMN last_scan_fetch_errors INTEGER DEFAULT 0")
        if "last_scan_error" not in colset:
            await conn.execute("ALTER TABLE win_notify_state ADD COLUMN last_scan_error TEXT")
        await conn.execute(
            """
            INSERT INTO win_notify_state (id)
            VALUES (1)
            ON CONFLICT (id) DO NOTHING
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
        title=f"?? Leaderboard {CLAN_DISPLAY} � Page {page}/{total_pages}",
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

    name_width = 14
    mention_width = 22

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
            footer = f"Mis � jour le {format_local_time(last_dt)} | Prochaine maj {format_local_time(next_dt)}"
        except Exception:
            footer = f"Mis � jour le {last_updated}"
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


async def get_ffa_player(discord_id: int):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT discord_id, pseudo, player_id FROM ffa_players WHERE discord_id = $1",
            discord_id,
        )


async def delete_ffa_player(discord_id: int):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "DELETE FROM ffa_players WHERE discord_id = $1 RETURNING player_id",
            discord_id,
        )


async def delete_ffa_stats_by_player_id(player_id: str):
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM ffa_stats WHERE player_id = $1",
            player_id,
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


async def is_ffa_win_notified(player_id: str, game_id: str) -> bool:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT 1 FROM ffa_win_notifications WHERE player_id = $1 AND game_id = $2",
            player_id,
            game_id,
        )
    return row is not None


async def mark_ffa_win_notified(player_id: str, game_id: str):
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO ffa_win_notifications (player_id, game_id)
            VALUES ($1, $2)
            ON CONFLICT DO NOTHING
            """,
            player_id,
            game_id,
        )


async def get_last_empty_notify():
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT last_empty_at FROM win_notify_state WHERE id = 1"
        )
    return row[0] if row else None


async def set_last_empty_notify(value: str):
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO win_notify_state (id, last_empty_at)
            VALUES (1, $1)
            ON CONFLICT (id) DO UPDATE SET
                last_empty_at = EXCLUDED.last_empty_at
            """,
            value,
        )


async def set_last_win_notify_stats(
    scan_at: str,
    sessions: int,
    wins: int,
    sent: int,
    skipped: int,
    missing_game_id: int,
    fetch_errors: int,
    error: Optional[str] = None,
):
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO win_notify_state (
                id,
                last_scan_at,
                last_scan_sessions,
                last_scan_wins,
                last_scan_sent,
                last_scan_skipped,
                last_scan_missing_game_id,
                last_scan_fetch_errors,
                last_scan_error
            )
            VALUES (1, $1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (id) DO UPDATE SET
                last_scan_at = EXCLUDED.last_scan_at,
                last_scan_sessions = EXCLUDED.last_scan_sessions,
                last_scan_wins = EXCLUDED.last_scan_wins,
                last_scan_sent = EXCLUDED.last_scan_sent,
                last_scan_skipped = EXCLUDED.last_scan_skipped,
                last_scan_missing_game_id = EXCLUDED.last_scan_missing_game_id,
                last_scan_fetch_errors = EXCLUDED.last_scan_fetch_errors,
                last_scan_error = EXCLUDED.last_scan_error
            """,
            scan_at,
            sessions,
            wins,
            sent,
            skipped,
            missing_game_id,
            fetch_errors,
            error,
        )


async def get_last_win_notify_stats():
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                last_scan_at,
                last_scan_sessions,
                last_scan_wins,
                last_scan_sent,
                last_scan_skipped,
                last_scan_missing_game_id,
                last_scan_fetch_errors,
                last_scan_error
            FROM win_notify_state
            WHERE id = 1
            """
        )
    if not row:
        return None
    return {
        "last_scan_at": row[0],
        "sessions": row[1],
        "wins": row[2],
        "sent": row[3],
        "skipped": row[4],
        "missing_game_id": row[5],
        "fetch_errors": row[6],
        "error": row[7],
    }

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
            """
            SELECT s.pseudo, s.wins_ffa, s.losses_ffa, s.updated_at, p.discord_id
            FROM ffa_stats s
            LEFT JOIN ffa_players p ON p.player_id = s.player_id
            """
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
                "discord_id": row[4],
            }
        )
        if row[3] and (last_updated is None or row[3] > last_updated):
            last_updated = row[3]
    players = [p for p in players if p["games"] >= MIN_GAMES]
    players.sort(key=lambda p: (p["score"], p["games"]), reverse=True)
    return players[:100], last_updated


async def refresh_ffa_stats():
    players = await get_ffa_players()
    success = 0
    failed = 0
    for _discord_id, pseudo, player_id in players:
        try:
            sessions = await fetch_player_sessions(player_id)
            wins, losses = compute_ffa_stats_from_sessions(sessions)
            await upsert_ffa_stats(player_id, pseudo, wins, losses)
            success += 1
        except Exception:
            failed += 1
            continue
    return {"total": len(players), "success": success, "failed": failed}


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
        title=f"Leaderboard FFA {CLAN_DISPLAY} - Page {page}/{total_pages}",
        color=discord.Color.orange(),
    )

    total_wins = sum(p["wins"] for p in top)
    total_losses = sum(p["losses"] for p in top)
    embed.description = f"**Wins:** {total_wins}  |  **Losses:** {total_losses}"
    if guild and guild.icon:
        embed.set_thumbnail(url=guild.icon.url)

    name_width = 10
    discord_width = 12

    def truncate_name(name: str) -> str:
        if len(name) <= name_width:
            return name
        return name[: name_width - 3] + "..."

    header = f"{'#':<3} {'JOUEUR':<{name_width}} {'DISCORD':<{discord_width}} {'SCORE':>5} {'W/L':>7} {'G':>3}"
    sep = "-" * (name_width + discord_width + 28)
    table = [header, sep]
    for i, p in enumerate(page_items, start + 1):
        name = truncate_name(p["display_name"])
        if p.get("discord_id") and guild:
            member = guild.get_member(p["discord_id"])
            if not member:
                try:
                    member = await guild.fetch_member(p["discord_id"])
                except Exception:
                    member = None
            discord_name = member.display_name if member else "-"
        else:
            discord_name = "-"
        if discord_name != "-":
            discord_name = re.sub(r"\[{}\]\s*".format(re.escape(CLAN_TAG)), "", discord_name, flags=re.IGNORECASE)
            discord_name = discord_name.strip()
        if len(discord_name) > discord_width:
            discord_name = discord_name[: discord_width - 3] + "..."
        score = f"{p['score']:.1f}"
        wl = f"{p['wins']}/{p['losses']}"
        games = f"{p['games']}"
        table.append(
            f"{i:<3} {name:<{name_width}} {discord_name:<{discord_width}} {score:>5} {wl:>7} {games:>3}"
        )

    embed.add_field(name="Classement FFA", value="```\n" + "\n".join(table) + "\n```", inline=False)

    if last_updated:
        try:
            last_dt = datetime.strptime(last_updated, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            next_dt = last_dt + timedelta(minutes=REFRESH_MINUTES)
            footer = f"Mis � jour le {format_local_time(last_dt)} | Prochaine maj {format_local_time(next_dt)}"
        except Exception:
            footer = f"Mis � jour le {last_updated}"
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
        title=f"?? Leaderboard 1v1 OpenFront � Top 100 � Page {page}/{total_pages}",
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
            name = f"?{name}"
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
            footer = f"Mis � jour le {format_local_time(last_dt)} | Prochaine maj {format_local_time(next_dt)}"
        except Exception:
            footer = f"Mis � jour le {last_updated}"
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
        title=f"?? Leaderboard 1v1 {CLAN_DISPLAY} � Top 100",
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
        return f"?{name}"

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
            footer = f"Mis � jour le {format_local_time(last_dt)} | Prochaine maj {format_local_time(next_dt)}"
        except Exception:
            footer = f"Mis � jour le {last_updated}"
        embed.set_footer(text=footer)

    return embed


DEFAULT_OFM_TEAM_NAME = os.getenv("DEFAULT_OFM_TEAM_NAME", "[GAL] Les gaulois")


async def build_ofm_board_embed(guild: discord.Guild):
    rows = await get_ofm_participants(guild.id, status="accepted")
    team_name = await get_ofm_team_name(guild.id) or DEFAULT_OFM_TEAM_NAME
    if not rows:
        description = "Aucun participant accepté pour l'instant."
    else:
        sub_role = guild.get_role(OFM_SUB_ROLE_ID) if OFM_SUB_ROLE_ID else None
        lines = []
        for idx, row in enumerate(rows, start=1):
            user_id = row["user_id"]
            member = guild.get_member(user_id)
            suffix = ""
            if sub_role and member and sub_role in member.roles:
                suffix = " (Remplaçant)"
            lines.append(f"{idx}. <@{user_id}>{suffix}")
        description = "\n".join(lines)
    embed = discord.Embed(
        title=f"Participants OFM \u2014 {team_name}",
        description=description,
        color=discord.Color.orange(),
    )
    if guild and guild.icon:
        embed.set_thumbnail(url=guild.icon.url)
    return embed


async def update_ofm_board(guild: discord.Guild):
    channel = guild.get_channel(OFM_BOARD_CHANNEL_ID)
    if not isinstance(channel, discord.TextChannel):
        return
    record = await get_ofm_board_message(guild.id)
    embed = await build_ofm_board_embed(guild)
    if record:
        try:
            message = await channel.fetch_message(record["message_id"])
            await message.edit(embed=embed)
            return
        except Exception:
            await clear_ofm_board_message(guild.id)
    message = await channel.send(embed=embed)
    await set_ofm_board_message(guild.id, channel.id, message.id)


async def build_ofm_admin_panel_embed(guild: discord.Guild):
    team_name = await get_ofm_team_name(guild.id) or DEFAULT_OFM_TEAM_NAME
    embed = discord.Embed(
        title="✨ Panel OFM Manager",
        description=(
            f"**Equipe : {team_name}**\n"
            "Gérez l'équipe, les rôles et les statuts en un clic."
        ),
        color=discord.Color.from_rgb(88, 101, 242),
    )
    embed.add_field(
        name="👥 Gestion des membres",
        value=(
            "➕ **Ajouter**\n"
            "❌ **Retirer**\n"
            "🔝 **Promouvoir**\n"
            "⬇️ **Rétrograder**\n"
            "📋 **Voir la liste**"
        ),
        inline=True,
    )
    embed.add_field(
        name="🛡️ Gestion de l'équipe",
        value=(
            "✏️ **Nom d'équipe**\n"
            "👑 **Définir leader**\n"
            "🔄 **Définir remplaçant**"
        ),
        inline=True,
    )
    embed.set_footer(text="Accès réservé • OFM Managers")
    return embed


async def update_ofm_admin_panel(guild: discord.Guild):
    channel = guild.get_channel(OFM_ADMIN_CHANNEL_ID)
    if not isinstance(channel, discord.TextChannel):
        return
    record = await get_ofm_admin_panel_message(guild.id)
    embed = await build_ofm_admin_panel_embed(guild)
    if record:
        try:
            message = await channel.fetch_message(record["message_id"])
            await message.edit(embed=embed, view=OFMConfigView())
            return
        except Exception:
            await clear_ofm_admin_panel_message(guild.id)
    message = await channel.send(embed=embed, view=OFMConfigView())
    await set_ofm_admin_panel_message(guild.id, channel.id, message.id)


def build_mod_admin_panel_embed(
    guild: discord.Guild,
    selected_role: Optional[discord.Role] = None,
    allowed_commands: Optional[list] = None,
    mode: str = "permissions",
):
    role_text = selected_role.mention if selected_role else "Aucun rôle sélectionné"
    allowed_text = ", ".join(allowed_commands) if allowed_commands else "Aucune autorisation"
    role_line = f"Rôle sélectionné : {role_text}\n" if mode == "permissions" else ""
    embed = discord.Embed(
        title="🛡️ Panel Administration",
        description=(
            "Configure les permissions et exécute les actions de modération.\n"
            f"{role_line}"
            f"Autorisations : {allowed_text}\n"
            f"Mode : **{mode}**"
        ),
        color=discord.Color.red(),
    )
    embed.add_field(
        name="Gestion des sanctions",
        value=(
            "warn / warnlist / clearwarn\n"
            "mute / kick / ban / unban\n"
            "case"
        ),
        inline=False,
    )
    embed.add_field(
        name="Casier & Logs",
        value="Historique complet et logs centralisés.",
        inline=False,
    )
    embed.set_footer(text="Accès réservé fondateur/admin")
    return embed


async def update_mod_admin_panel(
    guild: discord.Guild,
    selected_role_id: Optional[int] = None,
    mode: str = "permissions",
):
    channel = guild.get_channel(ADMIN_PANEL_CHANNEL_ID)
    if not isinstance(channel, discord.TextChannel):
        return
    record = await get_mod_admin_panel_message(guild.id)
    selected_role = guild.get_role(selected_role_id) if selected_role_id else None
    allowed_commands = None
    if selected_role:
        if selected_role.id in {FOUNDER_USER_ID, ADMIN_USER_ID}:
            allowed_commands = list(MOD_COMMANDS)
        else:
            perms = await get_permissions_for_role(guild.id, selected_role.id)
            allowed_commands = [p["command"] for p in perms if p["allowed"]]
    embed = build_mod_admin_panel_embed(guild, selected_role, allowed_commands, mode)
    view = ModAdminPanelView(guild_id=guild.id, selected_role_id=selected_role_id, mode=mode)
    if record:
        try:
            message = await channel.fetch_message(record["message_id"])
            await message.edit(embed=embed, view=view)
            return
        except Exception:
            await clear_mod_admin_panel_message(guild.id)
    message = await channel.send(embed=embed, view=view)
    await set_mod_admin_panel_message(guild.id, channel.id, message.id)


async def send_mod_log(guild: discord.Guild, embed: discord.Embed):
    config = await get_mod_config(guild.id)
    channel_id = config["log_channel_id"] if config and config["log_channel_id"] else MOD_LOG_CHANNEL_ID
    channel = guild.get_channel(channel_id)
    if isinstance(channel, discord.TextChannel):
        await channel.send(embed=embed)


def build_mod_log_embed(
    action: str,
    target: discord.abc.User,
    moderator: discord.abc.User,
    reason: Optional[str] = None,
    duration_seconds: Optional[int] = None,
):
    embed = discord.Embed(
        title="🧾 Log de modération",
        color=discord.Color.dark_red(),
    )
    embed.add_field(name="Action", value=action, inline=True)
    embed.add_field(name="Membre", value=f"{target.mention} (`{target.id}`)", inline=False)
    embed.add_field(name="Staff", value=f"{moderator.mention} (`{moderator.id}`)", inline=False)
    if reason:
        embed.add_field(name="Raison", value=reason, inline=False)
    if duration_seconds is not None:
        embed.add_field(name="Durée", value=format_duration(duration_seconds), inline=True)
    embed.timestamp = datetime.now(timezone.utc)
    return embed


def build_compare_chart(player_a: dict, player_b: dict, clan_tag: str) -> BytesIO:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    bg = "#0B0F1A"
    grid = "#1E2638"
    color_a = "#19E6FF"
    color_b = "#FF2ED1"

    stats_a = player_a["stats"]
    stats_b = player_b["stats"]

    fig = plt.figure(figsize=(12, 7), facecolor=bg)
    fig.subplots_adjust(left=0.06, right=0.97, top=0.9, bottom=0.08)

    ax_bar = fig.add_subplot(1, 2, 1)
    ax_radar = fig.add_subplot(1, 2, 2, polar=True)

    fig.text(0.02, 0.94, f"[{clan_tag}]", color=color_a, fontsize=14, fontweight="bold")
    fig.text(0.5, 0.94, "DUEL COMPARATIF", ha="center", color="white", fontsize=16, fontweight="bold")
    fig.text(
        0.5,
        0.90,
        f"{player_a['name']}  vs  {player_b['name']}",
        ha="center",
        color="#B5C3D6",
        fontsize=11,
    )

    metrics = ["Wins", "Losses", "Games", "Winrate%", "Streak"]
    values_a = [stats_a["wins"], stats_a["losses"], stats_a["games"], stats_a["winrate"], stats_a["streak"]]
    values_b = [stats_b["wins"], stats_b["losses"], stats_b["games"], stats_b["winrate"], stats_b["streak"]]

    y = range(len(metrics))
    ax_bar.barh([i + 0.15 for i in y], values_a, height=0.28, color=color_a, alpha=0.85, label=player_a["name"])
    ax_bar.barh([i - 0.15 for i in y], values_b, height=0.28, color=color_b, alpha=0.85, label=player_b["name"])
    ax_bar.set_yticks(list(y))
    ax_bar.set_yticklabels(metrics, color="white")
    ax_bar.tick_params(axis="x", colors="#9FB3C8")
    ax_bar.set_facecolor(bg)
    ax_bar.grid(axis="x", color=grid, alpha=0.4)
    ax_bar.spines["top"].set_visible(False)
    ax_bar.spines["right"].set_visible(False)
    ax_bar.spines["left"].set_color(grid)
    ax_bar.spines["bottom"].set_color(grid)
    ax_bar.legend(loc="lower right", frameon=False, fontsize=9, labelcolor="white")

    radar_labels = ["Wins", "Winrate", "Games", "Streak", "Losses"]
    loss_max = max(stats_a["losses"], stats_b["losses"], 1)
    radar_a = [
        stats_a["wins"],
        stats_a["winrate"],
        stats_a["games"],
        stats_a["streak"],
        loss_max - stats_a["losses"],
    ]
    radar_b = [
        stats_b["wins"],
        stats_b["winrate"],
        stats_b["games"],
        stats_b["streak"],
        loss_max - stats_b["losses"],
    ]
    max_vals = [max(radar_a[i], radar_b[i], 1) for i in range(len(radar_labels))]
    norm_a = [radar_a[i] / max_vals[i] for i in range(len(radar_labels))]
    norm_b = [radar_b[i] / max_vals[i] for i in range(len(radar_labels))]
    angles = [n / float(len(radar_labels)) * 2 * 3.14159 for n in range(len(radar_labels))]
    angles += angles[:1]
    norm_a += norm_a[:1]
    norm_b += norm_b[:1]

    ax_radar.set_facecolor(bg)
    ax_radar.set_theta_offset(3.14159 / 2)
    ax_radar.set_theta_direction(-1)
    ax_radar.set_xticks(angles[:-1])
    ax_radar.set_xticklabels(radar_labels, color="white", fontsize=9)
    ax_radar.set_yticklabels([])
    ax_radar.grid(color=grid, alpha=0.4)
    ax_radar.plot(angles, norm_a, color=color_a, linewidth=2)
    ax_radar.fill(angles, norm_a, color=color_a, alpha=0.25)
    ax_radar.plot(angles, norm_b, color=color_b, linewidth=2)
    ax_radar.fill(angles, norm_b, color=color_b, alpha=0.25)

    buffer = BytesIO()
    fig.savefig(buffer, format="png", dpi=160, facecolor=bg)
    plt.close(fig)
    buffer.seek(0)
    return buffer


async def ensure_mod_permission(interaction: discord.Interaction, command: str) -> bool:
    if not interaction.guild:
        await interaction.response.send_message("Commande disponible uniquement sur un serveur.", ephemeral=True)
        return False
    member = interaction.user
    if not await has_mod_permission(interaction.guild, member, command):
        await interaction.response.send_message("Accès refusé.", ephemeral=True)
        return False
    return True


async def schedule_unban(guild: discord.Guild, user_id: int, delay_seconds: int):
    await asyncio.sleep(delay_seconds)
    try:
        await guild.unban(discord.Object(id=user_id), reason="Fin de ban temporaire")
    except Exception:
        return


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


async def get_ofm_board_message(guild_id: int):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT guild_id, channel_id, message_id FROM ofm_board_message WHERE guild_id = $1",
            guild_id,
        )


async def set_ofm_board_message(guild_id: int, channel_id: int, message_id: int):
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO ofm_board_message (guild_id, channel_id, message_id)
            VALUES ($1, $2, $3)
            ON CONFLICT (guild_id) DO UPDATE SET
                channel_id = EXCLUDED.channel_id,
                message_id = EXCLUDED.message_id
            """,
            guild_id,
            channel_id,
            message_id,
        )


async def clear_ofm_board_message(guild_id: int):
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM ofm_board_message WHERE guild_id = $1",
            guild_id,
        )


async def upsert_ofm_participant(
    guild_id: int,
    user_id: int,
    status: str,
    team_role_id: Optional[int] = None,
):
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO ofm_participants (guild_id, user_id, status, team_role_id, updated_at)
            VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)
            ON CONFLICT (guild_id, user_id) DO UPDATE SET
                status = EXCLUDED.status,
                team_role_id = EXCLUDED.team_role_id,
                updated_at = CURRENT_TIMESTAMP
            """,
            guild_id,
            user_id,
            status,
            team_role_id,
        )


async def get_ofm_participants(guild_id: int, status: Optional[str] = None):
    async with pool.acquire() as conn:
        if status:
            return await conn.fetch(
                "SELECT user_id, status, team_role_id FROM ofm_participants WHERE guild_id = $1 AND status = $2",
                guild_id,
                status,
            )
        return await conn.fetch(
            "SELECT user_id, status, team_role_id FROM ofm_participants WHERE guild_id = $1",
            guild_id,
        )


async def get_ofm_participant(guild_id: int, user_id: int):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT user_id, status, team_role_id FROM ofm_participants WHERE guild_id = $1 AND user_id = $2",
            guild_id,
            user_id,
        )


async def get_ofm_admin_panel_message(guild_id: int):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT guild_id, channel_id, message_id FROM ofm_admin_panel_message WHERE guild_id = $1",
            guild_id,
        )


async def set_ofm_admin_panel_message(guild_id: int, channel_id: int, message_id: int):
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO ofm_admin_panel_message (guild_id, channel_id, message_id)
            VALUES ($1, $2, $3)
            ON CONFLICT (guild_id) DO UPDATE SET
                channel_id = EXCLUDED.channel_id,
                message_id = EXCLUDED.message_id
            """,
            guild_id,
            channel_id,
            message_id,
        )


async def clear_ofm_admin_panel_message(guild_id: int):
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM ofm_admin_panel_message WHERE guild_id = $1",
            guild_id,
        )


async def get_ofm_team_name(guild_id: int) -> Optional[str]:
    async with pool.acquire() as conn:
        record = await conn.fetchrow(
            "SELECT name FROM ofm_team_name WHERE guild_id = $1",
            guild_id,
        )
        return record["name"] if record else None


async def set_ofm_team_name(guild_id: int, name: str):
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO ofm_team_name (guild_id, name)
            VALUES ($1, $2)
            ON CONFLICT (guild_id) DO UPDATE SET
                name = EXCLUDED.name
            """,
            guild_id,
            name,
        )


async def get_mod_admin_panel_message(guild_id: int):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT guild_id, channel_id, message_id FROM mod_admin_panel_message WHERE guild_id = $1",
            guild_id,
        )


async def set_mod_admin_panel_message(guild_id: int, channel_id: int, message_id: int):
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO mod_admin_panel_message (guild_id, channel_id, message_id)
            VALUES ($1, $2, $3)
            ON CONFLICT (guild_id) DO UPDATE SET
                channel_id = EXCLUDED.channel_id,
                message_id = EXCLUDED.message_id
            """,
            guild_id,
            channel_id,
            message_id,
        )


async def clear_mod_admin_panel_message(guild_id: int):
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM mod_admin_panel_message WHERE guild_id = $1",
            guild_id,
        )


async def add_warning(guild_id: int, user_id: int, moderator_id: int, reason: str):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            """
            INSERT INTO mod_warnings (guild_id, user_id, moderator_id, reason)
            VALUES ($1, $2, $3, $4)
            RETURNING id, created_at
            """,
            guild_id,
            user_id,
            moderator_id,
            reason,
        )


async def list_warnings(guild_id: int, user_id: int):
    async with pool.acquire() as conn:
        return await conn.fetch(
            """
            SELECT id, moderator_id, reason, created_at
            FROM mod_warnings
            WHERE guild_id = $1 AND user_id = $2
            ORDER BY id DESC
            """,
            guild_id,
            user_id,
        )


async def delete_warning(guild_id: int, user_id: int, warn_id: int):
    async with pool.acquire() as conn:
        return await conn.execute(
            "DELETE FROM mod_warnings WHERE guild_id = $1 AND user_id = $2 AND id = $3",
            guild_id,
            user_id,
            warn_id,
        )


async def clear_all_warnings(guild_id: int, user_id: int):
    async with pool.acquire() as conn:
        return await conn.execute(
            "DELETE FROM mod_warnings WHERE guild_id = $1 AND user_id = $2",
            guild_id,
            user_id,
        )


async def add_mod_action(
    guild_id: int,
    user_id: int,
    moderator_id: int,
    action_type: str,
    reason: Optional[str] = None,
    duration_seconds: Optional[int] = None,
):
    async with pool.acquire() as conn:
        return await conn.execute(
            """
            INSERT INTO mod_actions (guild_id, user_id, moderator_id, action_type, reason, duration_seconds)
            VALUES ($1, $2, $3, $4, $5, $6)
            """,
            guild_id,
            user_id,
            moderator_id,
            action_type,
            reason,
            duration_seconds,
        )


async def list_mod_actions(guild_id: int, user_id: int):
    async with pool.acquire() as conn:
        return await conn.fetch(
            """
            SELECT action_type, reason, duration_seconds, created_at, moderator_id
            FROM mod_actions
            WHERE guild_id = $1 AND user_id = $2
            ORDER BY id DESC
            """,
            guild_id,
            user_id,
        )


async def set_mod_permission(guild_id: int, role_id: int, command: str, allowed: bool):
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO mod_permissions (guild_id, role_id, command, allowed)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (guild_id, role_id, command) DO UPDATE SET
                allowed = EXCLUDED.allowed
            """,
            guild_id,
            role_id,
            command,
            allowed,
        )


async def get_permissions_for_role(guild_id: int, role_id: int):
    async with pool.acquire() as conn:
        return await conn.fetch(
            """
            SELECT command, allowed
            FROM mod_permissions
            WHERE guild_id = $1 AND role_id = $2
            """,
            guild_id,
            role_id,
        )


async def get_allowed_roles_for_command(guild_id: int, command: str):
    async with pool.acquire() as conn:
        return await conn.fetch(
            """
            SELECT role_id
            FROM mod_permissions
            WHERE guild_id = $1 AND command = $2 AND allowed = TRUE
            """,
            guild_id,
            command,
        )


async def get_mod_config(guild_id: int):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            """
            SELECT log_channel_id, default_mute_seconds, default_ban_seconds
            FROM mod_config
            WHERE guild_id = $1
            """,
            guild_id,
        )


async def set_mod_config(
    guild_id: int,
    log_channel_id: Optional[int] = None,
    default_mute_seconds: Optional[int] = None,
    default_ban_seconds: Optional[int] = None,
):
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO mod_config (guild_id, log_channel_id, default_mute_seconds, default_ban_seconds)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (guild_id) DO UPDATE SET
                log_channel_id = COALESCE(EXCLUDED.log_channel_id, mod_config.log_channel_id),
                default_mute_seconds = COALESCE(EXCLUDED.default_mute_seconds, mod_config.default_mute_seconds),
                default_ban_seconds = COALESCE(EXCLUDED.default_ban_seconds, mod_config.default_ban_seconds)
            """,
            guild_id,
            log_channel_id,
            default_mute_seconds,
            default_ban_seconds,
        )


async def add_mod_note(guild_id: int, user_id: int, moderator_id: int, note: str):
    async with pool.acquire() as conn:
        return await conn.execute(
            """
            INSERT INTO mod_notes (guild_id, user_id, moderator_id, note)
            VALUES ($1, $2, $3, $4)
            """,
            guild_id,
            user_id,
            moderator_id,
            note,
        )


async def list_mod_notes(guild_id: int, user_id: int):
    async with pool.acquire() as conn:
        return await conn.fetch(
            """
            SELECT note, moderator_id, created_at
            FROM mod_notes
            WHERE guild_id = $1 AND user_id = $2
            ORDER BY id DESC
            """,
            guild_id,
            user_id,
        )


async def has_mod_permission(guild: discord.Guild, member: discord.Member, command: str) -> bool:
    if is_admin_member(member):
        return True
    allowed_roles = await get_allowed_roles_for_command(guild.id, command)
    if not allowed_roles:
        return False
    allowed_set = {r["role_id"] for r in allowed_roles}
    return any(role.id in allowed_set for role in member.roles)


def can_moderate_member(actor: discord.Member, target: discord.Member, bot_member: discord.Member) -> Optional[str]:
    if actor.id == target.id:
        return "Action impossible sur soi-même."
    if target == bot_member:
        return "Action impossible sur le bot."
    if target.id == actor.guild.owner_id or target.top_role >= actor.top_role:
        if not is_admin_member(actor) and actor.id != actor.guild.owner_id:
            return "Impossible de sanctionner un supérieur."
    if target.top_role >= bot_member.top_role:
        return "Le bot ne peut pas sanctionner ce membre (hiérarchie)."
    return None


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

    @discord.ui.button(label="?", style=discord.ButtonStyle.secondary, custom_id="lb_prev")
    async def prev(self, interaction: discord.Interaction, _button: discord.ui.Button):
        await self.update(interaction, max(1, self.page - 1))

    @discord.ui.button(label="?", style=discord.ButtonStyle.secondary, custom_id="lb_next")
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

    @discord.ui.button(label="?", style=discord.ButtonStyle.secondary, custom_id="ffa_prev")
    async def prev(self, interaction: discord.Interaction, _button: discord.ui.Button):
        await self.update(interaction, max(1, self.page - 1))

    @discord.ui.button(label="?", style=discord.ButtonStyle.secondary, custom_id="ffa_next")
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

    @discord.ui.button(label="?", style=discord.ButtonStyle.secondary, custom_id="1v1_prev")
    async def prev(self, interaction: discord.Interaction, _button: discord.ui.Button):
        await self.update(interaction, max(1, self.page - 1))

    @discord.ui.button(label="?", style=discord.ButtonStyle.secondary, custom_id="1v1_next")
    async def next(self, interaction: discord.Interaction, _button: discord.ui.Button):
        top, _ = await load_1v1_leaderboard()
        total_pages = get_total_pages(len(top), self.page_size)
        await self.update(interaction, min(total_pages, self.page + 1))


class OFMConfirmView(discord.ui.View):
    def __init__(self, user_id: int):
        super().__init__(timeout=180)
        self.user_id = user_id

    async def _ensure_user(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message(
                "Ce bouton ne t'est pas destin\u00e9.",
                ephemeral=True,
            )
            return False
        return True

    @staticmethod
    def _slugify_channel_name(raw_name: str, fallback: str) -> str:
        name = raw_name.strip().lower()
        name = re.sub(r"[^a-z0-9]+", "-", name)
        name = name.strip("-")
        if not name:
            return fallback
        return name[:60].strip("-") or fallback

    @discord.ui.button(label="Confirmer", style=discord.ButtonStyle.success, custom_id="ofm_confirm")
    async def confirm(self, interaction: discord.Interaction, _button: discord.ui.Button):
        if not await self._ensure_user(interaction):
            return
        if not interaction.guild:
            await interaction.response.send_message("Commande disponible uniquement sur un serveur.", ephemeral=True)
            return
        role = interaction.guild.get_role(OFM_ROLE_ID)
        if not role:
            await interaction.response.send_message("R\u00f4le OFM introuvable.", ephemeral=True)
            return
        member = interaction.guild.get_member(interaction.user.id)
        if not member:
            member = await interaction.guild.fetch_member(interaction.user.id)
        if role in member.roles:
            await interaction.response.send_message("Tu as d\u00e9j\u00e0 le r\u00f4le OFM.", ephemeral=True)
            return
        bot_member = interaction.guild.me
        if not bot_member or not bot_member.guild_permissions.manage_roles:
            await interaction.response.send_message("Je n'ai pas la permission de g\u00e9rer les r\u00f4les.", ephemeral=True)
            return
        if bot_member.top_role <= role:
            await interaction.response.send_message("Je ne peux pas attribuer ce r\u00f4le (hi\u00e9rarchie).", ephemeral=True)
            return
        await member.add_roles(role, reason="Inscription OFM")
        channel = None
        channel_created = False
        manager_role = interaction.guild.get_role(OFM_MANAGER_ROLE_ID)
        category = interaction.guild.get_channel(OFM_CATEGORY_ID)
        if not isinstance(category, discord.CategoryChannel):
            await interaction.response.send_message(
                "\u2705 Inscription valid\u00e9e. R\u00f4le OFM attribu\u00e9.\n"
                "\u26a0\ufe0f Cat\u00e9gorie OFM introuvable pour cr\u00e9er le salon priv\u00e9.",
                ephemeral=True,
            )
            return
        if not manager_role:
            await interaction.response.send_message(
                "\u2705 Inscription valid\u00e9e. R\u00f4le OFM attribu\u00e9.\n"
                "\u26a0\ufe0f R\u00f4le OFM manager introuvable pour cr\u00e9er le salon priv\u00e9.",
                ephemeral=True,
            )
            return
        if not bot_member.guild_permissions.manage_channels:
            await interaction.response.send_message(
                "\u2705 Inscription valid\u00e9e. R\u00f4le OFM attribu\u00e9.\n"
                "\u26a0\ufe0f Je n'ai pas la permission de g\u00e9rer les salons.",
                ephemeral=True,
            )
            return
        topic = f"OFM candidature: {interaction.user.id}"
        for existing in interaction.guild.text_channels:
            if existing.topic == topic:
                channel = existing
                break
        if not channel:
            overwrites = {
                interaction.guild.default_role: discord.PermissionOverwrite(view_channel=False),
                manager_role: discord.PermissionOverwrite(
                    view_channel=True,
                    send_messages=True,
                    read_message_history=True,
                ),
                member: discord.PermissionOverwrite(
                    view_channel=True,
                    send_messages=True,
                    read_message_history=True,
                ),
                bot_member: discord.PermissionOverwrite(
                    view_channel=True,
                    send_messages=True,
                    read_message_history=True,
                    manage_channels=True,
                ),
            }
            pretty = self._slugify_channel_name(member.display_name, f"ofm-{interaction.user.id}")
            channel_name = f"ofm-{pretty}" if pretty != f"ofm-{interaction.user.id}" else pretty
            channel = await interaction.guild.create_text_channel(
                channel_name,
                category=category,
                overwrites=overwrites,
                topic=topic,
                reason="Cr\u00e9ation salon candidature OFM",
            )
            channel_created = True
        team_role = interaction.guild.get_role(OFM_TEAM_ROLE_ID)
        await upsert_ofm_participant(
            interaction.guild.id,
            member.id,
            "pending",
            team_role.id if team_role else None,
        )
        await update_ofm_board(interaction.guild)
        if channel_created:
            await channel.send(
                f"{manager_role.mention} Nouvelle candidature OFM pour {member.mention}.",
                view=OFMReviewView(),
            )
        channel_line = f"Salon priv\u00e9: {channel.mention}" if channel else ""
        await interaction.response.send_message(
            "\u2705 Inscription valid\u00e9e. R\u00f4le OFM attribu\u00e9."
            + (f"\n{channel_line}" if channel_line else ""),
            ephemeral=True,
        )

    @discord.ui.button(label="Annuler", style=discord.ButtonStyle.secondary, custom_id="ofm_cancel")
    async def cancel(self, interaction: discord.Interaction, _button: discord.ui.Button):
        if not await self._ensure_user(interaction):
            return
        await interaction.response.send_message("Inscription annul\u00e9e.", ephemeral=True)


class OFMInscriptionView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="S'inscrire", style=discord.ButtonStyle.primary, custom_id="ofm_join")
    async def join(self, interaction: discord.Interaction, _button: discord.ui.Button):
        if not interaction.guild:
            await interaction.response.send_message("Commande disponible uniquement sur un serveur.", ephemeral=True)
            return
        embed = discord.Embed(
            title="Confirmation OFM",
            description=(
                "Es-tu s\u00fbr de vouloir \u00eatre l'un des participants ?\n"
                "Cette action est irr\u00e9m\u00e9diable."
            ),
            color=discord.Color.orange(),
        )
        await interaction.response.send_message(
            embed=embed,
            view=OFMConfirmView(interaction.user.id),
            ephemeral=True,
        )


class OFMReviewView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    async def _ensure_manager(self, interaction: discord.Interaction) -> bool:
        if not interaction.guild:
            await interaction.response.send_message(
                "Commande disponible uniquement sur un serveur.",
                ephemeral=True,
            )
            return False
        manager_role = interaction.guild.get_role(OFM_MANAGER_ROLE_ID)
        if not manager_role or manager_role not in interaction.user.roles:
            await interaction.response.send_message(
                "Acc\u00e8s r\u00e9serv\u00e9 aux OFM managers.",
                ephemeral=True,
            )
            return False
        return True

    def _extract_candidate_id(self, interaction: discord.Interaction) -> Optional[int]:
        channel = interaction.channel
        if not isinstance(channel, discord.TextChannel) or not channel.topic:
            return None
        prefix = "OFM candidature: "
        if not channel.topic.startswith(prefix):
            return None
        raw = channel.topic[len(prefix) :].strip()
        if not raw.isdigit():
            return None
        return int(raw)

    async def _get_candidate_member(self, interaction: discord.Interaction) -> Optional[discord.Member]:
        candidate_id = self._extract_candidate_id(interaction)
        if not candidate_id or not interaction.guild:
            await interaction.response.send_message(
                "Impossible de trouver le candidat (topic du salon).",
                ephemeral=True,
            )
            return None
        member = interaction.guild.get_member(candidate_id)
        if not member:
            try:
                member = await interaction.guild.fetch_member(candidate_id)
            except Exception:
                member = None
        if not member:
            await interaction.response.send_message(
                "Candidat introuvable sur ce serveur.",
                ephemeral=True,
            )
            return None
        return member

    @discord.ui.button(label="Accepter", style=discord.ButtonStyle.success, custom_id="ofm_review_accept")
    async def accept(self, interaction: discord.Interaction, _button: discord.ui.Button):
        if not await self._ensure_manager(interaction):
            return
        member = await self._get_candidate_member(interaction)
        if not member:
            return
        existing = await get_ofm_participant(interaction.guild.id, member.id)
        if existing and existing["status"] in ("accepted", "refused"):
            await interaction.response.send_message(
                "Cette candidature a déjà été traitée.",
                ephemeral=True,
            )
            return
        role = interaction.guild.get_role(OFM_ROLE_ID)
        if role and role not in member.roles:
            await member.add_roles(role, reason="Candidature OFM acceptée")
        team_role = interaction.guild.get_role(OFM_TEAM_ROLE_ID)
        manager_role = interaction.guild.get_role(OFM_MANAGER_ROLE_ID)
        if team_role and team_role not in member.roles:
            if not manager_role or team_role.id != manager_role.id:
                await member.add_roles(team_role, reason="Équipe OFM attribuée")
        await upsert_ofm_participant(
            interaction.guild.id,
            member.id,
            "accepted",
            team_role.id if team_role else None,
        )
        await update_ofm_board(interaction.guild)
        embed = discord.Embed(
            title="✅ Candidature OFM",
            description=f"Statut : **Acceptée**\nCandidat : {member.mention}",
            color=discord.Color.green(),
        )
        await interaction.response.send_message(embed=embed)
        if interaction.channel:
            await interaction.channel.delete(reason="Candidature OFM acceptée")

    @discord.ui.button(label="Refuser", style=discord.ButtonStyle.danger, custom_id="ofm_review_refuse")
    async def refuse(self, interaction: discord.Interaction, _button: discord.ui.Button):
        if not await self._ensure_manager(interaction):
            return
        member = await self._get_candidate_member(interaction)
        if not member:
            return
        existing = await get_ofm_participant(interaction.guild.id, member.id)
        if existing and existing["status"] in ("accepted", "refused"):
            await interaction.response.send_message(
                "Cette candidature a déjà été traitée.",
                ephemeral=True,
            )
            return
        role = interaction.guild.get_role(OFM_ROLE_ID)
        if role and role in member.roles:
            await member.remove_roles(role, reason="Candidature OFM refusée")
        team_role = interaction.guild.get_role(OFM_TEAM_ROLE_ID)
        manager_role = interaction.guild.get_role(OFM_MANAGER_ROLE_ID)
        if team_role and team_role in member.roles:
            if not manager_role or team_role.id != manager_role.id:
                await member.remove_roles(team_role, reason="Candidature OFM refusée")
        await upsert_ofm_participant(
            interaction.guild.id,
            member.id,
            "refused",
            team_role.id if team_role else None,
        )
        await update_ofm_board(interaction.guild)
        embed = discord.Embed(
            title="❌ Candidature OFM",
            description=f"Statut : **Refusée**\nCandidat : {member.mention}",
            color=discord.Color.red(),
        )
        await interaction.response.send_message(embed=embed)
        if interaction.channel:
            await interaction.channel.delete(reason="Candidature OFM refusée")

    @discord.ui.button(label="En attente", style=discord.ButtonStyle.secondary, custom_id="ofm_review_pending")
    async def pending(self, interaction: discord.Interaction, _button: discord.ui.Button):
        if not await self._ensure_manager(interaction):
            return
        member = await self._get_candidate_member(interaction)
        if not member:
            return
        team_role = interaction.guild.get_role(OFM_TEAM_ROLE_ID)
        await upsert_ofm_participant(
            interaction.guild.id,
            member.id,
            "pending",
            team_role.id if team_role else None,
        )
        await update_ofm_board(interaction.guild)
        embed = discord.Embed(
            title="⏳ Candidature OFM",
            description=f"Statut : **En attente d'examen**\nCandidat : {member.mention}",
            color=discord.Color.orange(),
        )
        await interaction.response.send_message(embed=embed)


class OFMMemberIdModal(discord.ui.Modal):
    def __init__(self, title: str, action_key: str):
        super().__init__(title=title)
        self.action_key = action_key
        self.user_id = discord.ui.TextInput(
            label="ID Discord",
            placeholder="Ex: 272094371711680512",
            required=True,
            max_length=32,
        )
        self.add_item(self.user_id)

    async def on_submit(self, interaction: discord.Interaction):
        if not interaction.guild:
            await interaction.response.send_message("Commande disponible uniquement sur un serveur.", ephemeral=True)
            return
        raw = str(self.user_id.value).strip()
        if not raw.isdigit():
            await interaction.response.send_message("ID invalide.", ephemeral=True)
            return
        member_id = int(raw)
        try:
            member = interaction.guild.get_member(member_id) or await interaction.guild.fetch_member(member_id)
        except Exception:
            member = None
        if not member:
            await interaction.response.send_message("Membre introuvable.", ephemeral=True)
            return
        manager_role = interaction.guild.get_role(OFM_MANAGER_ROLE_ID)
        if not manager_role or manager_role not in interaction.user.roles:
            await interaction.response.send_message("Accès réservé aux OFM managers.", ephemeral=True)
            return
        role = interaction.guild.get_role(OFM_ROLE_ID)
        team_role = interaction.guild.get_role(OFM_TEAM_ROLE_ID)
        leader_role = interaction.guild.get_role(OFM_LEADER_ROLE_ID) if OFM_LEADER_ROLE_ID else None
        sub_role = interaction.guild.get_role(OFM_SUB_ROLE_ID) if OFM_SUB_ROLE_ID else None

        if self.action_key == "add_member":
            if role and role not in member.roles:
                await member.add_roles(role, reason="OFM: ajout membre")
            if team_role and team_role not in member.roles:
                await member.add_roles(team_role, reason="OFM: ajout équipe")
            await upsert_ofm_participant(
                interaction.guild.id,
                member.id,
                "accepted",
                team_role.id if team_role else None,
            )
            await update_ofm_board(interaction.guild)
            await interaction.response.send_message(f"✅ {member.mention} ajouté.", ephemeral=True)
            return

        if self.action_key == "remove_member":
            if role and role in member.roles:
                await member.remove_roles(role, reason="OFM: retrait membre")
            if team_role and team_role in member.roles:
                await member.remove_roles(team_role, reason="OFM: retrait équipe")
            if leader_role and leader_role in member.roles:
                await member.remove_roles(leader_role, reason="OFM: retrait leader")
            if sub_role and sub_role in member.roles:
                await member.remove_roles(sub_role, reason="OFM: retrait remplaçant")
            await upsert_ofm_participant(
                interaction.guild.id,
                member.id,
                "removed",
                team_role.id if team_role else None,
            )
            await update_ofm_board(interaction.guild)
            await interaction.response.send_message(f"✅ {member.mention} retiré.", ephemeral=True)
            return

        if self.action_key == "promote":
            if not leader_role:
                await interaction.response.send_message("Rôle leader non configuré.", ephemeral=True)
                return
            if leader_role not in member.roles:
                await member.add_roles(leader_role, reason="OFM: promotion leader")
            await interaction.response.send_message(f"✅ {member.mention} promu.", ephemeral=True)
            return

        if self.action_key == "demote":
            if not leader_role:
                await interaction.response.send_message("Rôle leader non configuré.", ephemeral=True)
                return
            if leader_role in member.roles:
                await member.remove_roles(leader_role, reason="OFM: rétrogradation")
            await interaction.response.send_message(f"✅ {member.mention} rétrogradé.", ephemeral=True)
            return

        if self.action_key == "set_leader":
            if not leader_role:
                await interaction.response.send_message("Rôle leader non configuré.", ephemeral=True)
                return
            if leader_role not in member.roles:
                await member.add_roles(leader_role, reason="OFM: définir leader")
            await interaction.response.send_message(f"✅ {member.mention} est leader.", ephemeral=True)
            return

        if self.action_key == "set_sub":
            if not sub_role:
                await interaction.response.send_message("Rôle remplaçant non configuré.", ephemeral=True)
                return
            if sub_role not in member.roles:
                await member.add_roles(sub_role, reason="OFM: définir remplaçant")
            await interaction.response.send_message(f"✅ {member.mention} est remplaçant.", ephemeral=True)
            return


class OFMTeamNameModal(discord.ui.Modal):
    def __init__(self):
        super().__init__(title="Changer le nom de l'équipe")
        self.team_name = discord.ui.TextInput(
            label="Nouveau nom",
            placeholder="Ex: Team Gaulois",
            required=True,
            max_length=100,
        )
        self.add_item(self.team_name)

    async def on_submit(self, interaction: discord.Interaction):
        if not interaction.guild:
            await interaction.response.send_message("Commande disponible uniquement sur un serveur.", ephemeral=True)
            return
        manager_role = interaction.guild.get_role(OFM_MANAGER_ROLE_ID)
        if not manager_role or manager_role not in interaction.user.roles:
            await interaction.response.send_message("Accès réservé aux OFM managers.", ephemeral=True)
            return
        new_name = str(self.team_name.value).strip()
        if not new_name:
            await interaction.response.send_message("Nom invalide.", ephemeral=True)
            return
        await set_ofm_team_name(interaction.guild.id, new_name)
        await update_ofm_admin_panel(interaction.guild)
        await interaction.response.send_message("✅ Nom d'équipe mis à jour.", ephemeral=True)


class OFMReplacementModal(discord.ui.Modal):
    def __init__(self):
        super().__init__(title="Définir un remplaçant")
        self.absent_id = discord.ui.TextInput(
            label="ID du joueur absent",
            placeholder="Ex: 272094371711680512",
            required=False,
            max_length=32,
        )
        self.replacement_id = discord.ui.TextInput(
            label="ID du remplaçant",
            placeholder="Ex: 123456789012345678",
            required=True,
            max_length=32,
        )
        self.add_item(self.absent_id)
        self.add_item(self.replacement_id)

    async def on_submit(self, interaction: discord.Interaction):
        if not interaction.guild:
            await interaction.response.send_message("Commande disponible uniquement sur un serveur.", ephemeral=True)
            return
        manager_role = interaction.guild.get_role(OFM_MANAGER_ROLE_ID)
        if not manager_role or manager_role not in interaction.user.roles:
            await interaction.response.send_message("Accès réservé aux OFM managers.", ephemeral=True)
            return
        raw_replacement = str(self.replacement_id.value).strip()
        if not raw_replacement.isdigit():
            await interaction.response.send_message("ID remplaçant invalide.", ephemeral=True)
            return
        replacement_id = int(raw_replacement)
        try:
            replacement = (
                interaction.guild.get_member(replacement_id)
                or await interaction.guild.fetch_member(replacement_id)
            )
        except Exception:
            replacement = None
        if not replacement:
            await interaction.response.send_message("Remplaçant introuvable.", ephemeral=True)
            return
        role = interaction.guild.get_role(OFM_ROLE_ID)
        team_role = interaction.guild.get_role(OFM_TEAM_ROLE_ID)
        sub_role = interaction.guild.get_role(OFM_SUB_ROLE_ID) if OFM_SUB_ROLE_ID else None
        if role and role not in replacement.roles:
            await replacement.add_roles(role, reason="OFM: ajout remplaçant")
        if team_role and team_role not in replacement.roles:
            await replacement.add_roles(team_role, reason="OFM: ajout remplaçant équipe")
        if sub_role and sub_role not in replacement.roles:
            await replacement.add_roles(sub_role, reason="OFM: marquer remplaçant")
        await upsert_ofm_participant(
            interaction.guild.id,
            replacement.id,
            "accepted",
            team_role.id if team_role else None,
        )
        await update_ofm_board(interaction.guild)

        absent_line = ""
        raw_absent = str(self.absent_id.value).strip()
        if raw_absent.isdigit():
            absent_id = int(raw_absent)
            absent_line = f" (remplace <@{absent_id}>)"
        await interaction.response.send_message(
            f"✅ {replacement.mention} ajouté comme remplaçant{absent_line}.",
            ephemeral=True,
        )


class OFMConfigView(discord.ui.View):
    def __init__(self, section: str = "members"):
        super().__init__(timeout=None)
        self.section = section
        select = discord.ui.Select(
            placeholder="Choisir une section...",
            options=[
                discord.SelectOption(label="Gestion des membres", value="members", emoji="👥"),
                discord.SelectOption(label="Gestion de l'équipe", value="team", emoji="🛡️"),
            ],
            custom_id="ofm_section_select",
        )
        select.callback = self._on_select
        self.add_item(select)
        if self.section == "members":
            self._add_members_buttons()
        else:
            self._add_team_buttons()

    async def _ensure_manager(self, interaction: discord.Interaction) -> bool:
        if not interaction.guild:
            await interaction.response.send_message(
                "Commande disponible uniquement sur un serveur.",
                ephemeral=True,
            )
            return False
        manager_role = interaction.guild.get_role(OFM_MANAGER_ROLE_ID)
        if not manager_role or manager_role not in interaction.user.roles:
            await interaction.response.send_message(
                "Accès réservé aux OFM managers.",
                ephemeral=True,
            )
            return False
        return True

    async def _on_select(self, interaction: discord.Interaction):
        if not await self._ensure_manager(interaction):
            return
        selected = self.children[0].values[0]
        await interaction.response.edit_message(view=OFMConfigView(section=selected))

    def _add_button(self, label, emoji, style, custom_id, callback, row):
        button = discord.ui.Button(
            label=label,
            emoji=emoji,
            style=style,
            custom_id=custom_id,
            row=row,
        )
        button.callback = callback
        self.add_item(button)

    def _add_members_buttons(self):
        self._add_button("Ajouter un membre", "➕", discord.ButtonStyle.success, "ofm_add_member", self._add_member, 1)
        self._add_button("Retirer un membre", "❌", discord.ButtonStyle.danger, "ofm_remove_member", self._remove_member, 1)
        self._add_button("Promouvoir", "🔝", discord.ButtonStyle.primary, "ofm_promote", self._promote, 2)
        self._add_button("Rétrograder", "⬇️", discord.ButtonStyle.secondary, "ofm_demote", self._demote, 2)
        self._add_button("Voir la liste", "📋", discord.ButtonStyle.secondary, "ofm_list_members", self._list_members, 3)

    def _add_team_buttons(self):
        self._add_button("Changer le nom", "✏️", discord.ButtonStyle.primary, "ofm_team_name", self._change_team_name, 1)
        self._add_button("Définir leader", "👑", discord.ButtonStyle.primary, "ofm_set_leader", self._set_leader, 2)
        self._add_button("Définir remplaçant", "🔄", discord.ButtonStyle.secondary, "ofm_set_sub", self._set_sub, 2)

    async def _add_member(self, interaction: discord.Interaction):
        if not await self._ensure_manager(interaction):
            return
        await interaction.response.send_modal(OFMMemberIdModal("Ajouter un membre", "add_member"))

    async def _remove_member(self, interaction: discord.Interaction):
        if not await self._ensure_manager(interaction):
            return
        await interaction.response.send_modal(OFMMemberIdModal("Retirer un membre", "remove_member"))

    async def _promote(self, interaction: discord.Interaction):
        if not await self._ensure_manager(interaction):
            return
        await interaction.response.send_modal(OFMMemberIdModal("Promouvoir", "promote"))

    async def _demote(self, interaction: discord.Interaction):
        if not await self._ensure_manager(interaction):
            return
        await interaction.response.send_modal(OFMMemberIdModal("Rétrograder", "demote"))

    async def _list_members(self, interaction: discord.Interaction):
        if not await self._ensure_manager(interaction):
            return
        team_role = interaction.guild.get_role(OFM_TEAM_ROLE_ID)
        leader_role = interaction.guild.get_role(OFM_LEADER_ROLE_ID) if OFM_LEADER_ROLE_ID else None
        sub_role = interaction.guild.get_role(OFM_SUB_ROLE_ID) if OFM_SUB_ROLE_ID else None
        members = team_role.members if team_role else []
        if not members:
            await interaction.response.send_message("Aucun membre dans l'équipe.", ephemeral=True)
            return
        lines = []
        for member in members:
            tags = []
            if leader_role and leader_role in member.roles:
                tags.append("leader")
            if sub_role and sub_role in member.roles:
                tags.append("remplaçant")
            tag_text = f" ({', '.join(tags)})" if tags else ""
            lines.append(f"- {member.mention}{tag_text}")
        embed = discord.Embed(
            title="Membres OFM",
            description="\n".join(lines),
            color=discord.Color.blurple(),
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)

    async def _change_team_name(self, interaction: discord.Interaction):
        if not await self._ensure_manager(interaction):
            return
        await interaction.response.send_modal(OFMTeamNameModal())

    async def _set_leader(self, interaction: discord.Interaction):
        if not await self._ensure_manager(interaction):
            return
        await interaction.response.send_modal(OFMMemberIdModal("Définir leader", "set_leader"))

    async def _set_sub(self, interaction: discord.Interaction):
        if not await self._ensure_manager(interaction):
            return
        await interaction.response.send_modal(OFMReplacementModal())


class ModDefaultsModal(discord.ui.Modal):
    def __init__(self):
        super().__init__(title="Configurer les durées par défaut")
        self.default_mute = discord.ui.TextInput(
            label="Mute par défaut (ex: 1h, 30m)",
            required=False,
            max_length=16,
        )
        self.default_ban = discord.ui.TextInput(
            label="Ban par défaut (ex: 7d, vide=permanent)",
            required=False,
            max_length=16,
        )
        self.add_item(self.default_mute)
        self.add_item(self.default_ban)

    async def on_submit(self, interaction: discord.Interaction):
        if not interaction.guild:
            await interaction.response.send_message("Commande disponible uniquement sur un serveur.", ephemeral=True)
            return
        if not is_admin_member(interaction.user):
            await interaction.response.send_message("Accès réservé fondateur/admin.", ephemeral=True)
            return
        mute_value = str(self.default_mute.value).strip()
        ban_value = str(self.default_ban.value).strip()
        mute_seconds = parse_duration_seconds(mute_value) if mute_value else None
        ban_seconds = parse_duration_seconds(ban_value) if ban_value else None
        if mute_value and mute_seconds is None:
            await interaction.response.send_message("Durée mute invalide.", ephemeral=True)
            return
        if ban_value and ban_seconds is None:
            await interaction.response.send_message("Durée ban invalide.", ephemeral=True)
            return
        await set_mod_config(interaction.guild.id, default_mute_seconds=mute_seconds, default_ban_seconds=ban_seconds)
        await update_mod_admin_panel(interaction.guild)
        await interaction.response.send_message("✅ Durées mises à jour.", ephemeral=True)


class ModConfirmView(discord.ui.View):
    def __init__(self, requester_id: int, on_confirm):
        super().__init__(timeout=60)
        self.requester_id = requester_id
        self.on_confirm_callback = on_confirm

    async def _ensure_requester(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.requester_id:
            await interaction.response.send_message("Action réservée au demandeur.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Confirmer", style=discord.ButtonStyle.danger, custom_id="mod_confirm")
    async def confirm(self, interaction: discord.Interaction, _button: discord.ui.Button):
        if not await self._ensure_requester(interaction):
            return
        await self.on_confirm_callback(interaction)

    @discord.ui.button(label="Annuler", style=discord.ButtonStyle.secondary, custom_id="mod_cancel")
    async def cancel(self, interaction: discord.Interaction, _button: discord.ui.Button):
        if not await self._ensure_requester(interaction):
            return
        await interaction.response.send_message("Action annulée.", ephemeral=True)


class ModWarnModal(discord.ui.Modal):
    def __init__(self):
        super().__init__(title="Warn un membre")
        self.user_id = discord.ui.TextInput(label="ID Discord", required=True, max_length=32)
        self.reason = discord.ui.TextInput(label="Raison", required=True, max_length=200)
        self.add_item(self.user_id)
        self.add_item(self.reason)

    async def on_submit(self, interaction: discord.Interaction):
        if not await ensure_mod_permission(interaction, "warn"):
            return
        if not interaction.guild:
            await interaction.response.send_message("Commande disponible uniquement sur un serveur.", ephemeral=True)
            return
        raw = str(self.user_id.value).strip()
        if not raw.isdigit():
            await interaction.response.send_message("ID invalide.", ephemeral=True)
            return
        member = interaction.guild.get_member(int(raw)) or await interaction.guild.fetch_member(int(raw))
        bot_member = interaction.guild.me
        err = can_moderate_member(interaction.user, member, bot_member)
        if err:
            await interaction.response.send_message(err, ephemeral=True)
            return
        record = await add_warning(interaction.guild.id, member.id, interaction.user.id, str(self.reason.value))
        await add_mod_action(interaction.guild.id, member.id, interaction.user.id, "warn", str(self.reason.value))
        await send_mod_log(
            interaction.guild,
            build_mod_log_embed("warn", member, interaction.user, str(self.reason.value)),
        )
        await interaction.response.send_message(f"✅ Warn ajouté (ID {record['id']}).", ephemeral=True)


class ModWarnListModal(discord.ui.Modal):
    def __init__(self):
        super().__init__(title="Warnlist")
        self.user_id = discord.ui.TextInput(label="ID Discord", required=True, max_length=32)
        self.add_item(self.user_id)

    async def on_submit(self, interaction: discord.Interaction):
        if not await ensure_mod_permission(interaction, "warnlist"):
            return
        if not interaction.guild:
            await interaction.response.send_message("Commande disponible uniquement sur un serveur.", ephemeral=True)
            return
        raw = str(self.user_id.value).strip()
        if not raw.isdigit():
            await interaction.response.send_message("ID invalide.", ephemeral=True)
            return
        user_id = int(raw)
        rows = await list_warnings(interaction.guild.id, user_id)
        if not rows:
            await interaction.response.send_message("Aucun warn.", ephemeral=True)
            return
        lines = []
        for row in rows[:15]:
            lines.append(f"#{row['id']} • <@{row['moderator_id']}> • {row['created_at']}\n{row['reason']}")
        embed = discord.Embed(
            title=f"Warns de <@{user_id}>",
            description="\n\n".join(lines),
            color=discord.Color.orange(),
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)


class ModClearWarnModal(discord.ui.Modal):
    def __init__(self):
        super().__init__(title="Clearwarn")
        self.user_id = discord.ui.TextInput(label="ID Discord", required=True, max_length=32)
        self.warn_id = discord.ui.TextInput(label="ID du warn (vide = tout)", required=False, max_length=32)
        self.add_item(self.user_id)
        self.add_item(self.warn_id)

    async def on_submit(self, interaction: discord.Interaction):
        if not await ensure_mod_permission(interaction, "clearwarn"):
            return
        if not interaction.guild:
            await interaction.response.send_message("Commande disponible uniquement sur un serveur.", ephemeral=True)
            return
        raw = str(self.user_id.value).strip()
        if not raw.isdigit():
            await interaction.response.send_message("ID invalide.", ephemeral=True)
            return
        user_id = int(raw)
        warn_raw = str(self.warn_id.value).strip()
        if warn_raw:
            if not warn_raw.isdigit():
                await interaction.response.send_message("ID warn invalide.", ephemeral=True)
                return
            await delete_warning(interaction.guild.id, user_id, int(warn_raw))
            await add_mod_action(interaction.guild.id, user_id, interaction.user.id, "clearwarn", f"warn_id={warn_raw}")
            await send_mod_log(
                interaction.guild,
                build_mod_log_embed("clearwarn", discord.Object(id=user_id), interaction.user, f"warn_id={warn_raw}"),
            )
            await interaction.response.send_message("✅ Warn supprimé.", ephemeral=True)
            return

        async def do_clear(confirm_interaction: discord.Interaction):
            await clear_all_warnings(confirm_interaction.guild.id, user_id)
            await add_mod_action(confirm_interaction.guild.id, user_id, confirm_interaction.user.id, "clearwarn", "all")
            await send_mod_log(
                confirm_interaction.guild,
                build_mod_log_embed("clearwarn", discord.Object(id=user_id), confirm_interaction.user, "all"),
            )
            await confirm_interaction.response.send_message("✅ Tous les warns ont été supprimés.", ephemeral=True)

        await interaction.response.send_message(
            "Confirmer la suppression de tous les warns ?",
            view=ModConfirmView(interaction.user.id, do_clear),
            ephemeral=True,
        )


class ModMuteModal(discord.ui.Modal):
    def __init__(self):
        super().__init__(title="Mute/Timeout")
        self.user_id = discord.ui.TextInput(label="ID Discord", required=True, max_length=32)
        self.duration = discord.ui.TextInput(label="Durée (ex: 10m, 2h)", required=False, max_length=16)
        self.reason = discord.ui.TextInput(label="Raison", required=False, max_length=200)
        self.add_item(self.user_id)
        self.add_item(self.duration)
        self.add_item(self.reason)

    async def on_submit(self, interaction: discord.Interaction):
        if not await ensure_mod_permission(interaction, "mute"):
            return
        if not interaction.guild:
            await interaction.response.send_message("Commande disponible uniquement sur un serveur.", ephemeral=True)
            return
        raw = str(self.user_id.value).strip()
        if not raw.isdigit():
            await interaction.response.send_message("ID invalide.", ephemeral=True)
            return
        member = interaction.guild.get_member(int(raw)) or await interaction.guild.fetch_member(int(raw))
        bot_member = interaction.guild.me
        err = can_moderate_member(interaction.user, member, bot_member)
        if err:
            await interaction.response.send_message(err, ephemeral=True)
            return
        seconds = parse_duration_seconds(str(self.duration.value).strip()) if str(self.duration.value).strip() else None
        if seconds is None:
            config = await get_mod_config(interaction.guild.id)
            seconds = config["default_mute_seconds"] if config else 3600
        until = datetime.now(timezone.utc) + timedelta(seconds=seconds)
        reason = str(self.reason.value).strip() or None
        await member.timeout(until, reason=reason or "Mute")
        await add_mod_action(interaction.guild.id, member.id, interaction.user.id, "mute", reason, seconds)
        await send_mod_log(
            interaction.guild,
            build_mod_log_embed("mute", member, interaction.user, reason, seconds),
        )
        await interaction.response.send_message("✅ Mute appliqué.", ephemeral=True)


class ModKickModal(discord.ui.Modal):
    def __init__(self):
        super().__init__(title="Kick")
        self.user_id = discord.ui.TextInput(label="ID Discord", required=True, max_length=32)
        self.reason = discord.ui.TextInput(label="Raison", required=False, max_length=200)
        self.add_item(self.user_id)
        self.add_item(self.reason)

    async def on_submit(self, interaction: discord.Interaction):
        if not await ensure_mod_permission(interaction, "kick"):
            return
        if not interaction.guild:
            await interaction.response.send_message("Commande disponible uniquement sur un serveur.", ephemeral=True)
            return
        raw = str(self.user_id.value).strip()
        if not raw.isdigit():
            await interaction.response.send_message("ID invalide.", ephemeral=True)
            return
        member = interaction.guild.get_member(int(raw)) or await interaction.guild.fetch_member(int(raw))
        bot_member = interaction.guild.me
        err = can_moderate_member(interaction.user, member, bot_member)
        if err:
            await interaction.response.send_message(err, ephemeral=True)
            return
        reason = str(self.reason.value).strip() or None

        async def do_kick(confirm_interaction: discord.Interaction):
            await member.kick(reason=reason or "Kick")
            await add_mod_action(confirm_interaction.guild.id, member.id, confirm_interaction.user.id, "kick", reason)
            await send_mod_log(
                confirm_interaction.guild,
                build_mod_log_embed("kick", member, confirm_interaction.user, reason),
            )
            await confirm_interaction.response.send_message("✅ Membre kick.", ephemeral=True)

        await interaction.response.send_message(
            "Confirmer le kick ?",
            view=ModConfirmView(interaction.user.id, do_kick),
            ephemeral=True,
        )


class ModBanModal(discord.ui.Modal):
    def __init__(self):
        super().__init__(title="Ban")
        self.user_id = discord.ui.TextInput(label="ID Discord", required=True, max_length=32)
        self.duration = discord.ui.TextInput(label="Durée (ex: 7d, vide=permanent)", required=False, max_length=16)
        self.reason = discord.ui.TextInput(label="Raison", required=False, max_length=200)
        self.add_item(self.user_id)
        self.add_item(self.duration)
        self.add_item(self.reason)

    async def on_submit(self, interaction: discord.Interaction):
        if not await ensure_mod_permission(interaction, "ban"):
            return
        if not interaction.guild:
            await interaction.response.send_message("Commande disponible uniquement sur un serveur.", ephemeral=True)
            return
        raw = str(self.user_id.value).strip()
        if not raw.isdigit():
            await interaction.response.send_message("ID invalide.", ephemeral=True)
            return
        member = interaction.guild.get_member(int(raw)) or await interaction.guild.fetch_member(int(raw))
        bot_member = interaction.guild.me
        err = can_moderate_member(interaction.user, member, bot_member)
        if err:
            await interaction.response.send_message(err, ephemeral=True)
            return
        seconds = parse_duration_seconds(str(self.duration.value).strip()) if str(self.duration.value).strip() else None
        if str(self.duration.value).strip() and seconds is None:
            await interaction.response.send_message("Durée invalide.", ephemeral=True)
            return
        if seconds is None:
            config = await get_mod_config(interaction.guild.id)
            seconds = config["default_ban_seconds"] if config else 0
        reason = str(self.reason.value).strip() or None

        async def do_ban(confirm_interaction: discord.Interaction):
            await confirm_interaction.guild.ban(member, reason=reason or "Ban", delete_message_days=0)
            await add_mod_action(confirm_interaction.guild.id, member.id, confirm_interaction.user.id, "ban", reason, seconds or None)
            await send_mod_log(
                confirm_interaction.guild,
                build_mod_log_embed("ban", member, confirm_interaction.user, reason, seconds or None),
            )
            if seconds:
                bot.loop.create_task(schedule_unban(confirm_interaction.guild, member.id, seconds))
            await confirm_interaction.response.send_message("✅ Membre banni.", ephemeral=True)

        await interaction.response.send_message(
            "Confirmer le ban ?",
            view=ModConfirmView(interaction.user.id, do_ban),
            ephemeral=True,
        )


class ModUnbanModal(discord.ui.Modal):
    def __init__(self):
        super().__init__(title="Unban")
        self.user_id = discord.ui.TextInput(label="ID Discord", required=True, max_length=32)
        self.reason = discord.ui.TextInput(label="Raison", required=False, max_length=200)
        self.add_item(self.user_id)
        self.add_item(self.reason)

    async def on_submit(self, interaction: discord.Interaction):
        if not await ensure_mod_permission(interaction, "unban"):
            return
        if not interaction.guild:
            await interaction.response.send_message("Commande disponible uniquement sur un serveur.", ephemeral=True)
            return
        raw = str(self.user_id.value).strip()
        if not raw.isdigit():
            await interaction.response.send_message("ID invalide.", ephemeral=True)
            return
        target_id = int(raw)
        reason = str(self.reason.value).strip() or None

        async def do_unban(confirm_interaction: discord.Interaction):
            await confirm_interaction.guild.unban(discord.Object(id=target_id), reason=reason or "Unban")
            await add_mod_action(confirm_interaction.guild.id, target_id, confirm_interaction.user.id, "unban", reason)
            await send_mod_log(
                confirm_interaction.guild,
                build_mod_log_embed("unban", discord.Object(id=target_id), confirm_interaction.user, reason),
            )
            await confirm_interaction.response.send_message("✅ Membre débanni.", ephemeral=True)

        await interaction.response.send_message(
            "Confirmer l'unban ?",
            view=ModConfirmView(interaction.user.id, do_unban),
            ephemeral=True,
        )


class ModCaseModal(discord.ui.Modal):
    def __init__(self):
        super().__init__(title="Casier")
        self.user_id = discord.ui.TextInput(label="ID Discord", required=True, max_length=32)
        self.add_item(self.user_id)

    async def on_submit(self, interaction: discord.Interaction):
        if not await ensure_mod_permission(interaction, "case"):
            return
        if not interaction.guild:
            await interaction.response.send_message("Commande disponible uniquement sur un serveur.", ephemeral=True)
            return
        raw = str(self.user_id.value).strip()
        if not raw.isdigit():
            await interaction.response.send_message("ID invalide.", ephemeral=True)
            return
        member = interaction.guild.get_member(int(raw)) or await interaction.guild.fetch_member(int(raw))
        warnings = await list_warnings(interaction.guild.id, member.id)
        actions = await list_mod_actions(interaction.guild.id, member.id)
        notes = await list_mod_notes(interaction.guild.id, member.id)
        counts = {}
        for action in actions:
            action_type = action["action_type"]
            counts[action_type] = counts.get(action_type, 0) + 1
        warn_count = len(warnings)
        embed = discord.Embed(
            title=f"Casier de {member.display_name}",
            color=discord.Color.dark_gold(),
        )
        embed.add_field(name="Arrivée", value=str(member.joined_at) if member.joined_at else "Inconnue", inline=False)
        embed.add_field(
            name="Stats",
            value=f"Warns: {warn_count} | Kicks: {counts.get('kick', 0)} | Bans: {counts.get('ban', 0)}",
            inline=False,
        )
        if notes:
            last_notes = []
            for note in notes[:5]:
                last_notes.append(f"{note['created_at']} • <@{note['moderator_id']}> • {note['note']}")
            embed.add_field(name="Notes internes", value="\n".join(last_notes), inline=False)
        await interaction.response.send_message(embed=embed, ephemeral=True)


class ModNoteModal(discord.ui.Modal):
    def __init__(self):
        super().__init__(title="Note interne")
        self.user_id = discord.ui.TextInput(label="ID Discord", required=True, max_length=32)
        self.note = discord.ui.TextInput(label="Note", required=True, max_length=300)
        self.add_item(self.user_id)
        self.add_item(self.note)

    async def on_submit(self, interaction: discord.Interaction):
        if not await ensure_mod_permission(interaction, "note"):
            return
        if not interaction.guild:
            await interaction.response.send_message("Commande disponible uniquement sur un serveur.", ephemeral=True)
            return
        raw = str(self.user_id.value).strip()
        if not raw.isdigit():
            await interaction.response.send_message("ID invalide.", ephemeral=True)
            return
        member = interaction.guild.get_member(int(raw)) or await interaction.guild.fetch_member(int(raw))
        note_text = str(self.note.value).strip()
        await add_mod_note(interaction.guild.id, member.id, interaction.user.id, note_text)
        await add_mod_action(interaction.guild.id, member.id, interaction.user.id, "note", note_text)
        await send_mod_log(
            interaction.guild,
            build_mod_log_embed("note", member, interaction.user, note_text),
        )
        await interaction.response.send_message("✅ Note ajoutée.", ephemeral=True)
class ModAdminPanelView(discord.ui.View):
    def __init__(
        self,
        guild_id: Optional[int] = None,
        selected_role_id: Optional[int] = None,
        selected_command: Optional[str] = None,
        mode: str = "permissions",
    ):
        super().__init__(timeout=None)
        self.guild_id = guild_id
        self.selected_role_id = selected_role_id
        self.selected_command = selected_command
        self.mode = mode

        role_options = []
        guild = bot.get_guild(guild_id) if guild_id else None
        if guild:
            roles = [r for r in guild.roles if r != guild.default_role]
            roles.sort(key=lambda r: r.position, reverse=True)
            for role in roles[:25]:
                role_options.append(
                    discord.SelectOption(
                        label=role.name[:100],
                        value=str(role.id),
                        default=(role.id == selected_role_id),
                    )
                )
        mode_select = discord.ui.Select(
            placeholder="Choisir un mode...",
            options=[
                discord.SelectOption(label="Permissions", value="permissions", default=(mode == "permissions")),
                discord.SelectOption(label="Sanctions", value="sanctions", default=(mode == "sanctions")),
            ],
            custom_id="mod_mode_select",
            row=0,
        )
        mode_select.callback = self._on_mode_select
        self.add_item(mode_select)

        if self.mode == "permissions":
            if role_options:
                role_select = discord.ui.Select(
                    placeholder="Sélectionner un rôle...",
                    options=role_options,
                    custom_id="mod_role_select",
                    row=1,
                )
                role_select.callback = self._on_role_select
                self.add_item(role_select)

            command_options = [
                discord.SelectOption(
                    label=cmd,
                    value=cmd,
                    default=(cmd == selected_command),
                )
                for cmd in MOD_COMMANDS
            ]
            command_select = discord.ui.Select(
                placeholder="Sélectionner une commande...",
                options=command_options,
                custom_id="mod_command_select",
                row=2,
            )
            command_select.callback = self._on_command_select
            self.add_item(command_select)

            self._add_button("Autoriser", "✅", discord.ButtonStyle.success, "mod_allow", self._allow, 3)
            self._add_button("Retirer", "⛔", discord.ButtonStyle.danger, "mod_deny", self._deny, 3)
            self._add_button("Voir permissions", "📋", discord.ButtonStyle.secondary, "mod_view", self._view, 4)
            self._add_button("Configurer durées", "⏱️", discord.ButtonStyle.primary, "mod_defaults", self._defaults, 4)
        else:
            self._add_button("Warn", "⚠️", discord.ButtonStyle.primary, "mod_warn", self._panel_warn, 2)
            self._add_button("Warnlist", "📋", discord.ButtonStyle.secondary, "mod_warnlist", self._panel_warnlist, 2)
            self._add_button("Clearwarn", "🧹", discord.ButtonStyle.danger, "mod_clearwarn", self._panel_clearwarn, 2)
            self._add_button("Mute", "🔇", discord.ButtonStyle.secondary, "mod_mute", self._panel_mute, 3)
            self._add_button("Kick", "👢", discord.ButtonStyle.danger, "mod_kick", self._panel_kick, 3)
            self._add_button("Ban", "🔨", discord.ButtonStyle.danger, "mod_ban", self._panel_ban, 3)
            self._add_button("Unban", "🔓", discord.ButtonStyle.success, "mod_unban", self._panel_unban, 4)
            self._add_button("Case", "🗂️", discord.ButtonStyle.secondary, "mod_case", self._panel_case, 4)
            self._add_button("Note", "📝", discord.ButtonStyle.primary, "mod_note", self._panel_note, 4)

    def _add_button(self, label, emoji, style, custom_id, callback, row):
        button = discord.ui.Button(
            label=label,
            emoji=emoji,
            style=style,
            custom_id=custom_id,
            row=row,
        )
        button.callback = callback
        self.add_item(button)

    async def _ensure_admin(self, interaction: discord.Interaction) -> bool:
        if not interaction.guild:
            await interaction.response.send_message("Commande disponible uniquement sur un serveur.", ephemeral=True)
            return False
        if not is_admin_member(interaction.user):
            await interaction.response.send_message("Accès réservé fondateur/admin.", ephemeral=True)
            return False
        return True

    async def _on_role_select(self, interaction: discord.Interaction):
        if not await self._ensure_admin(interaction):
            return
        selected = int(interaction.data["values"][0])
        await update_mod_admin_panel(interaction.guild, selected_role_id=selected, mode=self.mode)
        await interaction.response.defer()

    async def _on_command_select(self, interaction: discord.Interaction):
        if not await self._ensure_admin(interaction):
            return
        selected = interaction.data["values"][0]
        view = ModAdminPanelView(
            guild_id=interaction.guild.id,
            selected_role_id=self.selected_role_id,
            selected_command=selected,
            mode=self.mode,
        )
        await interaction.response.edit_message(view=view)

    async def _on_mode_select(self, interaction: discord.Interaction):
        selected = interaction.data["values"][0]
        if selected == "permissions" and not await self._ensure_admin(interaction):
            return
        await update_mod_admin_panel(
            interaction.guild,
            selected_role_id=self.selected_role_id,
            mode=selected,
        )
        await interaction.response.defer()

    async def _allow(self, interaction: discord.Interaction):
        if not await self._ensure_admin(interaction):
            return
        if not self.selected_role_id or not self.selected_command:
            await interaction.response.send_message("Sélectionne un rôle et une commande.", ephemeral=True)
            return
        await set_mod_permission(interaction.guild.id, self.selected_role_id, self.selected_command, True)
        await update_mod_admin_panel(interaction.guild, selected_role_id=self.selected_role_id, mode=self.mode)
        await interaction.response.send_message("✅ Permission accordée.", ephemeral=True)

    async def _deny(self, interaction: discord.Interaction):
        if not await self._ensure_admin(interaction):
            return
        if not self.selected_role_id or not self.selected_command:
            await interaction.response.send_message("Sélectionne un rôle et une commande.", ephemeral=True)
            return
        await set_mod_permission(interaction.guild.id, self.selected_role_id, self.selected_command, False)
        await update_mod_admin_panel(interaction.guild, selected_role_id=self.selected_role_id, mode=self.mode)
        await interaction.response.send_message("✅ Permission retirée.", ephemeral=True)

    async def _view(self, interaction: discord.Interaction):
        if not await self._ensure_admin(interaction):
            return
        if not self.selected_role_id:
            await interaction.response.send_message("Sélectionne un rôle.", ephemeral=True)
            return
        perms = await get_permissions_for_role(interaction.guild.id, self.selected_role_id)
        allowed = [p["command"] for p in perms if p["allowed"]]
        allowed_text = ", ".join(allowed) if allowed else "Aucune"
        await interaction.response.send_message(
            f"Permissions: {allowed_text}",
            ephemeral=True,
        )

    async def _defaults(self, interaction: discord.Interaction):
        if not await self._ensure_admin(interaction):
            return
        await interaction.response.send_modal(ModDefaultsModal())

    async def _panel_warn(self, interaction: discord.Interaction):
        if not await ensure_mod_permission(interaction, "warn"):
            return
        await interaction.response.send_modal(ModWarnModal())

    async def _panel_warnlist(self, interaction: discord.Interaction):
        if not await ensure_mod_permission(interaction, "warnlist"):
            return
        await interaction.response.send_modal(ModWarnListModal())

    async def _panel_clearwarn(self, interaction: discord.Interaction):
        if not await ensure_mod_permission(interaction, "clearwarn"):
            return
        await interaction.response.send_modal(ModClearWarnModal())

    async def _panel_mute(self, interaction: discord.Interaction):
        if not await ensure_mod_permission(interaction, "mute"):
            return
        await interaction.response.send_modal(ModMuteModal())

    async def _panel_kick(self, interaction: discord.Interaction):
        if not await ensure_mod_permission(interaction, "kick"):
            return
        await interaction.response.send_modal(ModKickModal())

    async def _panel_ban(self, interaction: discord.Interaction):
        if not await ensure_mod_permission(interaction, "ban"):
            return
        await interaction.response.send_modal(ModBanModal())

    async def _panel_unban(self, interaction: discord.Interaction):
        if not await ensure_mod_permission(interaction, "unban"):
            return
        await interaction.response.send_modal(ModUnbanModal())

    async def _panel_case(self, interaction: discord.Interaction):
        if not await ensure_mod_permission(interaction, "case"):
            return
        await interaction.response.send_modal(ModCaseModal())

    async def _panel_note(self, interaction: discord.Interaction):
        if not await ensure_mod_permission(interaction, "note"):
            return
        await interaction.response.send_modal(ModNoteModal())


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


async def update_leaderboard_message_ffa_for_guild(guild: discord.Guild):
    record = await get_leaderboard_message_ffa(guild.id)
    if not record:
        return {"updated": False, "error": "no_record"}
    channel_id = record["channel_id"]
    message_id = record["message_id"]
    try:
        channel = bot.get_channel(channel_id) or await bot.fetch_channel(channel_id)
        message = await channel.fetch_message(message_id)
        embed = await build_leaderboard_ffa_embed(guild, 1, 20)
        if not embed:
            return {"updated": False, "error": "no_embed"}
        await message.edit(embed=embed, view=LeaderboardFfaView(1, 20))
        return {"updated": True, "error": None}
    except Exception as exc:
        return {"updated": False, "error": str(exc)[:200]}


async def recover_ffa_leaderboard_record(guild: discord.Guild):
    for channel in guild.text_channels:
        if not channel.permissions_for(guild.me).read_message_history:
            continue
        try:
            async for msg in channel.history(limit=30):
                if msg.author != bot.user or not msg.embeds:
                    continue
                title = msg.embeds[0].title or ""
                if "Leaderboard FFA" in title:
                    await set_leaderboard_message_ffa(guild.id, channel.id, msg.id)
                    return {"recovered": True, "channel_id": channel.id, "message_id": msg.id}
        except Exception:
            continue
    return {"recovered": False}


async def find_message_in_guild(guild: discord.Guild, message_id: int):
    for channel in guild.text_channels:
        if not channel.permissions_for(guild.me).read_message_history:
            continue
        try:
            msg = await channel.fetch_message(message_id)
            return msg, channel
        except Exception:
            continue
    return None, None


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


async def get_latest_ffa_updated_at():
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT MAX(updated_at) AS last_updated FROM ffa_stats"
        )
        return row["last_updated"] if row else None


async def resync_leaderboards(guild: discord.Guild):
    ONEV1_CACHE.clear()
    ffa_result = await refresh_ffa_stats()
    ffa_message = await update_leaderboard_message_ffa_for_guild(guild)
    ffa_recovered = None
    if ffa_message.get("error") == "no_record":
        ffa_recovered = await recover_ffa_leaderboard_record(guild)
        if ffa_recovered.get("recovered"):
            ffa_message = await update_leaderboard_message_ffa_for_guild(guild)
    await update_leaderboard_message_1v1()
    await update_leaderboard_message_1v1_gal()
    ffa_updated = await get_latest_ffa_updated_at()
    onev1_cached_at = ONEV1_CACHE.get("fetched_at")
    return {
        "ffa": ffa_result,
        "ffa_updated": ffa_updated,
        "onev1_cached_at": onev1_cached_at,
        "ffa_message": ffa_message,
        "ffa_recovered": ffa_recovered,
    }


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
        stats = {
            "sessions": 0,
            "wins_team": 0,
            "wins_ffa": 0,
            "sent_team": 0,
            "sent_ffa": 0,
            "skipped_notified": 0,
            "missing_game_id": 0,
            "fetch_errors": 0,
        }
        error_text = None
        try:
            channel = bot.get_channel(int(WIN_NOTIFY_CHANNEL_ID)) or await bot.fetch_channel(
                int(WIN_NOTIFY_CHANNEL_ID)
            )
            channel_error = get_notify_channel_error(channel)
            if channel_error:
                raise RuntimeError(channel_error)
            end_dt = datetime.now(timezone.utc)
            start_dt = end_dt - timedelta(hours=WIN_NOTIFY_RANGE_HOURS)
            start_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            end_iso = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

            headers = {"User-Agent": USER_AGENT}
            async with aiohttp.ClientSession(headers=headers) as session:
                sessions = await fetch_clan_sessions(session, start_iso, end_iso)
                stats["sessions"] = len(sessions)
                for s in sessions:
                    game_id = s.get("gameId")
                    if not game_id:
                        stats["missing_game_id"] += 1
                        continue
                    if await is_win_notified(game_id):
                        stats["skipped_notified"] += 1
                        continue
                    try:
                        info = await fetch_game_info(session, game_id)
                    except Exception:
                        stats["fetch_errors"] += 1
                        continue
                    if not clan_won_game(info):
                        continue
                    stats["wins_team"] += 1
                    if bootstrap:
                        await mark_win_notified(game_id)
                        continue
                    embed = build_win_embed(info)
                    await channel.send(embed=embed)
                    await mark_win_notified(game_id)
                    stats["sent_team"] += 1

                ffa_players = await get_ffa_players()
                for _discord_id, pseudo, player_id in ffa_players:
                    try:
                        player_sessions = await fetch_player_sessions(player_id)
                    except Exception:
                        stats["fetch_errors"] += 1
                        continue
                    for ps in player_sessions:
                        if not is_ffa_session(ps):
                            continue
                        if not ps.get("hasWon"):
                            continue
                        session_time = get_session_time(ps)
                        if not session_time:
                            continue
                        if session_time < start_dt or session_time > end_dt:
                            continue
                        game_id = get_session_game_id(ps)
                        if not game_id:
                            stats["missing_game_id"] += 1
                            continue
                        if await is_ffa_win_notified(player_id, game_id):
                            stats["skipped_notified"] += 1
                            continue
                        stats["wins_ffa"] += 1
                        if bootstrap:
                            await mark_ffa_win_notified(player_id, game_id)
                            continue
                        embed = build_ffa_win_embed(pseudo, player_id, ps, game_id)
                        await channel.send(embed=embed)
                        await mark_ffa_win_notified(player_id, game_id)
                        stats["sent_ffa"] += 1
        except Exception as exc:
            error_text = str(exc)[:500]
            print(f"Win notify failed: {exc}")
        finally:
            scan_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            await set_last_win_notify_stats(
                scan_at,
                stats["sessions"],
                stats["wins_team"] + stats["wins_ffa"],
                stats["sent_team"] + stats["sent_ffa"],
                stats["skipped_notified"],
                stats["missing_game_id"],
                stats["fetch_errors"],
                error_text,
            )
        bootstrap = False
        await asyncio.sleep(WIN_NOTIFY_POLL_SECONDS)


async def run_win_notify_once(force_empty: bool = False):
    if not WIN_NOTIFY_CHANNEL_ID:
        return {"status": "error", "error": "WIN_NOTIFY_CHANNEL_ID missing"}
    channel = bot.get_channel(int(WIN_NOTIFY_CHANNEL_ID)) or await bot.fetch_channel(int(WIN_NOTIFY_CHANNEL_ID))
    channel_error = get_notify_channel_error(channel)
    if channel_error:
        return {"status": "error", "error": channel_error}
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(hours=WIN_NOTIFY_RANGE_HOURS)
    start_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_iso = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    headers = {"User-Agent": USER_AGENT}
    notified_any = False
    stats = {
        "sessions": 0,
        "wins_team": 0,
        "wins_ffa": 0,
        "sent_team": 0,
        "sent_ffa": 0,
        "skipped_notified": 0,
        "missing_game_id": 0,
        "fetch_errors": 0,
    }
    error_text = None
    async with aiohttp.ClientSession(headers=headers) as session:
        sessions = await fetch_clan_sessions(session, start_iso, end_iso)
        stats["sessions"] = len(sessions)
        for s in sessions:
            game_id = s.get("gameId")
            if not game_id:
                stats["missing_game_id"] += 1
                continue
            if await is_win_notified(game_id):
                stats["skipped_notified"] += 1
                continue
            try:
                info = await fetch_game_info(session, game_id)
            except Exception:
                stats["fetch_errors"] += 1
                continue
            if not clan_won_game(info):
                continue
            stats["wins_team"] += 1
            embed = build_win_embed(info)
            await channel.send(embed=embed)
            await mark_win_notified(game_id)
            notified_any = True
            stats["sent_team"] += 1

        ffa_players = await get_ffa_players()
        for _discord_id, pseudo, player_id in ffa_players:
            try:
                player_sessions = await fetch_player_sessions(player_id)
            except Exception:
                stats["fetch_errors"] += 1
                continue
            for ps in player_sessions:
                if not is_ffa_session(ps):
                    continue
                if not ps.get("hasWon"):
                    continue
                session_time = get_session_time(ps)
                if not session_time:
                    continue
                if session_time < start_dt or session_time > end_dt:
                    continue
                game_id = get_session_game_id(ps)
                if not game_id:
                    stats["missing_game_id"] += 1
                    continue
                if await is_ffa_win_notified(player_id, game_id):
                    stats["skipped_notified"] += 1
                    continue
                stats["wins_ffa"] += 1
                embed = build_ffa_win_embed(pseudo, player_id, ps, game_id)
                await channel.send(embed=embed)
                await mark_ffa_win_notified(player_id, game_id)
                notified_any = True
                stats["sent_ffa"] += 1

    scan_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    await set_last_win_notify_stats(
        scan_at,
        stats["sessions"],
        stats["wins_team"] + stats["wins_ffa"],
        stats["sent_team"] + stats["sent_ffa"],
        stats["skipped_notified"],
        stats["missing_game_id"],
        stats["fetch_errors"],
        error_text,
    )
    return {"status": "ok", "notified": notified_any, **stats}


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
    bot.add_view(OFMInscriptionView())
    bot.add_view(OFMReviewView())
    bot.add_view(OFMConfigView())
    bot.add_view(ModAdminPanelView())
    for guild in bot.guilds:
        bot.loop.create_task(update_ofm_board(guild))
        bot.loop.create_task(update_ofm_admin_panel(guild))
        bot.loop.create_task(update_mod_admin_panel(guild))
    bot.loop.create_task(backfill_loop())
    bot.loop.create_task(live_loop())
    bot.loop.create_task(backfill_1v1_loop())
    bot.loop.create_task(live_1v1_loop())
    if WIN_NOTIFY_CHANNEL_ID:
        bot.loop.create_task(win_notify_loop())
    print(f"Bot connected: {bot.user}")


@bot.tree.command(name="inscriptionofm", description="Inscription tournoi OFM.")
async def inscription_ofm(interaction: discord.Interaction):
    embed = discord.Embed(
        title="Inscription Tournoi OFM",
        description=(
            "Clique sur le bouton si tu souhaites devenir l'un des participants\n"
            "pour le tournoi OFM sous le tag [GAL]."
        ),
        color=discord.Color.orange(),
    )
    await interaction.response.send_message(embed=embed, view=OFMInscriptionView())


@bot.tree.command(name="removeofm", description="Retire un joueur du tournoi OFM.")
@app_commands.describe(user="Joueur à retirer")
async def removeofm(interaction: discord.Interaction, user: discord.Member):
    if not interaction.guild:
        await interaction.response.send_message("Commande disponible uniquement sur un serveur.", ephemeral=True)
        return
    manager_role = interaction.guild.get_role(OFM_MANAGER_ROLE_ID)
    if not manager_role or manager_role not in interaction.user.roles:
        await interaction.response.send_message("Accès réservé aux OFM managers.", ephemeral=True)
        return
    role = interaction.guild.get_role(OFM_ROLE_ID)
    team_role = interaction.guild.get_role(OFM_TEAM_ROLE_ID)
    if role and role in user.roles:
        await user.remove_roles(role, reason="Retrait OFM demandé")
    if team_role and team_role in user.roles:
        if team_role.id != manager_role.id:
            await user.remove_roles(team_role, reason="Retrait OFM demandé")
    await upsert_ofm_participant(
        interaction.guild.id,
        user.id,
        "removed",
        team_role.id if team_role else None,
    )
    await update_ofm_board(interaction.guild)
    await interaction.response.send_message(
        f"✅ {user.mention} a été retiré du tournoi OFM.",
        ephemeral=True,
    )


@bot.tree.command(name="setofmpanel", description="Créer/mettre à jour le panel OFM manager.")
async def setofmpanel(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message("Commande disponible uniquement sur un serveur.", ephemeral=True)
        return
    manager_role = interaction.guild.get_role(OFM_MANAGER_ROLE_ID)
    if not manager_role or manager_role not in interaction.user.roles:
        await interaction.response.send_message("Accès réservé aux OFM managers.", ephemeral=True)
        return
    await update_ofm_admin_panel(interaction.guild)
    await interaction.response.send_message("✅ Panel OFM mis à jour.", ephemeral=True)


@bot.tree.command(name="setadminpanel", description="Créer/mettre à jour le panel admin.")
async def setadminpanel(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message("Commande disponible uniquement sur un serveur.", ephemeral=True)
        return
    if not is_admin_member(interaction.user):
        await interaction.response.send_message("Accès réservé fondateur/admin.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=False)
    try:
        await set_mod_config(interaction.guild.id, log_channel_id=MOD_LOG_CHANNEL_ID)
        await update_mod_admin_panel(interaction.guild)
        await interaction.followup.send("✅ Panel admin mis à jour.", ephemeral=True)
    except Exception as exc:
        await interaction.followup.send(f"❌ Erreur panel admin: {exc}", ephemeral=True)


@bot.tree.command(name="resyncleaderboards", description="Force la resync des leaderboards.")
async def resyncleaderboards(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message("Commande disponible uniquement sur un serveur.", ephemeral=True)
        return
    if not is_admin_member(interaction.user):
        await interaction.response.send_message("Accès réservé fondateur/admin.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    try:
        result = await resync_leaderboards(interaction.guild)
        ffa = result.get("ffa") or {}
        ffa_updated = result.get("ffa_updated") or "inconnue"
        onev1_cached_at = result.get("onev1_cached_at")
        onev1_text = onev1_cached_at.strftime("%Y-%m-%d %H:%M") if onev1_cached_at else "inconnue"
        ffa_msg = result.get("ffa_message") or {}
        ffa_msg_text = "OK" if ffa_msg.get("updated") else f"KO ({ffa_msg.get('error')})"
        ffa_rec = result.get("ffa_recovered") or {}
        rec_text = "oui" if ffa_rec.get("recovered") else "non"
        await interaction.followup.send(
            "✅ Resync terminée.\n"
            f"FFA: {ffa.get('success', 0)}/{ffa.get('total', 0)} OK, {ffa.get('failed', 0)} échecs\n"
            f"Dernière maj FFA: {ffa_updated}\n"
            f"Dernier fetch 1v1: {onev1_text}\n"
            f"Maj message FFA: {ffa_msg_text}\n"
            f"Record FFA récupéré: {rec_text}",
            ephemeral=True,
        )
    except Exception as exc:
        await interaction.followup.send(f"❌ Erreur resync: {exc}", ephemeral=True)


@bot.tree.command(name="setleaderboardffaid", description="Lier un message au leaderboard FFA.")
@app_commands.describe(message_id="ID du message leaderboard FFA")
async def setleaderboardffaid(interaction: discord.Interaction, message_id: str):
    if not interaction.guild:
        await interaction.response.send_message("Commande disponible uniquement sur un serveur.", ephemeral=True)
        return
    if not is_admin_member(interaction.user):
        await interaction.response.send_message("Accès réservé fondateur/admin.", ephemeral=True)
        return
    if not message_id.isdigit():
        await interaction.response.send_message("ID invalide.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    msg, channel = await find_message_in_guild(interaction.guild, int(message_id))
    if not msg or not channel:
        await interaction.followup.send("Message introuvable dans le serveur.", ephemeral=True)
        return
    await set_leaderboard_message_ffa(interaction.guild.id, channel.id, msg.id)
    await interaction.followup.send("✅ Leaderboard FFA lié.", ephemeral=True)


@bot.tree.command(name="setleaderboard1v1galid", description="Lier un message au leaderboard 1v1 [GAL].")
@app_commands.describe(message_id="ID du message leaderboard 1v1 [GAL]")
async def setleaderboard1v1galid(interaction: discord.Interaction, message_id: str):
    if not interaction.guild:
        await interaction.response.send_message("Commande disponible uniquement sur un serveur.", ephemeral=True)
        return
    if not is_admin_member(interaction.user):
        await interaction.response.send_message("Accès réservé fondateur/admin.", ephemeral=True)
        return
    if not message_id.isdigit():
        await interaction.response.send_message("ID invalide.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    msg, channel = await find_message_in_guild(interaction.guild, int(message_id))
    if not msg or not channel:
        await interaction.followup.send("Message introuvable dans le serveur.", ephemeral=True)
        return
    await set_leaderboard_message_1v1_gal(interaction.guild.id, channel.id, msg.id)
    await interaction.followup.send("✅ Leaderboard 1v1 [GAL] lié.", ephemeral=True)


@bot.tree.command(name="compare", description="Comparer deux joueurs enregistrés.")
@app_commands.describe(player1="Joueur 1", player2="Joueur 2")
async def compare(interaction: discord.Interaction, player1: discord.Member, player2: discord.Member):
    if not interaction.guild:
        await interaction.response.send_message("Commande disponible uniquement sur un serveur.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=False)
    rec1 = await get_ffa_player(player1.id)
    rec2 = await get_ffa_player(player2.id)
    if not rec1 or not rec2:
        await interaction.followup.send(
            "Les deux joueurs doivent être enregistrés via /register.",
            ephemeral=True,
        )
        return
    try:
        sessions1 = await fetch_player_sessions(rec1["player_id"])
        sessions2 = await fetch_player_sessions(rec2["player_id"])
    except Exception as exc:
        await interaction.followup.send(f"Erreur OpenFront: {exc}", ephemeral=True)
        return
    stats1 = summarize_ffa_sessions(sessions1)
    stats2 = summarize_ffa_sessions(sessions2)
    label1 = f"{rec1['pseudo']} ({player1.display_name})"
    label2 = f"{rec2['pseudo']} ({player2.display_name})"
    chart = build_compare_chart(
        {"name": label1, "stats": stats1},
        {"name": label2, "stats": stats2},
        CLAN_TAG,
    )
    file = discord.File(chart, filename="compare.png")
    embed = discord.Embed(
        title=f"Comparatif [GAL] {rec1['pseudo']} vs {rec2['pseudo']}",
        description="Comparaison basée sur les stats OpenFront FFA.",
        color=discord.Color.orange(),
    )
    embed.set_image(url="attachment://compare.png")
    await interaction.followup.send(embed=embed, file=file)


@bot.tree.command(name="warn", description="Avertir un membre.")
@app_commands.describe(member="Membre à avertir", reason="Raison obligatoire")
async def warn(interaction: discord.Interaction, member: discord.Member, reason: str):
    if not await ensure_mod_permission(interaction, "warn"):
        return
    bot_member = interaction.guild.me
    err = can_moderate_member(interaction.user, member, bot_member)
    if err:
        await interaction.response.send_message(err, ephemeral=True)
        return
    record = await add_warning(interaction.guild.id, member.id, interaction.user.id, reason)
    await add_mod_action(interaction.guild.id, member.id, interaction.user.id, "warn", reason)
    await send_mod_log(
        interaction.guild,
        build_mod_log_embed("warn", member, interaction.user, reason),
    )
    await interaction.response.send_message(
        f"✅ Warn ajouté (ID {record['id']}).",
        ephemeral=True,
    )


@bot.tree.command(name="warnlist", description="Liste des warns d'un membre.")
@app_commands.describe(member="Membre")
async def warnlist(interaction: discord.Interaction, member: discord.Member):
    if not await ensure_mod_permission(interaction, "warnlist"):
        return
    rows = await list_warnings(interaction.guild.id, member.id)
    if not rows:
        await interaction.response.send_message("Aucun warn.", ephemeral=True)
        return
    lines = []
    for row in rows[:15]:
        mod_id = row["moderator_id"]
        reason = row["reason"]
        created_at = row["created_at"]
        lines.append(f"#{row['id']} • <@{mod_id}> • {created_at}\n{reason}")
    embed = discord.Embed(
        title=f"Warns de {member.display_name}",
        description="\n\n".join(lines),
        color=discord.Color.orange(),
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="clearwarn", description="Supprime un warn d'un membre.")
@app_commands.describe(member="Membre", warn_id="ID du warn (vide = tout supprimer)")
async def clearwarn(interaction: discord.Interaction, member: discord.Member, warn_id: Optional[int] = None):
    if not await ensure_mod_permission(interaction, "clearwarn"):
        return
    if warn_id:
        await delete_warning(interaction.guild.id, member.id, warn_id)
        await add_mod_action(interaction.guild.id, member.id, interaction.user.id, "clearwarn", f"warn_id={warn_id}")
        await send_mod_log(
            interaction.guild,
            build_mod_log_embed("clearwarn", member, interaction.user, f"warn_id={warn_id}"),
        )
        await interaction.response.send_message("✅ Warn supprimé.", ephemeral=True)
        return

    async def do_clear(interaction_confirm: discord.Interaction):
        await clear_all_warnings(interaction_confirm.guild.id, member.id)
        await add_mod_action(interaction_confirm.guild.id, member.id, interaction_confirm.user.id, "clearwarn", "all")
        await send_mod_log(
            interaction_confirm.guild,
            build_mod_log_embed("clearwarn", member, interaction_confirm.user, "all"),
        )
        await interaction_confirm.response.send_message("✅ Tous les warns ont été supprimés.", ephemeral=True)

    await interaction.response.send_message(
        "Confirmer la suppression de tous les warns ?",
        view=ModConfirmView(interaction.user.id, do_clear),
        ephemeral=True,
    )


@bot.tree.command(name="mute", description="Mute/timeout un membre.")
@app_commands.describe(member="Membre", duration="Durée (ex: 10m, 2h)", reason="Raison")
async def mute(interaction: discord.Interaction, member: discord.Member, duration: Optional[str] = None, reason: Optional[str] = None):
    if not await ensure_mod_permission(interaction, "mute"):
        return
    bot_member = interaction.guild.me
    err = can_moderate_member(interaction.user, member, bot_member)
    if err:
        await interaction.response.send_message(err, ephemeral=True)
        return
    seconds = parse_duration_seconds(duration) if duration else None
    if seconds is None:
        config = await get_mod_config(interaction.guild.id)
        seconds = config["default_mute_seconds"] if config else 3600
    until = datetime.now(timezone.utc) + timedelta(seconds=seconds)
    await member.timeout(until, reason=reason or "Mute")
    await add_mod_action(interaction.guild.id, member.id, interaction.user.id, "mute", reason, seconds)
    await send_mod_log(
        interaction.guild,
        build_mod_log_embed("mute", member, interaction.user, reason, seconds),
    )
    await interaction.response.send_message("✅ Mute appliqué.", ephemeral=True)


@bot.tree.command(name="kick", description="Kick un membre.")
@app_commands.describe(member="Membre", reason="Raison")
async def kick(interaction: discord.Interaction, member: discord.Member, reason: Optional[str] = None):
    if not await ensure_mod_permission(interaction, "kick"):
        return
    bot_member = interaction.guild.me
    err = can_moderate_member(interaction.user, member, bot_member)
    if err:
        await interaction.response.send_message(err, ephemeral=True)
        return

    async def do_kick(confirm_interaction: discord.Interaction):
        await member.kick(reason=reason or "Kick")
        await add_mod_action(confirm_interaction.guild.id, member.id, confirm_interaction.user.id, "kick", reason)
        await send_mod_log(
            confirm_interaction.guild,
            build_mod_log_embed("kick", member, confirm_interaction.user, reason),
        )
        await confirm_interaction.response.send_message("✅ Membre kick.", ephemeral=True)

    await interaction.response.send_message(
        "Confirmer le kick ?",
        view=ModConfirmView(interaction.user.id, do_kick),
        ephemeral=True,
    )


@bot.tree.command(name="ban", description="Ban un membre.")
@app_commands.describe(member="Membre", duration="Durée (ex: 7d) ou vide = permanent", reason="Raison")
async def ban(interaction: discord.Interaction, member: discord.Member, duration: Optional[str] = None, reason: Optional[str] = None):
    if not await ensure_mod_permission(interaction, "ban"):
        return
    bot_member = interaction.guild.me
    err = can_moderate_member(interaction.user, member, bot_member)
    if err:
        await interaction.response.send_message(err, ephemeral=True)
        return
    seconds = parse_duration_seconds(duration) if duration else None
    if seconds is None and duration:
        await interaction.response.send_message("Durée invalide.", ephemeral=True)
        return
    if seconds is None:
        config = await get_mod_config(interaction.guild.id)
        seconds = config["default_ban_seconds"] if config else 0

    async def do_ban(confirm_interaction: discord.Interaction):
        await confirm_interaction.guild.ban(member, reason=reason or "Ban", delete_message_days=0)
        await add_mod_action(confirm_interaction.guild.id, member.id, confirm_interaction.user.id, "ban", reason, seconds or None)
        await send_mod_log(
            confirm_interaction.guild,
            build_mod_log_embed("ban", member, confirm_interaction.user, reason, seconds or None),
        )
        if seconds:
            bot.loop.create_task(schedule_unban(confirm_interaction.guild, member.id, seconds))
        await confirm_interaction.response.send_message("✅ Membre banni.", ephemeral=True)

    await interaction.response.send_message(
        "Confirmer le ban ?",
        view=ModConfirmView(interaction.user.id, do_ban),
        ephemeral=True,
    )


@bot.tree.command(name="unban", description="Unban un membre.")
@app_commands.describe(user_id="ID Discord du membre", reason="Raison")
async def unban(interaction: discord.Interaction, user_id: str, reason: Optional[str] = None):
    if not await ensure_mod_permission(interaction, "unban"):
        return
    if not user_id.isdigit():
        await interaction.response.send_message("ID invalide.", ephemeral=True)
        return
    target_id = int(user_id)

    async def do_unban(confirm_interaction: discord.Interaction):
        await confirm_interaction.guild.unban(discord.Object(id=target_id), reason=reason or "Unban")
        await add_mod_action(confirm_interaction.guild.id, target_id, confirm_interaction.user.id, "unban", reason)
        await send_mod_log(
            confirm_interaction.guild,
            build_mod_log_embed("unban", discord.Object(id=target_id), confirm_interaction.user, reason),
        )
        await confirm_interaction.response.send_message("✅ Membre débanni.", ephemeral=True)

    await interaction.response.send_message(
        "Confirmer l'unban ?",
        view=ModConfirmView(interaction.user.id, do_unban),
        ephemeral=True,
    )


@bot.tree.command(name="case", description="Afficher le casier d'un membre.")
@app_commands.describe(member="Membre")
async def case(interaction: discord.Interaction, member: discord.Member):
    if not await ensure_mod_permission(interaction, "case"):
        return
    warnings = await list_warnings(interaction.guild.id, member.id)
    actions = await list_mod_actions(interaction.guild.id, member.id)
    notes = await list_mod_notes(interaction.guild.id, member.id)
    counts = {}
    for action in actions:
        action_type = action["action_type"]
        counts[action_type] = counts.get(action_type, 0) + 1
    warn_count = len(warnings)
    embed = discord.Embed(
        title=f"Casier de {member.display_name}",
        color=discord.Color.dark_gold(),
    )
    embed.add_field(name="Arrivée", value=str(member.joined_at) if member.joined_at else "Inconnue", inline=False)
    embed.add_field(
        name="Stats",
        value=f"Warns: {warn_count} | Kicks: {counts.get('kick', 0)} | Bans: {counts.get('ban', 0)}",
        inline=False,
    )
    if actions:
        recent_actions = []
        for action in actions[:5]:
            mod_id = action["moderator_id"]
            reason = action["reason"] or "-"
            recent_actions.append(f"{action['created_at']} • {action['action_type']} • <@{mod_id}> • {reason}")
        embed.add_field(name="Historique récent", value="\n".join(recent_actions), inline=False)
    if notes:
        last_notes = []
        for note in notes[:5]:
            last_notes.append(f"{note['created_at']} • <@{note['moderator_id']}> • {note['note']}")
        embed.add_field(name="Notes internes", value="\n".join(last_notes), inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="note", description="Ajouter une note interne.")
@app_commands.describe(member="Membre", note="Note interne")
async def note(interaction: discord.Interaction, member: discord.Member, note: str):
    if not await ensure_mod_permission(interaction, "note"):
        return
    await add_mod_note(interaction.guild.id, member.id, interaction.user.id, note)
    await add_mod_action(interaction.guild.id, member.id, interaction.user.id, "note", note)
    await send_mod_log(
        interaction.guild,
        build_mod_log_embed("note", member, interaction.user, note),
    )
    await interaction.response.send_message("✅ Note ajoutée.", ephemeral=True)


@bot.tree.command(name="setleaderboard", description="Show the clan leaderboard.")
async def setleaderboard(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message("Commande disponible uniquement sur un serveur.", ephemeral=True)
        return
    
    record = await get_leaderboard_message(interaction.guild.id)
    if record:
        await interaction.response.send_message(
            "Un leaderboard est d�j� actif sur ce serveur. Utilise /removeleaderboard.",
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
    await interaction.response.defer(ephemeral=False)
    try:
        await upsert_ffa_player(interaction.user.id, pseudo, player_id)
        sessions = await fetch_player_sessions(player_id)
        wins, losses = compute_ffa_stats_from_sessions(sessions)
        await upsert_ffa_stats(player_id, pseudo, wins, losses)
    except Exception as exc:
        await interaction.followup.send(f"Erreur: {exc}", ephemeral=True)
        return
    await interaction.followup.send(f"? {pseudo} enregistr� pour le leaderboard FFA.", ephemeral=True)


@bot.tree.command(name="unregister", description="Supprime un joueur du leaderboard FFA.")
@app_commands.describe(user_id="ID Discord \u00e0 d\u00e9sinscrire")
async def unregister(interaction: discord.Interaction, user_id: str):
    if not interaction.guild:
        await interaction.response.send_message("Commande disponible uniquement sur un serveur.", ephemeral=True)
        return
    if not user_id.isdigit():
        await interaction.response.send_message("ID invalide.", ephemeral=True)
        return
    target_id = int(user_id)
    if interaction.user.id != target_id and not is_admin_member(interaction.user):
        await interaction.response.send_message("Acc\u00e8s refus\u00e9.", ephemeral=True)
        return
    record = await delete_ffa_player(target_id)
    if record and record.get("player_id"):
        await delete_ffa_stats_by_player_id(record["player_id"])
    await interaction.response.send_message("✅ Joueur d\u00e9sinscrit du leaderboard FFA.", ephemeral=True)


@bot.tree.command(name="setleaderboardffa", description="Show the FFA leaderboard.")
async def setleaderboardffa(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message("Commande disponible uniquement sur un serveur.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=False)

    record = await get_leaderboard_message_ffa(interaction.guild.id)
    if record:
        await interaction.followup.send(
            "Un leaderboard FFA est d�j� actif. Utilise /removeleaderboardffa.",
            ephemeral=True,
        )
        return

    embed = await build_leaderboard_ffa_embed(interaction.guild, 1, 20)
    if not embed:
        await interaction.followup.send(
            f"Aucune donn�e FFA. Enregistre-toi avec /register.",
            ephemeral=True,
        )
        return

    try:
        try:
            embed = await asyncio.wait_for(
                build_leaderboard_ffa_embed(interaction.guild, 1, 20),
                timeout=10,
            )
        except asyncio.TimeoutError:
            await interaction.followup.send(
                "Le leaderboard FFA prend trop de temps à se générer.",
                ephemeral=True,
            )
            return
        if not embed:
            await interaction.followup.send(
                f"Aucune donn\u00e9e FFA. Enregistre-toi avec /register.",
                ephemeral=True,
            )
            return
        message = await interaction.followup.send(embed=embed, view=LeaderboardFfaView(1, 20), wait=True)
        await set_leaderboard_message_ffa(interaction.guild.id, interaction.channel_id, message.id)
        bot.loop.create_task(refresh_ffa_stats())
    except Exception as exc:
        await interaction.followup.send(f"Erreur leaderboard FFA: {exc}", ephemeral=True)


@bot.tree.command(name="removeleaderboardffa", description="Supprime le leaderboard FFA du serveur.")
async def removeleaderboardffa(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message("Commande disponible uniquement sur un serveur.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    record = await get_leaderboard_message_ffa(interaction.guild.id)
    if not record:
        channel = interaction.channel
        if isinstance(channel, discord.TextChannel):
            try:
                async for msg in channel.history(limit=50):
                    if msg.author == bot.user and msg.embeds:
                        title = msg.embeds[0].title or ""
                        if "Leaderboard FFA" in title:
                            await msg.delete()
                            await clear_leaderboard_message_ffa(interaction.guild.id)
                            await interaction.followup.send("Leaderboard FFA supprim\u00e9.", ephemeral=True)
                            return
            except Exception:
                pass
        await interaction.followup.send("Aucun leaderboard FFA actif.", ephemeral=True)
        return
    try:
        channel = bot.get_channel(record["channel_id"]) or await bot.fetch_channel(record["channel_id"])
        message = await channel.fetch_message(record["message_id"])
        await message.delete()
    except Exception:
        pass
    await clear_leaderboard_message_ffa(interaction.guild.id)
    await interaction.followup.send("Leaderboard FFA supprim\u00e9.", ephemeral=True)


@bot.tree.command(name="setleaderboard1v1", description="Show the 1v1 leaderboard.")
async def setleaderboard1v1(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message("Commande disponible uniquement sur un serveur.", ephemeral=True)
        return

    record = await get_leaderboard_message_1v1(interaction.guild.id)
    if record:
        await interaction.response.send_message(
            "Un leaderboard 1v1 est d�j� actif. Utilise /removeleaderboard1v1.",
            ephemeral=True,
        )
        return

    embed = await build_leaderboard_1v1_embed(interaction.guild, 1, 20)
    if not embed:
        await interaction.response.send_message(
            "Aucune donn�e 1v1 disponible pour le moment.",
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
    await interaction.response.send_message("Leaderboard 1v1 supprim�.", ephemeral=True)


@bot.tree.command(name="setleaderboard1v1gal", description="Show the 1v1 leaderboard for [GAL] members.")
async def setleaderboard1v1gal(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message("Commande disponible uniquement sur un serveur.", ephemeral=True)
        return

    record = await get_leaderboard_message_1v1_gal(interaction.guild.id)
    if record:
        await interaction.response.send_message(
            "Un leaderboard 1v1 [GAL] est d�j� actif. Utilise /removeleaderboard1v1gal.",
            ephemeral=True,
        )
        return

    embed = await build_leaderboard_1v1_gal_embed(interaction.guild)
    if not embed:
        await interaction.response.send_message(
            "Aucune donn�e 1v1 [GAL] disponible pour le moment.",
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
    await interaction.response.send_message("Leaderboard 1v1 [GAL] supprim�.", ephemeral=True)


@bot.tree.command(name="checkwinsgal", description="Force un check des victoires [GAL].")
async def checkwinsgal(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    try:
        result = await run_win_notify_once(force_empty=True)
    except Exception as exc:
        await interaction.followup.send(f"Erreur: {exc}", ephemeral=True)
        return
    if result.get("status") != "ok":
        await interaction.followup.send(f"Erreur: {result.get('error')}", ephemeral=True)
        return
    wins_team = result.get("wins_team", 0)
    wins_ffa = result.get("wins_ffa", 0)
    sent_team = result.get("sent_team", 0)
    sent_ffa = result.get("sent_ffa", 0)
    skipped = result.get("skipped_notified", 0)
    missing = result.get("missing_game_id", 0)
    errors = result.get("fetch_errors", 0)
    if sent_team + sent_ffa > 0:
        message = (
            f"✅ Team envoyées: {sent_team} | FFA envoyées: {sent_ffa}.\n"
            f"Total Team: {wins_team} | Total FFA: {wins_ffa}.\n"
            f"Déjà notifiées: {skipped}."
        )
    else:
        message = (
            "❌ Aucune nouvelle victoire à envoyer.\n"
            f"Total Team: {wins_team} | Total FFA: {wins_ffa}.\n"
            f"Déjà notifiées: {skipped}.\n"
            f"Sans gameId: {missing} | Erreurs fetch: {errors}."
        )
    await interaction.followup.send(message, ephemeral=True)


@bot.tree.command(name="winscanstatus", description="Affiche le dernier scan auto des victoires [GAL].")
async def winscanstatus(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    stats = await get_last_win_notify_stats()
    if not stats:
        await interaction.followup.send("Aucun scan enregistré.", ephemeral=True)
        return
    scan_at = stats["last_scan_at"] or "inconnu"
    message = (
        f"Dernier scan: {scan_at}\n"
        f"Sessions: {stats['sessions']} | Wins: {stats['wins']} | Envoyées: {stats['sent']}\n"
        f"Déjà notifiées: {stats['skipped']} | Sans gameId: {stats['missing_game_id']} | "
        f"Erreurs fetch: {stats['fetch_errors']}"
    )
    if stats.get("error"):
        message += f"\nErreur: {stats['error']}"
    await interaction.followup.send(message, ephemeral=True)


@bot.tree.command(name="resetwinsnotify", description="Réinitialise les victoires déjà notifiées.")
async def resetwinsnotify(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    try:
        async with pool.acquire() as conn:
            await conn.execute("TRUNCATE TABLE win_notifications")
            await conn.execute("TRUNCATE TABLE ffa_win_notifications")
    except Exception as exc:
        await interaction.followup.send(f"Erreur: {exc}", ephemeral=True)
        return
    await interaction.followup.send(
        "✅ Notifications réinitialisées (Team + FFA). Les prochains scans renverront les victoires dans la fenêtre.",
        ephemeral=True,
    )


@bot.tree.command(name="winsessionsdebug", description="Debug sessions clan [GAL] (fenêtre de scan).")
async def winsessionsdebug(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(hours=WIN_NOTIFY_RANGE_HOURS)
    start_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_iso = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        async with aiohttp.ClientSession(headers={"User-Agent": USER_AGENT}) as session:
            sessions = await fetch_clan_sessions(session, start_iso, end_iso)
    except Exception as exc:
        await interaction.followup.send(f"Erreur API: {exc}", ephemeral=True)
        return

    total = len(sessions)
    wins = sum(1 for s in sessions if s.get("hasWon"))
    samples = []
    try:
        async with aiohttp.ClientSession(headers={"User-Agent": USER_AGENT}) as game_session:
            for s in sessions[:5]:
                game_id = s.get("gameId") or "?"
                has_won = s.get("hasWon")
                mode = s.get("gameMode") or s.get("mode") or "?"
                start = s.get("start") or s.get("startTime") or "?"
                gal_won = "?"
                if game_id != "?":
                    try:
                        info = await fetch_game_info(game_session, game_id)
                        gal_won = clan_won_game(info)
                    except Exception as exc:
                        gal_won = f"err:{str(exc)[:60]}"
                samples.append(
                    f"- gameId={game_id} | hasWon={has_won} | galWon={gal_won} | mode={mode} | start={start}"
                )
    except Exception as exc:
        samples.append(f"- erreur fetch game info: {exc}")
    sample_text = "\n".join(samples) if samples else "Aucune session."
    message = (
        f"Fenêtre: {start_iso} → {end_iso}\n"
        f"Sessions: {total} | Wins: {wins}\n"
        f"{sample_text}"
    )
    await interaction.followup.send(message, ephemeral=True)


@bot.tree.command(name="wingamedebug", description="Debug une game OpenFront par ID.")
@app_commands.describe(game_id="ID de la game OpenFront")
async def wingamedebug(interaction: discord.Interaction, game_id: str):
    await interaction.response.defer(ephemeral=True)
    try:
        async with aiohttp.ClientSession(headers={"User-Agent": USER_AGENT}) as session:
            info = await fetch_game_info(session, game_id)
    except Exception as exc:
        await interaction.followup.send(f"Erreur API: {exc}", ephemeral=True)
        return

    winner_raw = info.get("winner")
    winners_ids = sorted(get_winner_client_ids(info))
    mode = game_mode(info) or "?"
    teams = (info.get("config", {}) or {}).get("playerTeams") or "?"

    players = []
    for p in info.get("players", [])[:12]:
        username = p.get("username") or "?"
        client_id = p.get("clientID") or "?"
        players.append(f"{username}({client_id})")
    players_text = ", ".join(players) if players else "Aucun joueur"

    message = (
        f"Game: {game_id}\n"
        f"Mode: {mode} | playerTeams: {teams}\n"
        f"Winner raw: {str(winner_raw)[:200]}\n"
        f"Winner IDs: {winners_ids}\n"
        f"Players: {players_text}"
    )
    await interaction.followup.send(message, ephemeral=True)


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
    await interaction.response.send_message("Leaderboard supprim�.", ephemeral=True)


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


@bot.tree.command(name="reset_leaderboard", description="R�initialise le leaderboard (Postgres).")
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
        f"OK: leaderboard r�initialis�. Nouveau d�part: {BACKFILL_START}",
        ephemeral=True,
    )


if __name__ == "__main__":
    if not TOKEN:
        raise ValueError("DISCORD_TOKEN missing.")
    if not DB_URL:
        raise ValueError("DATABASE_URL missing (Postgres).")
bot.run(TOKEN)

