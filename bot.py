import os
import json
import sqlite3
import asyncio
from datetime import datetime, timezone, timedelta

import aiohttp
import discord
from discord.ext import commands

TOKEN = os.getenv("DISCORD_TOKEN") or os.getenv("DISCORD_BOT_TOKEN")
GUILD_ID = os.getenv("DISCORD_GUILD_ID")

DB_PATH = "leaderboard.db"
CLAN_TAG = os.getenv("CLAN_TAG", "GAL")
CLAN_DISPLAY = f"[{CLAN_TAG}]"

API_BASE = "https://api.openfront.io/public"
USER_AGENT = "Mozilla/5.0 (GauloisBot)"

REFRESH_MINUTES = int(os.getenv("LEADERBOARD_REFRESH_MINUTES", "30"))
RANGE_HOURS = int(os.getenv("LEADERBOARD_RANGE_HOURS", "24"))
MAX_SESSIONS = int(os.getenv("LEADERBOARD_MAX_SESSIONS", "300"))
BACKFILL_START = os.getenv("LEADERBOARD_BACKFILL_START", "2025-11-01T00:00:00Z")
BACKFILL_INTERVAL_MINUTES = int(os.getenv("LEADERBOARD_BACKFILL_INTERVAL_MINUTES", "10"))

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

intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)


def get_db():
    return sqlite3.connect(DB_PATH)


def init_db():
    with get_db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS player_stats (
                username TEXT PRIMARY KEY,
                wins_ffa INTEGER DEFAULT 0,
                losses_ffa INTEGER DEFAULT 0,
                wins_team INTEGER DEFAULT 0,
                losses_team INTEGER DEFAULT 0,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS processed_games (
                game_id TEXT PRIMARY KEY
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS backfill_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                cursor TEXT NOT NULL,
                completed INTEGER DEFAULT 0,
                last_attempt TEXT,
                last_error TEXT
            )
            """
        )
        # Migrations for older tables
        columns = {row[1] for row in conn.execute("PRAGMA table_info(backfill_state)").fetchall()}
        if "last_attempt" not in columns:
            conn.execute("ALTER TABLE backfill_state ADD COLUMN last_attempt TEXT")
        if "last_error" not in columns:
            conn.execute("ALTER TABLE backfill_state ADD COLUMN last_error TEXT")


def get_backfill_state():
    with get_db() as conn:
        row = conn.execute(
            "SELECT cursor, completed, last_attempt, last_error FROM backfill_state WHERE id = 1"
        ).fetchone()
    if row:
        return row[0], bool(row[1]), row[2], row[3]
    with get_db() as conn:
        conn.execute(
            "INSERT INTO backfill_state (id, cursor, completed) VALUES (1, ?, 0)",
            (BACKFILL_START,),
        )
    return BACKFILL_START, False, None, None


def set_backfill_state(cursor, completed, last_attempt=None, last_error=None):
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO backfill_state (id, cursor, completed, last_attempt, last_error)
            VALUES (1, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                cursor = excluded.cursor,
                completed = excluded.completed,
                last_attempt = excluded.last_attempt,
                last_error = excluded.last_error
            """,
            (cursor, 1 if completed else 0, last_attempt, last_error),
        )


def is_clan_username(username: str) -> bool:
    if not username:
        return False
    upper = username.upper()
    tag = CLAN_TAG.upper()
    return f"[{tag}]" in upper or upper.startswith(f"{tag} ")


def calculate_ratio(wins_ffa, losses_ffa, wins_team, losses_team):
    wins = wins_ffa + wins_team
    losses = losses_ffa + losses_team
    return wins / (losses + 1)


def is_game_processed(game_id: str) -> bool:
    with get_db() as conn:
        row = conn.execute(
            "SELECT 1 FROM processed_games WHERE game_id = ?",
            (game_id,),
        ).fetchone()
    return row is not None


def mark_game_processed(game_id: str):
    with get_db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO processed_games (game_id) VALUES (?)",
            (game_id,),
        )


def upsert_player(username, wins_ffa, losses_ffa, wins_team, losses_team):
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO player_stats (
                username, wins_ffa, losses_ffa, wins_team, losses_team, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(username) DO UPDATE SET
                wins_ffa = wins_ffa + excluded.wins_ffa,
                losses_ffa = losses_ffa + excluded.losses_ffa,
                wins_team = wins_team + excluded.wins_team,
                losses_team = losses_team + excluded.losses_team,
                updated_at = excluded.updated_at
            """,
            (
                username,
                wins_ffa,
                losses_ffa,
                wins_team,
                losses_team,
                datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )


def load_leaderboard():
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT username, wins_ffa, losses_ffa, wins_team, losses_team, updated_at
            FROM player_stats
            """
        ).fetchall()
    players = []
    last_updated = None
    for username, wins_ffa, losses_ffa, wins_team, losses_team, updated_at in rows:
        ratio = calculate_ratio(wins_ffa, losses_ffa, wins_team, losses_team)
        total_wins = wins_ffa + wins_team
        players.append(
            {
                "username": username,
                "wins_ffa": wins_ffa,
                "losses_ffa": losses_ffa,
                "wins_team": wins_team,
                "losses_team": losses_team,
                "ratio": ratio,
                "total_wins": total_wins,
            }
        )
        if updated_at:
            last_updated = updated_at
    players.sort(key=lambda p: (p["ratio"], p["total_wins"]), reverse=True)
    return players, last_updated


def get_progress_stats():
    with get_db() as conn:
        row = conn.execute(
            """
            SELECT
                COUNT(*) AS players,
                COALESCE(SUM(wins_ffa + wins_team), 0) AS wins_total,
                COALESCE(SUM(losses_ffa + losses_team), 0) AS losses_total
            FROM player_stats
            """
        ).fetchone()
        games_row = conn.execute(
            "SELECT COUNT(*) FROM processed_games"
        ).fetchone()
    return {
        "players": row[0] if row else 0,
        "wins_total": row[1] if row else 0,
        "losses_total": row[2] if row else 0,
        "games_processed": games_row[0] if games_row else 0,
    }


def game_mode(info):
    return (info.get("config", {}) or {}).get("gameMode") or ""


def process_game(info, clan_has_won):
    mode = game_mode(info).lower()
    is_ffa = "free for all" in mode or mode == "ffa"
    is_team = "team" in mode

    for p in info.get("players", []):
        username = p.get("username") or ""
        if not is_clan_username(username):
            continue

        if is_ffa:
            if clan_has_won:
                upsert_player(username, 1, 0, 0, 0)
            else:
                upsert_player(username, 0, 1, 0, 0)
        elif is_team:
            if clan_has_won:
                upsert_player(username, 0, 0, 1, 0)
            else:
                upsert_player(username, 0, 0, 0, 1)


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


async def refresh_from_range(start_dt, end_dt):
    start_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_iso = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    headers = {"User-Agent": USER_AGENT}
    async with aiohttp.ClientSession(headers=headers) as session:
        sessions = await fetch_clan_sessions(session, start_iso, end_iso)
        sessions = sessions[:MAX_SESSIONS]

        for s in sessions:
            game_id = s.get("gameId")
            if not game_id:
                continue
            if is_game_processed(game_id):
                continue
            try:
                info = await fetch_game_info(session, game_id)
            except Exception:
                continue
            clan_has_won = bool(s.get("hasWon"))
            process_game(info, clan_has_won)
            mark_game_processed(game_id)

        return len(sessions)


async def backfill_loop():
    while True:
        await run_backfill_step()
        await asyncio.sleep(BACKFILL_INTERVAL_MINUTES * 60)


async def run_backfill_step():
    cursor, completed, _last_attempt, _last_error = get_backfill_state()
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
        await refresh_from_range(start_dt, end_dt)
    except Exception as exc:
        last_error = str(exc)[:500]
        set_backfill_state(cursor, False, last_attempt, last_error)
        print(f"Backfill failed: {exc}")
        return {"status": "error", "cursor": cursor, "error": last_error}

    new_cursor = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    completed = end_dt >= now_dt
    set_backfill_state(new_cursor, completed, last_attempt, last_error)
    print(f"Backfill step: {cursor} -> {new_cursor} (done={completed})")
    return {"status": "ok", "cursor": new_cursor, "completed": completed}


async def live_loop():
    while True:
        try:
            end_dt = datetime.now(timezone.utc)
            start_dt = end_dt - timedelta(hours=RANGE_HOURS)
            await refresh_from_range(start_dt, end_dt)
            print("Live refresh done")
        except Exception as exc:
            print(f"Live refresh failed: {exc}")
        await asyncio.sleep(REFRESH_MINUTES * 60)


@bot.event
async def on_ready():
    init_db()
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

    bot.loop.create_task(backfill_loop())
    bot.loop.create_task(live_loop())
    print(f"Bot connected: {bot.user}")


@bot.tree.command(name="setleaderboard", description="Show the clan leaderboard.")
async def setleaderboard(interaction: discord.Interaction):
    players, last_updated = load_leaderboard()
    if not players:
        await interaction.response.send_message(
            f"No data for {CLAN_DISPLAY}. Wait for refresh.",
            ephemeral=True,
        )
        return

    embed = discord.Embed(
        title=f"Leaderboard {CLAN_DISPLAY} - Top 30",
        color=discord.Color.orange(),
    )

    for i, p in enumerate(players[:30], 1):
        embed.add_field(
            name=f"#{i} {p['username']}",
            value=(
                f"Ratio: {p['ratio']:.2f}\n"
                f"FFA: {p['wins_ffa']}W / {p['losses_ffa']}L\n"
                f"TEAM: {p['wins_team']}W / {p['losses_team']}L"
            ),
            inline=False,
        )

    if last_updated:
        embed.set_footer(text=f"Updated {last_updated}")

    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="refresh_leaderboard", description="Force a live refresh.")
async def refresh_leaderboard_cmd(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    try:
        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(hours=RANGE_HOURS)
        await refresh_from_range(start_dt, end_dt)
        await interaction.followup.send("OK: leaderboard refreshed.", ephemeral=True)
    except Exception as exc:
        await interaction.followup.send(f"Error: {exc}", ephemeral=True)


@bot.tree.command(name="backfill_step", description="Force une tranche de backfill (48h).")
async def backfill_step_cmd(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    result = await run_backfill_step()
    await interaction.followup.send(f"{result}", ephemeral=True)


@bot.tree.command(name="debug_api", description="Debug OpenFront API.")
async def debug_api(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    cursor, completed, last_attempt, last_error = get_backfill_state()
    msg = (
        f"Tag: {CLAN_TAG}\n"
        f"Backfill cursor: {cursor}\n"
        f"Backfill done: {completed}\n"
        f"Last attempt: {last_attempt}\n"
        f"Last error: {last_error}\n"
        f"Range hours: {RANGE_HOURS}\n"
        f"Refresh minutes: {REFRESH_MINUTES}\n"
        f"Backfill interval minutes: {BACKFILL_INTERVAL_MINUTES}"
    )
    await interaction.followup.send(msg, ephemeral=True)


@bot.tree.command(name="stats_progress", description="Affiche la progression du backfill.")
async def stats_progress(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    cursor, completed, last_attempt, last_error = get_backfill_state()
    stats = get_progress_stats()
    msg = (
        f"Backfill cursor: {cursor}\n"
        f"Backfill done: {completed}\n"
        f"Last attempt: {last_attempt}\n"
        f"Last error: {last_error}\n"
        f"Games traitées: {stats['games_processed']}\n"
        f"Joueurs connus: {stats['players']}\n"
        f"Wins total: {stats['wins_total']}\n"
        f"Losses total: {stats['losses_total']}"
    )
    await interaction.followup.send(msg, ephemeral=True)


if __name__ == "__main__":
    if not TOKEN:
        raise ValueError("DISCORD_TOKEN missing.")
    bot.run(TOKEN)
