import os
import json
import asyncio
from datetime import datetime, timezone, timedelta

import aiohttp
import asyncpg
import discord
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

API_BASE = "https://api.openfront.io/public"
USER_AGENT = "Mozilla/5.0 (GauloisBot)"

REFRESH_MINUTES = int(os.getenv("LEADERBOARD_REFRESH_MINUTES", "30"))
RANGE_HOURS = int(os.getenv("LEADERBOARD_RANGE_HOURS", "24"))
MAX_SESSIONS = int(os.getenv("LEADERBOARD_MAX_SESSIONS", "300"))
BACKFILL_START = os.getenv("LEADERBOARD_BACKFILL_START", "2026-01-01T00:00:00Z")
BACKFILL_INTERVAL_MINUTES = int(os.getenv("LEADERBOARD_BACKFILL_INTERVAL_MINUTES", "5"))

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

pool = None


def calculate_ratio(wins_ffa, losses_ffa, wins_team, losses_team):
    wins = wins_ffa + wins_team
    losses = losses_ffa + losses_team
    return wins / (losses + 1)


def is_clan_username(username: str) -> bool:
    if not username:
        return False
    upper = username.upper()
    tag = CLAN_TAG.upper()
    return f"[{tag}]" in upper or upper.startswith(f"{tag} ")


def game_mode(info):
    return (info.get("config", {}) or {}).get("gameMode") or ""


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
                wins_ffa INTEGER DEFAULT 0,
                losses_ffa INTEGER DEFAULT 0,
                wins_team INTEGER DEFAULT 0,
                losses_team INTEGER DEFAULT 0,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
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


async def mark_game_processed(game_id: str):
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO processed_games (game_id) VALUES ($1) ON CONFLICT DO NOTHING",
            game_id,
        )


async def upsert_player(username, wins_ffa, losses_ffa, wins_team, losses_team):
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO player_stats (
                username, wins_ffa, losses_ffa, wins_team, losses_team, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT(username) DO UPDATE SET
                wins_ffa = player_stats.wins_ffa + EXCLUDED.wins_ffa,
                losses_ffa = player_stats.losses_ffa + EXCLUDED.losses_ffa,
                wins_team = player_stats.wins_team + EXCLUDED.wins_team,
                losses_team = player_stats.losses_team + EXCLUDED.losses_team,
                updated_at = EXCLUDED.updated_at
            """,
            username,
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
            SELECT username, wins_ffa, losses_ffa, wins_team, losses_team, updated_at
            FROM player_stats
            """
        )
    players = []
    last_updated = None
    for row in rows:
        ratio = calculate_ratio(row[1], row[2], row[3], row[4])
        total_wins = row[1] + row[3]
        players.append(
            {
                "username": row[0],
                "wins_ffa": row[1],
                "losses_ffa": row[2],
                "wins_team": row[3],
                "losses_team": row[4],
                "ratio": ratio,
                "total_wins": total_wins,
            }
        )
        if row[5]:
            last_updated = row[5]
    players.sort(key=lambda p: (p["ratio"], p["total_wins"]), reverse=True)
    return players, last_updated


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
        username = p.get("username") or ""
        if not is_clan_username(username):
            continue

        if is_ffa:
            if clan_has_won:
                asyncio.create_task(upsert_player(username, 1, 0, 0, 0))
            else:
                asyncio.create_task(upsert_player(username, 0, 1, 0, 0))
        elif is_team:
            if clan_has_won:
                asyncio.create_task(upsert_player(username, 0, 0, 1, 0))
            else:
                asyncio.create_task(upsert_player(username, 0, 0, 0, 1))


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
        except Exception as exc:
            print(f"Live refresh failed: {exc}")
        await asyncio.sleep(REFRESH_MINUTES * 60)


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

    bot.loop.create_task(backfill_loop())
    bot.loop.create_task(live_loop())
    print(f"Bot connected: {bot.user}")


@bot.tree.command(name="setleaderboard", description="Show the clan leaderboard.")
async def setleaderboard(interaction: discord.Interaction):
    players, last_updated = await load_leaderboard()
    if not players:
        await interaction.response.send_message(
            f"No data for {CLAN_DISPLAY}. Wait for refresh.",
            ephemeral=True,
        )
        return

    embed = discord.Embed(
        title=f"🏆 Leaderboard {CLAN_DISPLAY} - Top 30",
        color=discord.Color.orange(),
    )

    top = players[:30]
    total_wins = sum(p["wins_ffa"] + p["wins_team"] for p in top)
    total_losses = sum(p["losses_ffa"] + p["losses_team"] for p in top)
    total_players = len(top)

    embed.description = (
        f"**Joueurs:** {total_players}  |  "
        f"**Wins:** {total_wins}  |  "
        f"**Losses:** {total_losses}"
    )
    if interaction.guild and interaction.guild.icon:
        embed.set_thumbnail(url=interaction.guild.icon.url)

    medals = ["🥇", "🥈", "🥉"]
    for idx, p in enumerate(top[:3]):
        ratio = f"{p['ratio']:.2f}"
        embed.add_field(
            name=f"{medals[idx]} {p['username']}",
            value=(
                f"Ratio: **{ratio}**\n"
                f"FFA: `{p['wins_ffa']}W / {p['losses_ffa']}L`\n"
                f"TEAM: `{p['wins_team']}W / {p['losses_team']}L`"
            ),
            inline=False,
        )

    def format_line(rank, player):
        username = player["username"][:14]
        ratio = f"{player['ratio']:.2f}"
        ffa = f"{player['wins_ffa']}W/{player['losses_ffa']}L"
        team = f"{player['wins_team']}W/{player['losses_team']}L"
        return f"{rank:<3} {username:<14} {ratio:>5}  {ffa:>7}  {team:>7}"

    header = f"{'#':<3} {'JOUEUR':<14} {'RATIO':>5}  {'FFA':>7}  {'TEAM':>7}"
    sep = "-" * 44

    col1 = [header, sep]
    col2 = [header, sep]
    col3 = [header, sep]

    for i, p in enumerate(top[3:], 4):
        if i <= 12:
            col1.append(format_line(i, p))
        elif i <= 21:
            col2.append(format_line(i, p))
        else:
            col3.append(format_line(i, p))

    if len(col1) > 2:
        embed.add_field(name="Top 4-12", value="```\n" + "\n".join(col1) + "\n```", inline=False)
    if len(col2) > 2:
        embed.add_field(name="Top 13-21", value="```\n" + "\n".join(col2) + "\n```", inline=False)
    if len(col3) > 2:
        embed.add_field(name="Top 22-30", value="```\n" + "\n".join(col3) + "\n```", inline=False)

    if last_updated:
        embed.set_footer(text=f"Mis à jour le {last_updated}")

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
