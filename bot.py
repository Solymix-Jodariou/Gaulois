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
VERSION = "2026-02-03-commands-sync-2"

DB_PATH = "leaderboard.db"
CLAN_TAG = os.getenv("CLAN_TAG", "GAL")
CLAN_DISPLAY = f"[{CLAN_TAG}]"

API_BASE = "https://api.openfront.io/public"
USER_AGENT = "Mozilla/5.0 (GauloisBot)"

REFRESH_MINUTES = int(os.getenv("LEADERBOARD_REFRESH_MINUTES", "30"))
RANGE_HOURS = int(os.getenv("LEADERBOARD_RANGE_HOURS", "24"))
MAX_SESSIONS = int(os.getenv("LEADERBOARD_MAX_SESSIONS", "300"))

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

intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)

leaderboard_cache = []
leaderboard_updated_at = None
leaderboard_meta = {"sessions": 0, "games": 0, "players": 0}


def get_db():
    return sqlite3.connect(DB_PATH)


def init_db():
    with get_db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS clan_cache (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                data TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                meta TEXT NOT NULL
            )
            """
        )


def load_cache():
    global leaderboard_cache, leaderboard_updated_at, leaderboard_meta
    with get_db() as conn:
        row = conn.execute(
            "SELECT data, updated_at, meta FROM clan_cache WHERE id = 1"
        ).fetchone()
    if row:
        leaderboard_cache = json.loads(row[0])
        leaderboard_updated_at = row[1]
        leaderboard_meta = json.loads(row[2])


def save_cache(data, updated_at, meta):
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO clan_cache (id, data, updated_at, meta)
            VALUES (1, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                data = excluded.data,
                updated_at = excluded.updated_at,
                meta = excluded.meta
            """,
            (json.dumps(data), updated_at, json.dumps(meta)),
        )


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


def compute_from_games(game_infos, session_wins):
    players = {}

    for info, has_won in game_infos:
        mode = game_mode(info).lower()
        for p in info.get("players", []):
            username = p.get("username") or ""
            if not is_clan_username(username):
                continue

            entry = players.setdefault(
                username,
                {"wins_ffa": 0, "losses_ffa": 0, "wins_team": 0, "losses_team": 0},
            )

            if "free for all" in mode or mode == "ffa":
                if has_won:
                    entry["wins_ffa"] += 1
                else:
                    entry["losses_ffa"] += 1
            elif "team" in mode:
                if has_won:
                    entry["wins_team"] += 1
                else:
                    entry["losses_team"] += 1

    results = []
    for username, stats in players.items():
        ratio = calculate_ratio(
            stats["wins_ffa"],
            stats["losses_ffa"],
            stats["wins_team"],
            stats["losses_team"],
        )
        total_wins = stats["wins_ffa"] + stats["wins_team"]
        results.append(
            {
                "pseudo": username,
                "wins_ffa": stats["wins_ffa"],
                "losses_ffa": stats["losses_ffa"],
                "wins_team": stats["wins_team"],
                "losses_team": stats["losses_team"],
                "ratio": ratio,
                "total_wins": total_wins,
            }
        )

    results.sort(key=lambda p: (p["ratio"], p["total_wins"]), reverse=True)
    return results


async def fetch_clan_sessions(start_iso, end_iso):
    url = f"{API_BASE}/clan/{CLAN_TAG}/sessions"
    params = {"start": start_iso, "end": end_iso}
    headers = {"User-Agent": USER_AGENT}
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(url, params=params, timeout=25) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise RuntimeError(f"HTTP {resp.status}: {text[:200]}")
            return await resp.json()


async def fetch_game_info(game_id):
    url = f"{API_BASE}/game/{game_id}"
    headers = {"User-Agent": USER_AGENT}
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(url, params={"turns": "false"}, timeout=25) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise RuntimeError(f"HTTP {resp.status}: {text[:200]}")
            data = await resp.json()
            return data.get("info", {})


async def refresh_leaderboard():
    global leaderboard_cache, leaderboard_updated_at, leaderboard_meta

    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(hours=RANGE_HOURS)
    start_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_iso = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    sessions = await fetch_clan_sessions(start_iso, end_iso)
    sessions = sessions[:MAX_SESSIONS]

    game_infos = []
    for s in sessions:
        game_id = s.get("gameId")
        if not game_id:
            continue
        has_won = bool(s.get("hasWon"))
        try:
            info = await fetch_game_info(game_id)
            game_infos.append((info, has_won))
        except Exception:
            continue

    leaderboard_cache = compute_from_games(game_infos, sessions)
    leaderboard_updated_at = end_dt.strftime("%Y-%m-%d %H:%M UTC")
    leaderboard_meta = {
        "sessions": len(sessions),
        "games": len(game_infos),
        "players": len(leaderboard_cache),
    }
    save_cache(leaderboard_cache, leaderboard_updated_at, leaderboard_meta)


async def refresh_loop():
    while True:
        try:
            await refresh_leaderboard()
            print("Leaderboard refreshed")
        except Exception as exc:
            print(f"Leaderboard refresh failed: {exc}")
        await asyncio.sleep(REFRESH_MINUTES * 60)


@bot.event
async def on_ready():
    init_db()
    load_cache()
    print(f"Bot version: {VERSION}")
    try:
        if GUILD_ID:
            guild = discord.Object(id=int(GUILD_ID))
            # Sync guild commands (fast)
            await bot.tree.sync(guild=guild)
            print(f"Commands synced for guild {GUILD_ID}")
            # Remove old global commands like /register
            bot.tree.clear_commands(guild=None)
            await bot.tree.sync(guild=None)
            print("Global commands cleared and synced")
        else:
            await bot.tree.sync()
            print("Commands synced globally")
    except Exception as exc:
        print(f"Command sync error: {exc}")

    bot.loop.create_task(refresh_loop())
    print(f"Bot connected: {bot.user}")
    print("Registered commands:", [c.name for c in bot.tree.get_commands()])


@bot.tree.command(name="setleaderboard", description="Show the clan leaderboard.")
async def setleaderboard(interaction: discord.Interaction):
    if not leaderboard_cache:
        await interaction.response.send_message(
            f"No data for {CLAN_DISPLAY}. Wait for refresh or increase range.",
            ephemeral=True,
        )
        return

    embed = discord.Embed(
        title=f"Leaderboard {CLAN_DISPLAY} - Top 30",
        color=discord.Color.orange(),
    )

    for i, p in enumerate(leaderboard_cache[:30], 1):
        embed.add_field(
            name=f"#{i} {p['pseudo']}",
            value=(
                f"Ratio: {p['ratio']:.2f}\n"
                f"FFA: {p['wins_ffa']}W / {p['losses_ffa']}L\n"
                f"TEAM: {p['wins_team']}W / {p['losses_team']}L"
            ),
            inline=False,
        )

    if leaderboard_updated_at:
        embed.set_footer(
            text=(
                f"Updated {leaderboard_updated_at} | "
                f"Sessions: {leaderboard_meta['sessions']} | "
                f"Games: {leaderboard_meta['games']}"
            )
        )

    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="debug_api", description="Debug OpenFront API.")
async def debug_api(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(hours=RANGE_HOURS)
    start_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_iso = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    try:
        sessions = await fetch_clan_sessions(start_iso, end_iso)
        count = len(sessions)
        sample = sessions[0] if count > 0 else None
        msg = (
            f"OK\n"
            f"Tag: {CLAN_TAG}\n"
            f"Range: {start_iso} -> {end_iso}\n"
            f"Sessions: {count}\n"
            f"Sample: {sample}"
        )
        await interaction.followup.send(msg, ephemeral=True)
    except Exception as exc:
        await interaction.followup.send(
            f"API error: {exc}\nTag: {CLAN_TAG}\nRange: {start_iso} -> {end_iso}",
            ephemeral=True,
        )


@bot.tree.command(name="refresh_leaderboard", description="Force un refresh du leaderboard.")
async def refresh_leaderboard_cmd(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    try:
        await refresh_leaderboard()
        await interaction.followup.send("OK: leaderboard mis à jour.", ephemeral=True)
    except Exception as exc:
        await interaction.followup.send(f"Erreur: {exc}", ephemeral=True)


if __name__ == "__main__":
    if not TOKEN:
        raise ValueError("DISCORD_TOKEN missing.")
    bot.run(TOKEN)
