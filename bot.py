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

if RANGE_HOURS > 48:
    RANGE_HOURS = 48
if RANGE_HOURS < 1:
    RANGE_HOURS = 1
if REFRESH_MINUTES < 5:
    REFRESH_MINUTES = 5

intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)

leaderboard_cache = []
leaderboard_updated_at = None


def get_db():
    return sqlite3.connect(DB_PATH)


def init_db():
    with get_db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS clan_cache (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                data TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )


def load_cache():
    global leaderboard_cache, leaderboard_updated_at
    with get_db() as conn:
        row = conn.execute(
            "SELECT data, updated_at FROM clan_cache WHERE id = 1"
        ).fetchone()
    if row:
        leaderboard_cache = json.loads(row[0])
        leaderboard_updated_at = row[1]


def save_cache(data, updated_at):
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO clan_cache (id, data, updated_at)
            VALUES (1, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                data = excluded.data,
                updated_at = excluded.updated_at
            """,
            (json.dumps(data), updated_at),
        )


def calculate_ratio(wins_ffa, losses_ffa, wins_team, losses_team):
    wins = wins_ffa + wins_team
    losses = losses_ffa + losses_team
    return wins / (losses + 1)


def is_clan_session(session):
    tag = session.get("clanTag")
    if not tag:
        return False
    return tag.upper() == CLAN_TAG.upper()


def compute_from_sessions(sessions):
    players = {}
    for s in sessions:
        if not is_clan_session(s):
            continue
        username = s.get("username") or "Unknown"
        entry = players.setdefault(
            username,
            {"wins_ffa": 0, "losses_ffa": 0, "wins_team": 0, "losses_team": 0},
        )

        mode = (s.get("gameMode") or "").lower()
        has_won = bool(s.get("hasWon"))
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
                "pseudo": f"{username}{CLAN_DISPLAY}",
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
        async with session.get(url, params=params, timeout=20) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise RuntimeError(f"HTTP {resp.status}: {text[:200]}")
            return await resp.json()


async def refresh_leaderboard():
    global leaderboard_cache, leaderboard_updated_at
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(hours=RANGE_HOURS)
    start_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_iso = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    sessions = await fetch_clan_sessions(start_iso, end_iso)
    leaderboard_cache = compute_from_sessions(sessions)
    leaderboard_updated_at = end_dt.strftime("%Y-%m-%d %H:%M UTC")
    save_cache(leaderboard_cache, leaderboard_updated_at)


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
    try:
        if GUILD_ID:
            guild = discord.Object(id=int(GUILD_ID))
            await bot.tree.sync(guild=guild)
            print(f"Commands synced for guild {GUILD_ID}")
        else:
            await bot.tree.sync()
            print("Commands synced globally")
    except Exception as exc:
        print(f"Command sync error: {exc}")

    bot.loop.create_task(refresh_loop())
    print(f"Bot connected: {bot.user}")


@bot.tree.command(name="setleaderboard", description="Show the [GAL] leaderboard.")
async def setleaderboard(interaction: discord.Interaction):
    if not leaderboard_cache:
        await interaction.response.send_message(
            f"Aucun joueur avec le tag {CLAN_DISPLAY} trouve. "
            f"Essaie d'augmenter la periode (LEADERBOARD_RANGE_HOURS) ou attends le prochain refresh.",
            ephemeral=True,
        )
        return

    embed = discord.Embed(
        title="Leaderboard [GAL] - Top 30",
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
        embed.set_footer(text=f"Updated {leaderboard_updated_at}")

    await interaction.response.send_message(embed=embed)


if __name__ == "__main__":
    if not TOKEN:
        raise ValueError("DISCORD_TOKEN missing.")
    bot.run(TOKEN)
