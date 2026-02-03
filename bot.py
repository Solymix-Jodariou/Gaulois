import os
import sqlite3
from datetime import datetime, timezone

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands

TOKEN = os.getenv("DISCORD_TOKEN") or os.getenv("DISCORD_BOT_TOKEN")
GUILD_ID = os.getenv("DISCORD_GUILD_ID")
DB_PATH = "leaderboard.db"
CLAN_TAG = "[GAL]"

API_BASE = "https://api.openfront.io/public"
USER_AGENT = "Mozilla/5.0 (GauloisBot)"

intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)


def get_db():
    return sqlite3.connect(DB_PATH)


def init_db():
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
                last_updated TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )


def is_pseudo_valid(pseudo: str) -> bool:
    return "#" not in pseudo


def has_clan_tag(pseudo: str) -> bool:
    return CLAN_TAG.upper() in pseudo.upper()


def calculate_ratio(wins_ffa, losses_ffa, wins_team, losses_team):
    wins = wins_ffa + wins_team
    losses = losses_ffa + losses_team
    return wins / (losses + 1)


def compute_stats_from_sessions(sessions):
    wins_ffa = losses_ffa = wins_team = losses_team = 0
    for s in sessions:
        mode = (s.get("gameMode") or "").lower()
        has_won = bool(s.get("hasWon"))
        if "free for all" in mode or mode == "ffa":
            if has_won:
                wins_ffa += 1
            else:
                losses_ffa += 1
        elif "team" in mode:
            if has_won:
                wins_team += 1
            else:
                losses_team += 1
    return wins_ffa, losses_ffa, wins_team, losses_team


async def fetch_player_sessions(player_id: str):
    url = f"{API_BASE}/player/{player_id}/sessions"
    headers = {"User-Agent": USER_AGENT}
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(url, timeout=15) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise RuntimeError(f"HTTP {resp.status}: {text[:200]}")
            return await resp.json()


async def fetch_player_info(player_id: str):
    url = f"{API_BASE}/player/{player_id}"
    headers = {"User-Agent": USER_AGENT}
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(url, timeout=15) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise RuntimeError(f"HTTP {resp.status}: {text[:200]}")
            return await resp.json()


def upsert_player(discord_id, pseudo, player_id, wins_ffa, losses_ffa, wins_team, losses_team):
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
                str(discord_id),
                pseudo,
                player_id,
                wins_ffa,
                losses_ffa,
                wins_team,
                losses_team,
                datetime.now(timezone.utc).isoformat(),
            ),
        )


def get_gal_players():
    with get_db() as conn:
        return conn.execute(
            """
            SELECT discord_id, pseudo, player_id, wins_ffa, losses_ffa, wins_team, losses_team
            FROM players
            WHERE pseudo LIKE ?
            """,
            (f"%{CLAN_TAG}%",),
        ).fetchall()


def update_cached_stats(player_id, wins_ffa, losses_ffa, wins_team, losses_team):
    with get_db() as conn:
        conn.execute(
            """
            UPDATE players
            SET wins_ffa = ?, losses_ffa = ?, wins_team = ?, losses_team = ?, last_updated = ?
            WHERE player_id = ?
            """,
            (
                wins_ffa,
                losses_ffa,
                wins_team,
                losses_team,
                datetime.now(timezone.utc).isoformat(),
                player_id,
            ),
        )


@bot.event
async def on_ready():
    init_db()
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
    print(f"Bot connected: {bot.user}")


@bot.tree.command(name="register", description="Register a player for the leaderboard.")
@app_commands.describe(pseudo="Pseudo without Discord tag (#)", player_id="OpenFront player ID")
async def register(interaction: discord.Interaction, pseudo: str, player_id: str):
    if not is_pseudo_valid(pseudo):
        await interaction.response.send_message(
            "Error: pseudo must not contain '#'.",
            ephemeral=True,
        )
        return
    if not has_clan_tag(pseudo):
        await interaction.response.send_message(
            f"Error: pseudo must include {CLAN_TAG}.",
            ephemeral=True,
        )
        return

    await interaction.response.defer(ephemeral=True)

    try:
        await fetch_player_info(player_id)
        sessions = await fetch_player_sessions(player_id)
        wins_ffa, losses_ffa, wins_team, losses_team = compute_stats_from_sessions(sessions)
    except Exception as exc:
        await interaction.followup.send(
            f"Error: unable to fetch player stats. ({exc})",
            ephemeral=True,
        )
        return

    try:
        upsert_player(interaction.user.id, pseudo, player_id, wins_ffa, losses_ffa, wins_team, losses_team)
    except Exception as exc:
        await interaction.followup.send(
            f"Error: database error. ({exc})",
            ephemeral=True,
        )
        return

    await interaction.followup.send(
        f"OK: {pseudo} registered with ID {player_id}.",
        ephemeral=True,
    )


@bot.tree.command(name="setleaderboard", description="Show the [GAL] leaderboard.")
@app_commands.describe(refresh="Refresh stats from OpenFront before ranking")
async def setleaderboard(interaction: discord.Interaction, refresh: bool = False):
    rows = get_gal_players()
    if not rows:
        await interaction.response.send_message(
            "Error: no [GAL] players registered.",
            ephemeral=True,
        )
        return

    await interaction.response.defer()

    players = []
    for _discord_id, pseudo, player_id, wins_ffa, losses_ffa, wins_team, losses_team in rows:
        if refresh:
            try:
                sessions = await fetch_player_sessions(player_id)
                wins_ffa, losses_ffa, wins_team, losses_team = compute_stats_from_sessions(sessions)
                update_cached_stats(player_id, wins_ffa, losses_ffa, wins_team, losses_team)
            except Exception:
                pass

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
        title="Leaderboard [GAL] - Top 30",
        color=discord.Color.orange(),
    )

    for i, p in enumerate(top, 1):
        embed.add_field(
            name=f"#{i} {p['pseudo']}",
            value=(
                f"Ratio: {p['ratio']:.2f}\n"
                f"FFA: {p['wins_ffa']}W / {p['losses_ffa']}L\n"
                f"TEAM: {p['wins_team']}W / {p['losses_team']}L"
            ),
            inline=False,
        )

    embed.set_footer(text=f"Updated {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")

    await interaction.followup.send(embed=embed)


if __name__ == "__main__":
    if not TOKEN:
        raise ValueError("DISCORD_TOKEN missing.")
    bot.run(TOKEN)
