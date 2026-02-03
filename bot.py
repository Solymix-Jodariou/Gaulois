import os
import sqlite3
from datetime import datetime

import discord
from discord import app_commands
from discord.ext import commands

TOKEN = os.getenv("DISCORD_TOKEN") or os.getenv("DISCORD_BOT_TOKEN")
GUILD_ID = os.getenv("DISCORD_GUILD_ID")
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
        print(f"âŒ Erreur DB: {exc}")


@bot.event
async def on_ready():
    init_db()
    try:
        if GUILD_ID:
            guild = discord.Object(id=int(GUILD_ID))
            await bot.tree.sync(guild=guild)
            print(f"âœ… Commandes synchronisees pour le serveur {GUILD_ID}")
        else:
            await bot.tree.sync()
            print("âœ… Commandes synchronisees globalement")
    except Exception as exc:
        print(f"âŒ Erreur sync commandes: {exc}")
    print(f"âœ… Bot connecte : {bot.user}")


if __name__ == "__main__":
    if not TOKEN:
        print("âŒ DISCORD_TOKEN manquant.")
        print(
            "ðŸ” Variables detectees: DISCORD_TOKEN=%s, DISCORD_BOT_TOKEN=%s"
            % (bool(os.getenv("DISCORD_TOKEN")), bool(os.getenv("DISCORD_BOT_TOKEN")))
        )
        raise ValueError("DISCORD_TOKEN manquant.")
    bot.run(TOKEN)
