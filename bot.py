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


def is_pseudo_valid(pseudo: str) -> bool:
    return "#" not in pseudo


def has_clan_tag(pseudo: str) -> bool:
    return CLAN_TAG.upper() in pseudo.upper()


def calculate_ratio(wins_ffa, losses_ffa, wins_team, losses_team):
    wins = wins_ffa + wins_team
    losses = losses_ffa + losses_team
    return wins / (losses + 1)


def fetch_player_stats(player_id: str):
    """
    Placeholder de rÃ©cupÃ©ration de stats.
    A remplacer par une source externe plus tard.
    """
    # TODO: implementer la recuperation reelle des stats
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


@bot.tree.command(name="register", description="Enregistre un joueur dans le leaderboard.")
@app_commands.describe(pseudo="Pseudo sans tag Discord (#)", player_id="ID Player OpenFront")
async def register(interaction: discord.Interaction, pseudo: str, player_id: str):
    if not is_pseudo_valid(pseudo):
        await interaction.response.send_message(
            "âŒ Le pseudo ne doit pas contenir de tag Discord (#).",
            ephemeral=True,
        )
        return
    if not has_clan_tag(pseudo):
        await interaction.response.send_message(
            f"âŒ Le pseudo doit contenir le tag {CLAN_TAG}.",
            ephemeral=True,
        )
        return

    try:
        stats = fetch_player_stats(player_id)
        if not stats:
            await interaction.response.send_message(
                "âŒ Erreur : Impossible de recuperer les stats du joueur.",
                ephemeral=True,
            )
            return
    except Exception:
        await interaction.response.send_message(
            "âŒ Erreur : Impossible de recuperer les stats du joueur.",
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
            f"âŒ Erreur : base de donnees inaccessible. ({exc})",
            ephemeral=True,
        )
        return

    await interaction.response.send_message(
        f"âœ… {pseudo} a ete enregistre avec l'ID {player_id} !"
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
            f"âŒ Erreur : base de donnees inaccessible. ({exc})",
            ephemeral=True,
        )
        return

    if not rows:
        await interaction.response.send_message(
            "âŒ Aucun joueur avec le tag [GAL] trouve.",
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
        title="ðŸ† Leaderboard [GAL] - Top 30",
        color=discord.Color.orange(),
    )

    for i, p in enumerate(top, 1):
        embed.add_field(
            name=f"#{i} {p['pseudo']}",
            value=(
                f"Ratio: `{p['ratio']:.2f}`\n"
                f"FFA: `{p['wins_ffa']}W / {p['losses_ffa']}L`\n"
                f"EQUIPE: `{p['wins_team']}W / {p['losses_team']}L`"
            ),
            inline=False,
        )

    embed.set_footer(text=f"Mis a jour le {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")

    await interaction.response.send_message(embed=embed)


if __name__ == "__main__":
    if not TOKEN:
        print("âŒ DISCORD_TOKEN manquant.")
        print(
            "ðŸ” Variables detectees: DISCORD_TOKEN=%s, DISCORD_BOT_TOKEN=%s"
            % (bool(os.getenv("DISCORD_TOKEN")), bool(os.getenv("DISCORD_BOT_TOKEN")))
        )
        raise ValueError("DISCORD_TOKEN manquant.")
    bot.run(TOKEN)
