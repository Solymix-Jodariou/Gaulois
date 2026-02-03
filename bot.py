import discord
from discord.ext import commands
import aiohttp
import json
from datetime import datetime

# Configuration
import os
TOKEN = os.getenv('DISCORD_TOKEN')  # prend la variable d'environnement
TAG_CLAN = 'GAL'
API_BASE = 'https://api.openfront.io'

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents)

# Base de données simple (pseudo discord -> pseudo openfront)
registered_users = {}

# ==================== FONCTIONS API ====================

async def get_leaderboard():
    """Récupère le leaderboard complet"""
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(f'{API_BASE}/leaderboard') as resp:
                if resp.status == 200:
                    return await resp.json()
                return None
        except Exception as e:
            print(f"❌ Erreur API leaderboard: {e}")
            return None

async def get_game_data(game_id):
    """Récupère les données d'une partie"""
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(f'{API_BASE}/game/{game_id}') as resp:
                if resp.status == 200:
                    return await resp.json()
                return None
        except Exception as e:
            print(f"❌ Erreur API game: {e}")
            return None

def get_clan_stats(leaderboard_data, clan_tag):
    """Extrait les stats d'un clan du leaderboard"""
    if not leaderboard_data or 'clans' not in leaderboard_data:
        return None
    
    for clan in leaderboard_data['clans']:
        if clan['clanTag'].upper() == clan_tag.upper():
            return clan
    return None

# ==================== COMMANDES ====================

@bot.event
async def on_ready():
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
        name="!leaderboard_clans [top]",
        value="Affiche le classement des clans (défaut: top 10)",
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
    await ctx.send(f"✅ {ctx.author.mention} enregistré avec le pseudo **{pseudo}**")

@bot.command(name='unregister')
async def unregister(ctx):
    """Supprime l'enregistrement"""
    user_id = str(ctx.author.id)
    if user_id in registered_users:
        del registered_users[user_id]
        await ctx.send("✅ Enregistrement supprimé")
    else:
        await ctx.send("❌ Tu n'es pas enregistré")

@bot.command(name='myinfo')
async def myinfo(ctx):
    """Affiche les infos de l'utilisateur"""
    user_id = str(ctx.author.id)
    if user_id in registered_users:
        pseudo = registered_users[user_id]
        await ctx.send(f"📋 Ton pseudo Openfront : **{pseudo}**")
    else:
        await ctx.send("❌ Tu n'es pas enregistré. Utilise `!register <pseudo>`")

@bot.command(name='stats_gal')
async def stats_gal(ctx):
    """Affiche les stats du clan GAL"""
    await ctx.send("🔄 Récupération des stats...")
    
    data = await get_leaderboard()
    if not data:
        await ctx.send("❌ Impossible de récupérer les données du leaderboard")
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
async def leaderboard_clans(ctx, top: int = 10):
    """Affiche le classement des clans"""
    await ctx.send(f"🔄 Récupération du top {top}...")
    
    data = await get_leaderboard()
    if not data or 'clans' not in data:
        await ctx.send("❌ Impossible de récupérer le leaderboard")
        return
    
    # Trier par W/L ratio
    clans_sorted = sorted(
        data['clans'], 
        key=lambda x: x['weightedWLRatio'], 
        reverse=True
    )[:top]
    
    embed = discord.Embed(
        title=f"🏆 Top {top} Clans - Classement W/L Ratio",
        color=discord.Color.purple()
    )
    
    description = "```\n"
    description += f"{'#':<3} {'TAG':<8} {'W/L':<8} {'Games':<8}\n"
    description += "-" * 35 + "\n"
    
    for i, clan in enumerate(clans_sorted, 1):
        tag = clan['clanTag']
        wlr = clan['weightedWLRatio']
        games = clan['games']
        
        # Highlight du clan GAL
        if tag == TAG_CLAN:
            description += f"►{i:<2} {tag:<8} {wlr:<8.2f} {games:<8}\n"
        else:
            description += f"{i:<3} {tag:<8} {wlr:<8.2f} {games:<8}\n"
    
    description += "```"
    embed.description = description
    
    # Trouver la position du clan GAL
    gal_position = next((i+1 for i, c in enumerate(clans_sorted) if c['clanTag'] == TAG_CLAN), None)
    if gal_position:
        embed.set_footer(text=f"📍 {TAG_CLAN} est #{gal_position}")
    
    await ctx.send(embed=embed)

@bot.command(name='game')
async def game_info(ctx, game_id: str = None):
    """Affiche les infos d'une partie"""
    if not game_id:
        await ctx.send("❌ Usage : `!game <game_id>`")
        return
    
    await ctx.send(f"🔄 Récupération de la partie {game_id}...")
    
    data = await get_game_data(game_id)
    if not data:
        await ctx.send(f"❌ Impossible de récupérer les données de la partie {game_id}")
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
    
    data = await get_game_data(game_id)
    if not data or 'info' not in data or 'players' not in data['info']:
        await ctx.send("❌ Données de partie invalides")
        return
    
    gal_players = []
    for player in data['info']['players']:
        username = player.get('username', 'Unknown')
        if f'[{TAG_CLAN}]' in username or username.startswith(f'{TAG_CLAN} '):
            gal_players.append(username)
    
    if gal_players:
        players_list = "\n".join(f"• {p}" for p in gal_players)
        await ctx.send(f"✅ Joueurs **{TAG_CLAN}** trouvés :\n{players_list}")
    else:
        await ctx.send(f"❌ Aucun joueur **{TAG_CLAN}** dans cette partie")

# ==================== LANCEMENT ====================

bot.run(TOKEN)
