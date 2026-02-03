import discord
from discord.ext import commands
import aiohttp
import os
from datetime import datetime
import asyncio

# Configuration
TOKEN = os.environ.get('DISCORD_TOKEN')
TAG = "GAL"  # Change si besoin

# Intents Discord
intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(command_prefix='!', intents=intents)

# Dictionnaire pour stocker les pseudos Openfront des membres
player_names = {}

@bot.event
async def on_ready():
    print(f'✅ {bot.user} est connecté !')
    print(f'📊 Serveurs : {len(bot.guilds)}')
    for guild in bot.guilds:
        print(f'  - {guild.name} ({guild.id})')

@bot.command(name='register')
async def register(ctx, *, openfront_pseudo: str):
    """Enregistre ton pseudo Openfront.io"""
    player_names[ctx.author.id] = openfront_pseudo
    await ctx.send(f"✅ Pseudo Openfront enregistré : **{openfront_pseudo}**")
    print(f"Enregistrement : {ctx.author.name} -> {openfront_pseudo}")

@bot.command(name='unregister')
async def unregister(ctx):
    """Retire ton pseudo enregistré"""
    if ctx.author.id in player_names:
        del player_names[ctx.author.id]
        await ctx.send("✅ Pseudo supprimé !")
    else:
        await ctx.send("❌ Tu n'as pas de pseudo enregistré.")

@bot.command(name='myinfo')
async def myinfo(ctx):
    """Affiche ton pseudo enregistré"""
    if ctx.author.id in player_names:
        pseudo = player_names[ctx.author.id]
        await ctx.send(f"📝 Ton pseudo enregistré : **{pseudo}**")
    else:
        await ctx.send("❌ Tu n'as pas encore enregistré ton pseudo. Utilise `!register <pseudo>`")

async def get_player_stats(session, player_name):
    """Récupère les stats d'un joueur depuis l'API Openfront"""
    try:
        # API Openfront - Stats du joueur
        url = f"https://api.openfront.io/player/{player_name}"
        
        async with session.get(url, timeout=10) as response:
            if response.status == 200:
                data = await response.json()
                
                # Extraction des stats
                wins = data.get('wins', 0)
                losses = data.get('losses', 0)
                games = wins + losses
                winrate = (wins / games * 100) if games > 0 else 0
                
                return {
                    'name': player_name,
                    'wins': wins,
                    'losses': losses,
                    'games': games,
                    'winrate': winrate,
                    'rank': data.get('rank', 'N/A'),
                    'elo': data.get('elo', 0)
                }
            elif response.status == 404:
                print(f"❌ Joueur non trouvé : {player_name}")
                return None
            else:
                print(f"⚠️ Erreur API ({response.status}) pour {player_name}")
                return None
                
    except asyncio.TimeoutError:
        print(f"⏱️ Timeout pour {player_name}")
        return None
    except Exception as e:
        print(f"❌ Erreur pour {player_name}: {e}")
        return None

@bot.command(name='stats')
async def stats(ctx):
    """Affiche les stats de tous les membres GAL enregistrés"""
    
    msg = await ctx.send("🔄 Récupération des statistiques...")
    
    # Récupérer tous les membres avec le tag GAL
    members_with_tag = []
    for member in ctx.guild.members:
        # Vérifier le pseudo Discord ou le nickname
        display_name = member.nick if member.nick else member.name
        if TAG in display_name.upper():
            if member.id in player_names:
                members_with_tag.append({
                    'discord_member': member,
                    'openfront_name': player_names[member.id]
                })
    
    if not members_with_tag:
        await msg.edit(content=f"❌ Aucun membre avec le tag **{TAG}** n'a enregistré son pseudo.\n"
                               f"Utilisez `!register <pseudo_openfront>` pour vous enregistrer.")
        return
    
    # Récupérer les stats de chaque membre
    stats_list = []
    
    async with aiohttp.ClientSession() as session:
        tasks = []
        for member_data in members_with_tag:
            task = get_player_stats(session, member_data['openfront_name'])
            tasks.append(task)
        
        results = await asyncio.gather(*tasks)
        
        for i, result in enumerate(results):
            if result:
                result['discord_name'] = members_with_tag[i]['discord_member'].display_name
                stats_list.append(result)
    
    if not stats_list:
        await msg.edit(content="❌ Impossible de récupérer les statistiques. Vérifiez que les pseudos sont corrects.")
        return
    
    # Trier par taux de victoire
    stats_list.sort(key=lambda x: x['winrate'], reverse=True)
    
    # Créer l'embed
    embed = discord.Embed(
        title=f"📊 Statistiques Openfront - {TAG}",
        description=f"Classement par taux de victoire ({len(stats_list)} joueurs)",
        color=discord.Color.blue(),
        timestamp=datetime.now()
    )
    
    for i, player_stat in enumerate(stats_list, 1):
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"**{i}.**"
        
        embed.add_field(
            name=f"{medal} {player_stat['discord_name']}",
            value=f"```"
                  f"Pseudo    : {player_stat['name']}\n"
                  f"Victoires : {player_stat['wins']}\n"
                  f"Défaites  : {player_stat['losses']}\n"
                  f"Total     : {player_stat['games']}\n"
                  f"Winrate   : {player_stat['winrate']:.1f}%\n"
                  f"ELO       : {player_stat['elo']}\n"
                  f"Rank      : {player_stat['rank']}"
                  f"```",
            inline=False
        )
    
    embed.set_footer(text="Données fournies par l'API Openfront.io")
    
    await msg.edit(content=None, embed=embed)

@bot.command(name='leaderboard')
async def leaderboard(ctx, limit: int = 10):
    """Affiche le classement complet (par défaut top 10)"""
    
    if limit > 25:
        await ctx.send("⚠️ Limite maximale : 25 joueurs")
        limit = 25
    
    msg = await ctx.send(f"🔄 Récupération du top {limit}...")
    
    members_with_tag = []
    for member in ctx.guild.members:
        display_name = member.nick if member.nick else member.name
        if TAG in display_name.upper() and member.id in player_names:
            members_with_tag.append({
                'discord_member': member,
                'openfront_name': player_names[member.id]
            })
    
    if not members_with_tag:
        await msg.edit(content=f"❌ Aucun membre trouvé.")
        return
    
    stats_list = []
    async with aiohttp.ClientSession() as session:
        tasks = [get_player_stats(session, m['openfront_name']) for m in members_with_tag]
        results = await asyncio.gather(*tasks)
        
        for i, result in enumerate(results):
            if result:
                result['discord_name'] = members_with_tag[i]['discord_member'].display_name
                stats_list.append(result)
    
    stats_list.sort(key=lambda x: x['winrate'], reverse=True)
    stats_list = stats_list[:limit]
    
    embed = discord.Embed(
        title=f"🏆 TOP {limit} - {TAG}",
        color=discord.Color.gold(),
        timestamp=datetime.now()
    )
    
    leaderboard_text = ""
    for i, p in enumerate(stats_list, 1):
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
        leaderboard_text += f"{medal} **{p['discord_name']}** - {p['winrate']:.1f}% ({p['wins']}V/{p['losses']}D)\n"
    
    embed.description = leaderboard_text
    embed.set_footer(text="Données fournies par l'API Openfront.io")
    
    await msg.edit(content=None, embed=embed)

@bot.command(name='help_bot')
async def help_command(ctx):
    """Affiche l'aide"""
    embed = discord.Embed(
        title="🤖 Commandes du Bot Openfront",
        description="Bot de statistiques pour Openfront.io",
        color=discord.Color.green()
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
        name="!stats",
        value=f"Affiche les stats de tous les membres {TAG}",
        inline=False
    )
    embed.add_field(
        name="!leaderboard [nombre]",
        value="Affiche le classement (par défaut top 10)",
        inline=False
    )
    embed.set_footer(text=f"Tag recherché : {TAG}")
    await ctx.send(embed=embed)

# Lancer le bot
if TOKEN:
    bot.run(TOKEN)
else:
    print("❌ ERREUR : Token Discord manquant !")
