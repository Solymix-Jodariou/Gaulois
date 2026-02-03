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
OPENFRONT_API_KEY = os.getenv('OPENFRONT_API_KEY')
MAX_GAMES_DEFAULT = 10
MAX_GAMES_CAP = 30

# Vérification du token
if not TOKEN:
    raise ValueError("❌ DISCORD_TOKEN n'est pas défini. Veuillez configurer la variable d'environnement sur Railway.")

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents)

# Base de données simple (pseudo discord -> pseudo openfront)
registered_users = {}

# ==================== FONCTIONS API ====================

def build_api_headers():
    """Construit les headers pour l'API (si clé fournie)."""
    if not OPENFRONT_API_KEY:
        return {}
    # Compatibilité : certaines APIs utilisent Authorization, d'autres X-API-Key
    return {
        "Authorization": f"Bearer {OPENFRONT_API_KEY}",
        "X-API-Key": OPENFRONT_API_KEY,
    }

def format_api_error(error: str) -> str:
    if not error:
        return ""
    if "401" in error:
        return "Accès refusé (401). L'API semble privée : configure `OPENFRONT_API_KEY`."
    if "404" in error:
        return "Endpoint introuvable (404). L'API a peut-être changé."
    return f"Erreur API : {error}"

async def get_leaderboard():
    """Récupère le leaderboard complet"""
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(
                f'{API_BASE}/leaderboard',
                headers=build_api_headers(),
                timeout=10,
            ) as resp:
                if resp.status == 200:
                    return await resp.json(), None
                text = await resp.text()
                return None, f"HTTP {resp.status}: {text[:200]}"
        except Exception as e:
            print(f"❌ Erreur API leaderboard: {e}")
            return None, str(e)

async def get_recent_games():
    """Récupère une liste de parties récentes (ids)."""
    endpoints_to_try = [
        "games",
        "games/recent",
        "matches",
    ]
    async with aiohttp.ClientSession() as session:
        for endpoint in endpoints_to_try:
            try:
                async with session.get(
                    f'{API_BASE}/{endpoint}',
                    headers=build_api_headers(),
                    timeout=10,
                ) as resp:
                    if resp.status == 200:
                        return await resp.json(), None
                    text = await resp.text()
                    last_error = f"HTTP {resp.status}: {text[:200]}"
            except Exception as e:
                last_error = str(e)
        return None, last_error

async def get_game_data(game_id):
    """Récupère les données d'une partie"""
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(
                f'{API_BASE}/game/{game_id}',
                headers=build_api_headers(),
                timeout=10,
            ) as resp:
                if resp.status == 200:
                    return await resp.json(), None
                text = await resp.text()
                return None, f"HTTP {resp.status}: {text[:200]}"
        except Exception as e:
            print(f"❌ Erreur API game: {e}")
            return None, str(e)

def get_clan_stats(leaderboard_data, clan_tag):
    """Extrait les stats d'un clan du leaderboard"""
    if not leaderboard_data or 'clans' not in leaderboard_data:
        return None
    
    for clan in leaderboard_data['clans']:
        if clan['clanTag'].upper() == clan_tag.upper():
            return clan
    return None

# ==================== HELPERS ====================

def is_tagged_user(username: str, tag: str) -> bool:
    """Détecte le tag clan dans un pseudo (ex: [GAL] ou GAL <pseudo>)."""
    if not username:
        return False
    upper_name = username.upper()
    upper_tag = tag.upper()
    return f'[{upper_tag}]' in upper_name or upper_name.startswith(f'{upper_tag} ')

def extract_game_id(item):
    """Extrait un identifiant de partie d'un objet API."""
    if isinstance(item, str):
        return item
    if not isinstance(item, dict):
        return None
    for key in ("gameId", "game_id", "id", "_id"):
        value = item.get(key)
        if value:
            return value
    return None

def get_metric_key(players):
    """Trouve une stat numérique utilisable pour trier un leaderboard."""
    metric_keys = ("score", "kills", "wins", "points", "territory", "land", "power")
    for key in metric_keys:
        if any(isinstance(p.get(key), (int, float)) for p in players):
            return key
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
        name="!leaderboard_clans [max_games] [top]",
        value="Scanne des parties récentes et affiche le top [GAL]",
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
    
    data, error = await get_leaderboard()
    if not data:
        await ctx.send(f"❌ Impossible de récupérer les données du leaderboard. {format_api_error(error)}")
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
async def leaderboard_clans(ctx, max_games: int = MAX_GAMES_DEFAULT, top: int = 10):
    """Scanne les parties récentes et affiche le top GAL"""
    if max_games < 1:
        await ctx.send("❌ Usage : `!leaderboard_clans [max_games] [top]`")
        return

    max_games = min(max_games, MAX_GAMES_CAP)
    await ctx.send(f"🔄 Scan de {max_games} parties récentes pour le tag [{TAG_CLAN}]...")

    recent_data, error = await get_recent_games()
    if not recent_data:
        await ctx.send(f"❌ Impossible de récupérer les parties récentes. {format_api_error(error)}")
        return

    if isinstance(recent_data, dict):
        game_items = recent_data.get('games') or recent_data.get('matches') or recent_data.get('data')
    else:
        game_items = recent_data

    if not game_items:
        await ctx.send("❌ Liste des parties récentes introuvable")
        return

    game_ids = []
    for item in game_items:
        game_id = extract_game_id(item)
        if game_id:
            game_ids.append(game_id)
        if len(game_ids) >= max_games:
            break

    if not game_ids:
        await ctx.send("❌ Aucun game_id trouvé dans les parties récentes")
        return

    leaderboard = {}
    games_with_gal = 0

    for game_id in game_ids:
        data, _error = await get_game_data(game_id)
        if not data:
            continue

        players = None
        if isinstance(data, dict):
            players = data.get('info', {}).get('players') or data.get('players')

        if not players:
            continue

        gal_players = [p for p in players if is_tagged_user(p.get('username', ''), TAG_CLAN)]
        if not gal_players:
            continue

        games_with_gal += 1
        metric_key = get_metric_key(gal_players)

        for player in gal_players:
            username = player.get('username', 'Unknown')
            entry = leaderboard.setdefault(username, {"count": 0, "score": 0})
            entry["count"] += 1
            if metric_key:
                value = player.get(metric_key, 0)
                if isinstance(value, (int, float)):
                    entry["score"] += value

    if games_with_gal == 0 or not leaderboard:
        await ctx.send(f"❌ Aucune partie avec **{TAG_CLAN}** dans les {len(game_ids)} dernières parties")
        return

    leaderboard_sorted = sorted(
        leaderboard.items(),
        key=lambda x: (x[1]["score"], x[1]["count"]),
        reverse=True
    )[:top]

    embed = discord.Embed(
        title=f"🏆 Top {top} Joueurs [{TAG_CLAN}] - {games_with_gal}/{len(game_ids)} parties",
        color=discord.Color.purple()
    )

    description = "```\n"
    description += f"{'#':<3} {'JOUEUR':<20} {'SCORE':<8} {'GAMES':<8}\n"
    description += "-" * 43 + "\n"

    for i, (username, stats) in enumerate(leaderboard_sorted, 1):
        display_name = username[:20]
        description += f"{i:<3} {display_name:<20} {stats['score']:<8.2f} {stats['count']:<8}\n"

    description += "```"
    embed.description = description

    await ctx.send(embed=embed)

@bot.command(name='leaderboard_gal')
async def leaderboard_gal(ctx, max_games: int = MAX_GAMES_DEFAULT, top: int = 10):
    """Alias de leaderboard_clans"""
    await leaderboard_clans(ctx, max_games, top)

@bot.command(name='game')
async def game_info(ctx, game_id: str = None):
    """Affiche les infos d'une partie"""
    if not game_id:
        await ctx.send("❌ Usage : `!game <game_id>`")
        return
    
    await ctx.send(f"🔄 Récupération de la partie {game_id}...")
    
    data, error = await get_game_data(game_id)
    if not data:
        await ctx.send(f"❌ Impossible de récupérer les données de la partie {game_id}. {format_api_error(error)}")
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
    
    data, error = await get_game_data(game_id)
    if not data:
        await ctx.send(f"❌ Données de partie invalides. {format_api_error(error)}")
        return
    if 'info' not in data or 'players' not in data['info']:
        await ctx.send("❌ Données de partie invalides")
        return
    
    gal_players = []
    for player in data['info']['players']:
        username = player.get('username', 'Unknown')
        if is_tagged_user(username, TAG_CLAN):
            gal_players.append(username)
    
    if gal_players:
        players_list = "\n".join(f"• {p}" for p in gal_players)
        await ctx.send(f"✅ Joueurs **{TAG_CLAN}** trouvés :\n{players_list}")
    else:
        await ctx.send(f"❌ Aucun joueur **{TAG_CLAN}** dans cette partie")

# ==================== LANCEMENT ====================

if __name__ == '__main__':
    if not TOKEN:
        print("❌ ERREUR: DISCORD_TOKEN n'est pas défini dans les variables d'environnement")
        print("💡 Configurez la variable DISCORD_TOKEN sur Railway")
        exit(1)
    bot.run(TOKEN)
