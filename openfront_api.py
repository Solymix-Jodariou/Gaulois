import aiohttp
import discord
from discord.ext import commands
from datetime import datetime
from collections import defaultdict

class OpenFrontAPI:
    """Client pour l'API OpenFront officielle UNIQUEMENT"""
    
    BASE_URL = "https://api.openfront.io"
    
    def __init__(self):
        self.session = None
    
    async def _get(self, endpoint):
        """Fait une requête GET à l'API"""
        if not self.session:
            self.session = aiohttp.ClientSession()
        
        url = f"{self.BASE_URL}/{endpoint}"
        
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    print(f"❌ Erreur API {response.status}: {url}")
                    text = await response.text()
                    print(f"Réponse: {text[:200]}")
                    return None
        except Exception as e:
            print(f"❌ Erreur réseau: {e}")
            return None
    
    async def get_game(self, game_id):
        """Récupère les détails d'une partie
        
        Args:
            game_id: ID de la partie
        """
        return await self._get(f"game/{game_id}")
    
    async def get_recent_games(self):
        """Récupère les parties récentes (À ADAPTER selon l'API réelle)"""
        # À remplacer par le vrai endpoint quand on le connaîtra
        endpoints_to_try = [
            "games",
            "games/recent",
            "matches",
            "leaderboard"
        ]
        
        for endpoint in endpoints_to_try:
            data = await self._get(endpoint)
            if data:
                print(f"✅ Endpoint trouvé: {endpoint}")
                return data
        
        return None
    
    async def get_player_games(self, username):
        """Récupère les parties d'un joueur (À ADAPTER)"""
        return await self._get(f"player/{username}/games")
    
    async def get_leaderboard(self):
        """Récupère le classement"""
        return await self._get("leaderboard")
    
    async def close(self):
        """Ferme la session HTTP"""
        if self.session:
            await self.session.close()


class OpenFrontCommands(commands.Cog):
    """Commandes Discord pour OpenFront"""
    
    def __init__(self, bot):
        self.bot = bot
        self.api = OpenFrontAPI()
    
    @commands.command(name='test_api')
    async def test_api(self, ctx):
        """Teste différents endpoints de l'API
        
        Usage: !test_api
        """
        await ctx.send("🔍 Test des endpoints API OpenFront...")
        
        endpoints = [
            "games",
            "games/recent",
            "matches",
            "leaderboard",
            "players"
        ]
        
        results = []
        
        for endpoint in endpoints:
            async with ctx.typing():
                data = await self.api._get(endpoint)
                
                if data:
                    results.append(f"✅ `{endpoint}` - FONCTIONNE")
                else:
                    results.append(f"❌ `{endpoint}` - Erreur")
        
        embed = discord.Embed(
            title="📊 Résultats des tests API",
            description="\n".join(results),
            color=discord.Color.blue()
        )
        
        await ctx.send(embed=embed)
    
    @commands.command(name='game')
    async def get_game_info(self, ctx, game_id: str):
        """Récupère les infos d'une partie
        
        Usage: !game GAME_ID
        Exemple: !game iMkyreI1
        """
        async with ctx.typing():
            data = await self.api.get_game(game_id)
            
            if not data:
                await ctx.send(f"❌ Impossible de récupérer la partie `{game_id}`")
                return
            
            # Affiche les données (à adapter selon le format réel)
            embed = discord.Embed(
                title=f"🎮 Partie {game_id}",
                color=discord.Color.green()
            )
            
            # Affiche tout le JSON pour voir le format
            import json
            json_str = json.dumps(data, indent=2)
            
            # Discord limite à 1024 caractères par field
            if len(json_str) > 1000:
                json_str = json_str[:1000] + "..."
            
            embed.add_field(
                name="Données brutes",
                value=f"```json\n{json_str}\n```",
                inline=False
            )
            
            await ctx.send(embed=embed)
    
    @commands.command(name='leaderboard', aliases=['top', 'classement'])
    async def show_leaderboard(self, ctx):
        """Affiche le classement
        
        Usage: !leaderboard
        """
        async with ctx.typing():
            data = await self.api.get_leaderboard()
            
            if not data:
                await ctx.send("❌ Impossible de récupérer le classement")
                return
            
            # Affiche les données
            import json
            json_str = json.dumps(data, indent=2)[:1000]
            
            embed = discord.Embed(
                title="🏆 Classement OpenFront",
                description=f"```json\n{json_str}\n```",
                color=discord.Color.gold()
            )
            
            await ctx.send(embed=embed)


async def setup(bot):
    await bot.add_cog(OpenFrontCommands(bot))
