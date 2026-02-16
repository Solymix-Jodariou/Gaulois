import re
from typing import Optional
from datetime import datetime, timezone, timedelta

import discord

from parametres import (
    CLAN_DISPLAY,
    CLAN_TAG,
    OFM_SUB_ROLE_ID,
    ONEV1_REFRESH_MINUTES,
    OPENFRONT_GAME_URL_TEMPLATE,
    REFRESH_MINUTES,
)
from utilitaires import (
    format_duration,
    format_local_time,
    game_mode,
    get_winner_client_ids,
    is_clan_player,
    is_clan_username,
    parse_openfront_time,
)


def extract_gal_players(info):
    names = []
    for p in info.get("players", []):
        username = p.get("username") or ""
        if is_clan_player(p):
            names.append(username or CLAN_DISPLAY)
    return sorted(set(names))


def extract_clan_tag_from_player(player: dict) -> Optional[str]:
    tag = player.get("clanTag")
    if tag:
        return f"[{str(tag).upper()}]"
    username = player.get("username") or ""
    match = re.search(r"\[([A-Za-z0-9]+)\]", username)
    if match:
        return f"[{match.group(1).upper()}]"
    return None


def extract_winner_names(info):
    winners = get_winner_client_ids(info)
    names = []
    for p in info.get("players", []):
        if winners and p.get("clientID") not in winners:
            continue
        username = p.get("username") or p.get("name") or p.get("player") or ""
        if username:
            names.append(username)
    if not names:
        names = extract_gal_players(info)
    seen = set()
    ordered = []
    for name in names:
        if name not in seen:
            seen.add(name)
            ordered.append(name)
    return ordered


def extract_opponent_clans(info):
    winners = get_winner_client_ids(info)
    tags = []
    for p in info.get("players", []):
        if winners and p.get("clientID") in winners:
            continue
        tag = extract_clan_tag_from_player(p)
        if not tag or tag.upper() == CLAN_DISPLAY.upper():
            continue
        tags.append(tag.upper())
    return sorted(set(tags))


def build_win_embed(info):
    mode = game_mode(info) or "Team"
    start_raw = info.get("start")
    end_raw = info.get("end")
    game_id = info.get("gameID") or "?"
    winners = extract_winner_names(info)
    opponent_clans = extract_opponent_clans(info)

    game_url = None
    if game_id and game_id != "?":
        try:
            game_url = OPENFRONT_GAME_URL_TEMPLATE.format(game_id=game_id)
        except Exception:
            game_url = None

    embed = discord.Embed(
        title=f"🏆 OpenFront Game {game_id}",
        url=game_url,
        description=f"{CLAN_DISPLAY} vient de gagner une partie !",
        color=discord.Color.orange(),
    )

    winners_by_id = {
        p.get("clientID"): p for p in info.get("players", []) if p.get("clientID")
    }
    winner_ids = get_winner_client_ids(info)
    winner_players = [
        winners_by_id.get(cid)
        for cid in winner_ids
        if winners_by_id.get(cid) and is_clan_player(winners_by_id.get(cid))
    ]

    if winner_players:
        name_width = 22

        def format_winner_row(player):
            username = player.get("username") or "Unknown"
            name = username if len(username) <= name_width else username[: name_width - 3] + "..."
            return f"{name}"

        lines = [format_winner_row(p) for p in winner_players[:12]]
        more = len(winner_players) - len(lines)
        if more > 0:
            lines.append(f"... +{more}")
        embed.add_field(
            name=f"Gagnants ({CLAN_DISPLAY})",
            value="```\n" + "\n".join(lines) + "\n```",
            inline=True,
        )
    elif winners:
        gal_winners = [name for name in winners if is_clan_username(name)]
        shown = gal_winners[:12]
        more = len(winners) - len(shown)
        lines = [name for name in shown]
        if more > 0:
            lines.append(f"... +{more}")
        embed.add_field(
            name=f"Gagnants ({CLAN_DISPLAY})",
            value="```\n" + "\n".join(lines) + "\n```",
            inline=True,
        )
    else:
        embed.add_field(name="Gagnants", value=CLAN_DISPLAY, inline=True)

    if opponent_clans:
        clans_text = " ".join(opponent_clans)
        embed.add_field(name="Clans affront�s", value=clans_text, inline=True)
    else:
        embed.add_field(name="Clans affront�s", value="Aucun tag d�tect�", inline=True)

    embed.add_field(name="Mode", value=str(mode), inline=True)

    footer_time = None
    if end_raw:
        end_dt = parse_openfront_time(end_raw)
        if end_dt:
            footer_time = format_local_time(end_dt)
        else:
            footer_time = str(end_raw)
    elif start_raw:
        start_dt = parse_openfront_time(start_raw)
        if start_dt:
            footer_time = format_local_time(start_dt)
        else:
            footer_time = str(start_raw)
    if footer_time:
        embed.add_field(name="Heure victoire", value=footer_time, inline=True)
        embed.set_footer(text=f"Mis � jour le {footer_time}")

    return embed


def build_gal_only_winners_value(lines):
    kept = []
    for line in lines:
        raw = line.strip()
        if not raw:
            continue
        raw = raw.lstrip("★").strip()
        if is_clan_username(raw):
            kept.append(raw)
    if not kept:
        kept = [CLAN_DISPLAY]
    return "```\n" + "\n".join(kept) + "\n```"


def update_win_embed_url(embed: discord.Embed) -> discord.Embed:
    new_embed = embed.copy()
    game_id = None
    title = new_embed.title or ""
    match = re.search(r"OpenFront Game\s+([A-Za-z0-9_-]+)", title)
    if match:
        game_id = match.group(1)
    elif new_embed.url:
        url_match = re.search(r"/game/([A-Za-z0-9_-]+)", new_embed.url)
        if url_match:
            game_id = url_match.group(1)
    if game_id:
        try:
            new_embed.url = OPENFRONT_GAME_URL_TEMPLATE.format(game_id=game_id)
        except Exception:
            pass
    return new_embed


def cleanup_win_embed(embed: discord.Embed) -> discord.Embed:
    new_embed = embed.copy()
    for idx, field in enumerate(new_embed.fields):
        if field.name.startswith("Gagnants"):
            value = field.value.strip()
            if value.startswith("```") and value.endswith("```"):
                content = value.strip("`").strip()
            else:
                content = value
            lines = [line.rstrip() for line in content.splitlines() if line.strip()]
            new_value = build_gal_only_winners_value(lines)
            new_embed.set_field_at(
                idx,
                name=f"Gagnants ({CLAN_DISPLAY})",
                value=new_value,
                inline=field.inline,
            )
            break
    return update_win_embed_url(new_embed)


def build_ffa_win_embed(pseudo: str, player_id: str, session: dict, game_id: str, discord_id: Optional[int] = None):
    mode = session.get("gameMode") or session.get("mode") or "FFA"
    end_raw = session.get("end") or session.get("endTime")
    start_raw = session.get("start") or session.get("startTime")
    map_name = (
        session.get("mapName")
        or session.get("map")
        or session.get("mapTitle")
        or session.get("mapId")
    )

    game_url = None
    if game_id:
        try:
            game_url = OPENFRONT_GAME_URL_TEMPLATE.format(game_id=game_id)
        except Exception:
            game_url = None

    display_name = f"★ {pseudo}" if is_clan_username(pseudo) else pseudo
    embed = discord.Embed(
        title=f"🏆 Victoire FFA — {display_name}",
        url=game_url,
        description="Victoire FFA détectée via /register",
        color=discord.Color.orange(),
    )
    embed.add_field(name="Player ID", value=str(player_id), inline=True)
    embed.add_field(name="Game ID", value=str(game_id), inline=True)
    if map_name:
        embed.add_field(name="Map", value=str(map_name), inline=True)
    embed.add_field(name="Mode", value=str(mode), inline=True)
    if discord_id:
        embed.add_field(name="Joueur", value=f"<@{discord_id}>", inline=True)
    if game_url:
        embed.add_field(name="Lien", value=f"[Ouvrir la partie]({game_url})", inline=False)

    footer_time = None
    if end_raw:
        end_dt = parse_openfront_time(end_raw)
        if end_dt:
            footer_time = format_local_time(end_dt)
        else:
            footer_time = str(end_raw)
    elif start_raw:
        start_dt = parse_openfront_time(start_raw)
        if start_dt:
            footer_time = format_local_time(start_dt)
        else:
            footer_time = str(start_raw)
    if footer_time:
        embed.add_field(name="Heure victoire", value=footer_time, inline=True)
        embed.set_footer(text=f"Mis � jour le {footer_time}")
    return embed


def _total_pages(total_items, page_size):
    if total_items <= 0:
        return 1
    return (total_items + page_size - 1) // page_size


def build_leaderboard_embed_from_data(guild, page: int, page_size: int, top, last_updated):
    total_pages = _total_pages(len(top), page_size)
    page = max(1, min(page, total_pages))
    start = (page - 1) * page_size
    end = start + page_size
    page_items = top[start:end]

    embed = discord.Embed(
        title=f"?? Leaderboard {CLAN_DISPLAY} � Page {page}/{total_pages}",
        color=discord.Color.orange(),
    )
    total_wins = sum(p["wins_ffa"] + p["wins_team"] for p in top)
    total_losses = sum(p["losses_ffa"] + p["losses_team"] for p in top)
    total_players = len(top)

    embed.description = (
        f"**Joueurs:** {total_players}  |  "
        f"**Wins:** {total_wins}  |  "
        f"**Losses:** {total_losses}"
    )
    if guild and guild.icon:
        embed.set_thumbnail(url=guild.icon.url)

    name_width = 14
    mention_width = 22

    def truncate_name(name: str) -> str:
        if len(name) <= name_width:
            return name
        return name[: name_width - 3] + "..."

    truncated_counts = {}
    for p in page_items:
        t = truncate_name(p["display_name"])
        truncated_counts[t] = truncated_counts.get(t, 0) + 1

    def format_table_name(player):
        display = player["display_name"]
        name = truncate_name(display)
        if truncated_counts.get(name, 0) > 1:
            suffix = player["username"][-3:]
            base = display[: name_width - 4] if len(display) >= name_width - 3 else display
            name = base[: name_width - 4] + "+" + suffix
        return name

    def format_line(rank, player):
        username = format_table_name(player)
        score = f"{player['score']:.1f}"
        team = f"{player['wins_team']}W/{player['losses_team']}L"
        games = f"{player['total_games']}"
        return f"{rank:<3} {username:<{name_width}} {score:>5}  {team:>7}  {games:>3}"

    header = f"{'#':<3} {'JOUEUR':<{name_width}} {'SCORE':>5} {'TEAM':>7} {'G':>3}"
    sep = "-" * (name_width + 22)
    table = [header, sep]
    for i, p in enumerate(page_items, start + 1):
        table.append(format_line(i, p))

    embed.add_field(name="Classement", value="```\n" + "\n".join(table) + "\n```", inline=False)

    if last_updated:
        try:
            last_dt = datetime.strptime(last_updated, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            next_dt = last_dt + timedelta(minutes=REFRESH_MINUTES)
            footer = f"Mis � jour le {format_local_time(last_dt)} | Prochaine maj {format_local_time(next_dt)}"
        except Exception:
            footer = f"Mis � jour le {last_updated}"
        embed.set_footer(text=footer)

    return embed


async def build_leaderboard_ffa_embed_from_data(guild, page: int, page_size: int, top, last_updated):
    total_pages = _total_pages(len(top), page_size)
    page = max(1, min(page, total_pages))
    start = (page - 1) * page_size
    end = start + page_size
    page_items = top[start:end]

    embed = discord.Embed(
        title=f"Leaderboard FFA {CLAN_DISPLAY} - Page {page}/{total_pages}",
        color=discord.Color.orange(),
    )

    total_wins = sum(p["wins"] for p in top)
    total_losses = sum(p["losses"] for p in top)
    embed.description = f"**Wins:** {total_wins}  |  **Losses:** {total_losses}"
    if guild and guild.icon:
        embed.set_thumbnail(url=guild.icon.url)

    name_width = 10
    discord_width = 12

    def truncate_name(name: str) -> str:
        if len(name) <= name_width:
            return name
        return name[: name_width - 3] + "..."

    header = f"{'#':<3} {'JOUEUR':<{name_width}} {'DISCORD':<{discord_width}} {'SCORE':>5} {'W/L':>7} {'G':>3}"
    sep = "-" * (name_width + discord_width + 28)
    table = [header, sep]
    for i, p in enumerate(page_items, start + 1):
        name = truncate_name(p["display_name"])
        if p.get("discord_id") and guild:
            member = guild.get_member(p["discord_id"])
            if not member:
                try:
                    member = await guild.fetch_member(p["discord_id"])
                except Exception:
                    member = None
            discord_name = member.display_name if member else "-"
        else:
            discord_name = "-"
        if discord_name != "-":
            discord_name = re.sub(r"\[{}\]\s*".format(re.escape(CLAN_TAG)), "", discord_name, flags=re.IGNORECASE)
            discord_name = discord_name.strip()
        if len(discord_name) > discord_width:
            discord_name = discord_name[: discord_width - 3] + "..."
        score = f"{p['score']:.1f}"
        wl = f"{p['wins']}/{p['losses']}"
        games = f"{p['games']}"
        table.append(
            f"{i:<3} {name:<{name_width}} {discord_name:<{discord_width}} {score:>5} {wl:>7} {games:>3}"
        )

    embed.add_field(name="Classement FFA", value="```\n" + "\n".join(table) + "\n```", inline=False)

    if last_updated:
        try:
            last_dt = datetime.strptime(last_updated, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            next_dt = last_dt + timedelta(minutes=REFRESH_MINUTES)
            footer = f"Mis � jour le {format_local_time(last_dt)} | Prochaine maj {format_local_time(next_dt)}"
        except Exception:
            footer = f"Mis � jour le {last_updated}"
        embed.set_footer(text=footer)

    return embed


def build_leaderboard_1v1_embed_from_data(guild, page: int, page_size: int, top, last_updated):
    total_pages = _total_pages(len(top), page_size)
    page = max(1, min(page, total_pages))
    start = (page - 1) * page_size
    end = start + page_size
    page_items = top[start:end]

    embed = discord.Embed(
        title=f"?? Leaderboard 1v1 OpenFront � Top 100 � Page {page}/{total_pages}",
        color=discord.Color.orange(),
    )

    total_games = sum(p.get("games", 0) for p in top)
    embed.description = f"**Joueurs:** {len(top)}  |  **Games:** {total_games}"
    if guild and guild.icon:
        embed.set_thumbnail(url=guild.icon.url)

    name_width = 16

    def truncate_name(name: str) -> str:
        if len(name) <= name_width:
            return name
        return name[: name_width - 3] + "..."

    truncated_counts = {}
    for p in page_items:
        t = truncate_name(p.get("name") or "Unknown")
        truncated_counts[t] = truncated_counts.get(t, 0) + 1

    def format_table_name(player):
        raw_name = player.get("name") or "Unknown"
        name = truncate_name(raw_name)
        if truncated_counts.get(name, 0) > 1 and len(raw_name) >= 3:
            suffix = raw_name[-3:]
            base = raw_name[: name_width - 4] if len(raw_name) >= name_width - 3 else raw_name
            name = base[: name_width - 4] + "+" + suffix
        if is_clan_username(raw_name):
            if len(name) >= name_width:
                name = name[: name_width - 1]
            name = f"?{name}"
        return name

    def format_line(rank, player):
        username = format_table_name(player)
        elo = player.get("elo")
        elo_text = f"{int(elo)}" if isinstance(elo, (int, float)) else "?"
        games = f"{player.get('games', 0)}"
        ratio_pct = player.get("ratio_pct")
        ratio_text = f"{ratio_pct:.1f}%" if isinstance(ratio_pct, (int, float)) else "?"
        return f"{rank:<3} {username:<{name_width}} {elo_text:>5}  {games:>5}  {ratio_text:>6}"

    header = f"{'#':<3} {'JOUEUR':<{name_width}} {'ELO':>5} {'GAMES':>5} {'RATIO':>6}"
    sep = "-" * (name_width + 24)
    table = [header, sep]
    for i, p in enumerate(page_items, start + 1):
        table.append(format_line(i, p))

    embed.add_field(name="Classement 1v1", value="```\n" + "\n".join(table) + "\n```", inline=False)

    if last_updated:
        try:
            if isinstance(last_updated, datetime):
                last_dt = last_updated
            else:
                last_dt = datetime.strptime(last_updated, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            next_dt = last_dt + timedelta(minutes=ONEV1_REFRESH_MINUTES)
            footer = f"Mis � jour le {format_local_time(last_dt)} | Prochaine maj {format_local_time(next_dt)}"
        except Exception:
            footer = f"Mis � jour le {last_updated}"
        embed.set_footer(text=footer)

    return embed


def build_leaderboard_1v1_gal_embed_from_data(guild, gal_items, last_updated):
    embed = discord.Embed(
        title=f"?? Leaderboard 1v1 {CLAN_DISPLAY} � Top 100",
        color=discord.Color.orange(),
    )
    total_games = sum(p.get("games", 0) for p in gal_items)
    embed.description = f"**Joueurs:** {len(gal_items)}  |  **Games:** {total_games}"
    if guild and guild.icon:
        embed.set_thumbnail(url=guild.icon.url)

    name_width = 16

    def truncate_name(name: str) -> str:
        if len(name) <= name_width:
            return name
        return name[: name_width - 3] + "..."

    truncated_counts = {}
    for p in gal_items:
        t = truncate_name(p.get("name") or "Unknown")
        truncated_counts[t] = truncated_counts.get(t, 0) + 1

    def format_table_name(player):
        raw_name = player.get("name") or "Unknown"
        name = truncate_name(raw_name)
        if truncated_counts.get(name, 0) > 1 and len(raw_name) >= 3:
            suffix = raw_name[-3:]
            base = raw_name[: name_width - 4] if len(raw_name) >= name_width - 3 else raw_name
            name = base[: name_width - 4] + "+" + suffix
        if len(name) >= name_width:
            name = name[: name_width - 1]
        name = f"?{name}"
        return name

    def format_line(player):
        username = format_table_name(player)
        rank = player.get("rank", 0)
        elo = player.get("elo")
        elo_text = f"{int(elo)}" if isinstance(elo, (int, float)) else "?"
        games = f"{player.get('games', 0)}"
        ratio_pct = player.get("ratio_pct")
        ratio_text = f"{ratio_pct:.1f}%" if isinstance(ratio_pct, (int, float)) else "?"
        return f"{rank:<3} {username:<{name_width}} {elo_text:>5}  {games:>5}  {ratio_text:>6}"

    header = f"{'#':<3} {'JOUEUR':<{name_width}} {'ELO':>5} {'GAMES':>5} {'RATIO':>6}"
    sep = "-" * (name_width + 24)
    table = [header, sep]
    for p in gal_items:
        table.append(format_line(p))

    embed.add_field(name="Classement 1v1 [GAL]", value="```\n" + "\n".join(table) + "\n```", inline=False)

    if last_updated:
        try:
            if isinstance(last_updated, datetime):
                last_dt = last_updated
            else:
                last_dt = datetime.strptime(last_updated, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            next_dt = last_dt + timedelta(minutes=ONEV1_REFRESH_MINUTES)
            footer = f"Mis � jour le {format_local_time(last_dt)} | Prochaine maj {format_local_time(next_dt)}"
        except Exception:
            footer = f"Mis � jour le {last_updated}"
        embed.set_footer(text=footer)

    return embed


def build_ofm_board_embed_from_data(guild: discord.Guild, rows, team_name: str):
    if not rows:
        description = "Aucun participant accepté pour l'instant."
    else:
        sub_role = guild.get_role(OFM_SUB_ROLE_ID) if OFM_SUB_ROLE_ID else None
        lines = []
        for idx, row in enumerate(rows, start=1):
            user_id = row["user_id"]
            member = guild.get_member(user_id)
            suffix = ""
            if sub_role and member and sub_role in member.roles:
                suffix = " (Remplaçant)"
            lines.append(f"{idx}. <@{user_id}>{suffix}")
        description = "\n".join(lines)
    embed = discord.Embed(
        title=f"Participants OFM — {team_name}",
        description=description,
        color=discord.Color.orange(),
    )
    if guild and guild.icon:
        embed.set_thumbnail(url=guild.icon.url)
    return embed


def build_ofm_admin_panel_embed_from_data(team_name: str):
    embed = discord.Embed(
        title="✨ Panel OFM Manager",
        description=(
            f"**Equipe : {team_name}**\n"
            "Gérez l'équipe, les rôles et les statuts en un clic."
        ),
        color=discord.Color.from_rgb(88, 101, 242),
    )
    embed.add_field(
        name="👥 Gestion des membres",
        value=(
            "➕ **Ajouter**\n"
            "❌ **Retirer**\n"
            "🔝 **Promouvoir**\n"
            "⬇️ **Rétrograder**\n"
            "📋 **Voir la liste**"
        ),
        inline=True,
    )
    embed.add_field(
        name="🛡️ Gestion de l'équipe",
        value=(
            "✏️ **Nom d'équipe**\n"
            "👑 **Définir leader**\n"
            "🔄 **Définir remplaçant**"
        ),
        inline=True,
    )
    embed.set_footer(text="Accès réservé • OFM Managers")
    return embed


def build_mod_admin_panel_embed(
    guild: discord.Guild,
    selected_role: Optional[discord.Role] = None,
    allowed_commands: Optional[list] = None,
    mode: str = "permissions",
):
    role_text = selected_role.mention if selected_role else "Aucun rôle sélectionné"
    allowed_text = ", ".join(allowed_commands) if allowed_commands else "Aucune autorisation"
    role_line = f"Rôle sélectionné : {role_text}\n" if mode == "permissions" else ""
    embed = discord.Embed(
        title="🛡️ Panel Administration",
        description=(
            "Configure les permissions et exécute les actions de modération.\n"
            f"{role_line}"
            f"Autorisations : {allowed_text}\n"
            f"Mode : **{mode}**"
        ),
        color=discord.Color.red(),
    )
    embed.add_field(
        name="Gestion des sanctions",
        value=(
            "warn / warnlist / clearwarn\n"
            "mute / kick / ban / unban\n"
            "case"
        ),
        inline=False,
    )
    embed.add_field(
        name="Casier & Logs",
        value="Historique complet et logs centralisés.",
        inline=False,
    )
    embed.set_footer(text="Accès réservé fondateur/admin")
    return embed


def build_mod_log_embed(
    action: str,
    target: discord.abc.User,
    moderator: discord.abc.User,
    reason: Optional[str] = None,
    duration_seconds: Optional[int] = None,
):
    embed = discord.Embed(
        title="🧾 Log de modération",
        color=discord.Color.dark_red(),
    )
    embed.add_field(name="Action", value=action, inline=True)
    embed.add_field(name="Membre", value=f"{target.mention} (`{target.id}`)", inline=False)
    embed.add_field(name="Staff", value=f"{moderator.mention} (`{moderator.id}`)", inline=False)
    if reason:
        embed.add_field(name="Raison", value=reason, inline=False)
    if duration_seconds is not None:
        embed.add_field(name="Durée", value=format_duration(duration_seconds), inline=True)
    embed.timestamp = datetime.now(timezone.utc)
    return embed
