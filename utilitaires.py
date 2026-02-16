import os
import re
from typing import Optional
from datetime import datetime, timezone, timedelta

import discord

from parametres import (
    ADMIN_USER_ID,
    CLAN_DISPLAY,
    CLAN_TAG,
    FOUNDER_USER_ID,
    SCORE_GAMES_WEIGHT,
    SCORE_RATIO_WEIGHT,
)


def calculate_ratio(wins_ffa, losses_ffa, wins_team, losses_team):
    wins = wins_ffa + wins_team
    losses = losses_ffa + losses_team
    total = wins + losses
    if total == 0:
        return 0.0
    return wins / total


def calculate_score(wins, losses, games):
    if games <= 0:
        return 0.0
    ratio = wins / games
    return ratio * SCORE_RATIO_WEIGHT + games * SCORE_GAMES_WEIGHT


def format_local_time(dt: datetime) -> str:
    offset_hours = int(os.getenv("LEADERBOARD_TIMEZONE_OFFSET_HOURS", "1"))
    local_dt = dt + timedelta(hours=offset_hours)
    return local_dt.strftime("%Y-%m-%d %H:%M")


def parse_openfront_time(value) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        ts = int(value)
        if ts > 1_000_000_000_000:
            return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
        if ts > 1_000_000_000:
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        return None
    if isinstance(value, str):
        raw = value.strip()
        if raw.isdigit():
            ts = int(raw)
            if ts > 1_000_000_000_000:
                return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
            if ts > 1_000_000_000:
                return datetime.fromtimestamp(ts, tz=timezone.utc)
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except Exception:
            return None
    return None


def parse_duration_seconds(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    raw = value.strip().lower()
    if raw.isdigit():
        return int(raw)
    match = re.fullmatch(r"(\d+)([smhd])", raw)
    if not match:
        return None
    amount = int(match.group(1))
    unit = match.group(2)
    if unit == "s":
        return amount
    if unit == "m":
        return amount * 60
    if unit == "h":
        return amount * 3600
    if unit == "d":
        return amount * 86400
    return None


def format_duration(seconds: Optional[int]) -> str:
    if not seconds:
        return "Permanent"
    if seconds % 86400 == 0:
        return f"{seconds // 86400}d"
    if seconds % 3600 == 0:
        return f"{seconds // 3600}h"
    if seconds % 60 == 0:
        return f"{seconds // 60}m"
    return f"{seconds}s"


def format_uptime(delta: timedelta) -> str:
    total = int(delta.total_seconds())
    days, rem = divmod(total, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)
    parts = []
    if days:
        parts.append(f"{days}j")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if seconds or not parts:
        parts.append(f"{seconds}s")
    return " ".join(parts)


def is_admin_user_id(user_id: int) -> bool:
    return user_id in {FOUNDER_USER_ID, ADMIN_USER_ID}


def is_admin_member(member: discord.Member) -> bool:
    if member.id == member.guild.owner_id:
        return True
    if is_admin_user_id(member.id):
        return True
    role_ids = {role.id for role in member.roles}
    return bool(role_ids.intersection({FOUNDER_USER_ID, ADMIN_USER_ID}))


def is_clan_username(username: str) -> bool:
    if not username:
        return False
    upper = username.upper()
    tag = CLAN_TAG.upper()
    return f"[{tag}]" in upper or upper.startswith(f"{tag} ")


def is_clan_player(player: dict) -> bool:
    tag = player.get("clanTag")
    if tag and str(tag).upper() == CLAN_TAG.upper():
        return True
    return is_clan_username(player.get("username") or "")


def game_mode(info):
    return (info.get("config", {}) or {}).get("gameMode") or ""


def get_winner_client_ids(info):
    winner = info.get("winner")
    if not winner or not isinstance(winner, list) or len(winner) < 3:
        return set()
    winners = winner[2]
    if isinstance(winners, list):
        return set(winners)
    # Format observed: ["team", "Purple", "id1", "id2", ...]
    tail = winner[2:]
    if all(isinstance(x, str) for x in tail):
        return set(tail)
    return set()
