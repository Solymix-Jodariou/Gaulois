import aiohttp
from datetime import datetime, timezone

from parametres import (
    API_BASE,
    CLAN_TAG,
    ONEV1_LEADERBOARD_URL,
    ONEV1_REFRESH_MINUTES,
    OPENFRONT_API_KEY,
    USER_AGENT,
)


ONEV1_CACHE = {"items": [], "fetched_at": None}


def build_api_headers():
    headers = {"User-Agent": USER_AGENT}
    if OPENFRONT_API_KEY:
        headers["X-API-Key"] = OPENFRONT_API_KEY
        headers["Authorization"] = f"Bearer {OPENFRONT_API_KEY}"
    return headers


async def fetch_player_sessions(player_id: str):
    url = f"{API_BASE}/player/{player_id}/sessions"
    headers = {"User-Agent": USER_AGENT}
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(url, timeout=25) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise RuntimeError(f"HTTP {resp.status}: {text[:200]}")
            return await resp.json()


async def fetch_clan_sessions(session, start_iso, end_iso):
    url = f"{API_BASE}/clan/{CLAN_TAG}/sessions"
    params = {"start": start_iso, "end": end_iso}
    async with session.get(url, params=params, timeout=25) as resp:
        if resp.status != 200:
            text = await resp.text()
            raise RuntimeError(f"HTTP {resp.status}: {text[:200]}")
        return await resp.json()


async def fetch_game_info(session, game_id):
    url = f"{API_BASE}/game/{game_id}"
    async with session.get(url, params={"turns": "false"}, timeout=25) as resp:
        if resp.status != 200:
            text = await resp.text()
            raise RuntimeError(f"HTTP {resp.status}: {text[:200]}")
        data = await resp.json()
        return data.get("info", {})


async def fetch_games_list(session, start_iso: str, end_iso: str, max_games: int):
    games = []
    offset = 0
    while len(games) < max_games:
        limit = min(1000, max_games - len(games))
        params = {
            "start": start_iso,
            "end": end_iso,
            "type": "Public",
            "limit": str(limit),
            "offset": str(offset),
        }
        url = f"{API_BASE}/games"
        async with session.get(url, params=params, timeout=25) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise RuntimeError(f"HTTP {resp.status}: {text[:200]}")
            batch = await resp.json()
        if not batch:
            break
        games.extend(batch)
        offset += len(batch)
        if len(batch) < limit:
            break
    return games


def _extract_list(payload):
    if isinstance(payload, list):
        return payload
    for key in ("items", "data", "players", "leaderboard", "results"):
        if isinstance(payload, dict) and isinstance(payload.get(key), list):
            return payload[key]
    return []


def _get_first_value(entry, keys, default=None):
    for key in keys:
        if key in entry and entry[key] is not None:
            return entry[key]
    return default


def _normalize_1v1_entry(entry):
    name = _get_first_value(entry, ["username", "player", "name", "displayName", "user"])
    if not name:
        return None
    clan_tag = entry.get("clanTag")
    if clan_tag and f"[{clan_tag}]".upper() not in str(name).upper():
        name = f"[{clan_tag}] {name}"
    elo = _get_first_value(entry, ["elo", "rating", "mmr", "score"])
    wins = _get_first_value(entry, ["wins", "win", "victories"], 0)
    losses = _get_first_value(entry, ["losses", "loss", "defeats"], 0)
    games = _get_first_value(entry, ["games", "matches", "totalGames", "played"])
    if games is None:
        games = (wins or 0) + (losses or 0)
    ratio = _get_first_value(entry, ["winRate", "winrate", "ratio", "winLossRatio"])
    if ratio is None and games:
        ratio = (wins / games) if games > 0 else 0.0
    if isinstance(ratio, (int, float)) and ratio <= 1.0:
        ratio_pct = ratio * 100
    elif isinstance(ratio, (int, float)):
        ratio_pct = float(ratio)
    else:
        ratio_pct = None
    return {
        "name": str(name),
        "elo": elo,
        "games": int(games) if games is not None else 0,
        "wins": int(wins) if wins is not None else 0,
        "losses": int(losses) if losses is not None else 0,
        "ratio_pct": ratio_pct,
    }


async def fetch_official_1v1_leaderboard(limit: int):
    headers = build_api_headers()
    items = []
    page = 1
    async with aiohttp.ClientSession(headers=headers) as session:
        while len(items) < limit:
            params = {"page": str(page)}
            async with session.get(ONEV1_LEADERBOARD_URL, params=params, timeout=25) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    raise RuntimeError(f"HTTP {resp.status}: {text[:200]}")
                payload = await resp.json()
            raw_items = payload.get("1v1") or payload.get("oneVone") or _extract_list(payload)
            if not raw_items:
                break
            for entry in raw_items:
                norm = _normalize_1v1_entry(entry)
                if norm:
                    items.append(norm)
                    if len(items) >= limit:
                        break
            if len(raw_items) < 50:
                break
            page += 1
    return items[:limit]


async def get_official_1v1_leaderboard_cached(limit: int):
    now = datetime.now(timezone.utc)
    cached_at = ONEV1_CACHE.get("fetched_at")
    cached_items = ONEV1_CACHE.get("items") or []
    if cached_items and cached_at:
        age = (now - cached_at).total_seconds()
        if age < ONEV1_REFRESH_MINUTES * 60:
            return cached_items[:limit], cached_at
    items = await fetch_official_1v1_leaderboard(limit)
    ONEV1_CACHE["items"] = items
    ONEV1_CACHE["fetched_at"] = now
    return items[:limit], now
