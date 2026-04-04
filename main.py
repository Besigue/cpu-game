# ============================
# main.py — Besigue Server (v77.9b-CPU-GAME-LIVE-CONVERT)
#
# Based on your pasted v76.8 (NO rewrites / no new files).
#
# ✅ FIXES in this build:
#  1) Winner detection is robust even if a score becomes a string (e.g. "780").
#  2) After Count Aces & Tens, if someone crosses 400+, server hard-stops immediately
#     via _end_game_now() (always sends show_winner + game_over + state).
#  3) Standardize no-winner show_winner text:
#     "No Winner Yet, Let's Keep Going!"
#  4) Emit optional WS "deck_count" message after deck changes (front-end nudge),
#     including when deck_count hits 0 so drawPile can show 0.png.
#  5) Deck count in state payloads is always a safe int >= 0.
# ============================

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Query
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import uuid
import random
import asyncio
import logging
import secrets
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional, Tuple

log = logging.getLogger("besigue")
logging.basicConfig(level=logging.INFO)

app = FastAPI()



# Basic health check for Render / uptime probes
@app.get("/")
async def root_health():
    return {"ok": True}

# ---------------------------------------------------
# CORS
# ---------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------
# Static
# ---------------------------------------------------
app.mount("/static", StaticFiles(directory="server/static"), name="static")

# ---------------------------------------------------
# In-memory data
# ---------------------------------------------------
ROOMS: Dict[str, Dict[str, Any]] = {}
WS_CLIENTS: Dict[str, List[WebSocket]] = {}
ROOM_SOCKETS: Dict[str, Dict[str, List[WebSocket]]] = {}

RANKS = ["ace", "king", "queen", "jack", "10", "9", "8", "7"]
rank_value_map = {
    "ace": 7,
    "10": 6,
    "king": 5,
    "queen": 4,
    "jack": 3,
    "9": 2,
    "8": 1,
    "7": 0
}
SUITS = ["hearts", "diamonds", "clubs", "spades"]

MAX_SEATS = 4
WINNING_SCORE = 400
RECONNECT_GRACE_SECONDS = 30
CPU_TAKEOVER_SLOW_PLAY_SECONDS = 30
CPU_TAKEOVER_SLOW_WINDOW_SECONDS = 120

NO_WINNER_TEXT = "No Winner Yet, Let's Keep Going!"

# Actions that should be blocked once game_over
GAMEPLAY_ACTIONS = (
    "play_card", "draw_card",
    "start_marriage", "score_marriage", "cancel_marriage",
    "score_meld", "cancel_meld",
    "pickup_melds",
    "count_aces_tens",
    "next_round",
)

# ---------------------------------------------------
# Utility Helpers
# ---------------------------------------------------
def _normalize_name(raw: Any) -> str:
    pn = (raw or "").strip()
    if not pn:
        return f"Guest{random.randint(1, 999):03d}"
    return pn


def _ensure_player_reconnect_token(player: dict) -> str:
    token = player.get("reconnect_token")
    if not token or not isinstance(token, str):
        token = make_reconnect_token()
        player["reconnect_token"] = token
    return token


def _unique_name_in_room(room: dict, desired: str) -> str:
    desired = desired.strip()
    existing = set(p.get("name") for p in room.get("players", []) if p.get("name"))
    if desired not in existing:
        return desired

    for _ in range(2000):
        candidate = f"{desired}{random.randint(1, 999):03d}"
        if candidate not in existing:
            return candidate

    return f"{desired}{uuid.uuid4().hex[:4]}"



def utc_now():
    return datetime.now(timezone.utc)


def make_reconnect_token() -> str:
    return secrets.token_urlsafe(32)


def _seconds_left_from_iso(raw: Optional[str]) -> int:
    if not raw:
        return 0
    try:
        dt = datetime.fromisoformat(raw)
        secs = int((dt - utc_now()).total_seconds())
        return max(0, secs)
    except Exception:
        return 0


def _ensure_player_runtime_meta(player: dict):
    if player.get("is_cpu"):
        player.setdefault("connection_state", "cpu_builtin")
        player.setdefault("cpu_playing_for_name", None)
        return
    _ensure_player_reconnect_token(player)
    player.setdefault("connection_state", "human_active")
    player.setdefault("disconnect_at", None)
    player.setdefault("reconnect_deadline", None)
    player.setdefault("cpu_takeover_started_at", None)
    player.setdefault("cpu_playing_for_name", None)


def _set_player_connected(player: dict):
    _ensure_player_runtime_meta(player)
    player["connection_state"] = "human_active"
    player["disconnect_at"] = None
    player["reconnect_deadline"] = None
    player["cpu_takeover_started_at"] = None
    player["cpu_playing_for_name"] = None


def _set_player_disconnected_reserved(player: dict):
    _ensure_player_runtime_meta(player)
    now = utc_now()
    player["connection_state"] = "human_disconnected_reserved"
    player["disconnect_at"] = now.isoformat()
    player["reconnect_deadline"] = (now + timedelta(seconds=RECONNECT_GRACE_SECONDS)).isoformat()
    player["cpu_takeover_started_at"] = None
    player["cpu_playing_for_name"] = None


def _set_player_cpu_takeover(player: dict):
    _ensure_player_runtime_meta(player)
    now = utc_now()
    player["connection_state"] = "cpu_takeover_active"
    player["cpu_takeover_started_at"] = now.isoformat()
    player["cpu_playing_for_name"] = player.get("name")
    player["disconnect_at"] = player.get("disconnect_at") or now.isoformat()
    player["reconnect_deadline"] = None


def canonical_card_with_uid(code: str):
    return {"code": code, "uid": str(uuid.uuid4())}


def card_image_url_for_code(code: str):
    if code.startswith("joker_red"):
        return "/static/cards/joker_red.png"
    if code.startswith("joker_black"):
        return "/static/cards/joker_black.png"
    return f"/static/cards/{code}.svg"


def new_deck_full_132():
    base = []
    for _ in range(4):
        for r in RANKS:
            for s in SUITS:
                base.append(f"{r}_of_{s}")
    base += ["joker_red", "joker_red", "joker_black", "joker_black"]
    random.shuffle(base)
    return base


def _safe_remove_ws(room_id: str, player_name: str, ws: WebSocket):
    # Remove from WS_CLIENTS room list
    try:
        if room_id in WS_CLIENTS and ws in WS_CLIENTS[room_id]:
            WS_CLIENTS[room_id].remove(ws)
    except:
        pass

    # Remove from ROOM_SOCKETS[player] list
    try:
        plist = ROOM_SOCKETS.get(room_id, {}).get(player_name, [])
        if ws in plist:
            plist.remove(ws)
    except:
        pass


async def _register_single_socket(room_id: str, player_name: str, ws: WebSocket):
    """
    ✅ Critical fix:
    Enforce ONE active websocket per (room_id, player_name).
    If player reconnects, close & remove old sockets and keep the new one.
    """
    WS_CLIENTS.setdefault(room_id, [])
    ROOM_SOCKETS.setdefault(room_id, {})
    ROOM_SOCKETS[room_id].setdefault(player_name, [])

    # Close & remove any old sockets for this player
    old_list = list(ROOM_SOCKETS[room_id].get(player_name, []))
    for old in old_list:
        try:
            _safe_remove_ws(room_id, player_name, old)
        except:
            pass
        try:
            await old.close()
        except:
            pass

    ROOM_SOCKETS[room_id][player_name] = []
    # Add new socket
    if ws not in WS_CLIENTS[room_id]:
        WS_CLIENTS[room_id].append(ws)
    ROOM_SOCKETS[room_id][player_name].append(ws)


async def _send_to_room(room_id: str, message: dict):
    clients = WS_CLIENTS.get(room_id, [])
    dead = []
    for ws in list(clients):
        try:
            await ws.send_json(message)
        except:
            dead.append(ws)
    for ws in dead:
        try:
            clients.remove(ws)
        except:
            pass
    WS_CLIENTS[room_id] = clients

async def _send_to_player(room_id: str, player_name: str, message: dict):
    """Send a WS message ONLY to a specific player in a room (all tabs for that player)."""
    try:
        plist = ROOM_SOCKETS.get(room_id, {}).get(player_name, [])
        dead = []
        for ws in list(plist):
            try:
                await ws.send_json(message)
            except:
                dead.append(ws)
        for ws in dead:
            try:
                plist.remove(ws)
            except:
                pass
    except:
        pass


async def _emit_deck_count(room_id: str, deck_count: int):
    """
    Optional UI nudge: safe even if frontend ignores it.
    Helps force drawPile to update, including to 0.png when deck_count hits 0.
    """
    try:
        await _send_to_player(room_id, player, {"type": "deck_count", "deck_count": int(deck_count)})
    except:
        pass


async def broadcast_lobby_rooms():
    rooms = []
    for r in ROOMS.values():
        if r.get("phase") == "waiting":
            rooms.append({
                "room_id": r["room_id"],
                "label": r["label"],
                "players": len(r.get("players", []))
            })
    for _rid, clients in WS_CLIENTS.items():
        dead = []
        for ws in list(clients):
            try:
                await ws.send_json({"type": "rooms_update", "rooms": rooms})
            except:
                dead.append(ws)
        for ws in dead:
            try:
                clients.remove(ws)
            except:
                pass


def build_used_uids_by_category(scored_list: List[dict]) -> Dict[str, set]:
    used: Dict[str, set] = {}
    for rec in scored_list or []:
        cat = rec.get("category")
        uids = rec.get("uids") or []
        if not cat:
            continue
        used.setdefault(cat, set()).update(uids)
    return used


def build_used_uids_all(scored_list: List[dict]) -> set:
    used = set()
    for rec in scored_list or []:
        uids = rec.get("uids") or []
        for u in uids:
            used.add(u)
    return used


def suit_of(code: str) -> str:
    if not code or code.startswith("joker"):
        return ""
    if "_of_" not in code:
        return ""
    return code.split("_of_")[1]


def rank_of(code: str) -> int:
    if not code or code.startswith("joker"):
        return -99
    if "_of_" not in code:
        return -99
    r = code.split("_of_")[0]
    return rank_value_map.get(r, -99)


def phase3_legal_uids_for_player(room: dict, player: str) -> set:
    hand = room.get("hands", {}).get(player, [])
    if not hand:
        return set()

    trick = room.get("current_trick", []) or []
    trump = room.get("trump_suit") or ""

    if len(trick) == 0:
        return set(c["uid"] for c in hand)

    lead_card = trick[0]["card"]
    lead_suit = suit_of(lead_card)

    best_lead_rank = -999
    for t in trick:
        if lead_suit and suit_of(t["card"]) == lead_suit:
            best_lead_rank = max(best_lead_rank, rank_of(t["card"]))

    any_trump_played = False
    best_trump_rank = -999
    for t in trick:
        if trump and suit_of(t["card"]) == trump and not t["card"].startswith("joker"):
            any_trump_played = True
            best_trump_rank = max(best_trump_rank, rank_of(t["card"]))

    lead_suit_cards = [
        c for c in hand
        if lead_suit and suit_of(c["code"]) == lead_suit and not c["code"].startswith("joker")
    ]
    trump_cards = [
        c for c in hand
        if trump and suit_of(c["code"]) == trump and not c["code"].startswith("joker")
    ]

    if lead_suit_cards:
        higher = [c for c in lead_suit_cards if rank_of(c["code"]) > best_lead_rank]
        if higher:
            return set(c["uid"] for c in higher)
        return set(c["uid"] for c in lead_suit_cards)

    if trump_cards:
        if any_trump_played:
            over = [c for c in trump_cards if rank_of(c["code"]) > best_trump_rank]
            if over:
                return set(c["uid"] for c in over)
        return set(c["uid"] for c in trump_cards)

    return set(c["uid"] for c in hand)


def phase3_determine_winner(room: dict) -> str:
    trick = room.get("current_trick", []) or []
    if not trick:
        return ""

    trump = room.get("trump_suit") or ""
    lead_card = trick[0]["card"]
    lead_suit = suit_of(lead_card)

    trump_cards = []
    if trump:
        trump_cards = [
            t for t in trick
            if not t["card"].startswith("joker") and suit_of(t["card"]) == trump
        ]
    if trump_cards:
        best_rank = -999
        winner = trump_cards[0]["player"]
        for t in trump_cards:
            rv = rank_of(t["card"])
            if rv > best_rank:
                best_rank = rv
                winner = t["player"]
        return winner

    if lead_suit:
        followers = [t for t in trick if suit_of(t["card"]) == lead_suit]
        if followers:
            best_rank = -999
            winner = followers[0]["player"]
            for t in followers:
                rv = rank_of(t["card"])
                if rv > best_rank:
                    best_rank = rv
                    winner = t["player"]
            return winner

    return trick[0]["player"]


def _players_order(room: dict) -> List[str]:
    return [p["name"] for p in room.get("players", [])]


def _count_total_cards_for_player(room: dict, player: str) -> int:
    return len(room.get("hands", {}).get(player, []) or []) + len(room.get("melds", {}).get(player, []) or [])


def _remove_specific_uid_from_hand_or_meld(room: dict, player: str, uid: str) -> Tuple[Optional[dict], Optional[str]]:
    hand = room.get("hands", {}).get(player, [])
    for c in list(hand):
        if c.get("uid") == uid:
            hand.remove(c)
            return c, "hand"

    meld = room.get("melds", {}).get(player, [])
    for c in list(meld):
        if c.get("uid") == uid:
            meld.remove(c)
            return c, "meld"

    return None, None



def _remove_specific_card_uid(card_list, uid):
    """Remove a single card dict with matching uid from a list, in-place.
    Returns True if removed, else False.

    This is intentionally tiny and defensive because several action paths
    remove 2-card combos by uid (marriages, etc.).
    """
    if not uid or not isinstance(card_list, list):
        return False
    for i, c in enumerate(list(card_list)):
        if isinstance(c, dict) and c.get("uid") == uid:
            try:
                card_list.pop(i)
            except Exception:
                return False
            return True
    return False

def _remove_last_remaining_card(room: dict, player: str) -> Optional[dict]:
    """
    For forced auto-play: remove ONE remaining card.
    Prefer hand, else meld.
    """
    hand = room.get("hands", {}).get(player, [])
    if hand:
        return hand.pop(0)
    meld = room.get("melds", {}).get(player, [])
    if meld:
        return meld.pop(0)
    return None


def _append_trick_to_won_pile(room: dict, winner: str, trick: List[dict]):
    """Append a completed trick's cards into the winner's won-tricks pile.

    Defensive guard:
    During CPU auto-play / edge-case transitions, the same trick can be processed twice.
    That would double-count Aces/10s at round-end (e.g., total 330 instead of 320).
    We prevent this by tracking a per-room set of trick signatures.
    """
    room.setdefault("won_tricks", {})
    room["won_tricks"].setdefault(winner, [])

    # Signature is stable across re-processing and does not rely on winner.
    # Uses (uid, card) in play order when available.
    sig_items = []
    for t in (trick or []):
        if not isinstance(t, dict):
            continue
        sig_items.append((t.get("uid"), t.get("card")))
    sig = tuple(sig_items)

    sigset = room.setdefault("_won_trick_sigs", set())
    if sig in sigset:
        return
    sigset.add(sig)

    for t in (trick or []):
        if not isinstance(t, dict):
            continue
        code = t.get("card")
        if code:
            room["won_tricks"][winner].append(code)


def _bonus_points_from_won_tricks(room: dict) -> Dict[str, int]:
    """
    +10 per Ace and +10 per 10 found in each player's won-tricks pile.
    Jokers do not count.
    """
    bonus = {}
    players = _players_order(room)
    won = room.get("won_tricks", {}) or {}
    for p in players:
        pile = won.get(p, []) or []
        count = 0
        for code in pile:
            if not code or code.startswith("joker") or "_of_" not in code:
                continue
            r = code.split("_of_")[0]
            if r == "ace" or r == "10":
                count += 1
        bonus[p] = count * 10
    return bonus


def _format_winner_text(room: dict, winner: str) -> str:
    """
    Formats text like:
      "P3 is the Winner with 780 Points!
       P1 has 770, P4 has 670 and P2 has 540.
       Thank You For Playing!"
    Uses the room's current scores dict.
    """
    scores = room.get("scores", {}) or {}
    entries = []
    for p in _players_order(room):
        try:
            entries.append((p, int(scores.get(p, 0) or 0)))
        except:
            entries.append((p, 0))
    for p, s in scores.items():
        if p not in [x[0] for x in entries]:
            try:
                entries.append((p, int(s or 0)))
            except:
                entries.append((p, 0))

    entries.sort(key=lambda x: x[1], reverse=True)

    try:
        win_score = int(scores.get(winner, 0) or 0)
    except:
        win_score = 0

    others = [(p, s) for (p, s) in entries if p != winner]

    if not others:
        return f"{winner} is the Winner with {win_score} Points!\nThank You For Playing!"

    parts = [f"{p} has {s}" for (p, s) in others]
    if len(parts) == 1:
        others_line = parts[0]
    else:
        others_line = ", ".join(parts[:-1]) + f" and {parts[-1]}"

    return (
        f"{winner} is the Winner with {win_score} Points!\n"
        f"{others_line}.\n"
        f"Thank You For Playing!"
    )


def _evaluate_winner_after_round(room: dict) -> Tuple[str, str]:
    """
    ✅ Robust winner evaluation:
    - Coerce scores to int to prevent string compare edge cases.
    """
    scores_raw = room.get("scores", {}) or {}
    if not scores_raw:
        return "", NO_WINNER_TEXT

    scores: Dict[str, int] = {}
    for k, v in scores_raw.items():
        try:
            scores[k] = int(v or 0)
        except:
            scores[k] = 0

    max_score = max(scores.values()) if scores else 0
    if max_score < WINNING_SCORE:
        return "", NO_WINNER_TEXT

    tied = [p for p, s in scores.items() if s == max_score]
    if len(tied) == 1:
        w = tied[0]
        return w, _format_winner_text(room, w)

    last = room.get("last_trick_winner") or ""
    if last and last in tied:
        return last, _format_winner_text(room, last)

    return "TIE", "Game is a TIE"


async def _end_game_now(room_id: str, room: dict, winner_code: str, text: str, bonus: Optional[dict] = None):
    """
    ✅ Immediate stop: called when someone hits 400+ mid-round.
    Keeps existing show_winner UI contract.
    """
    order = _players_order(room)

    room["phase"] = "game_over"
    room["awaiting_next_round"] = False
    room["count_required"] = False
    room["count_done"] = False

    room["post_trick_draws"] = False
    room["draw_order"] = []
    room["draw_index"] = 0
    room["current_turn"] = None

    await _send_to_room(room_id, {
        "type": "show_winner",
        "winner": winner_code,
        "text": text,
        "scores": room.get("scores", {}),
        "last_trick_winner": room.get("last_trick_winner"),
        "bonus": bonus or {}
    })
    await _send_to_room(room_id, {"type": "game_over", "msg": "Game Over"})

    await broadcast_state_without_hands(room_id)
    for p in order:
        await send_state_update_to_player(room_id, p)


async def _check_instant_win(room_id: str, room: dict) -> bool:
    scores_raw = room.get("scores", {}) or {}
    if not scores_raw:
        return False

    scores: Dict[str, int] = {}
    for k, v in scores_raw.items():
        try:
            scores[k] = int(v or 0)
        except:
            scores[k] = 0

    max_score = max(scores.values()) if scores else 0
    if max_score < WINNING_SCORE:
        return False

    winner_code, text = _evaluate_winner_after_round(room)
    if not winner_code:
        top = None
        topv = -1
        for p, v in scores.items():
            if v > topv:
                topv = v
                top = p
        winner_code = top or ""
        text = _format_winner_text(room, winner_code) if winner_code else "Game Over"

    await _end_game_now(room_id, room, winner_code, text, bonus={})
    return True



async def _schedule_phase3_auto_pickup(room_id: str, delay_seconds: float = 6.0):
    """After Phase 3 starts, auto-pickup melds after a short pause.
    This covers the case where the leader is a HUMAN and no one clicks a meld card,
    especially when NO melds exist at all (Phase 2 was skipped).
    """
    room = ROOMS.get(room_id)
    if not room:
        return

    # Cancel any previous scheduled task for this room
    try:
        t = room.get("_phase3_auto_pickup_task")
        if t and not t.done():
            t.cancel()
    except Exception:
        pass

    async def _job():
        await asyncio.sleep(delay_seconds)
        r = ROOMS.get(room_id)
        if not r:
            return
        if r.get("phase") != "phase3":
            return
        if r.get("phase3_melds_picked"):
            return
        await perform_global_meld_pickup(room_id)

    room["_phase3_auto_pickup_task"] = asyncio.create_task(_job())


async def enter_phase3_if_ready(room_id: str):
    room = ROOMS.get(room_id)
    if not room:
        return

    if len(room.get("deck", [])) != 0:
        return

    # Phase 3 normally begins after Phase 2 when the deck hits 0.
    # CPU testing revealed a rare edge-case: if NO first marriage is ever scored,
    # trump_suit remains None and the game can stay in Phase 1 until deck=0.
    # In that case, we bridge directly Phase1 -> Phase3 (no-trump Phase3).
    phase = room.get("phase")
    if phase == "phase2":
        pass  # normal path
    elif phase == "phase1" and room.get("trump_suit") in (None, "", False):
        pass  # bridge path (no trump was ever determined)
    else:
        return


    room["phase"] = "phase3"
    room["post_trick_draws"] = False
    room["draw_order"] = []
    room["draw_index"] = 0

    room["current_trick"] = []
    room["pending_trick_clear"] = False

    lead = room.get("last_trick_winner")
    if lead:
        room["current_turn"] = lead

    room["phase3_melds_picked"] = False

    # If nobody has meld cards, Phase 2 was effectively skipped.
    has_any_melds = False
    try:
        for pp in [pp["name"] for pp in room.get("players", [])]:
            if (room.get("melds", {}) or {}).get(pp, []):
                has_any_melds = True
                break
    except Exception:
        has_any_melds = False

    await _send_to_room(room_id, {
        "type": "phase3_start",
        "leader": room.get("current_turn"),
        "msg": ("No More Scoring, Pick Up Your Meld Cards..." if has_any_melds else "No More Scoring, Let\'s Keep Playing!"),
        "has_any_melds": has_any_melds
    })

    # Always schedule auto-pickup after 6 seconds so Phase 3 can begin even if the leader is human.
    await _schedule_phase3_auto_pickup(room_id, 6.0)

    # ✅ nudge deck count = 0 so UI can show 0.png immediately
    await _emit_deck_count(room_id, len(room.get("deck", [])))

    await broadcast_state_without_hands(room_id)
    for p in [pp["name"] for pp in room.get("players", [])]:
        await send_state_update_to_player(room_id, p)


async def perform_global_meld_pickup(room_id: str):
    room = ROOMS.get(room_id)
    if not room:
        return

    room.setdefault("phase3_melds_picked", False)
    if room["phase3_melds_picked"]:
        return

    room.setdefault("hands", {})
    room.setdefault("melds", {})

    for p in [pp["name"] for pp in room.get("players", [])]:
        room["hands"].setdefault(p, [])
        room["melds"].setdefault(p, [])
        meld_arr = room["melds"][p]
        if meld_arr:
            for c in meld_arr:
                room["hands"][p].append(c)
            room["melds"][p] = []

    room["phase3_melds_picked"] = True

    room["current_trick"] = []
    room["pending_trick_clear"] = False
    room["post_trick_draws"] = False
    room["draw_order"] = []
    room["draw_index"] = 0
    room["meld_scored_this_trick"] = False

    await _send_to_room(room_id, {
        "type": "phase3_melds_picked",
        "msg": "Meld cards picked up. Phase 3 begins."
    })

    await _send_to_room(room_id, {"type": "clear_trick"})

    await broadcast_state_without_hands(room_id)
    for p in [pp["name"] for pp in room.get("players", [])]:
        await send_state_update_to_player(room_id, p)


# ---------------------------------------------------
# Dealing
# ---------------------------------------------------
def deal_cards_to_players(room: dict, amount: int = 9):
    deck = room["deck"]
    hands = {}
    for p in room["players"]:
        name = p["name"]
        hand = []
        for _ in range(amount):
            if not deck:
                break
            code = deck.pop(0)
            obj = canonical_card_with_uid(code)
            obj["image"] = card_image_url_for_code(code)
            hand.append(obj)
        hands[name] = hand
    room["deck"] = deck
    return hands


# ---------------------------------------------------
# NEW ROUND (after Phase 3)
# ---------------------------------------------------
async def start_next_round(room_id: str):
    room = ROOMS.get(room_id)
    if not room:
        return

    players = _players_order(room)
    if len(players) != MAX_SEATS:
        return

    room.setdefault("round_start_lead", room.get("lead") or room.get("current_turn") or players[0])

    cur_start = room.get("round_start_lead")
    if cur_start not in players:
        cur_start = players[0]

    idx = players.index(cur_start)
    next_start = players[(idx + 1) % MAX_SEATS]
    room["round_start_lead"] = next_start

    room["lead"] = next_start
    room["current_turn"] = next_start
    room["phase"] = "phase1"

    room["deck"] = new_deck_full_132()
    room["hands"] = deal_cards_to_players(room, 9)

    room["current_trick"] = []
    room["pending_trick_clear"] = False

    room["marriage_pending"] = None
    room["temp_melds"] = {}
    room["post_trick_draws"] = False
    room["draw_order"] = []
    room["draw_index"] = 0
    room["last_trick_winner"] = None
    room["trump_suit"] = None
    room["meld_scored_this_trick"] = False
    room["phase3_melds_picked"] = False

    room["awaiting_next_round"] = False
    room["count_required"] = False
    room["count_done"] = False

    room["melds"] = {p: [] for p in players}
    room["scored_melds"] = {p: [] for p in players}
    room["won_tricks"] = {p: [] for p in players}
    room["_won_trick_sigs"] = set()

    await _send_to_room(room_id, {"type": "clear_trick"})

    # ✅ nudge deck count (fresh deck)
    await _emit_deck_count(room_id, len(room.get("deck", [])))

    await broadcast_state_without_hands(room_id)
    for p in players:
        await send_state_update_to_player(room_id, p)

    await _send_to_room(room_id, {
        "type": "next_round",
        "lead": next_start,
        "msg": f"Next Round — {next_start} leads"
    })


# ---------------------------------------------------
# SHARED STATE (no hands)
# ---------------------------------------------------
def _build_public_trick_payload(room: dict) -> List[dict]:
    """
    Send the full trick so clients can rebuild #currentTrick reliably.
    Includes play_index and image.
    """
    trick = room.get("current_trick", []) or []
    payload = []
    for i, t in enumerate(trick[:4]):
        card_code = t.get("card") or ""
        # Defensive: sometimes upstream code can store booleans/objects here
        if not isinstance(card_code, str):
            card_code = ""
        payload.append({
            "player": t.get("player"),
            "card": {"code": card_code, "uid": t.get("uid")},
            "image": card_image_url_for_code(card_code) if card_code else "",
            "play_index": i
        })
    return payload


async def broadcast_state_without_hands(room_id: str):
    room = ROOMS.get(room_id)
    if not room:
        return
    players = [p["name"] for p in room.get("players", [])]
    player_statuses = _get_public_player_statuses(room)

    current_trick_codes = [t.get("card") for t in (room.get("current_trick", []) or []) if t.get("card")]
    current_trick_full = _build_public_trick_payload(room)

    # ✅ safe deck_count always >= 0 and int
    deck_count = len(room.get("deck", []) or [])
    if deck_count < 0:
        deck_count = 0

    data = {
        "phase": room.get("phase"),
        "players": players,
        "player_statuses": player_statuses,
        "deck_count": int(deck_count),
        "melds": room.get("melds", {}),
        "scores": room.get("scores", {}),
        "room_label": room.get("label"),
        "current_turn": room.get("current_turn"),
        "lead": room.get("lead"),
        "marriage_pending": ({"player": room.get("marriage_pending", {}).get("player"), "type": room.get("marriage_pending", {}).get("type")} if room.get("marriage_pending") else None),  # cards are private
        "host": room.get("host"),
        "trump_suit": room.get("trump_suit"),
        "post_trick_draws": room.get("post_trick_draws"),
        "draw_order": room.get("draw_order"),
        "draw_index": room.get("draw_index"),
        "last_trick_winner": room.get("last_trick_winner"),
        "meld_scored_this_trick": room.get("meld_scored_this_trick", False),
        "phase3_melds_picked": room.get("phase3_melds_picked", False),
        "ready_to_start": room.get("ready_to_start", False),

        "current_trick_codes": current_trick_codes,
        "current_trick": current_trick_full,

        "pending_trick_clear": room.get("pending_trick_clear", False),
        "awaiting_next_round": room.get("awaiting_next_round", False),

        "count_required": room.get("count_required", False),
        "count_done": room.get("count_done", False),
    }
    await _send_to_room(room_id, {"type": "state_update", "data": data})


# ---------------------------------------------------
# PRIVATE HAND update
# ---------------------------------------------------
async def send_state_update_to_player(room_id: str, player_name: str):
    room = ROOMS.get(room_id)
    if not room:
        return

    players = [p["name"] for p in room.get("players", [])]
    player_statuses = _get_public_player_statuses(room)
    all_hands = room.get("hands", {})
    player_hand = all_hands.get(player_name, [])

    current_trick_codes = [t.get("card") for t in (room.get("current_trick", []) or []) if t.get("card")]
    current_trick_full = _build_public_trick_payload(room)

    deck_count = len(room.get("deck", []) or [])
    if deck_count < 0:
        deck_count = 0

    data = {
        "phase": room.get("phase"),
        "players": players,
        "player_statuses": player_statuses,
        "deck_count": int(deck_count),
        "melds": room.get("melds", {}),
        "scores": room.get("scores", {}),
        "room_label": room.get("label"),
        "current_turn": room.get("current_turn"),
        "lead": room.get("lead"),
        "marriage_pending": (room.get("marriage_pending") if (room.get("marriage_pending") and room.get("marriage_pending", {}).get("player") == player_name) else None),
        "host": room.get("host"),
        "trump_suit": room.get("trump_suit"),
        "post_trick_draws": room.get("post_trick_draws"),
        "draw_order": room.get("draw_order"),
        "draw_index": room.get("draw_index"),
        "last_trick_winner": room.get("last_trick_winner"),
        "meld_scored_this_trick": room.get("meld_scored_this_trick", False),
        "phase3_melds_picked": room.get("phase3_melds_picked", False),
        "ready_to_start": room.get("ready_to_start", False),

        "current_trick_codes": current_trick_codes,
        "current_trick": current_trick_full,

        "pending_trick_clear": room.get("pending_trick_clear", False),
        "awaiting_next_round": room.get("awaiting_next_round", False),

        "count_required": room.get("count_required", False),
        "count_done": room.get("count_done", False),

        "hands": {player_name: player_hand},
    }

    msg = {"type": "state_update", "data": data}

    room_sockets = ROOM_SOCKETS.get(room_id, {})
    client_list = room_sockets.get(player_name, [])

    dead = []
    for ws in list(client_list):
        try:
            await ws.send_json(msg)
        except:
            dead.append(ws)

    for ws in dead:
        try:
            client_list.remove(ws)
        except:
            pass

    room_sockets[player_name] = client_list
    ROOM_SOCKETS[room_id] = room_sockets


# ---------------------------------------------------
# CREATE ROOM
# ---------------------------------------------------
@app.post("/api/create_room")
async def api_create_room(req: Request):
    b = await req.json()
    pn = _normalize_name(b.get("player_name"))

    rid = f"{pn}_Room"
    counter = 1
    while rid in ROOMS:
        rid = f"{pn}_Room_{counter}"
        counter += 1

    room = {
        "room_id": rid,
        "label": f"{pn}'s Room, 4 seats",
        "host": pn,
        "players": [{"name": pn, "is_cpu": False, "reconnect_token": make_reconnect_token(), "connection_state": "human_active", "disconnect_at": None, "reconnect_deadline": None, "cpu_takeover_started_at": None, "cpu_playing_for_name": None}],
        "phase": "waiting",
        "deck": [],
        "melds": {pn: []},
        "scores": {pn: 0},
        "ready_to_start": False,
        "current_trick": [],
        "pending_trick_clear": False,
        "current_turn": None,
        "marriage_pending": None,
        "temp_melds": {},
        "post_trick_draws": False,
        "draw_order": [],
        "draw_index": 0,
        "last_trick_winner": None,
        "trump_suit": None,
        "scored_melds": {pn: []},
        "meld_scored_this_trick": False,
        "phase3_melds_picked": False,
        "won_tricks": {pn: []},
        "_won_trick_sigs": set(),
        "round_start_lead": None,

        "awaiting_next_round": False,
        "count_required": False,
        "count_done": False,
        "_disconnect_tasks": {},
    }

    ROOMS[rid] = room
    WS_CLIENTS.setdefault(rid, [])
    ROOM_SOCKETS.setdefault(rid, {})

    await broadcast_lobby_rooms()
    reconnect_token = _ensure_player_reconnect_token(room["players"][0])
    log.info(f"[RECONNECT] create_room room={rid} player={pn} token_present={bool(reconnect_token)}")
    return {
        "room_id": rid,
        "room_label": room["label"],
        "player_name": pn,
        "room_host": room["host"],
        "reconnect_token": reconnect_token
    }


# ---------------------------------------------------
# LIST ROOMS
# ---------------------------------------------------
@app.get("/api/list_rooms")
async def api_list_rooms():
    rooms = []
    for r in ROOMS.values():
        if r.get("phase") == "waiting":
            rooms.append({
                "room_id": r["room_id"],
                "label": r["label"],
                "players": len(r.get("players", []))
            })
    return {"rooms": rooms}


# ---------------------------------------------------
# JOIN ROOM
# ---------------------------------------------------
@app.post("/api/join_room")
async def api_join_room(req: Request):
    b = await req.json()
    rid = b.get("room_id")
    pn_req = _normalize_name(b.get("player_name"))

    if rid not in ROOMS:
        return JSONResponse({"error": "room_not_found"}, 400)

    room = ROOMS[rid]

    room.setdefault("players", [])
    room.setdefault("scores", {})
    room.setdefault("melds", {})
    room.setdefault("scored_melds", {})
    room.setdefault("won_tricks", {})
    room.setdefault("awaiting_next_round", False)
    room.setdefault("count_required", False)
    room.setdefault("count_done", False)
    room.setdefault("_disconnect_tasks", {})

    existing_names = [p.get("name") for p in room["players"] if p.get("name")]

    # Waiting-room rule:
    # if a new player types a name that already exists in the room,
    # assign a unique visible name like "PlayerName123".
    # Do NOT treat that as a reconnect while the room is still waiting.
    if room.get("phase") == "waiting":
        if len(room["players"]) >= MAX_SEATS:
            return JSONResponse({"error": "room_full"}, 400)

        pn = _unique_name_in_room(room, pn_req)

        room["players"].append({"name": pn, "is_cpu": False, "reconnect_token": make_reconnect_token(), "connection_state": "human_active", "disconnect_at": None, "reconnect_deadline": None, "cpu_takeover_started_at": None, "cpu_playing_for_name": None})
        room["scores"][pn] = 0
        room["melds"].setdefault(pn, [])
        room["scored_melds"].setdefault(pn, [])
        room["won_tricks"].setdefault(pn, [])

        room["ready_to_start"] = (len(room["players"]) >= MAX_SEATS)

        await broadcast_state_without_hands(rid)
        await broadcast_lobby_rooms()

        player_obj = next((p for p in room.get("players", []) if p.get("name") == pn), None)
        reconnect_token = _ensure_player_reconnect_token(player_obj) if player_obj else None
        log.info(f"[RECONNECT] join_room(waiting) room={rid} player={pn} token_present={bool(reconnect_token)}")

        return {
            "joined": True,
            "room_id": rid,
            "player_name": pn,
            "room_label": room["label"],
            "room_host": room["host"],
            "reconnect_token": reconnect_token
        }

    # Mid-game rule:
    # only allow a true rejoin if the exact existing player name already belongs
    # to this room. New joins after game start are blocked.
    if pn_req not in existing_names:
        return JSONResponse({"error": "game_started"}, 400)

    room_sockets = ROOM_SOCKETS.get(rid, {})
    active_socks = room_sockets.get(pn_req, []) or []

    if active_socks:
        room["ready_to_start"] = (len(room["players"]) >= MAX_SEATS)
        await broadcast_state_without_hands(rid)
        await broadcast_lobby_rooms()
        player_obj = next((p for p in room.get("players", []) if p.get("name") == pn_req), None)
        reconnect_token = _ensure_player_reconnect_token(player_obj) if player_obj else None
        log.info(f"[RECONNECT] join_room(existing-active) room={rid} player={pn_req} token_present={bool(reconnect_token)}")
        return {
            "joined": True,
            "already_in_room": True,
            "room_id": rid,
            "player_name": pn_req,
            "room_label": room["label"],
            "room_host": room["host"],
            "reconnect_token": reconnect_token
        }

    player_obj = next((p for p in room.get("players", []) if p.get("name") == pn_req), None)
    reconnect_token = _ensure_player_reconnect_token(player_obj) if player_obj else None
    log.info(f"[RECONNECT] join_room(mid-game) room={rid} player={pn_req} token_present={bool(reconnect_token)}")
    return {
        "joined": True,
        "room_id": rid,
        "player_name": pn_req,
        "room_label": room["label"],
        "room_host": room["host"],
        "reconnect_token": reconnect_token
    }


# ---------------------------------------------------
# LEAVE ROOM
# ---------------------------------------------------

@app.post("/api/reconnect_room")
async def api_reconnect_room(req: Request):
    b = await req.json()
    rid = b.get("room_id")
    token = b.get("reconnect_token")

    if not rid or rid not in ROOMS:
        return JSONResponse({"error": "room_not_found"}, 404)
    if not token or not isinstance(token, str):
        return JSONResponse({"error": "bad_reconnect_token"}, 400)

    room = ROOMS[rid]
    for p in room.get("players", []):
        if p.get("is_cpu"):
            continue
        _ensure_player_runtime_meta(p)
        if p.get("reconnect_token") != token:
            continue

        reconnect_token = _ensure_player_reconnect_token(p)
        log.info(f"[RECONNECT] reconnect_room accepted room={rid} player={p.get('name')} token_present={bool(reconnect_token)}")

        await _mark_player_connected(rid, p.get("name"))
        await broadcast_state_without_hands(rid)
        for pp in [x.get("name") for x in room.get("players", []) if x.get("name")]:
            await send_state_update_to_player(rid, pp)

        return {
            "ok": True,
            "room_id": rid,
            "player_name": p.get("name"),
            "room_label": room.get("label"),
            "room_host": room.get("host"),
            "reconnect_token": reconnect_token,
        }

    return JSONResponse({"error": "reconnect_not_allowed"}, 403)


@app.post("/api/leave_room")
async def api_leave_room(req: Request):
    b = await req.json()
    rid = b.get("room_id")
    pn = _normalize_name(b.get("player_name"))

    if not rid or rid not in ROOMS:
        return JSONResponse({"error": "room_not_found"}, 404)

    room = ROOMS[rid]
    players = room.get("players", [])

    if not any(p["name"] == pn for p in players):
        return JSONResponse({"error": "not_in_room"}, 400)

    if room["phase"] == "waiting" and room["host"] == pn and len(players) > 1:
        return JSONResponse({"error": "host_cannot_leave_with_players"}, 400)

    room["players"] = [p for p in players if p["name"] != pn]
    room.get("scores", {}).pop(pn, None)
    room.get("melds", {}).pop(pn, None)
    if "hands" in room and isinstance(room["hands"], dict):
        room["hands"].pop(pn, None)
    if "scored_melds" in room and isinstance(room["scored_melds"], dict):
        room["scored_melds"].pop(pn, None)
    room.get("won_tricks", {}).pop(pn, None)

    room_sockets = ROOM_SOCKETS.get(rid, {})
    personal_sockets = room_sockets.pop(pn, [])
    for sock in list(personal_sockets):
        try:
            _safe_remove_ws(rid, pn, sock)
        except:
            pass
        try:
            await sock.close()
        except:
            pass

    ROOM_SOCKETS[rid] = room_sockets

    room_deleted = False

    if not room["players"]:
        ROOMS.pop(rid, None)
        WS_CLIENTS.pop(rid, None)
        ROOM_SOCKETS.pop(rid, None)
        room_deleted = True
    else:
        if room["host"] == pn:
            new_host = room["players"][0]["name"]
            room["host"] = new_host
            room["label"] = f"{new_host}'s Room, 4 seats"

        room["ready_to_start"] = len(room["players"]) >= MAX_SEATS
        await broadcast_state_without_hands(rid)

    await broadcast_lobby_rooms()

    return {
        "left": True,
        "room_id": rid,
        "room_deleted": room_deleted,
        "player_name": pn
    }


# ---------------------------------------------------
# START GAME
# ---------------------------------------------------

# ============================================================
# CPU SUPPORT (v1) — cpu-test only (no VIP gating yet)
# ============================================================
CPU_NORMAL_DELAY_RANGE = (0.6, 1.2)  # seconds

def _get_player_obj(room: dict, name: str):
    for p in room.get('players', []) or []:
        if p.get('name') == name:
            return p
    return None


def _get_public_player_statuses(room: dict) -> Dict[str, dict]:
    out: Dict[str, dict] = {}
    for p in room.get("players", []) or []:
        _ensure_player_runtime_meta(p)
        _ensure_player_reconnect_token(p)
        state = p.get("connection_state")
        item = {
            "connection_state": state,
            "cpu_playing_for_name": p.get("cpu_playing_for_name"),
        }
        if state == "human_disconnected_reserved":
            item["reconnect_seconds_left"] = _seconds_left_from_iso(p.get("reconnect_deadline"))
        out[p.get("name")] = item
    return out


async def _cancel_disconnect_task(room: dict, player_name: str):
    room.setdefault("_disconnect_tasks", {})
    task = room["_disconnect_tasks"].pop(player_name, None)
    if task:
        try:
            task.cancel()
        except Exception:
            pass


async def _schedule_disconnect_takeover(room_id: str, player_name: str):
    room = ROOMS.get(room_id)
    if not room:
        return
    room.setdefault("_disconnect_tasks", {})
    await _cancel_disconnect_task(room, player_name)

    async def _runner():
        try:
            await asyncio.sleep(RECONNECT_GRACE_SECONDS)
            r = ROOMS.get(room_id)
            if not r:
                return
            p = _get_player_obj(r, player_name)
            if not p or p.get("is_cpu"):
                return
            remaining = ROOM_SOCKETS.get(room_id, {}).get(player_name, []) or []
            if remaining:
                return
            if r.get("phase") == "waiting":
                return
            if p.get("connection_state") != "human_disconnected_reserved":
                return

            _set_player_cpu_takeover(p)
            await broadcast_state_without_hands(room_id)
            for pp in [x.get("name") for x in r.get("players", []) if x.get("name")]:
                await send_state_update_to_player(room_id, pp)
            await cpu_maybe_act(room_id)
        except asyncio.CancelledError:
            return
        finally:
            r2 = ROOMS.get(room_id)
            if r2 is not None:
                r2.setdefault("_disconnect_tasks", {}).pop(player_name, None)

    room["_disconnect_tasks"][player_name] = asyncio.create_task(_runner())


async def _mark_player_connected(room_id: str, player_name: str):
    room = ROOMS.get(room_id)
    if not room:
        return
    p = _get_player_obj(room, player_name)
    if not p or p.get("is_cpu"):
        return
    _set_player_connected(p)
    await _cancel_disconnect_task(room, player_name)


async def _mark_player_disconnected(room_id: str, player_name: str):
    room = ROOMS.get(room_id)
    if not room:
        return
    p = _get_player_obj(room, player_name)
    if not p or p.get("is_cpu"):
        return
    if room.get("phase") == "waiting":
        return

    _set_player_disconnected_reserved(p)
    await broadcast_state_without_hands(room_id)
    for pp in [x.get("name") for x in room.get("players", []) if x.get("name")]:
        await send_state_update_to_player(room_id, pp)
    await _schedule_disconnect_takeover(room_id, player_name)


def _cpu_delay_for_player(player: dict) -> float:
    _ensure_player_runtime_meta(player)
    if player.get("connection_state") != "cpu_takeover_active":
        return 0.0
    started_raw = player.get("cpu_takeover_started_at")
    try:
        started = datetime.fromisoformat(started_raw) if started_raw else None
    except Exception:
        started = None
    if started is None:
        return 0.0
    elapsed = (utc_now() - started).total_seconds()
    if elapsed < CPU_TAKEOVER_SLOW_WINDOW_SECONDS:
        return float(CPU_TAKEOVER_SLOW_PLAY_SECONDS)
    return 0.0


def _is_cpu(room_or_name, maybe_name=None):
    """Return True if the given player is a CPU.

    Backward/forward compatible: older call sites used _is_cpu(room, name);
    newer ones sometimes accidentally call _is_cpu(name).
    """
    # Called as _is_cpu(name)
    if maybe_name is None:
        name = room_or_name
        return isinstance(name, str) and name.startswith("CPU")

    room = room_or_name
    name = maybe_name
    try:
        pobj = _get_player_obj(room, name)
        if pobj is not None:
            _ensure_player_runtime_meta(pobj)
            return bool(pobj.get("is_cpu")) or pobj.get("connection_state") == "cpu_takeover_active"
    except Exception:
        pass
    return isinstance(name, str) and name.startswith("CPU")

def _cpu_fill_room_to_four(room: dict):
    """Fill empty seats with CPU players (CPU 1..CPU 3) until 4 total."""
    room.setdefault('players', [])
    existing_names = [p.get('name') for p in room['players']]
    cpu_idx = 1
    while len(room['players']) < MAX_SEATS:
        cpu_name = f"CPU {cpu_idx}"
        cpu_idx += 1
        if cpu_name in existing_names:
            continue
        room['players'].append({'name': cpu_name, 'is_cpu': True, 'controller': 'cpu', 'connection_state': 'cpu_builtin', 'cpu_playing_for_name': None})
        existing_names.append(cpu_name)

def _rank_value(code: str) -> int:
    if not code or str(code).startswith('joker'):
        return 99
    if '_of_' not in code:
        return 50
    r = code.split('_of_')[0]
    order = {'7':1,'8':2,'9':3,'10':4,'jack':5,'queen':6,'king':7,'ace':8}
    return order.get(r, 50)

def _choose_cpu_play_uid(room: dict, cpu_name: str):
    hand = room.get('hands', {}).get(cpu_name, []) or []
    if not hand:
        return None
    hand_sorted = sorted(hand, key=lambda c: (_rank_value(c.get('code','')), c.get('code','')))
    return (hand_sorted[0] or {}).get('uid')

class _NoopWS:
    async def send_json(self, *args, **kwargs):
        return

_DUMMY_WS = _NoopWS()


async def cpu_maybe_act(room_id: str):
    """If it's currently a CPU's turn, have CPUs take actions until a human's turn.
    Runs in a background task and is safe to call repeatedly.
    """
    room = ROOMS.get(room_id)
    if not room:
        return

    # prevent overlapping CPU runners per-room
    if room.get("_cpu_running"):
        return

    async def _runner():
        r = ROOMS.get(room_id)
        if not r:
            return
        r["_cpu_running"] = True
        try:
            steps = 0
            # hard cap prevents runaway loops if something unexpected happens
            while steps < 50:
                r2 = ROOMS.get(room_id)
                if not r2:
                    break

                phase = (r2.get("phase") or "").strip()
                if phase in ("", "waiting"):
                    break

                cur = r2.get("current_turn")
                if not cur or not _is_cpu(r2, cur):
                    break

                # CPU delay (simple for now; reconnect/takeover delays come later)
                # Phase 3 pacing: slow down CPU chains so humans can see the table.
                if room.get("phase") == "phase3":
                    # After a trick completes, keep the last trick visible a bit before the winning CPU leads.
                    if room.get("pending_trick_clear") and _is_cpu(room, room.get("current_turn")):
                        await asyncio.sleep(3.0)
                    else:
                        await asyncio.sleep(2.0)
                else:
                    await asyncio.sleep(random.uniform(*CPU_NORMAL_DELAY_RANGE))

                # ------------------------------------------------------------
                # CPU ACTION CHOICE
                # ------------------------------------------------------------
                # Phase 3 begins with a mandatory "pickup_melds" action by the leader.
                cur_player = _get_player_obj(r2, cur) or {}
                takeover_delay = _cpu_delay_for_player(cur_player)

                if phase == "phase3" and not r2.get("phase3_melds_picked"):
                    # CPU_PHASE3_PICKUP_DELAY: give humans time to see melds before pickup
                    await asyncio.sleep(max(6.0, takeover_delay))
                    r_check = ROOMS.get(room_id) or {}
                    if not _is_cpu(r_check, cur):
                        break
                    await process_action(_DUMMY_WS, room_id, cur, {"action": "pickup_melds"})

                elif r2.get("post_trick_draws"):
                    # CPU_DRAW_DELAY_POST_TRICK: give humans time to see the completed trick
                    await asyncio.sleep(max(2.5, takeover_delay))
                    r_check = ROOMS.get(room_id) or {}
                    if not _is_cpu(r_check, cur):
                        break
                    await process_action(_DUMMY_WS, room_id, cur, {"action": "draw_card"})

                else:
                    # Try to play a legal card. If the first choice is illegal (Phase 3 follow-suit rules),
                    # iterate through the hand until one succeeds.
                    hand = (r2.get("hands", {}).get(cur) or [])
                    # stable ordering so behavior is reproducible
                    hand_sorted = sorted(hand, key=lambda c: (_rank_value(c.get("code","")), c.get("code","")))
                    before_turn = r2.get("current_turn")
                    before_trick_len = len(r2.get("current_trick", []) or [])
                    played = False

                    for c in hand_sorted:
                        uid = (c or {}).get("uid")
                        if not uid:
                            continue
                        r_check = ROOMS.get(room_id) or {}
                        if not _is_cpu(r_check, cur):
                            break
                        if takeover_delay > 0:
                            await asyncio.sleep(takeover_delay)
                            r_check = ROOMS.get(room_id) or {}
                            if not _is_cpu(r_check, cur):
                                break
                        await process_action(_DUMMY_WS, room_id, cur, {"action": "play_card", "card": {"uid": uid}})

                        r_after = ROOMS.get(room_id) or {}
                        after_turn = r_after.get("current_turn")
                        after_trick_len = len(r_after.get("current_trick", []) or [])
                        # success heuristic: trick advanced OR turn advanced
                        if after_turn != before_turn or after_trick_len != before_trick_len:
                            played = True
                            break

                    if not played:
                        # No legal play found (should be rare). Bail out to avoid infinite loop.
                        break


                steps += 1
                # yield control; allows WS messages/state broadcasts to flush
                await asyncio.sleep(0)
        finally:
            r3 = ROOMS.get(room_id)
            if r3 is not None:
                r3["_cpu_running"] = False

    asyncio.create_task(_runner())

@app.post("/api/start_cpu_game")
async def api_start_cpu_game(req: Request):
    b = await req.json()
    rid = b.get('room_id')
    pn = _normalize_name(b.get('player_name'))

    if rid not in ROOMS:
        return JSONResponse({'error': 'room_not_found'}, 400)

    room = ROOMS[rid]
    if room.get('host') != pn:
        return JSONResponse({'error': 'not_host'}, 403)
    if room.get('phase') != 'waiting':
        return JSONResponse({'error': 'bad_phase'}, 400)

    _cpu_fill_room_to_four(room)
    players = [p['name'] for p in room.get('players', [])]
    if len(players) != MAX_SEATS:
        return JSONResponse({'error': 'need_4_seats'}, 400)

    lead = random.choice(players)
    room['lead'] = lead
    room['current_turn'] = lead
    room['phase'] = 'lead_selection'
    room['round_start_lead'] = lead

    room['awaiting_next_round'] = False
    room['count_required'] = False
    room['count_done'] = False

    await broadcast_state_without_hands(rid)

    deck = new_deck_full_132()
    room['deck'] = deck
    room['hands'] = deal_cards_to_players(room, 9)

    room['current_trick'] = []
    room['phase'] = 'phase1'
    room['marriage_pending'] = None
    room['post_trick_draws'] = False
    room['draw_order'] = []
    room['draw_index'] = 0
    room['last_trick_winner'] = None
    room['trump_suit'] = None
    room['temp_melds'] = {}
    room['scored_melds'] = {}
    room['meld_scored_this_trick'] = False
    room['phase3_melds_picked'] = False
    room['ready_to_start'] = True

    room['won_tricks'] = {}
    room['_won_trick_sigs'] = set()
    for p in players:
        room['scores'][p] = 0
        room['melds'].setdefault(p, [])
        room['scored_melds'].setdefault(p, [])
        room['won_tricks'].setdefault(p, [])

    await _emit_deck_count(rid, len(room.get('deck', [])))
    for p in players:
        await send_state_update_to_player(rid, p)

    await _send_to_room(rid, {'type': 'game_start', 'phase': 'phase1', 'lead': lead})
    await broadcast_lobby_rooms()
    await cpu_maybe_act(rid)
    return {'started': True, 'room_id': rid, 'cpu_filled': True}

@app.post("/api/start_game")
async def api_start_game(req: Request):
    b = await req.json()
    rid = b.get("room_id")
    pn = _normalize_name(b.get("player_name"))

    if rid not in ROOMS:
        return JSONResponse({"error": "room_not_found"}, 400)

    room = ROOMS[rid]

    if room.get("host") != pn:
        return JSONResponse({"error": "not_host"}, 403)

    if room.get("phase") != "waiting":
        return JSONResponse({"error": "bad_phase"}, 400)

    players = [p["name"] for p in room.get("players", [])]

    if len(players) != MAX_SEATS:
        return JSONResponse({"error": "need_4_players"}, 400)

    lead = random.choice(players)
    room["lead"] = lead
    room["current_turn"] = lead
    room["phase"] = "lead_selection"

    room["round_start_lead"] = lead

    room["awaiting_next_round"] = False
    room["count_required"] = False
    room["count_done"] = False

    await broadcast_state_without_hands(rid)

    deck = new_deck_full_132()
    room["deck"] = deck
    room["hands"] = deal_cards_to_players(room, 9)

    room["current_trick"] = []
    room["pending_trick_clear"] = False
    room["phase"] = "phase1"
    room["marriage_pending"] = None
    room["post_trick_draws"] = False
    room["draw_order"] = []
    room["draw_index"] = 0
    room["last_trick_winner"] = None
    room["trump_suit"] = None
    room["temp_melds"] = {}
    room["scored_melds"] = {}
    room["meld_scored_this_trick"] = False
    room["phase3_melds_picked"] = False
    room["ready_to_start"] = True

    room["won_tricks"] = {}
    room["_won_trick_sigs"] = set()
    for p in players:
        room["scores"][p] = 0
        room["melds"].setdefault(p, [])
        room["scored_melds"].setdefault(p, [])
        room["won_tricks"].setdefault(p, [])

    # ✅ nudge deck count (132 at start)
    await _emit_deck_count(rid, len(room.get("deck", [])))

    for p in players:
        await send_state_update_to_player(rid, p)

    await _send_to_room(rid, {
        "type": "game_start",
        "phase": "phase1",
        "lead": lead
    })

    await broadcast_lobby_rooms()
    return {"started": True, "room_id": rid}


# ============================================================
# WEBSOCKET — Game Flow
# ============================================================
async def process_action(ws: WebSocket, room_id: str, player_name: str, msg: dict):
    """Process a single WS-style game action. Used by both humans (WS) and CPUs (server)."""
    action = msg.get("action")
    player = player_name
    room = ROOMS.get(room_id)
    if not room:
        return

    if room.get("phase") == "game_over":
        if action in GAMEPLAY_ACTIONS:
            await ws.send_json({"type": "info", "msg": "game_over"})
        return

    # ============================================================
    # ROUND END WAIT: Host must Count A/10 first, then Continue next round
    # ============================================================
    if room.get("awaiting_next_round"):
        if action == "count_aces_tens":
            if player != room.get("host"):
                await ws.send_json({"type": "info", "msg": "only_host_can_count"})
                return
            if not room.get("count_required", False):
                await ws.send_json({"type": "info", "msg": "count_not_required"})
                await broadcast_state_without_hands(room_id)
                for p in _players_order(room):
                    await send_state_update_to_player(room_id, p)
                return
            if room.get("count_done", False):
                await ws.send_json({"type": "info", "msg": "count_already_done"})
                await broadcast_state_without_hands(room_id)
                for p in _players_order(room):
                    await send_state_update_to_player(room_id, p)
                return

            bonus = _bonus_points_from_won_tricks(room)
            for p, add in bonus.items():
                # ✅ robust int conversion
                try:
                    cur = int(room["scores"].get(p, 0) or 0)
                except:
                    cur = 0
                room["scores"][p] = cur + int(add)

            room["count_done"] = True
            room["count_required"] = False

            # Clear trick display now
            room["current_trick"] = []
            room["pending_trick_clear"] = False
            await _send_to_room(room_id, {"type": "clear_trick"})

            # ✅ IMPORTANT: if bonus pushed someone to 400+, end game NOW.
            winner_code, text = _evaluate_winner_after_round(room)
            if winner_code and winner_code != "":
                await _end_game_now(room_id, room, winner_code, text, bonus=bonus)
                return

            await _send_to_room(room_id, {
                "type": "aces_tens_counted",
                "bonus": bonus,
                "scores": room.get("scores", {}),
                "msg": "Aces and 10s counted."
            })

            # ✅ standardized "no winner" text
            await _send_to_room(room_id, {
                "type": "show_winner",
                "winner": "",
                "text": NO_WINNER_TEXT,
                "scores": room.get("scores", {}),
                "last_trick_winner": room.get("last_trick_winner"),
                "bonus": bonus
            })

            room["phase"] = "round_end_wait"

            await broadcast_state_without_hands(room_id)
            for p in _players_order(room):
                await send_state_update_to_player(room_id, p)
            return

        if action == "next_round":
            if player != room.get("host"):
                await ws.send_json({"type": "info", "msg": "only_host_can_start_next_round"})
                return
            if room.get("count_required", False) and not room.get("count_done", False):
                await ws.send_json({"type": "info", "msg": "must_count_aces_tens_first"})
                await broadcast_state_without_hands(room_id)
                for p in _players_order(room):
                    await send_state_update_to_player(room_id, p)
                return

            room["awaiting_next_round"] = False
            await start_next_round(room_id)
            return

        if action in (
            "play_card", "draw_card",
            "start_marriage", "score_marriage", "cancel_marriage",
            "score_meld", "cancel_meld",
            "pickup_melds",
        ):
            await ws.send_json({"type": "info", "msg": "waiting_for_next_round_host_click"})
            return

    # ============================================================
    # PHASE 3: PICKUP MELDS
    # ============================================================
    if action == "pickup_melds":
        if room.get("phase") != "phase3":
            await ws.send_json({"type": "info", "msg": "pickup_only_in_phase3"})
            return
        if room.get("phase3_melds_picked"):
            await ws.send_json({"type": "info", "msg": "melds_already_picked"})
            return

        leader = room.get("current_turn")
        if leader != player:
            await ws.send_json({"type": "info", "msg": "only_leader_can_pickup_melds"})
            return

        await perform_global_meld_pickup(room_id)
        await ws.send_json({"type": "info", "msg": "melds_picked_up"})
        return

    # ============================================================
    # PLAY CARD
    # ============================================================
    if action == "play_card":
        if room.get("post_trick_draws"):
            await ws.send_json({"type": "info", "msg": "cannot_play_during_draw_phase"})
            return

        if room.get("current_turn") != player:
            await ws.send_json({"type": "info", "msg": "not_your_turn"})
            return

        card_obj = msg.get("card", {}) or {}
        uid = card_obj.get("uid")
        if not uid:
            await ws.send_json({"type": "info", "msg": "missing_uid"})
            return

        # ✅ Phase 3 auto-pickup if leader has no meld cards
        if room.get("phase") == "phase3" and not room.get("phase3_melds_picked", False):
            leader = room.get("current_turn")
            if player == leader:
                leader_melds = room.get("melds", {}).get(player, []) or []
                if len(leader_melds) == 0:
                    await perform_global_meld_pickup(room_id)

        # ✅ Only clear previous trick when the next lead is actually played
        if room.get("phase") == "phase3" and room.get("pending_trick_clear", False):
            room["current_trick"] = []
            room["pending_trick_clear"] = False
            await _send_to_room(room_id, {"type": "clear_trick"})
            await broadcast_state_without_hands(room_id)
            for p in _players_order(room):
                await send_state_update_to_player(room_id, p)

        # ✅ Phase 3 legality check must happen BEFORE removal
        if room.get("phase") == "phase3":
            if _count_total_cards_for_player(room, player) > 1:
                legal = phase3_legal_uids_for_player(room, player)
                if uid not in legal:
                    await ws.send_json({
                        "type": "info",
                        "msg": "illegal_play_phase3",
                        "current_trick_codes": [t.get("card") for t in (room.get("current_trick", []) or []) if t.get("card")],
                        "legal_uids": list(legal),
                    })
                    return

        removed, _removed_from = _remove_specific_uid_from_hand_or_meld(room, player, uid)
        if not removed:
            await ws.send_json({"type": "info", "msg": "card_not_found"})
            return

        card_code = removed["code"]

        room.setdefault("current_trick", [])
        room["current_trick"].append({
            "player": player,
            "card": card_code,
            "uid": uid
        })
        play_index = len(room["current_trick"]) - 1

        await _send_to_room(room_id, {
            "type": "card_played",
            "player": player,
            "card": card_code,
            "uid": uid,
            "image": card_image_url_for_code(card_code),
            "play_index": play_index,
            "deck_count": len(room.get("deck", []))
        })

        await send_state_update_to_player(room_id, player)
        await broadcast_state_without_hands(room_id)

        # ============================================================
        # PHASE 3 LAST-TRICK AUTO-PLAY
        # ============================================================
        if room.get("phase") == "phase3":
            order = _players_order(room)

            if len(room["current_trick"]) < 4:
                ci = order.index(player)
                nxt = order[(ci + 1) % 4]
                room["current_turn"] = nxt

                # Phase 3 last-trick auto-play (ONLY when it is truly the last trick):
                # After the lead card is played, if there are exactly 3 total cards remaining
                # across all hands+melds (one per each of the other 3 players), then the
                # remaining 3 plays are forced and we can auto-play them to complete the trick.
                total_remaining = 0
                for _pn in _players_order(room):
                    total_remaining += _count_total_cards_for_player(room, _pn)

                # Only trigger immediately after the lead card is played.
                if total_remaining == 3 and len(room.get("current_trick", []) or []) == 1:
                    for _ in range(3):
                        if len(room.get("current_trick", []) or []) >= 4:
                            break
                        nxtp = room.get("current_turn")
                        if not nxtp:
                            break

                        # Remaining card could be in hand OR melds.
                        cand = (room.get("hands", {}).get(nxtp) or []) + (room.get("melds", {}).get(nxtp) or [])
                        if not cand:
                            break
                        uid2 = (cand[0] or {}).get("uid")
                        if not uid2:
                            break

                        # Reuse the normal play_card pipeline (legality checks + broadcasts).
                        await process_action(_DUMMY_WS, room_id, nxtp, {"action": "play_card", "card": {"uid": uid2}})

            if len(room["current_trick"]) < 4:
                await broadcast_state_without_hands(room_id)
                return

            winner = phase3_determine_winner(room)
            room["last_trick_winner"] = winner
            room["current_turn"] = winner

            _append_trick_to_won_pile(room, winner, room["current_trick"])

            await _send_to_room(room_id, {
                "type": "trick_complete_phase3",
                "winner": winner,
                "trick_cards": room["current_trick"]
            })

            room["pending_trick_clear"] = True

            await broadcast_state_without_hands(room_id)
            for p in order:
                await send_state_update_to_player(room_id, p)

            all_empty = True
            for p in order:
                if _count_total_cards_for_player(room, p) > 0:
                    all_empty = False
                    break

            if all_empty:
                winner_code, text = _evaluate_winner_after_round(room)

                if winner_code == "":
                    room["awaiting_next_round"] = True
                    room["phase"] = "round_end_wait"
                    room["count_required"] = True
                    room["count_done"] = False

                    await _send_to_room(room_id, {
                        "type": "round_end_wait",
                        "msg": "No winner yet. Host must click Count All Aces and 10s.",
                        "host": room.get("host"),
                    })

                    await _send_to_room(room_id, {
                        "type": "show_winner",
                        "winner": "",
                        "text": NO_WINNER_TEXT,
                        "scores": room.get("scores", {}),
                        "last_trick_winner": room.get("last_trick_winner"),
                        "bonus": {}
                    })

                    await broadcast_state_without_hands(room_id)
                    for p in order:
                        await send_state_update_to_player(room_id, p)

                else:
                    room["phase"] = "game_over"
                    await _send_to_room(room_id, {
                        "type": "show_winner",
                        "winner": winner_code,
                        "text": text,
                        "scores": room.get("scores", {}),
                        "last_trick_winner": room.get("last_trick_winner"),
                        "bonus": {}
                    })
                    await _send_to_room(room_id, {"type": "game_over", "msg": "Game Over"})
                    await broadcast_state_without_hands(room_id)

            return

        # ============================================================
        # NON-PHASE3 trick progression:
        # ============================================================
        if len(room["current_trick"]) < 4:
            order = _players_order(room)
            ci = order.index(player)
            nxt = order[(ci + 1) % 4]
            room["current_turn"] = nxt
            await broadcast_state_without_hands(room_id)
            return

        # ============================================================
        # DETERMINE TRICK WINNER (phase1/phase2)
        # ============================================================
        trick = room["current_trick"]
        lead = trick[0]["card"]
        lead_suit = lead.split("_of_")[1] if "_of_" in lead else ""
        trump = room.get("trump_suit")
        phase = room.get("phase")

        def suit_of_local(c: str):
            return c.split("_of_")[1] if "_of_" in c else ""

        def rank_of_local(c: str):
            if c.startswith("joker"):
                return -99
            r = c.split("_of_")[0]
            return rank_value_map.get(r, -1)

        if lead.startswith("joker"):
            if phase in ("phase2", "phase3") and trump:
                trump_cards = [
                    t for t in trick
                    if not t["card"].startswith("joker")
                    and suit_of_local(t["card"]) == trump
                ]
                if trump_cards:
                    best_rank = -999
                    winner = trump_cards[0]["player"]
                    for t in trump_cards:
                        rv = rank_of_local(t["card"])
                        if rv > best_rank:
                            best_rank = rv
                            winner = t["player"]
                else:
                    winner = trick[0]["player"]
            else:
                winner = trick[0]["player"]
        else:
            if trump:
                trump_cards = [
                    t for t in trick
                    if not t["card"].startswith("joker")
                    and suit_of_local(t["card"]) == trump
                ]
            else:
                trump_cards = []

            if trump_cards:
                best_rank = -999
                winner = trump_cards[0]["player"]
                for t in trump_cards:
                    rv = rank_of_local(t["card"])
                    if rv > best_rank:
                        best_rank = rv
                        winner = t["player"]
            else:
                suit_followers = [t for t in trick if suit_of_local(t["card"]) == lead_suit]
                best_rank = -999
                winner = suit_followers[0]["player"]
                for t in suit_followers:
                    rv = rank_of_local(t["card"])
                    if rv > best_rank:
                        best_rank = rv
                        winner = t["player"]

        room["last_trick_winner"] = winner

        _append_trick_to_won_pile(room, winner, trick)

        room["meld_scored_this_trick"] = False

        # ============================================================
        # FIND MARRIAGES FOR WINNER
        # ============================================================
        marriage_click = {}
        options = []
        whand = room.get("hands", {}).get(winner, [])
        wmeld = room.get("melds", {}).get(winner, [])

        room.setdefault("scored_melds", {})
        room["scored_melds"].setdefault(winner, [])
        used_in_marriage = set()
        for rec in room["scored_melds"][winner]:
            if rec.get("category") == "marriage":
                for u in rec.get("uids", []):
                    used_in_marriage.add(u)

        candidate_cards = []
        for c in whand + wmeld:
            code = c["code"]
            if "_of_" not in code:
                continue
            uid2 = c["uid"]
            if uid2 in used_in_marriage:
                continue
            r, _s = code.split("_of_")
            if r not in ("king", "queen"):
                continue
            candidate_cards.append(c)

        uid_source = {}
        for c in whand:
            uid_source[c["uid"]] = "hand"
        for c in wmeld:
            if c["uid"] not in uid_source:
                uid_source[c["uid"]] = "meld"

        by_suit = {}
        for c in candidate_cards:
            code = c["code"]
            r, s = code.split("_of_")
            by_suit.setdefault(s, {"king": [], "queen": []})
            if r == "king":
                by_suit[s]["king"].append(c)
            if r == "queen":
                by_suit[s]["queen"].append(c)

        for suit_name, g in by_suit.items():
            if g["king"] and g["queen"]:
                valid_pairs = []
                for k in g["king"]:
                    for q in g["queen"]:
                        if (uid_source.get(k["uid"]) == "hand" or uid_source.get(q["uid"]) == "hand"):
                            valid_pairs.append((k, q))
                if not valid_pairs:
                    continue

                k, q = valid_pairs[0]
                options.append({
                    "suit": suit_name,
                    "king_uid": k["uid"],
                    "queen_uid": q["uid"],
                    "king_code": k["code"],
                    "queen_code": q["code"]
                })

        if options:
            room["marriage_pending"] = {
                "player": winner,
                "options": options,
                "selected": [],
                "selected_uids": [],
            }
            room.setdefault("temp_melds", {})
            room["temp_melds"][winner] = []

            clickable_codes = []
            for o in options:
                clickable_codes.append(o["king_code"])
                clickable_codes.append(o["queen_code"])
            marriage_click[winner] = clickable_codes
        else:
            room["marriage_pending"] = None

        # ============================================================
        # DRAW PHASE SETUP
        # ============================================================
        # Post-trick draw window (only while deck has cards).
        if room.get("deck"):
            room["post_trick_draws"] = True
            order = _players_order(room)
            si = order.index(winner)
            room["draw_order"] = order[si:] + order[:si]
            room["draw_index"] = 0
        else:
            # Deck is empty: skip draws and allow phase3 transition immediately.
            room["post_trick_draws"] = False
            room["draw_order"] = []
            room["draw_index"] = 0
        room["current_turn"] = winner

        await _send_to_room(room_id, {
            "type": "trick_complete",
            "winner": winner,
            "trick_cards": trick,
            "marriage_clickable": marriage_click
        })

        await send_state_update_to_player(room_id, winner)
        await broadcast_state_without_hands(room_id)
        if not room.get("deck"):
            # No draw window: clear trick UI now and continue into Phase 3 when ready.
            room["meld_scored_this_trick"] = False
            room["current_trick"] = []
            await _send_to_room(room_id, {"type": "clear_trick"})
            await broadcast_state_without_hands(room_id)
            await enter_phase3_if_ready(room_id)
        return

    # ============================================================
    # DRAW CARD
    # ============================================================
    if action == "draw_card":
        if room.get("current_turn") != player:
            await ws.send_json({"type": "info", "msg": "not_your_turn_to_draw"})
            return

        if not room.get("deck"):
            # ✅ nudge deck_count (0) in case UI is stale
            await _emit_deck_count(room_id, 0)
            # If we're in the post-trick draw window and the deck just hit 0,
            # end the draw window cleanly (otherwise the UI can get stuck).
            if room.get("post_trick_draws"):
                room["post_trick_draws"] = False
                room["draw_order"] = []
                room["draw_index"] = 0
                room["meld_scored_this_trick"] = False
                # Clear the trick visuals and continue (phase3 may start now).
                room["current_trick"] = []
                await _send_to_room(room_id, {"type": "clear_trick"})
                await broadcast_state_without_hands(room_id)
                await enter_phase3_if_ready(room_id)
            await ws.send_json({"type": "info", "msg": "deck_empty"})
            return
        order = room.get("draw_order", [])
        idx = room.get("draw_index", 0)

        if room.get("post_trick_draws"):
            if idx >= len(order) or order[idx] != player:
                await ws.send_json({"type": "info", "msg": "wrong_draw_order"})
                return

            while room["draw_index"] < len(order) and room["deck"]:
                dp = order[room["draw_index"]]
                code = room["deck"].pop(0)
                obj = canonical_card_with_uid(code)
                obj["image"] = card_image_url_for_code(code)
                room["hands"][dp].append(obj)

                await _send_to_room(room_id, {
                    "type": "drawn_card",
                    "player": dp,
                    "card": obj,
                    "deck_count": len(room["deck"])
                })

                # ✅ nudge deck count after each draw (safe)
                await _emit_deck_count(room_id, len(room.get("deck", [])))

                await send_state_update_to_player(room_id, dp)
                room["draw_index"] += 1

            room["post_trick_draws"] = False
            winner = room.get("last_trick_winner")
            room["current_turn"] = winner

            room["current_trick"] = []
            room["draw_order"] = []
            room["draw_index"] = 0
            room["meld_scored_this_trick"] = False

            await _send_to_room(room_id, {"type": "clear_trick"})

            # ✅ nudge deck count after draw window closes
            await _emit_deck_count(room_id, len(room.get("deck", [])))

            await broadcast_state_without_hands(room_id)

            if len(room.get("deck", [])) == 0:
                await enter_phase3_if_ready(room_id)
            return

        code = room["deck"].pop(0)
        obj = canonical_card_with_uid(code)
        obj["image"] = card_image_url_for_code(code)
        room["hands"][player].append(obj)

        await _send_to_room(room_id, {
            "type": "drawn_card",
            "player": player,
            "card": obj,
            "deck_count": len(room["deck"])
        })

        # ✅ nudge deck count after draw
        await _emit_deck_count(room_id, len(room.get("deck", [])))

        await send_state_update_to_player(room_id, player)
        await broadcast_state_without_hands(room_id)

        if len(room.get("deck", [])) == 0:
            await enter_phase3_if_ready(room_id)

        return

    # ============================================================
    # START MARRIAGE
    # ============================================================
    if action == "start_marriage":
        mp = room.get("marriage_pending")
        if not mp or mp["player"] != player:
            await ws.send_json({"type": "info", "msg": "no_marriage_pending"})
            return

        card = msg.get("card") or {}
        code = card.get("code")
        uid = card.get("uid")

        if not uid:
            await ws.send_json({"type": "info", "msg": "missing_uid"})
            return

        valid = set()
        for o in mp["options"]:
            valid.add(o["king_code"])
            valid.add(o["queen_code"])

        if code not in valid:
            await ws.send_json({"type": "info", "msg": "card_not_marriage_option"})
            return

        if code in mp["selected"]:
            await ws.send_json({"type": "info", "msg": "already_selected"})
            return

        if len(mp["selected"]) >= 2:
            await ws.send_json({"type": "info", "msg": "too_many_selected"})
            return

        mp["selected"].append(code)
        mp["selected_uids"].append(uid)
        room["marriage_pending"] = mp

        room.setdefault("temp_melds", {})
        room["temp_melds"][player] = mp["selected"][:4]

        await send_state_update_to_player(room_id, player)
        await broadcast_state_without_hands(room_id)

        await _send_to_player(room_id, player, {
            "type": "temp_meld_update",
            "player": player,
            "selected_codes": mp["selected"],
            "selected_uids": mp["selected_uids"],
            "temp_melds": room["temp_melds"]
        })
        return

    # ============================================================
    # SCORE MARRIAGE
    # ============================================================
    if action == "score_marriage":
        mp = room.get("marriage_pending")
        if not mp or mp["player"] != player:
            await ws.send_json({"type": "info", "msg": "no_marriage_pending"})
            return

        if len(mp["selected"]) != 2:
            await ws.send_json({"type": "info", "msg": "need_two_cards"})
            return

        sel = mp["selected"]
        uids = mp["selected_uids"]

        if len(uids) != 2 or (not uids[0]) or (not uids[1]) or (uids[0] == uids[1]):
            await ws.send_json({"type": "info", "msg": "missing_or_duplicate_uid"})
            return

        room.setdefault("scored_melds", {})
        room["scored_melds"].setdefault(player, [])
        used_in_marriage = set()
        for rec in room["scored_melds"][player]:
            if rec.get("category") == "marriage":
                for u in rec.get("uids", []):
                    used_in_marriage.add(u)
        if any(u in used_in_marriage for u in uids):
            await ws.send_json({"type": "info", "msg": "marriage_uid_already_used"})
            return

        suit_a = sel[0].split("_of_")[1] if sel[0] and "_of_" in sel[0] else None
        suit_b = sel[1].split("_of_")[1] if sel[1] and "_of_" in sel[1] else None
        if not suit_a or not suit_b or suit_a != suit_b:
            await ws.send_json({"type": "info", "msg": "invalid_suit"})
            return
        suit = suit_a

        hand = room.get("hands", {}).get(player, [])
        meld = room.get("melds", {}).setdefault(player, [])
        uid_source = {}
        for c in hand:
            uid_source[c["uid"]] = "hand"
        for c in meld:
            if c["uid"] not in uid_source:
                uid_source[c["uid"]] = "meld"

        has_hand_kq = any(uid_source.get(uid) == "hand" for uid in uids)
        if not has_hand_kq:
            await ws.send_json({"type": "info", "msg": "marriage_requires_hand_card"})
            return

        trump = room.get("trump_suit")
        first_trump = trump is None

        if first_trump:
            points = 40
            room["trump_suit"] = suit
            room["phase"] = "phase2"
        else:
            points = 40 if suit == trump else 20

        try:
            cur = int(room["scores"].get(player, 0) or 0)
        except:
            cur = 0
        room["scores"][player] = cur + int(points)

        for uid2 in uids:
            found = None
            for c in hand:
                if c["uid"] == uid2:
                    found = c
                    break
            if found:
                hand.remove(found)
                meld.append(found)

        room["scored_melds"][player].append({
            "category": "marriage",
            "uids": list(uids),
            "suit": suit,
        })

        room["marriage_pending"] = None
        room.setdefault("temp_melds", {})
        room["temp_melds"][player] = []

        await send_state_update_to_player(room_id, player)
        await broadcast_state_without_hands(room_id)

        await _send_to_room(room_id, {
            "type": "marriage_scored",
            "player": player,
            "suit": suit,
            "points": points,
            "scores": room["scores"],
            "temp_melds": room["temp_melds"],
            "trump_suit": room.get("trump_suit"),
            "phase": room.get("phase"),
        })

        if await _check_instant_win(room_id, room):
            return

        return

    # ============================================================
    # CANCEL MARRIAGE
    # ============================================================
    if action == "cancel_marriage":
        mp = room.get("marriage_pending")
        if not mp or mp["player"] != player:
            await ws.send_json({"type": "info", "msg": "no_marriage_pending"})
            return

        mp["selected"] = []
        mp["selected_uids"] = []
        room["marriage_pending"] = mp

        room.setdefault("temp_melds", {})
        room["temp_melds"][player] = []

        await send_state_update_to_player(room_id, player)
        await broadcast_state_without_hands(room_id)

        await _send_to_room(room_id, {
            "type": "temp_meld_update",
            "player": player,
            "selected_codes": [],
            "selected_uids": [],
            "temp_melds": room["temp_melds"]
        })
        return

    # ============================================================
    # CANCEL MELD (Phase 2)
    # ============================================================
    if action == "cancel_meld":
        room.setdefault("temp_melds", {})
        room["temp_melds"][player] = []

        await send_state_update_to_player(room_id, player)
        await broadcast_state_without_hands(room_id)

        await ws.send_json({"type": "info", "msg": "meld_canceled"})
        return

    # ============================================================
    # SCORE MELD (Phase 2) — v75.3 rules
    # ============================================================
    if action == "score_meld":
        if room.get("phase") != "phase2":
            await ws.send_json({"type": "info", "msg": "melds_only_in_phase2"})
            return

        if not room.get("post_trick_draws"):
            await ws.send_json({"type": "info", "msg": "meld_only_right_after_trick"})
            return

        if room.get("last_trick_winner") != player:
            await ws.send_json({"type": "info", "msg": "only_trick_winner_melds"})
            return

        if room.get("current_turn") != player:
            await ws.send_json({"type": "info", "msg": "not_your_turn_to_meld"})
            return

        if room.get("meld_scored_this_trick"):
            await ws.send_json({"type": "info", "msg": "only_one_meld_per_trick_then_draw"})
            return

        raw_cards = msg.get("cards") or []
        if not raw_cards:
            await ws.send_json({"type": "info", "msg": "no_cards_selected"})
            return

        raw_uids = [rc.get("uid") for rc in raw_cards if rc.get("uid")]
        if len(raw_uids) != len(raw_cards) or len(set(raw_uids)) != len(raw_uids):
            await ws.send_json({"type": "info", "msg": "duplicate_card_in_selection"})
            return

        hand = room.get("hands", {}).get(player, [])
        meld_area = room.get("melds", {}).setdefault(player, [])
        uid_to_card: Dict[str, Any] = {}
        uid_source: Dict[str, str] = {}

        for c in hand:
            uid_to_card[c["uid"]] = c
            uid_source[c["uid"]] = "hand"

        for c in meld_area:
            if c["code"].startswith("joker"):
                continue
            if c["uid"] not in uid_to_card:
                uid_to_card[c["uid"]] = c
                uid_source[c["uid"]] = "meld"

        chosen_cards: List[dict] = []
        chosen_uids: List[str] = []

        invalid = False
        for rc in raw_cards:
            uid2 = rc.get("uid")
            if not uid2 or uid2 not in uid_to_card:
                invalid = True
                break
            chosen_cards.append(uid_to_card[uid2])
            chosen_uids.append(uid2)

        if invalid:
            await ws.send_json({"type": "info", "msg": "card_not_found"})
            return

        if len(chosen_cards) < 2 or len(chosen_cards) > 5:
            await ws.send_json({"type": "info", "msg": "invalid_meld_size"})
            return

        has_non_joker_from_hand = any(
            uid_source.get(c["uid"]) == "hand" and not c["code"].startswith("joker")
            for c in chosen_cards
        )
        if not has_non_joker_from_hand:
            await ws.send_json({"type": "info", "msg": "meld_requires_hand_card"})
            return

        def rs(code: str):
            if code.startswith("joker"):
                return "joker", None
            if "_of_" not in code:
                return None, None
            r, s = code.split("_of_")
            return r, s

        ranks = []
        suits = []
        joker_cards = []
        non_joker_cards = []

        for c in chosen_cards:
            r, s = rs(c["code"])
            if r == "joker":
                joker_cards.append(c)
            else:
                non_joker_cards.append(c)
                ranks.append(r)
                suits.append(s)

        joker_count = len(joker_cards)
        non_joker_count = len(non_joker_cards)

        category = None
        points = 0
        trump = room.get("trump_suit")

        if len(chosen_cards) == 2:
            sset = set(c["code"] for c in chosen_cards)
            if sset == {"queen_of_spades", "jack_of_diamonds"}:
                category = "besigue"
                points = 40

        if category is None and len(chosen_cards) == 5 and joker_count == 0:
            suit_set = set(suits)
            rank_set = set(ranks)
            if trump and len(suit_set) == 1 and trump in suit_set and rank_set == {"ace", "king", "queen", "jack", "10"}:
                k_cards = [c for c in chosen_cards if rs(c["code"])[0] == "king"]
                q_cards = [c for c in chosen_cards if rs(c["code"])[0] == "queen"]
                if k_cards and q_cards:
                    k_uid = k_cards[0]["uid"]
                    q_uid = q_cards[0]["uid"]
                    if uid_source.get(k_uid) == "meld" and uid_source.get(q_uid) == "meld":
                        category = "quinte_trump"
                        points = 250
                    else:
                        await ws.send_json({"type": "info", "msg": "quinte_requires_marriage_in_meld"})
                        return

        if category is None and len(chosen_cards) == 4:
            non_joker_ranks = [rs(c["code"])[0] for c in non_joker_cards]
            base_rank_set = set(non_joker_ranks)
            if len(base_rank_set) == 1:
                base_rank = non_joker_ranks[0]
                if base_rank in ("ace", "king", "queen", "jack"):
                    if joker_count == 0 and non_joker_count == 4:
                        if base_rank == "ace":
                            category = "four_aces"; points = 100
                        elif base_rank == "king":
                            category = "four_kings"; points = 80
                        elif base_rank == "queen":
                            category = "four_queens"; points = 60
                        elif base_rank == "jack":
                            category = "four_jacks"; points = 40
                    elif joker_count == 1 and non_joker_count == 3:
                        if base_rank == "ace":
                            category = "three_aces_joker"; points = 100
                        elif base_rank == "king":
                            category = "three_kings_joker"; points = 80
                        elif base_rank == "queen":
                            category = "three_queens_joker"; points = 60
                        elif base_rank == "jack":
                            category = "three_jacks_joker"; points = 40

        if category not in (
            None,
            "four_aces", "four_kings", "four_queens", "four_jacks",
            "three_aces_joker", "three_kings_joker", "three_queens_joker", "three_jacks_joker"
        ):
            if joker_count > 0 and category is not None:
                await ws.send_json({"type": "info", "msg": "joker_only_in_four_kind"})
                return

        if category is None:
            await ws.send_json({"type": "info", "msg": "invalid_meld_selection"})
            return

        room.setdefault("scored_melds", {})
        room["scored_melds"].setdefault(player, [])
        scored_list = room["scored_melds"][player]

        used_by_cat = build_used_uids_by_category(scored_list)
        already_used_uids = used_by_cat.get(category, set())
        if any(uid3 in already_used_uids for uid3 in chosen_uids):
            await ws.send_json({"type": "info", "msg": "uid_reused_in_same_category"})
            return

        uid_set = frozenset(chosen_uids)
        for prev in scored_list:
            if prev.get("category") == category and frozenset(prev.get("uids", [])) == uid_set:
                await ws.send_json({"type": "info", "msg": "meld_already_scored_exact"})
                break
        else:
            try:
                cur = int(room["scores"].get(player, 0) or 0)
            except:
                cur = 0
            room["scores"][player] = cur + int(points)

            for c in chosen_cards:
                uid4 = c["uid"]
                if uid_source.get(uid4) == "hand":
                    for h in list(hand):
                        if h["uid"] == uid4:
                            hand.remove(h)
                            meld_area.append(h)
                            break

            scored_list.append({
                "category": category,
                "uids": list(uid_set),
                "points": points,
            })
            room["scored_melds"][player] = scored_list

            room.setdefault("temp_melds", {})
            room["temp_melds"][player] = []

            room["meld_scored_this_trick"] = True

            await send_state_update_to_player(room_id, player)
            await broadcast_state_without_hands(room_id)

            await _send_to_room(room_id, {
                "type": "meld_scored",
                "player": player,
                "category": category,
                "points": points,
                "scores": room["scores"],
                "temp_melds": room["temp_melds"],
            })

            if await _check_instant_win(room_id, room):
                return

        return

    # Unknown / no-op actions: ignore quietly


@app.websocket("/ws")
async def websocket_endpoint(
    ws: WebSocket,
    player_name: str = Query(...),
    room_id: str = Query(...)
):
    await ws.accept()
    log.info(f"[WS] Opening ws for {player_name} @ {room_id}")

    if room_id not in ROOMS:
        await ws.send_json({"type": "error", "error": "room_not_found"})
        await ws.close()
        return

    room = ROOMS[room_id]
    player_name = _normalize_name(player_name)

    room.setdefault("players", [])
    room.setdefault("scores", {})
    room.setdefault("melds", {})
    room.setdefault("scored_melds", {})
    room.setdefault("won_tricks", {})
    room.setdefault("awaiting_next_round", False)
    room.setdefault("count_required", False)
    room.setdefault("count_done", False)

    name_exists = any(p["name"] == player_name for p in room.get("players", []))

    if (not name_exists) and room.get("phase") == "waiting" and len(room.get("players", [])) >= MAX_SEATS:
        await ws.send_json({"type": "error", "error": "room_full"})
        await ws.close()
        return

    if not name_exists:
        room["players"].append({"name": player_name, "is_cpu": False, "reconnect_token": make_reconnect_token(), "connection_state": "human_active", "disconnect_at": None, "reconnect_deadline": None, "cpu_takeover_started_at": None, "cpu_playing_for_name": None})
        room["scores"].setdefault(player_name, 0)
        room["melds"].setdefault(player_name, [])
        room["scored_melds"].setdefault(player_name, [])
        room["won_tricks"].setdefault(player_name, [])

    for p in room.get("players", []) or []:
        _ensure_player_runtime_meta(p)

    room.setdefault("meld_scored_this_trick", False)
    room.setdefault("phase3_melds_picked", False)
    room.setdefault("pending_trick_clear", False)
    room.setdefault("ready_to_start", len(room.get("players", [])) >= MAX_SEATS)

    await _mark_player_connected(room_id, player_name)
    await _register_single_socket(room_id, player_name, ws)
    await broadcast_state_without_hands(room_id)
    await send_state_update_to_player(room_id, player_name)

    try:
        while True:
            msg = await ws.receive_json()
            await process_action(ws, room_id, player_name, msg)
            await cpu_maybe_act(room_id)
    except WebSocketDisconnect:
        _safe_remove_ws(room_id, player_name, ws)
        try:
            room = ROOMS.get(room_id)
            if room and room.get("phase") == "waiting":
                remaining = ROOM_SOCKETS.get(room_id, {}).get(player_name, []) or []
                if not remaining:
                    room["players"] = [p for p in room.get("players", []) if p.get("name") != player_name]
                    room.get("scores", {}).pop(player_name, None)
                    room.get("melds", {}).pop(player_name, None)
                    if "hands" in room and isinstance(room["hands"], dict):
                        room["hands"].pop(player_name, None)
                    if "scored_melds" in room and isinstance(room["scored_melds"], dict):
                        room["scored_melds"].pop(player_name, None)
                    room.get("won_tricks", {}).pop(player_name, None)

                    if not room.get("players"):
                        ROOMS.pop(room_id, None)
                        WS_CLIENTS.pop(room_id, None)
                        ROOM_SOCKETS.pop(room_id, None)
                    else:
                        room["ready_to_start"] = len(room.get("players", [])) >= MAX_SEATS
                        await broadcast_state_without_hands(room_id)

                    await broadcast_lobby_rooms()
            elif room:
                remaining = ROOM_SOCKETS.get(room_id, {}).get(player_name, []) or []
                if not remaining:
                    await _mark_player_disconnected(room_id, player_name)
        except:
            pass

    except Exception as e:
        log.exception(f"WS ERROR: {e}")
        _safe_remove_ws(room_id, player_name, ws)
        try:
            room = ROOMS.get(room_id)
            if room and room.get("phase") == "waiting":
                remaining = ROOM_SOCKETS.get(room_id, {}).get(player_name, []) or []
                if not remaining:
                    room["players"] = [p for p in room.get("players", []) if p.get("name") != player_name]
                    room.get("scores", {}).pop(player_name, None)
                    room.get("melds", {}).pop(player_name, None)
                    if "hands" in room and isinstance(room["hands"], dict):
                        room["hands"].pop(player_name, None)
                    if "scored_melds" in room and isinstance(room["scored_melds"], dict):
                        room["scored_melds"].pop(player_name, None)
                    room.get("won_tricks", {}).pop(player_name, None)

                    if not room.get("players"):
                        ROOMS.pop(room_id, None)
                        WS_CLIENTS.pop(room_id, None)
                        ROOM_SOCKETS.pop(room_id, None)
                    else:
                        room["ready_to_start"] = len(room.get("players", [])) >= MAX_SEATS
                        await broadcast_state_without_hands(room_id)

                    await broadcast_lobby_rooms()
            elif room:
                remaining = ROOM_SOCKETS.get(room_id, {}).get(player_name, []) or []
                if not remaining:
                    await _mark_player_disconnected(room_id, player_name)
        except:
            pass


# ---------------------------------------------------
# Score Mark Images (static passthrough)
# ---------------------------------------------------
@app.get("/server/static/marks/{fname}")
async def serve_mark(fname: str):
    try:
        return FileResponse(f"./server/static/marks/{fname}")
    except:
        return JSONResponse({"error": "not_found"}, 404)