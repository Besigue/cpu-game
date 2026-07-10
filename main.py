# ============================
# main.py — Besigue Server (v82.4.10b-CPU-RUNNER-RESTART)
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
#  6) Fix GP3 CPU takeover no-progress by using hand+meld cards for legal UID checks.
#  7) Intentional active leave (#leaveGame2) gets immediate CPU takeover actions.
# ============================

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Query
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import uuid
import random
import asyncio
import logging
import time
from typing import Dict, List, Any, Optional, Tuple
from itertools import combinations

log = logging.getLogger("besigue")
logging.basicConfig(level=logging.INFO)
log.info("BOOT main.py v82.4.10b CPU_RUNNER_RESTART loaded")

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
DEFAULT_WINNING_SCORE = 400

def room_winning_score(room):
    try:
        return int(room.get("winning_score", DEFAULT_WINNING_SCORE))
    except:
        return DEFAULT_WINNING_SCORE
FINAL_TRICK_ROUND_END_DELAY = 3.0
POST_SCORE_SHOWWINNER_DELAY = 2.5
FINAL_TRICK_ROUND_END_DELAY_SAFE = globals().get("FINAL_TRICK_ROUND_END_DELAY", 3.0)
ORDERED_TEST_DECK_ENABLED = False  # ordered test deck disabled after quinte verification

NO_WINNER_TEXT = "No Winner Yet, Let's Keep Going!"
PRACTICE_ROOM_PUBLIC_ID = "practice_room_1"
PRACTICE_ROOM_PUBLIC_LABEL = "Practice Room, 400 Points, 1 Seat"

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


def _normalize_identity(raw: Any) -> str:
    """Stable browser/member identity used only to prevent one person taking
    multiple seats in the same room. Empty is allowed for older clients.
    """
    ident = str(raw or "").strip()
    if not ident:
        return ""
    # Keep it small and log-safe. Wix member ids/local ids are much shorter.
    return ident[:180]


def _seat_identity_for_player(room: dict, player_name: str) -> str:
    try:
        room.setdefault("seat_identities", {})
        ident = room["seat_identities"].get(player_name, "")
        if ident:
            return ident
        for pobj in room.get("players", []) or []:
            if pobj.get("name") == player_name:
                return _normalize_identity(pobj.get("identity") or pobj.get("player_identity") or "")
    except Exception:
        pass
    return ""


def _set_seat_identity(room: dict, player_name: str, identity: str):
    try:
        identity = _normalize_identity(identity)
        if not identity:
            return
        room.setdefault("seat_identities", {})
        room["seat_identities"][player_name] = identity
        for pobj in room.get("players", []) or []:
            if pobj.get("name") == player_name:
                pobj["identity"] = identity
                break
    except Exception:
        pass




def _is_identity_blocked_from_room(room: dict, identity: str) -> bool:
    """True when a player intentionally left this active room and may not re-enter."""
    identity = _normalize_identity(identity)
    if not room or not identity:
        return False
    try:
        blocked = set(room.get("left_identities", []) or [])
        return identity in blocked
    except Exception:
        return False


def _remember_left_identity(room: dict, player_name: str, identity: str = ""):
    """Record explicit active-game leavers so reconnect/join cannot reclaim this room."""
    try:
        room.setdefault("left_players", [])
        if player_name and player_name not in room["left_players"]:
            room["left_players"].append(player_name)
    except Exception:
        pass
    try:
        identity = _normalize_identity(identity or _seat_identity_for_player(room, player_name))
        if identity:
            room.setdefault("left_identities", [])
            if identity not in room["left_identities"]:
                room["left_identities"].append(identity)
    except Exception:
        pass

def _find_player_by_identity(room: dict, identity: str) -> str:
    """Return an existing non-CPU seat name for this identity in this room."""
    identity = _normalize_identity(identity)
    if not room or not identity:
        return ""
    try:
        for pobj in room.get("players", []) or []:
            name = pobj.get("name") or ""
            if not name or bool(pobj.get("is_cpu")) or str(name).startswith("CPU"):
                continue
            if _seat_identity_for_player(room, name) == identity:
                return name
    except Exception:
        pass
    return ""


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


def _ensure_reconnect_token(room: dict, player_name: str) -> str:
    """Return/create a stable reconnect token for this physical seat."""
    if not room or not player_name:
        return ""
    room.setdefault("reconnect_tokens", {})
    tok = room["reconnect_tokens"].get(player_name)
    if not tok:
        tok = uuid.uuid4().hex + uuid.uuid4().hex
        room["reconnect_tokens"][player_name] = tok
    return tok


def _player_exists_in_room(room: dict, player_name: str) -> bool:
    try:
        return any(p.get("name") == player_name for p in room.get("players", []) or [])
    except Exception:
        return False


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



def _apply_ordered_test_deck_for_host(room: dict, deck: List[str]) -> List[str]:
    """
    Temporary test helper:
    Force the HOST to start with two marriages in spades plus A/J/10 of spades
    so quinte can be tested deliberately.

    Host opening 9 cards become:
      Ks, Qs, Ks, Qs, As, Js, 10s, Ac, Ah

    The rest of the deck stays in shuffled order with those exact copies removed.
    """
    if not ORDERED_TEST_DECK_ENABLED:
        return deck

    desired_prefix = [
        "king_of_spades",
        "queen_of_spades",
        "king_of_spades",
        "queen_of_spades",
        "ace_of_spades",
        "jack_of_spades",
        "10_of_spades",
        "ace_of_clubs",
        "ace_of_hearts",
    ]

    working = list(deck)
    prefix = []
    for code in desired_prefix:
        try:
            idx = working.index(code)
        except ValueError:
            return deck
        prefix.append(working.pop(idx))

    return prefix + working


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
        await _send_to_room(room_id, {"type": "deck_count", "deck_count": int(deck_count)})
    except:
        pass


async def broadcast_lobby_rooms():
    rooms = []
    for r in ROOMS.values():
        if r.get("phase") == "waiting" and r.get("is_open", False):
            display_label = f"{r.get('host', 'Host')}\'s Room, {room_winning_score(r)} Points, {len(r.get('players', []))}/4 Seated"
            rooms.append({
                "room_id": r["room_id"],
                "label": display_label,
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
    """
    Return Phase 3 legal UIDs from the server-authoritative available cards.

    Important for CPU takeover:
    during the transition into GP3, a player's remaining cards may temporarily
    exist in hand OR meld until global pickup has fully completed. Using hand only
    can make a CPU-controlled disconnected/left seat select a UID that later gets
    rejected as illegal and causes a no-progress stall.
    """
    hand = list(room.get("hands", {}).get(player, []) or [])
    meld = list(room.get("melds", {}).get(player, []) or [])

    available_cards = []
    seen = set()
    for c in hand + meld:
        if not isinstance(c, dict):
            continue
        uid = c.get("uid")
        if not uid or uid in seen:
            continue
        seen.add(uid)
        available_cards.append(c)

    if not available_cards:
        return set()

    trick = room.get("current_trick", []) or []
    trump = (room.get("trump_suit") or "").strip().lower()

    if len(trick) == 0:
        return set(c["uid"] for c in available_cards if c.get("uid"))

    lead_card = trick[0].get("card") or ""
    lead_is_joker = str(lead_card).startswith("joker")
    lead_suit = (suit_of(lead_card) or "").strip().lower()

    any_trump_played = False
    best_trump_rank = -999
    best_lead_rank = -999

    for t in trick:
        card_code = t.get("card") or ""
        if not card_code or str(card_code).startswith("joker"):
            continue

        s = (suit_of(card_code) or "").strip().lower()
        rv = rank_of(card_code)

        if lead_suit and s == lead_suit:
            best_lead_rank = max(best_lead_rank, rv)

        if trump and s == trump:
            any_trump_played = True
            best_trump_rank = max(best_trump_rank, rv)

    lead_suit_cards = [
        c for c in available_cards
        if lead_suit and (suit_of(c.get("code", "")) or "").strip().lower() == lead_suit
        and not str(c.get("code", "")).startswith("joker")
    ]

    trump_cards = [
        c for c in available_cards
        if trump and (suit_of(c.get("code", "")) or "").strip().lower() == trump
        and not str(c.get("code", "")).startswith("joker")
    ]

    legal = set()

    # Joker-led trick in Phase 3:
    # must play trump if possible; overtrump if possible; otherwise any card.
    if lead_is_joker:
        if trump_cards:
            if any_trump_played:
                over = [c for c in trump_cards if rank_of(c.get("code", "")) > best_trump_rank]
                legal = set(c.get("uid") for c in (over or trump_cards) if c.get("uid"))
            else:
                legal = set(c.get("uid") for c in trump_cards if c.get("uid"))
        else:
            legal = set(c.get("uid") for c in available_cards if c.get("uid"))

        if not legal:
            log.error(f"[PHASE3 LEGAL EMPTY] joker_lead player={player} available={[c.get('code') for c in available_cards]} trump={trump!r} trick={[t.get('card') for t in trick]}")
            return set(c.get("uid") for c in available_cards if c.get("uid"))
        return legal

    # MUST FOLLOW LEAD SUIT if possible.
    if lead_suit_cards:
        # If lead suit itself is trump, the trump rule applies.
        if trump and lead_suit == trump:
            over = [c for c in lead_suit_cards if rank_of(c.get("code", "")) > best_trump_rank]
            legal = set(c.get("uid") for c in (over or lead_suit_cards) if c.get("uid"))
            if not legal:
                log.error(f"[PHASE3 LEGAL EMPTY] lead_is_trump player={player} available={[c.get('code') for c in available_cards]} trump={trump!r} trick={[t.get('card') for t in trick]}")
                return set(c.get("uid") for c in available_cards if c.get("uid"))
            return legal

        # If a trump is already winning, any lead-suit card is legal.
        if any_trump_played:
            legal = set(c.get("uid") for c in lead_suit_cards if c.get("uid"))
            if not legal:
                log.error(f"[PHASE3 LEGAL EMPTY] trump_already_winning player={player} available={[c.get('code') for c in available_cards]} trump={trump!r} trick={[t.get('card') for t in trick]}")
                return set(c.get("uid") for c in available_cards if c.get("uid"))
            return legal

        # Otherwise must beat current winning lead-suit card if possible.
        over_lead = [c for c in lead_suit_cards if rank_of(c.get("code", "")) > best_lead_rank]
        legal = set(c.get("uid") for c in (over_lead or lead_suit_cards) if c.get("uid"))
        if not legal:
            log.error(f"[PHASE3 LEGAL EMPTY] follow_lead player={player} available={[c.get('code') for c in available_cards]} trump={trump!r} trick={[t.get('card') for t in trick]}")
            return set(c.get("uid") for c in available_cards if c.get("uid"))
        return legal

    # NO LEAD SUIT -> MUST TRUMP / OVERTRUMP IF POSSIBLE.
    if trump_cards:
        if any_trump_played:
            over = [c for c in trump_cards if rank_of(c.get("code", "")) > best_trump_rank]
            legal = set(c.get("uid") for c in (over or trump_cards) if c.get("uid"))
        else:
            legal = set(c.get("uid") for c in trump_cards if c.get("uid"))

        if not legal:
            log.error(f"[PHASE3 LEGAL EMPTY] must_trump player={player} available={[c.get('code') for c in available_cards]} trump={trump!r} trick={[t.get('card') for t in trick]}")
            return set(c.get("uid") for c in available_cards if c.get("uid"))
        return legal

    # OTHERWISE ANY CARD.
    legal = set(c.get("uid") for c in available_cards if c.get("uid"))
    if not legal:
        log.error(f"[PHASE3 LEGAL EMPTY] fallback_any player={player} available={[c.get('code') for c in available_cards]} trump={trump!r} trick={[t.get('card') for t in trick]}")
    return legal
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


def _next_player_ccw(room: dict, current_player: str) -> str:
    order = _players_order(room)
    if not order:
        return current_player or ""
    if current_player not in order:
        return order[0]
    idx = order.index(current_player)
    return order[(idx + 1) % len(order)]


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

def _append_card_to_meld_unique(meld_list: list, card_obj: dict) -> bool:
    """Append card to meld area only if this physical UID is not already present.

    This preserves the game rule that a card may be reused across different meld
    categories later, while preventing duplicate storage of the same physical card
    in the visible meld area. Safe for normal CPU seats and CPU takeover seats.
    """
    if not isinstance(meld_list, list) or not isinstance(card_obj, dict):
        return False
    uid = card_obj.get("uid")
    if not uid:
        return False
    for c in meld_list:
        if isinstance(c, dict) and c.get("uid") == uid:
            return False
    meld_list.append(card_obj)
    return True

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
    target_score = room_winning_score(room)

    log.info(
        f"[WIN CHECK] target={target_score} scores={room.get('scores', {})}"
    )
    if max_score < target_score:
        return "", NO_WINNER_TEXT

    tied = [p for p, s in scores.items() if s == max_score]
    if len(tied) == 1:
        w = tied[0]
        log.info(
            f"[WINNER FOUND] winner={w} max_score={max_score} target={target_score}"
        )
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

    log.info(
        f"[GAME OVER] winner={winner_code} target={room_winning_score(room)} scores={room.get('scores', {})}"
    )

    room["phase"] = "game_over"
    room["awaiting_next_round"] = False
    room["count_required"] = False
    room["count_done"] = False
    room["round_control_player"] = ""

    room["post_trick_draws"] = False
    room["draw_order"] = []
    room["draw_index"] = 0
    room["current_turn"] = None

    await asyncio.sleep(POST_SCORE_SHOWWINNER_DELAY)

    await _send_to_room(room_id, {
        "type": "show_winner",
        "winner": winner_code,
        "text": text,
        "scores": room.get("scores", {}),
        "winning_score": room.get("winning_score", 400),
        "winning_score": room.get("winning_score", 400),
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
    target_score = room_winning_score(room)
    if max_score < target_score:
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




async def _award_trump_seven_if_applicable(room_id: str, room: dict, player: str, card_code: str) -> bool:
    """Award +10 immediately when a player legally plays the 7 of trump.

    Returns True if this scoring event ended the game.
    This is intentionally server-authoritative and runs after the card has already
    passed normal play legality and has been added to current_trick.
    """
    try:
        trump = (room.get("trump_suit") or "").strip().lower()
        if not trump or not card_code or str(card_code).startswith("joker"):
            return False
        if rank_of(card_code) != rank_value_map.get("7"):
            return False
        if (suit_of(card_code) or "").strip().lower() != trump:
            return False

        room.setdefault("scores", {})
        try:
            before = int(room["scores"].get(player, 0) or 0)
        except Exception:
            before = 0
        room["scores"][player] = before + 10

        log.info(f"[TRUMP SEVEN SCORE] room={room_id} player={player} card={card_code} trump={trump} +10 score={room['scores'].get(player)}")

        # Give every browser time to show the trump 7 and hear the alert before
        # another CPU-controlled player immediately covers the next trick slot.
        # Human turns are not delayed; the CPU runner checks this timestamp before
        # continuing automated play.
        room["_pause_after_trump_seven_until"] = time.monotonic() + 2.0

        await _send_to_room(room_id, {
            "type": "trump_seven_scored",
            "player": player,
            "card": card_code,
            "points": 10,
            "scores": room.get("scores", {}),
            "play_turn_alert": True,
            "msg": f"{player} scored 10 points for playing the 7 of trump."
        })

        # If this immediate bonus reaches the winning score, let the scoring/alert
        # sound happen first; _end_game_now already includes a short delay before
        # show_winner / game_over.
        return await _check_instant_win(room_id, room)
    except Exception as e:
        log.exception(f"[TRUMP SEVEN SCORE ERROR] room={room_id} player={player} card={card_code} err={e}")
        return False


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
    room["last_completed_trick"] = []
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
    room["_opening_cpu_lead_delay_pending"] = bool(_is_cpu_controlled(room, next_start) or _is_cpu(room, next_start))

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
    room["round_control_player"] = ""

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

def _build_last_completed_trick_payload(room: dict) -> List[dict]:
    trick = room.get("last_completed_trick", []) or []
    payload = []
    for i, t in enumerate(trick[:4]):
        card_code = (t.get("card") if isinstance(t, dict) else "") or ""
        if not isinstance(card_code, str):
            card_code = ""
        payload.append({
            "player": t.get("player") if isinstance(t, dict) else "",
            "card": {"code": card_code, "uid": (t.get("uid") if isinstance(t, dict) else None)},
            "image": card_image_url_for_code(card_code) if card_code else "",
            "play_index": i
        })
    return payload


async def broadcast_state_without_hands(room_id: str):
    room = ROOMS.get(room_id)
    if not room:
        return
    players = [p["name"] for p in room.get("players", [])]

    current_trick_codes = [t.get("card") for t in (room.get("current_trick", []) or []) if t.get("card")]
    current_trick_full = _build_public_trick_payload(room)

    # ✅ safe deck_count always >= 0 and int
    deck_count = len(room.get("deck", []) or [])
    if deck_count < 0:
        deck_count = 0

    data = {
        "phase": room.get("phase"),
        "players": players,
        "deck_count": int(deck_count),
        "melds": room.get("melds", {}),
        "scores": room.get("scores", {}),
        "winning_score": room_winning_score(room),
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
        "last_completed_trick": _build_last_completed_trick_payload(room),

        "pending_trick_clear": room.get("pending_trick_clear", False),
        "awaiting_next_round": room.get("awaiting_next_round", False),

        "count_required": room.get("count_required", False),
        "count_done": room.get("count_done", False),
        "round_control_player": (_sync_round_control_player(room_id, room) if room.get("awaiting_next_round") else ""),
        "player_statuses": _room_statuses(room),
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
        "deck_count": int(deck_count),
        "melds": room.get("melds", {}),
        "scores": room.get("scores", {}),
        "winning_score": room_winning_score(room),
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
        "last_completed_trick": _build_last_completed_trick_payload(room),

        "pending_trick_clear": room.get("pending_trick_clear", False),
        "awaiting_next_round": room.get("awaiting_next_round", False),

        "count_required": room.get("count_required", False),
        "count_done": room.get("count_done", False),
        "round_control_player": (_sync_round_control_player(room_id, room) if room.get("awaiting_next_round") else ""),

        "hands": {player_name: player_hand},
        "player_statuses": _room_statuses(room),
    }

    if room.get("awaiting_next_round"):
        data["round_control_player"] = _sync_round_control_player(room_id, room)
        data["round_end_wait_text"] = _round_control_wait_text(room_id, room, player_name)

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
    player_identity = _normalize_identity(b.get("player_identity") or b.get("identity") or "")

    rid = f"{pn}_Room"
    counter = 1
    while rid in ROOMS:
        rid = f"{pn}_Room_{counter}"
        counter += 1

    room = {
        "room_id": rid,
        "label": f"{pn}'s Room, 4 seats",
        "host": pn,
        "players": [{"name": pn, "identity": player_identity} if player_identity else {"name": pn}],
        "phase": "waiting",
        "is_open": False,
        "deck": [],
        "melds": {pn: []},
        "scores": {pn: 0},
        "winning_score": 400,
        "ready_to_start": False,
        "current_trick": [],
        "last_completed_trick": [],
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
        "reconnect_tokens": {},
        "seat_identities": {},
        "player_statuses": {},
        "round_control_player": "",
        "left_players": [],
        "left_identities": [],
    }

    _set_seat_identity(room, pn, player_identity)
    _ensure_reconnect_token(room, pn)
    ROOMS[rid] = room
    WS_CLIENTS.setdefault(rid, [])
    ROOM_SOCKETS.setdefault(rid, {})

    await broadcast_lobby_rooms()
    return {
        "room_id": rid,
        "room_label": room["label"],
        "player_name": pn,
        "room_host": room["host"],
        "reconnect_token": _ensure_reconnect_token(room, pn)
    }


@app.post("/api/open_room")
async def api_open_room(req: Request):
    b = await req.json()
    rid = b.get("room_id")
    player_name = _normalize_name(b.get("player_name"))

    if not rid or rid not in ROOMS:
        return JSONResponse({"error": "room_not_found"}, 404)

    room = ROOMS[rid]

    if room.get("host") != player_name:
        return JSONResponse({"error": "host_only"}, 403)

    room["winning_score"] = int(
        b.get("winning_score", room.get("winning_score", DEFAULT_WINNING_SCORE))
    )

    room["is_open"] = True
    await broadcast_lobby_rooms()

    return {"opened": True, "room_id": rid}

# ---------------------------------------------------
# LIST ROOMS
# ---------------------------------------------------
@app.get("/api/list_rooms")
async def api_list_rooms():
    rooms = [{
        "room_id": PRACTICE_ROOM_PUBLIC_ID,
        "label": PRACTICE_ROOM_PUBLIC_LABEL,
        "players": 1
    }]
    for r in ROOMS.values():
        if r.get("phase") == "waiting" and r.get("is_open", False):
            seated = len(r.get("players", []))
            win_score = room_winning_score(r)
            display_label = (
                f"{r.get('host', 'Host')}'s Room, "
                f"{win_score} Points, "
                f"{seated}/4 Seated"
            )
            rooms.append({
                "room_id": r["room_id"],
                "label": display_label,
                "players": seated
            })
    return {"rooms": rooms}


def _create_started_practice_room_for_player(pn: str, player_identity: str = "") -> dict:
    player_identity = _normalize_identity(player_identity)
    rid = f"{pn}_Practice_Room"
    counter = 1
    while rid in ROOMS:
        rid = f"{pn}_Practice_Room_{counter}"
        counter += 1

    room = {
        "room_id": rid,
        "label": f"{pn}'s Practice Room",
        "host": pn,
        "players": [
            {"name": pn, "identity": player_identity} if player_identity else {"name": pn},
            {"name": "CPU 1", "is_cpu": True, "controller": "cpu"},
            {"name": "CPU 2", "is_cpu": True, "controller": "cpu"},
            {"name": "CPU 3", "is_cpu": True, "controller": "cpu"},
        ],
        "phase": "lead_selection",
        "deck": [],
        "melds": {pn: [], "CPU 1": [], "CPU 2": [], "CPU 3": []},
        "scores": {pn: 0, "CPU 1": 0, "CPU 2": 0, "CPU 3": 0},
        "winning_score": 400,
        "ready_to_start": True,
        "current_trick": [],
        "last_completed_trick": [],
        "pending_trick_clear": False,
        "current_turn": None,
        "marriage_pending": None,
        "temp_melds": {},
        "post_trick_draws": False,
        "draw_order": [],
        "draw_index": 0,
        "last_trick_winner": None,
        "trump_suit": None,
        "scored_melds": {pn: [], "CPU 1": [], "CPU 2": [], "CPU 3": []},
        "meld_scored_this_trick": False,
        "phase3_melds_picked": False,
        "won_tricks": {pn: [], "CPU 1": [], "CPU 2": [], "CPU 3": []},
        "_won_trick_sigs": set(),
        "round_start_lead": None,
        "awaiting_next_round": False,
        "count_required": False,
        "count_done": False,
        "cpu_difficulty": 1,
        "cpu_level": 1,
        "reconnect_tokens": {},
        "seat_identities": {},
        "player_statuses": {},
        "round_control_player": "",
        "left_players": [],
        "left_identities": [],
    }
    _set_seat_identity(room, pn, player_identity)
    _ensure_reconnect_token(room, pn)
    ROOMS[rid] = room
    WS_CLIENTS.setdefault(rid, [])
    ROOM_SOCKETS.setdefault(rid, {})
    return room


# ---------------------------------------------------
# JOIN ROOM
# ---------------------------------------------------
@app.post("/api/join_room")
async def api_join_room(req: Request):
    b = await req.json()
    rid = b.get("room_id")
    pn_req = _normalize_name(b.get("player_name"))
    player_identity = _normalize_identity(b.get("player_identity") or b.get("identity") or "")

    if rid == PRACTICE_ROOM_PUBLIC_ID:
        pn = pn_req
        room = _create_started_practice_room_for_player(pn, player_identity)

        try:
            await _start_cpu_style_game(room["room_id"], pn, cpu_level=2, cpu_scores_enabled=False)
        except Exception as e:
            log.exception(f"[PRACTICE START ERROR] room={room.get('room_id')} host={pn} err={e}")
            return JSONResponse({"error": "practice_start_failed"}, 500)

        return {
            "joined": True,
            "practice_room": True,
            "room_id": room["room_id"],
            "player_name": pn,
            "room_label": room["label"],
            "room_host": room["host"],
            "reconnect_token": _ensure_reconnect_token(room, pn)
        }

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
    room.setdefault("reconnect_tokens", {})
    room.setdefault("seat_identities", {})
    room.setdefault("left_players", [])
    room.setdefault("left_identities", [])

    if _is_identity_blocked_from_room(room, player_identity):
        return JSONResponse({"error": "left_game_no_rejoin"}, 403)

    existing_identity_player = _find_player_by_identity(room, player_identity)
    if existing_identity_player:
        # Same Wix/member/browser identity is already seated in this room.
        # Return that seat instead of creating a second human seat. The websocket
        # layer will then keep the newest tab as the active connection for that seat.
        room["ready_to_start"] = (len(room.get("players", []) or []) >= MAX_SEATS)
        await broadcast_state_without_hands(rid)
        await broadcast_lobby_rooms()
        return {
            "joined": True,
            "already_in_room": True,
            "same_identity": True,
            "room_id": rid,
            "player_name": existing_identity_player,
            "room_label": room.get("label", rid),
            "room_host": room.get("host", ""),
            "reconnect_token": _ensure_reconnect_token(room, existing_identity_player)
        }

    existing_names = [p.get("name") for p in room["players"] if p.get("name")]

    if room.get("phase") == "waiting":
        if len(room["players"]) >= MAX_SEATS:
            return JSONResponse({"error": "room_full"}, 400)

        pn = _unique_name_in_room(room, pn_req)

        room["players"].append({"name": pn, "identity": player_identity} if player_identity else {"name": pn})
        _set_seat_identity(room, pn, player_identity)
        room["scores"][pn] = 0
        room["melds"].setdefault(pn, [])
        room["scored_melds"].setdefault(pn, [])
        room["won_tricks"].setdefault(pn, [])

        room["ready_to_start"] = (len(room["players"]) >= MAX_SEATS)

        await broadcast_state_without_hands(rid)
        await broadcast_lobby_rooms()

        return {
            "joined": True,
            "room_id": rid,
            "player_name": pn,
            "room_label": room["label"],
            "room_host": room["host"],
            "reconnect_token": _ensure_reconnect_token(room, pn)
        }

    if pn_req not in existing_names:
        return JSONResponse({"error": "game_started"}, 400)

    if pn_req in (room.get("left_players", []) or []):
        return JSONResponse({"error": "left_game_no_rejoin"}, 403)

    room_sockets = ROOM_SOCKETS.get(rid, {})
    active_socks = room_sockets.get(pn_req, []) or []

    if active_socks:
        room["ready_to_start"] = (len(room["players"]) >= MAX_SEATS)
        await broadcast_state_without_hands(rid)
        await broadcast_lobby_rooms()
        return {
            "joined": True,
            "already_in_room": True,
            "room_id": rid,
            "player_name": pn_req,
            "room_label": room["label"],
            "room_host": room["host"],
            "reconnect_token": _ensure_reconnect_token(room, pn_req)
        }

    return {
        "joined": True,
        "room_id": rid,
        "player_name": pn_req,
        "room_label": room["label"],
        "room_host": room["host"],
        "reconnect_token": _ensure_reconnect_token(room, pn_req)
    }


# ---------------------------------------------------
# RECONNECT ROOM
# ---------------------------------------------------
@app.post("/api/reconnect_room")
async def api_reconnect_room(req: Request):
    b = await req.json()
    rid = b.get("room_id")
    token = (b.get("reconnect_token") or "").strip()

    if not rid or rid not in ROOMS:
        return JSONResponse({"ok": False, "error": "room_not_found"}, 404)
    if not token:
        return JSONResponse({"ok": False, "error": "missing_reconnect_token"}, 400)

    room = ROOMS[rid]
    room.setdefault("reconnect_tokens", {})

    matched_player = ""
    for pname, stored in (room.get("reconnect_tokens", {}) or {}).items():
        if stored and stored == token:
            matched_player = pname
            break

    if not matched_player or not _player_exists_in_room(room, matched_player):
        return JSONResponse({"ok": False, "error": "invalid_reconnect_token"}, 404)

    if matched_player in (room.get("left_players", []) or []):
        return JSONResponse({"ok": False, "error": "left_game_no_rejoin"}, 403)

    room["ready_to_start"] = (len(room.get("players", []) or []) >= MAX_SEATS)

    return {
        "ok": True,
        "room_id": rid,
        "player_name": matched_player,
        "room_label": room.get("label", rid),
        "room_host": room.get("host", ""),
        "phase": room.get("phase", ""),
        "winning_score": room_winning_score(room),
        "reconnect_token": token
    }


@app.post("/api/reconnect_by_identity")
async def api_reconnect_by_identity(req: Request):
    """Restore an active seat by stable Wix/member/browser identity.

    This is a fallback for cases where a player opens the game in a new tab/browser
    and the reconnect token is not present in local storage. It does not allow
    re-entry after an intentional active-game leave.
    """
    b = await req.json()
    player_identity = _normalize_identity(b.get("player_identity") or b.get("identity") or "")
    rid_hint = (b.get("room_id") or "").strip()

    if not player_identity:
        return JSONResponse({"ok": False, "error": "missing_identity"}, 400)

    candidates = []
    room_items = []
    if rid_hint and rid_hint in ROOMS:
        room_items.append((rid_hint, ROOMS[rid_hint]))
    room_items.extend([(rid, room) for rid, room in ROOMS.items() if rid != rid_hint])

    for rid, room in room_items:
        try:
            if room.get("phase") in (None, "", "game_over"):
                continue
            if _is_identity_blocked_from_room(room, player_identity):
                continue
            matched = _find_player_by_identity(room, player_identity)
            if not matched or not _player_exists_in_room(room, matched):
                continue
            if matched in (room.get("left_players", []) or []):
                continue
            candidates.append((rid, room, matched))
        except Exception:
            continue

    if not candidates:
        return JSONResponse({"ok": False, "error": "no_active_seat_for_identity"}, 404)

    rid, room, matched_player = candidates[0]
    token = _ensure_reconnect_token(room, matched_player)
    room["ready_to_start"] = (len(room.get("players", []) or []) >= MAX_SEATS)

    return {
        "ok": True,
        "room_id": rid,
        "player_name": matched_player,
        "room_label": room.get("label", rid),
        "room_host": room.get("host", ""),
        "phase": room.get("phase", ""),
        "winning_score": room_winning_score(room),
        "reconnect_token": token,
        "identity_reconnect": True
    }


# ---------------------------------------------------
# LEAVE ROOM
# ---------------------------------------------------
@app.post("/api/leave_room")
async def api_leave_room(req: Request):
    b = await req.json()
    rid = b.get("room_id")
    pn = _normalize_name(b.get("player_name"))
    player_identity = _normalize_identity(b.get("player_identity") or b.get("identity") or "")
    active_leave = bool(b.get("active_leave") or b.get("force_active_leave"))

    if not rid or rid not in ROOMS:
        return JSONResponse({"error": "room_not_found"}, 404)

    room = ROOMS[rid]
    players = room.get("players", [])

    if not any(p["name"] == pn for p in players):
        return JSONResponse({"error": "not_in_room"}, 400)

    phase_now = room.get("phase") or ""

    # Explicit active-game leave from #leaveGame2:
    # - Keep the seat in the game so CPU takeover can control it.
    # - Remove sockets/reconnect token so the player cannot re-enter this same room.
    # - Return the leaving client to the lobby.
    if phase_now not in ("", "waiting", "game_over"):
        if not active_leave:
            return JSONResponse({"error": "active_game_leave_requires_confirmed_active_leave"}, 400)

        room.setdefault("reconnect_tokens", {})
        room.setdefault("seat_identities", {})
        room.setdefault("player_statuses", {})
        room.setdefault("left_players", [])
        room.setdefault("left_identities", [])

        if player_identity:
            _set_seat_identity(room, pn, player_identity)
        _remember_left_identity(room, pn, player_identity)

        room_sockets = ROOM_SOCKETS.get(rid, {})
        personal_sockets = room_sockets.pop(pn, [])
        for sock in list(personal_sockets):
            try:
                _safe_remove_ws(rid, pn, sock)
            except Exception:
                pass
            try:
                await sock.close()
            except Exception:
                pass
        ROOM_SOCKETS[rid] = room_sockets

        try:
            room.get("reconnect_tokens", {}).pop(pn, None)
        except Exception:
            pass

        try:
            _mark_human_disconnected(rid, pn)
            if _activate_cpu_takeover(rid, pn):
                # Explicit Leave Game / Room Info leave is intentional, not accidental.
                # Do not wait through the reconnect-grace slow CPU-turn window.
                st = _room_statuses(room).setdefault(pn, {})
                st["intentional_active_leave"] = True
                st["cpu_takeover_started_at"] = time.monotonic() - CPU_TAKEOVER_SLOW_WINDOW_SECONDS - 1.0
        except Exception:
            pass

        if room.get("awaiting_next_round"):
            _sync_round_control_player(rid, room)

        await broadcast_state_without_hands(rid)
        for p in _players_order(room):
            await send_state_update_to_player(rid, p)
        await broadcast_lobby_rooms()
        await cpu_maybe_act(rid)

        return {
            "left": True,
            "active_leave": True,
            "room_id": rid,
            "room_deleted": False,
            "player_name": pn,
            "cpu_takeover": True,
            "cannot_rejoin": True,
        }

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
    try:
        room.get("reconnect_tokens", {}).pop(pn, None)
    except Exception:
        pass
    try:
        room.get("seat_identities", {}).pop(pn, None)
    except Exception:
        pass

    room_sockets = ROOM_SOCKETS.get(rid, {})
    personal_sockets = room_sockets.pop(pn, [])
    for sock in list(personal_sockets):
        try:
            _safe_remove_ws(rid, pn, sock)
        except Exception:
            pass
        try:
            await sock.close()
        except Exception:
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
# Reconnect / CPU takeover timing (test branch)
CPU_TAKEOVER_GRACE_SECONDS = 30.0
CPU_TAKEOVER_SLOW_TURN_SECONDS = 30.0
CPU_TAKEOVER_SLOW_WINDOW_SECONDS = 120.0


def _get_player_obj(room: dict, name: str):
    for p in room.get('players', []) or []:
        if p.get('name') == name:
            return p
    return None

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
            return bool(pobj.get("is_cpu"))
    except Exception:
        pass
    return isinstance(name, str) and name.startswith("CPU")



def _room_statuses(room: dict) -> dict:
    room.setdefault("player_statuses", {})
    for pobj in room.get("players", []) or []:
        name = pobj.get("name")
        if not name:
            continue
        st = room["player_statuses"].setdefault(name, {})
        st.setdefault("is_cpu", bool(pobj.get("is_cpu")) or str(name).startswith("CPU"))
        if st.get("is_cpu"):
            st["connection_state"] = "cpu_player"
        else:
            st.setdefault("connection_state", "human_connected")
            st.setdefault("disconnected_at", None)
            st.setdefault("cpu_takeover_active", False)
            st.setdefault("cpu_takeover_started_at", None)
    return room["player_statuses"]


def _seat_has_active_socket(room_id: str, player_name: str) -> bool:
    try:
        return bool(ROOM_SOCKETS.get(room_id, {}).get(player_name, []) or [])
    except Exception:
        return False


def _connected_humans_in_order(room_id: str, room: dict) -> List[str]:
    """Connected, non-CPU seats in table order."""
    out = []
    try:
        for p in room.get("players", []) or []:
            name = p.get("name")
            if not name or _is_cpu(room, name):
                continue
            if _seat_has_active_socket(room_id, name):
                out.append(name)
    except Exception:
        pass
    return out


def _resolve_round_control_player(room_id: str, room: dict) -> str:
    """Choose who may click Count Aces & Tens / Play Next Round.

    Default: host if connected.
    If host is disconnected or under CPU takeover, delegate to the next connected
    human in seating order. If no human is connected, return empty string so the
    round pauses until a human reconnects. Applies to Practice Room, mixed CPU,
    and all-human games.
    """
    connected = _connected_humans_in_order(room_id, room)
    if not connected:
        return ""

    current = room.get("round_control_player") or ""
    if current in connected:
        return current

    host = room.get("host") or ""
    if host in connected:
        return host

    order = _players_order(room)
    if host in order:
        hidx = order.index(host)
        for offset in range(1, len(order) + 1):
            cand = order[(hidx + offset) % len(order)]
            if cand in connected:
                return cand

    return connected[0]


def _sync_round_control_player(room_id: str, room: dict) -> str:
    control = _resolve_round_control_player(room_id, room)
    room["round_control_player"] = control
    return control


def _round_control_wait_text(room_id: str, room: dict, viewer: str) -> str:
    control = _sync_round_control_player(room_id, room)
    if not control:
        return "Waiting for a human player to reconnect."
    if viewer == control:
        return "Click Count Aces & Tens" if room.get("count_required", False) else "Click Play Next Round"
    return f"Waiting for {control} to Count Aces & Tens" if room.get("count_required", False) else f"Waiting for {control} to Play Next Round"


def _mark_human_connected(room_id: str, player_name: str):
    room = ROOMS.get(room_id)
    if not room or _is_cpu(room, player_name):
        return
    st = _room_statuses(room).setdefault(player_name, {})
    st["is_cpu"] = False
    st["connection_state"] = "human_connected"
    st["disconnected_at"] = None
    st["cpu_takeover_active"] = False
    st["cpu_takeover_started_at"] = None


def _mark_human_disconnected(room_id: str, player_name: str):
    room = ROOMS.get(room_id)
    if not room or _is_cpu(room, player_name):
        return
    st = _room_statuses(room).setdefault(player_name, {})
    now = time.monotonic()
    st["is_cpu"] = False
    st["connection_state"] = "human_disconnected_reserved"
    st["disconnected_at"] = now
    st["cpu_takeover_active"] = False
    st["cpu_takeover_started_at"] = None


def _activate_cpu_takeover(room_id: str, player_name: str):
    room = ROOMS.get(room_id)
    if not room or _is_cpu(room, player_name):
        return False
    st = _room_statuses(room).setdefault(player_name, {})
    if _seat_has_active_socket(room_id, player_name):
        _mark_human_connected(room_id, player_name)
        return False
    st["is_cpu"] = False
    st["connection_state"] = "cpu_takeover_active"
    st["cpu_takeover_active"] = True
    st["cpu_takeover_started_at"] = st.get("cpu_takeover_started_at") or time.monotonic()
    return True


def _is_cpu_controlled(room: dict, player_name: str) -> bool:
    if _is_cpu(room, player_name):
        return True
    try:
        st = _room_statuses(room).get(player_name, {})
        return bool(st.get("cpu_takeover_active"))
    except Exception:
        return False


def _cpu_takeover_delay_for_turn(room: dict, player_name: str) -> Optional[float]:
    """Return a special delay for CPU takeover seats, else None for normal CPU delay."""
    try:
        if _is_cpu(room, player_name):
            return None
        st = _room_statuses(room).get(player_name, {})
        if not st.get("cpu_takeover_active"):
            return None
        if st.get("intentional_active_leave"):
            return None
        started = float(st.get("cpu_takeover_started_at") or time.monotonic())
        if (time.monotonic() - started) < CPU_TAKEOVER_SLOW_WINDOW_SECONDS:
            return CPU_TAKEOVER_SLOW_TURN_SECONDS
    except Exception:
        pass
    return None


async def _cpu_takeover_after_grace(room_id: str, player_name: str):
    """After a human disconnects, reserve their seat, then let CPU control it if they do not return."""
    await asyncio.sleep(CPU_TAKEOVER_GRACE_SECONDS)
    room = ROOMS.get(room_id)
    if not room or room.get("phase") in (None, "", "waiting", "game_over"):
        return
    if _seat_has_active_socket(room_id, player_name):
        _mark_human_connected(room_id, player_name)
        return
    if _activate_cpu_takeover(room_id, player_name):
        log.info(f"[CPU TAKEOVER START] room={room_id} player={player_name}")
        await broadcast_state_without_hands(room_id)
        for p in _players_order(room):
            await send_state_update_to_player(room_id, p)
        await cpu_maybe_act(room_id)

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
        room['players'].append({'name': cpu_name, 'is_cpu': True, 'controller': 'cpu'})
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

def _cpu_room_level(room: dict) -> int:
    try:
        return int(room.get("cpu_difficulty", room.get("cpu_level", 2)) or 2)
    except Exception:
        return 1

def _phase12_determine_winner_from_trick(trick: list, trump: str, phase: str) -> str:
    if not trick:
        return ""

    lead = trick[0].get("card") or ""
    lead_suit = lead.split("_of_")[1] if "_of_" in lead else ""

    def suit_of_local(c: str):
        return c.split("_of_")[1] if "_of_" in c else ""

    def rank_of_local(c: str):
        if not c or c.startswith("joker"):
            return -99
        r = c.split("_of_")[0] if "_of_" in c else ""
        return rank_value_map.get(r, -1)

    if lead.startswith("joker"):
        if phase in ("phase2", "phase3") and trump:
            trump_cards = [
                t for t in trick
                if not str(t.get("card", "")).startswith("joker")
                and suit_of_local(t.get("card", "")) == trump
            ]
            if trump_cards:
                best_rank = -999
                winner = trump_cards[0].get("player", "")
                for t in trump_cards:
                    rv = rank_of_local(t.get("card", ""))
                    if rv > best_rank:
                        best_rank = rv
                        winner = t.get("player", "")
                return winner
        return trick[0].get("player", "")

    trump_cards = []
    if trump:
        trump_cards = [
            t for t in trick
            if not str(t.get("card", "")).startswith("joker")
            and suit_of_local(t.get("card", "")) == trump
        ]

    if trump_cards:
        best_rank = -999
        winner = trump_cards[0].get("player", "")
        for t in trump_cards:
            rv = rank_of_local(t.get("card", ""))
            if rv > best_rank:
                best_rank = rv
                winner = t.get("player", "")
        return winner

    suit_followers = [t for t in trick if suit_of_local(t.get("card", "")) == lead_suit]
    if suit_followers:
        best_rank = -999
        winner = suit_followers[0].get("player", "")
        for t in suit_followers:
            rv = rank_of_local(t.get("card", ""))
            if rv > best_rank:
                best_rank = rv
                winner = t.get("player", "")
        return winner

    return trick[0].get("player", "")

def _cpu_card_wins_current_trick(room: dict, cpu_name: str, card: dict) -> bool:
    try:
        trick = list(room.get("current_trick", []) or [])
        if not card or not card.get("code"):
            return False
        sim = trick + [{"player": cpu_name, "card": card.get("code"), "uid": card.get("uid")}]
        winner = _phase12_determine_winner_from_trick(
            sim,
            room.get("trump_suit") or "",
            (room.get("phase") or "").strip()
        )
        return winner == cpu_name
    except Exception:
        return False

def _cpu_phase1_marriage_candidates(room: dict, player: str):
    hand = room.get("hands", {}).get(player, []) or []
    meld = room.get("melds", {}).get(player, []) or []

    room.setdefault("scored_melds", {})
    room["scored_melds"].setdefault(player, [])
    used_in_marriage = set()
    for rec in room["scored_melds"][player]:
        if rec.get("category") == "marriage":
            for u in rec.get("uids", []):
                used_in_marriage.add(u)

    candidate_cards = []
    for c in hand + meld:
        code = c.get("code", "")
        uid = c.get("uid")
        if not code or "_of_" not in code or not uid or uid in used_in_marriage:
            continue
        r, s = code.split("_of_")
        if r in ("king", "queen"):
            candidate_cards.append(c)

    uid_source = {}
    for c in hand:
        uid_source[c["uid"]] = "hand"
    for c in meld:
        if c["uid"] not in uid_source:
            uid_source[c["uid"]] = "meld"

    by_suit = {}
    for c in candidate_cards:
        r, s = c["code"].split("_of_")
        by_suit.setdefault(s, {"king": [], "queen": []})
        by_suit[s][r].append(c)

    out = []
    for suit_name, g in by_suit.items():
        if g["king"] and g["queen"]:
            valid_pairs = []
            for k in g["king"]:
                for q in g["queen"]:
                    if (uid_source.get(k["uid"]) == "hand" or uid_source.get(q["uid"]) == "hand"):
                        valid_pairs.append((k, q))
            if valid_pairs:
                k, q = valid_pairs[0]
                points = 40 if (room.get("trump_suit") is None or room.get("trump_suit") == suit_name) else 20
                out.append({
                    "type": "marriage",
                    "points": points,
                    "cards": [k, q],
                    "suit": suit_name,
                })
    return out

def _cpu_phase2_meld_candidates(room: dict, player: str):
    hand = list(room.get("hands", {}).get(player, []) or [])
    meld = list(room.get("melds", {}).get(player, []) or [])
    trump = room.get("trump_suit") or ""
    scored = room.get("scored_melds", {}).get(player, []) or []

    used_by_category = build_used_uids_by_category(scored)

    def eval_sel(selection):
        if not selection:
            return None

        # Prevent duplicate physical card reuse inside one CPU-generated meld candidate.
        # This does NOT block reusing the same card across DIFFERENT meld types later;
        # it only blocks impossible duplicate use of the same UID within a single selection.
        sel_uids = [c.get("uid") for c in selection if c.get("uid")]
        if len(sel_uids) != len(selection) or len(set(sel_uids)) != len(sel_uids):
            return None

        if not any(((c.get("source") or "hand") == "hand") and (not str(c.get("code","")).startswith("joker")) for c in selection):
            return None
        if any(rank_of(c.get("code","")) in (0,1,2) for c in selection if not str(c.get("code","")).startswith("joker")):
            return None

        codes = [c.get("code","") for c in selection]

        if len(selection) == 5:
            nonj = [c for c in selection if not str(c.get("code","")).startswith("joker")]
            if len(nonj) != 5:
                return None
            suits = {suit_of(c["code"]) for c in nonj}
            if len(suits) != 1:
                return None
            suit_name = next(iter(suits))
            if not trump or suit_name != trump:
                return None
            ranks = {c["code"].split("_of_")[0] for c in nonj}
            if ranks != {"ace", "king", "queen", "jack", "10"}:
                return None
            king_from_meld = any(c["code"].startswith("king_of_") and (c.get("source") or "hand") == "meld" for c in selection)
            queen_from_meld = any(c["code"].startswith("queen_of_") and (c.get("source") or "hand") == "meld" for c in selection)
            if not king_from_meld or not queen_from_meld:
                return None
            return {"type": "quinte", "points": 250, "cards": selection}

        if len(selection) == 4:
            nonj = [c for c in selection if not str(c.get("code","")).startswith("joker")]
            jokers = [c for c in selection if str(c.get("code","")).startswith("joker")]
            if len(jokers) > 1 or len(nonj) < 3:
                return None
            rank0 = nonj[0]["code"].split("_of_")[0]
            if rank0 not in ("ace", "king", "queen", "jack"):
                return None
            if any(c["code"].split("_of_")[0] != rank0 for c in nonj):
                return None
            used_fk = set()
            for fk_cat in (
                "four_aces", "four_kings", "four_queens", "four_jacks",
                "three_aces_joker", "three_kings_joker",
                "three_queens_joker", "three_jacks_joker",
            ):
                used_fk.update(used_by_category.get(fk_cat, set()))
            if any(c.get("uid") in used_fk for c in selection):
                return None
            pts = 100 if rank0 == "ace" else 80 if rank0 == "king" else 60 if rank0 == "queen" else 40
            return {"type": "four_kind", "points": pts, "cards": selection}

        if len(selection) == 2:
            have_qs = "queen_of_spades" in codes
            have_jd = "jack_of_diamonds" in codes
            if have_qs and have_jd:
                used_b = used_by_category.get("besigue", set())
                if any(c.get("uid") in used_b for c in selection):
                    return None
                return {"type": "besigue", "points": 40, "cards": selection}

            a, b = selection[0], selection[1]
            if str(a.get("code","")).startswith("joker") or str(b.get("code","")).startswith("joker"):
                return None
            used_m = used_by_category.get("marriage", set())
            if a.get("uid") in used_m or b.get("uid") in used_m:
                return None
            ra, rb = a["code"].split("_of_")[0], b["code"].split("_of_")[0]
            sa, sb = suit_of(a["code"]), suit_of(b["code"])
            is_kq = ((ra == "king" and rb == "queen") or (ra == "queen" and rb == "king"))
            if is_kq and sa and sa == sb:
                pts = 40 if trump and sa == trump else 20
                return {"type": "marriage", "points": pts, "cards": selection, "suit": sa}

        return None

    augmented_hand = [dict(c, source="hand") for c in hand]
    augmented_meld = [dict(c, source="meld") for c in meld]

    pool_raw = augmented_hand + augmented_meld

    # Deduplicate by physical UID in case the same card appears
    # in both hand and meld views during CPU candidate generation.
    # This is safe for normal CPU seats and future CPU takeover seats,
    # because it only affects temporary candidate evaluation, not stored state.
    seen_pool_uids = set()
    pool = []
    for c in pool_raw:
        uid = c.get("uid")
        if not uid:
            continue
        if uid in seen_pool_uids:
            continue
        seen_pool_uids.add(uid)
        pool.append(c)

    out = []
    seen = set()
    for r in (2, 4, 5):
        for combo in combinations(pool, r):
            ev = eval_sel(list(combo))
            if not ev:
                continue
            key = (ev["type"], tuple(sorted(c.get("uid") for c in ev["cards"] if c.get("uid"))))
            if key in seen:
                continue
            seen.add(key)
            out.append(ev)
    return out

def _cpu_available_meld_candidates(room: dict, player: str):
    phase = (room.get("phase") or "").strip()
    out = []
    if phase == "phase1":
        out.extend(_cpu_phase1_marriage_candidates(room, player))
    elif phase == "phase2":
        out.extend(_cpu_phase2_meld_candidates(room, player))
        out.extend(_cpu_phase1_marriage_candidates(room, player))
    return out

def _cpu_protected_uids_now(room: dict, player: str):
    protected = set()
    for cand in _cpu_available_meld_candidates(room, player):
        for c in cand.get("cards", []) or []:
            uid = c.get("uid")
            if uid:
                protected.add(uid)
    return protected

def _cpu_choose_play_uid_level2(room: dict, cpu_name: str):
    hand = list(room.get("hands", {}).get(cpu_name, []) or [])
    meld = list(room.get("melds", {}).get(cpu_name, []) or [])
    combined = hand + meld
    if not combined:
        return None

    phase = (room.get("phase") or "").strip()

    if phase == "phase3":
        legal_uids = phase3_legal_uids_for_player(room, cpu_name)
        legal_cards = [c for c in combined if c.get("uid") in legal_uids]
        if not legal_cards:
            return None
    else:
        legal_cards = combined[:]

    scoreable_now = bool(_cpu_available_meld_candidates(room, cpu_name)) if phase in ("phase1", "phase2") else False
    if scoreable_now and room.get("current_trick"):
        winning_cards = [c for c in legal_cards if _cpu_card_wins_current_trick(room, cpu_name, c)]
        if winning_cards:
            winning_cards = sorted(winning_cards, key=lambda c: (_rank_value(c.get("code","")), c.get("code","")))
            return (winning_cards[0] or {}).get("uid")

    protected = _cpu_protected_uids_now(room, cpu_name)
    unprotected = [c for c in legal_cards if c.get("uid") not in protected]
    candidates = unprotected if unprotected else legal_cards
    candidates = sorted(candidates, key=lambda c: (_rank_value(c.get("code","")), c.get("code","")))
    return (candidates[0] or {}).get("uid") if candidates else None



def _cpu_choose_phase3_legal_fallback_uid(room: dict, player_name: str):
    """Return the cheapest server-legal Phase 3 UID from hand+meld.

    Used only as a safety net for CPU takeover seats, so a rejected illegal
    fallback card cannot stop GP3 progression.
    """
    try:
        combined = list(room.get("hands", {}).get(player_name, []) or []) + list(room.get("melds", {}).get(player_name, []) or [])
        if not combined:
            return None
        legal = phase3_legal_uids_for_player(room, player_name)
        legal_cards = [c for c in combined if c.get("uid") in legal]
        if not legal_cards:
            # If there is exactly one physical card left for this player, allow it.
            # The normal play pipeline will still remove it safely.
            if _count_total_cards_for_player(room, player_name) == 1:
                return (combined[0] or {}).get("uid")
            return None
        legal_cards = sorted(legal_cards, key=lambda c: (_rank_value(c.get("code", "")), c.get("code", "")))
        return (legal_cards[0] or {}).get("uid")
    except Exception as e:
        log.exception(f"[CPU PHASE3 FALLBACK ERROR] player={player_name} err={e}")
        return None



def _cpu_choose_phase3_legal_fallback_uid(room: dict, player_name: str):
    """Return the cheapest server-legal GP3 UID from hand+meld.

    Safety net for CPU takeover seats. It avoids the old behavior where fallback
    blindly picked the lowest hand card, which could be illegal in GP3 and leave
    the game stopped on "CPU is currently playing for ...".
    """
    try:
        combined = list(room.get("hands", {}).get(player_name, []) or []) + list(room.get("melds", {}).get(player_name, []) or [])
        seen = set()
        cards = []
        for c in combined:
            if not isinstance(c, dict):
                continue
            uid = c.get("uid")
            if not uid or uid in seen:
                continue
            seen.add(uid)
            cards.append(c)

        if not cards:
            return None

        legal = phase3_legal_uids_for_player(room, player_name)
        legal_cards = [c for c in cards if c.get("uid") in legal]

        if not legal_cards:
            # Last physical card is always forced; normal process_action removal still protects state.
            if _count_total_cards_for_player(room, player_name) == 1:
                return (cards[0] or {}).get("uid")
            log.info(
                f"[CPU PHASE3 NO LEGAL FALLBACK] player={player_name} "
                f"hand={[c.get('code') for c in (room.get('hands', {}).get(player_name) or [])]} "
                f"meld={[c.get('code') for c in (room.get('melds', {}).get(player_name) or [])]} "
                f"trick={[t.get('card') for t in (room.get('current_trick', []) or [])]} "
                f"trump={room.get('trump_suit')!r}"
            )
            return None

        legal_cards = sorted(legal_cards, key=lambda c: (_rank_value(c.get("code", "")), c.get("code", "")))
        return (legal_cards[0] or {}).get("uid")
    except Exception as e:
        log.exception(f"[CPU PHASE3 FALLBACK ERROR] player={player_name} err={e}")
        return None


async def _cpu_try_score_now(room_id: str, player_name: str) -> bool:
    _cpu_score_t0 = time.monotonic()
    room = ROOMS.get(room_id)
    if not room:
        return False
    if room.get("phase") != "phase2":
        return False
    if room.get("current_turn") != player_name:
        return False
    if not room.get("post_trick_draws"):
        return False
    if room.get("meld_scored_this_trick"):
        return False
    # v81.96: Practice Room rule — CPU players may play/draw, but may not score meld points.
    # Aces & Tens are still counted normally at round end.
    if _is_cpu(room, player_name) and not bool(room.get("cpu_scores_enabled", True)):
        log.info(f"[CPU SCORE SKIP] room={room_id} player={player_name} reason=cpu_scores_disabled")
        return False
    if _cpu_room_level(room) < 2:
        return False

    try:
        candidates = _cpu_available_meld_candidates(room, player_name)
        log.info(f"[CPU SCORE CHECK] player={player_name} phase={room.get('phase')} candidates={len(candidates)}")
        if not candidates:
            log.info(f"[CPU SCORE END] room={room_id} player={player_name} result=no_candidates dt={time.monotonic() - _cpu_score_t0:.3f}s")
            return False

        type_priority = {"quinte": 4, "four_kind": 3, "besigue": 2, "marriage": 1}
        candidates = sorted(
            candidates,
            key=lambda c: (
                -int(c.get("points", 0) or 0),
                -type_priority.get(c.get("type"), 0),
                -sum(1 for x in (c.get("cards") or []) if (x.get("source") or "hand") == "hand"),
                tuple(sorted(x.get("uid","") for x in (c.get("cards") or [])))
            )
        )

        best = candidates[0]
        cards = best.get("cards") or []
        mtype = best.get("type")
        log.info(f"[CPU SCORE ATTEMPT] player={player_name} meld_type={mtype} points={best.get('points')} cards={[c.get('code') for c in cards]}")

        before_score = int((room.get("scores", {}) or {}).get(player_name, 0) or 0)
        before_flag = bool(room.get("meld_scored_this_trick"))
        before_len = len((room.get("scored_melds", {}) or {}).get(player_name, []) or [])

        if mtype == "marriage":
            for c in cards:
                await process_action(_DUMMY_WS, room_id, player_name, {
                    "action": "start_marriage",
                    "card": {"code": c.get("code"), "uid": c.get("uid")}
                })
            await process_action(_DUMMY_WS, room_id, player_name, {"action": "score_marriage"})
        else:
            payload = [{"code": c.get("code"), "uid": c.get("uid")} for c in cards]
            await process_action(_DUMMY_WS, room_id, player_name, {"action": "score_meld", "cards": payload})

        room_after = ROOMS.get(room_id) or room
        after_score = int((room_after.get("scores", {}) or {}).get(player_name, 0) or 0)
        after_flag = bool(room_after.get("meld_scored_this_trick"))
        after_len = len((room_after.get("scored_melds", {}) or {}).get(player_name, []) or [])

        committed = (after_score > before_score) or (after_len > before_len) or (after_flag and not before_flag)
        if not committed:
            log.info(f"[CPU SCORE NO-COMMIT] player={player_name} meld_type={mtype} cards={[c.get('code') for c in cards]}")
            log.info(f"[CPU SCORE END] room={room_id} player={player_name} result=no_commit meld_type={mtype} dt={time.monotonic() - _cpu_score_t0:.3f}s")
            return False
        log.info(f"[CPU SCORE END] room={room_id} player={player_name} result=committed meld_type={mtype} dt={time.monotonic() - _cpu_score_t0:.3f}s")
        return True
    except Exception as e:
        log.exception(f"[CPU SCORE ERROR] room={room_id} player={player_name} err={e}")
        log.info(f"[CPU SCORE END] room={room_id} player={player_name} result=exception dt={time.monotonic() - _cpu_score_t0:.3f}s")
        return False

class _NoopWS:
    async def send_json(self, *args, **kwargs):
        # CPU/server actions use this dummy websocket. Log info/error responses
        # so no-progress stalls reveal whether the action was illegal, missing UID, etc.
        try:
            if args:
                msg = args[0]
                if isinstance(msg, dict) and msg.get("type") in ("info", "error"):
                    log.info(f"[CPU DUMMY WS] {msg}")
        except Exception:
            pass
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
        _runner_t0 = time.monotonic()
        r = ROOMS.get(room_id)
        if not r:
            return
        log.info(f"[CPU RUNNER START] room={room_id}")
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
                    log.info(f"[CPU RUNNER BREAK] room={room_id} reason=bad_phase phase={phase!r} steps={steps}")
                    break

                cur = r2.get("current_turn")
                if not cur or not _is_cpu_controlled(r2, cur):
                    log.info(f"[CPU RUNNER BREAK] room={room_id} reason=human_or_no_turn phase={phase} current_turn={cur!r} steps={steps}")
                    break

                _step_t0 = time.monotonic()
                log.info(
                    f"[CPU STEP START] room={room_id} step={steps+1} phase={phase} current_turn={cur} "
                    f"post_trick_draws={bool(r2.get('post_trick_draws'))} trick_len={len(r2.get('current_trick', []) or [])} "
                    f"deck={len(r2.get('deck', []) or [])}"
                )

                # If a 7 of trump was just scored and automated CPU play is about to continue,
                # pause briefly so the played card and turn-alert sound are visible/audible.
                try:
                    trump7_until = float(r2.get("_pause_after_trump_seven_until") or 0.0)
                except Exception:
                    trump7_until = 0.0
                if trump7_until > time.monotonic():
                    pause_for = max(0.0, trump7_until - time.monotonic())
                    log.info(f"[CPU TRUMP7 PAUSE] room={room_id} player={cur} seconds={pause_for:.2f}")
                    await asyncio.sleep(pause_for)
                    r2 = ROOMS.get(room_id) or r2
                    if r2.get("current_turn") != cur:
                        # A human/other action advanced the game while waiting. Re-evaluate loop.
                        await asyncio.sleep(0)
                        continue
                try:
                    if float((ROOMS.get(room_id) or {}).get("_pause_after_trump_seven_until") or 0.0) <= time.monotonic():
                        (ROOMS.get(room_id) or {}).pop("_pause_after_trump_seven_until", None)
                except Exception:
                    pass

                # CPU delay. If a CPU is selected to lead the first trick of any round,
                # give humans a short moment to see their newly dealt cards first.
                opening_cpu_lead_delay = False
                try:
                    opening_cpu_lead_delay = bool(
                        r2.get("_opening_cpu_lead_delay_pending")
                        and phase == "phase1"
                        and len(r2.get("current_trick", []) or []) == 0
                        and not r2.get("post_trick_draws")
                        and r2.get("current_turn") == cur
                    )
                except Exception:
                    opening_cpu_lead_delay = False

                if opening_cpu_lead_delay:
                    r2["_opening_cpu_lead_delay_pending"] = False
                    log.info(f"[CPU OPENING LEAD DELAY] room={room_id} player={cur} seconds=2.75")
                    await asyncio.sleep(2.75)
                else:
                    # For CPU takeover seats, pause before each action during the reclaim window.
                    takeover_delay = _cpu_takeover_delay_for_turn(r2, cur)
                    if takeover_delay is not None:
                        await asyncio.sleep(takeover_delay)
                        r2_after_wait = ROOMS.get(room_id) or {}
                        if r2_after_wait.get("current_turn") != cur or not _is_cpu_controlled(r2_after_wait, cur):
                            log.info(f"[CPU RUNNER BREAK] room={room_id} reason=takeover_reclaimed_or_turn_changed phase={phase} current_turn={cur!r} steps={steps}")
                            break
                    elif room.get("phase") == "phase3":
                        # After a trick completes, keep the last trick visible a bit before the winning CPU leads.
                        if room.get("pending_trick_clear") and _is_cpu_controlled(room, room.get("current_turn")):
                            await asyncio.sleep(3.0)
                        else:
                            await asyncio.sleep(2.0)
                    else:
                        await asyncio.sleep(random.uniform(*CPU_NORMAL_DELAY_RANGE))

                # ------------------------------------------------------------
                # CPU ACTION CHOICE
                # ------------------------------------------------------------
                # Phase 3 begins with a mandatory "pickup_melds" action by the leader.
                if phase == "phase3" and not r2.get("phase3_melds_picked"):
                    # CPU_PHASE3_PICKUP_DELAY: give humans time to see melds before pickup
                    await asyncio.sleep(6.0)
                    log.info(f"[CPU ACTION] room={room_id} player={cur} action=pickup_melds")
                    await process_action(_DUMMY_WS, room_id, cur, {"action": "pickup_melds"})
                    log.info(f"[CPU STEP END] room={room_id} step={steps+1} player={cur} action=pickup_melds dt={time.monotonic() - _step_t0:.3f}s")

                elif r2.get("post_trick_draws"):
                    if _cpu_room_level(r2) >= 2 and not r2.get("meld_scored_this_trick"):
                        try:
                            log.info(f"[CPU ACTION] room={room_id} player={cur} action=try_score")
                            scored = await _cpu_try_score_now(room_id, cur)
                        except Exception as e:
                            log.exception(f"[CPU LOOP SCORE CRASH] room={room_id} player={cur} err={e}")
                            scored = False
                        if scored:
                            log.info(f"[CPU STEP END] room={room_id} step={steps+1} player={cur} action=score_then_continue dt={time.monotonic() - _step_t0:.3f}s")
                            await asyncio.sleep(1.0)
                            steps += 1
                            await asyncio.sleep(0)
                            continue

                    # CPU_DRAW_DELAY_POST_TRICK: give humans time to see the completed trick
                    await asyncio.sleep(2.5)
                    log.info(f"[CPU ACTION] room={room_id} player={cur} action=draw_card")
                    await process_action(_DUMMY_WS, room_id, cur, {"action": "draw_card"})
                    log.info(f"[CPU STEP END] room={room_id} step={steps+1} player={cur} action=draw_card dt={time.monotonic() - _step_t0:.3f}s")

                else:
                    uid = None

                    try:
                        if _cpu_room_level(r2) >= 2:
                            uid = _cpu_choose_play_uid_level2(r2, cur)
                    except Exception as e:
                        log.exception(f"[CPU CHOOSE ERROR] room={room_id} player={cur} err={e}")
                        uid = None

                    if not uid:
                        if phase == "phase3":
                            uid = _cpu_choose_phase3_legal_fallback_uid(r2, cur)
                        else:
                            uid = _choose_cpu_play_uid(r2, cur)

                    if not uid:
                        hand = (r2.get("hands", {}).get(cur) or [])
                        meld = (r2.get("melds", {}).get(cur) or [])
                        combined = hand + meld
                        if combined:
                            uid = (combined[0] or {}).get("uid")

                    if not uid:
                        break

                    before_turn = r2.get("current_turn")
                    before_trick_len = len(r2.get("current_trick", []) or [])
                    log.info(f"[CPU ACTION] room={room_id} player={cur} action=play_card uid={uid}")
                    await process_action(_DUMMY_WS, room_id, cur, {"action": "play_card", "card": {"uid": uid}})

                    r_after = ROOMS.get(room_id) or {}
                    after_turn = r_after.get("current_turn")
                    after_trick_len = len(r_after.get("current_trick", []) or [])

                    if after_turn == before_turn and after_trick_len == before_trick_len:
                        fallback_uid = _cpu_choose_phase3_legal_fallback_uid(r_after, cur) if phase == "phase3" else _choose_cpu_play_uid(r_after, cur)

                        if fallback_uid and fallback_uid != uid:
                            log.info(f"[CPU ACTION] room={room_id} player={cur} action=play_card_fallback uid={fallback_uid}")
                            await process_action(_DUMMY_WS, room_id, cur, {
                                "action": "play_card",
                                "card": {"uid": fallback_uid}
                            })
                            log.info(f"[CPU STEP END] room={room_id} step={steps+1} player={cur} action=play_card_fallback dt={time.monotonic() - _step_t0:.3f}s")
                            await asyncio.sleep(0)
                            continue

                        try:
                            legal_dbg = list(phase3_legal_uids_for_player(r_after, cur)) if phase == "phase3" else []
                            hand_dbg = [c.get("code") for c in (r_after.get("hands", {}).get(cur) or [])]
                            meld_dbg = [c.get("code") for c in (r_after.get("melds", {}).get(cur) or [])]
                            trick_dbg = [t.get("card") for t in (r_after.get("current_trick", []) or [])]
                            log.info(f"[CPU RUNNER BREAK] room={room_id} reason=no_progress phase={phase} player={cur} steps={steps} legal={legal_dbg} hand={hand_dbg} meld={meld_dbg} trick={trick_dbg} phase3_picked={r_after.get('phase3_melds_picked')}")
                        except Exception:
                            log.info(f"[CPU RUNNER BREAK] room={room_id} reason=no_progress phase={phase} player={cur} steps={steps}")
                        break


                log.info(f"[CPU STEP END] room={room_id} step={steps+1} player={cur} action=complete dt={time.monotonic() - _step_t0:.3f}s")
                steps += 1
                # yield control; allows WS messages/state broadcasts to flush
                await asyncio.sleep(0)
        finally:
            log.info(f"[CPU RUNNER END] room={room_id} total_dt={time.monotonic() - _runner_t0:.3f}s")
            r3 = ROOMS.get(room_id)
            if r3 is not None:
                r3["_cpu_running"] = False

                # v82.4.10b:
                # If a human reconnects/reclaims a seat while an old CPU-takeover
                # runner is sleeping, the runner can wake, detect the reclaim, and
                # exit after that human has already played a card. In that sequence,
                # the current turn may now belong to a normal CPU seat, but the
                # process_action-triggered cpu_maybe_act() call was skipped because
                # _cpu_running was still True. Restart the runner once after release.
                try:
                    phase_now = (r3.get("phase") or "").strip()
                    turn_now = r3.get("current_turn")
                    if (
                        phase_now not in ("", "waiting", "game_over", "round_end_wait")
                        and turn_now
                        and _is_cpu_controlled(r3, turn_now)
                        and not r3.get("awaiting_next_round")
                    ):
                        log.info(f"[CPU RUNNER RESTART NEEDED] room={room_id} phase={phase_now} current_turn={turn_now}")
                        asyncio.create_task(cpu_maybe_act(room_id))
                except Exception as e:
                    log.exception(f"[CPU RUNNER RESTART CHECK ERROR] room={room_id} err={e}")

    asyncio.create_task(_runner())


async def _start_cpu_style_game(room_id: str, host_name: str, cpu_level: int = 2, cpu_scores_enabled: bool = True):
    room = ROOMS.get(room_id)
    if not room:
        return None

    _cpu_fill_room_to_four(room)

    players = [p['name'] for p in room.get('players', [])]
    if len(players) != MAX_SEATS:
        return {"error": "need_4_seats"}

    lead = random.choice(players)
    room['lead'] = lead
    room['current_turn'] = lead
    room['phase'] = 'lead_selection'
    room['round_start_lead'] = lead
    room['_opening_cpu_lead_delay_pending'] = bool(_is_cpu_controlled(room, lead) or _is_cpu(room, lead))

    room['awaiting_next_round'] = False
    room['count_required'] = False
    room['count_done'] = False
    room['round_control_player'] = ""

    await broadcast_state_without_hands(room_id)

    deck = new_deck_full_132()
    room['deck'] = deck
    room['hands'] = deal_cards_to_players(room, 9)

    room['current_trick'] = []
    room['last_completed_trick'] = []
    room['pending_trick_clear'] = False
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

    room['cpu_difficulty'] = cpu_level
    room['cpu_level'] = cpu_level
    room['cpu_scores_enabled'] = bool(cpu_scores_enabled)

    for p in players:
        room['scores'][p] = 0
        room['melds'].setdefault(p, [])
        room['scored_melds'].setdefault(p, [])
        room['won_tricks'].setdefault(p, [])

    await _emit_deck_count(room_id, len(room.get('deck', [])))
    for p in players:
        await send_state_update_to_player(room_id, p)

    await _send_to_room(room_id, {'type': 'game_start', 'phase': 'phase1', 'lead': lead})
    await broadcast_lobby_rooms()
    await cpu_maybe_act(room_id)

    return {
        'started': True,
        'room_id': room_id,
        'cpu_filled': True,
        'cpu_level': cpu_level,
        'cpu_scores_enabled': bool(cpu_scores_enabled),
    }

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

    cpu_level = int(b.get('cpu_level', 2) or 2)
    room['winning_score'] = int(b.get('winning_score', room.get('winning_score', DEFAULT_WINNING_SCORE)) or DEFAULT_WINNING_SCORE)

    log.info(
        f"[WIN SCORE START CPU] room={rid} "
        f"stored={room.get('winning_score')} "
        f"request={b.get('winning_score')}"
    )

    result = await _start_cpu_style_game(rid, pn, cpu_level=cpu_level, cpu_scores_enabled=True)
    if isinstance(result, dict) and result.get("error"):
        return JSONResponse(result, 400)
    return result or {'started': True, 'room_id': rid, 'cpu_filled': True}

@app.post("/api/start_game")
async def api_start_game(req: Request):
    b = await req.json()
    rid = b.get("room_id")
    pn = _normalize_name(b.get("player_name"))

    if rid not in ROOMS:
        return JSONResponse({"error": "room_not_found"}, 400)

    room = ROOMS[rid]
    room["winning_score"] = int(b.get("winning_score", room.get("winning_score", DEFAULT_WINNING_SCORE)) or DEFAULT_WINNING_SCORE)

    log.info(
        f"[WIN SCORE START HUMAN] room={rid} "
        f"stored={room.get('winning_score')} "
        f"request={b.get('winning_score')}"
    )

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
    room["_opening_cpu_lead_delay_pending"] = bool(_is_cpu_controlled(room, lead) or _is_cpu(room, lead))

    room["awaiting_next_round"] = False
    room["count_required"] = False
    room["count_done"] = False
    room["round_control_player"] = ""

    await broadcast_state_without_hands(rid)

    deck = new_deck_full_132()
    room["deck"] = deck
    room["hands"] = deal_cards_to_players(room, 9)

    room["current_trick"] = []
    room["last_completed_trick"] = []
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

    # All-human rooms may later need CPU takeover for disconnected/left human seats.
    # Treat takeover seats like Easy CPU players for meld scoring, while no actual
    # CPU seats exist until a human disconnects or intentionally leaves.
    room["cpu_difficulty"] = 2
    room["cpu_level"] = 2
    room["cpu_scores_enabled"] = True

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
    # ROUND END WAIT: connected human control must Count A/10 first, then Continue next round
    # ============================================================
    if room.get("awaiting_next_round"):
        control_player = _sync_round_control_player(room_id, room)
        if action == "count_aces_tens":
            if not control_player:
                await ws.send_json({"type": "info", "msg": "waiting_for_human_reconnect"})
                await broadcast_state_without_hands(room_id)
                for p in _players_order(room):
                    await send_state_update_to_player(room_id, p)
                return
            if player != control_player:
                await ws.send_json({"type": "info", "msg": "only_round_control_can_count", "round_control_player": control_player})
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

            # Count Aces & Tens is always a scoring event. Broadcast it before any
            # winner/game-over message so every browser can play SCORE_ANY and update
            # scores first.
            await _send_to_room(room_id, {
                "type": "aces_tens_counted",
                "bonus": bonus,
                "scores": room.get("scores", {}),
                "msg": "Aces and 10s counted.",
                "play_score_any": True,
                "round_control_player": room.get("round_control_player", "")
            })

            # ✅ IMPORTANT: if bonus pushed someone to 400+, end game AFTER the
            # score-any event has had a short lead.
            winner_code, text = _evaluate_winner_after_round(room)
            if winner_code and winner_code != "":
                await asyncio.sleep(1.25)
                await _end_game_now(room_id, room, winner_code, text, bonus=bonus)
                return

            # ✅ standardized "no winner" text
            await _send_to_room(room_id, {
                "type": "show_winner",
                "winner": "",
                "text": NO_WINNER_TEXT,
                "scores": room.get("scores", {}),
                "last_trick_winner": room.get("last_trick_winner"),
                "bonus": bonus,
                "round_control_player": room.get("round_control_player", "")
            })

            room["phase"] = "round_end_wait"
            _sync_round_control_player(room_id, room)

            await broadcast_state_without_hands(room_id)
            for p in _players_order(room):
                await send_state_update_to_player(room_id, p)
            return

        if action == "next_round":
            control_player = _sync_round_control_player(room_id, room)
            if not control_player:
                await ws.send_json({"type": "info", "msg": "waiting_for_human_reconnect"})
                await broadcast_state_without_hands(room_id)
                for p in _players_order(room):
                    await send_state_update_to_player(room_id, p)
                return
            if player != control_player:
                await ws.send_json({"type": "info", "msg": "only_round_control_can_start_next_round", "round_control_player": control_player})
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
        room = ROOMS.get(room_id) or room
        room["current_turn"] = leader
        await broadcast_state_without_hands(room_id)
        for p in _players_order(room):
            await send_state_update_to_player(room_id, p)
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

        # Trump 7 rule: after a legal play is visible, award +10 immediately.
        # If this reaches the winning score, the game ends after the alert sound lead.
        if await _award_trump_seven_if_applicable(room_id, room, player, card_code):
            return

        # ============================================================
        # PHASE 3 LAST-TRICK AUTO-PLAY
        # ============================================================
        if room.get("phase") == "phase3":
            order = _players_order(room)

            if len(room["current_trick"]) < 4:
                nxt = _next_player_ccw(room, player)
                room["current_turn"] = nxt
                log.info(f"[PHASE3 TURN ADVANCE] room={room_id} played_by={player} next_turn={nxt} trick_len={len(room.get('current_trick', []) or [])} trump={room.get('trump_suit')!r}")

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
            room["last_completed_trick"] = [dict(t) for t in (room.get("current_trick", []) or [])]
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
                # Keep the final completed trick visible briefly before the round-end / winner UI appears.
                # This covers both normal and auto-completed final tricks.
                await asyncio.sleep(FINAL_TRICK_ROUND_END_DELAY_SAFE)

                winner_code, text = _evaluate_winner_after_round(room)

                if winner_code == "":
                    room["awaiting_next_round"] = True
                    room["phase"] = "round_end_wait"
                    room["count_required"] = True
                    room["count_done"] = False
                    control_player = _sync_round_control_player(room_id, room)

                    await _send_to_room(room_id, {
                        "type": "round_end_wait",
                        "msg": ("No winner yet. Count Aces and 10s." if control_player else "No winner yet. Waiting for a human player to reconnect."),
                        "host": room.get("host"),
                        "round_control_player": control_player,
                    })

                    await _send_to_room(room_id, {
                        "type": "show_winner",
                        "winner": "",
                        "text": NO_WINNER_TEXT,
                        "scores": room.get("scores", {}),
                        "last_trick_winner": room.get("last_trick_winner"),
                        "bonus": {},
                        "round_control_player": room.get("round_control_player", "")
                    })

                    await broadcast_state_without_hands(room_id)
                    for p in order:
                        await send_state_update_to_player(room_id, p)

                else:
                    room["phase"] = "game_over"
                    await asyncio.sleep(POST_SCORE_SHOWWINNER_DELAY)
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
        room["last_completed_trick"] = [dict(t) for t in (trick or [])]

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
        # v81.96: Practice Room rule — CPU players may not score meld/marriage points.
        if _is_cpu(room, player) and not bool(room.get("cpu_scores_enabled", True)):
            await ws.send_json({"type": "info", "msg": "cpu_scoring_disabled"})
            return

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
                _append_card_to_meld_unique(meld, found)

        room["scored_melds"][player].append({
            "category": "marriage",
            "uids": list(uids),
            "suit": suit,
        })

        room["marriage_pending"] = None
        room.setdefault("temp_melds", {})
        room["temp_melds"][player] = []
        room["meld_scored_this_trick"] = True

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
        # v81.96: Practice Room rule — CPU players may not score meld points.
        if _is_cpu(room, player) and not bool(room.get("cpu_scores_enabled", True)):
            await ws.send_json({"type": "info", "msg": "cpu_scoring_disabled"})
            return

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

                    marriage_pair_in_meld = False
                    if uid_source.get(k_uid) == "meld" and uid_source.get(q_uid) == "meld":
                        for rec in room.get("scored_melds", {}).get(player, []) or []:
                            if rec.get("category") != "marriage":
                                continue
                            if rec.get("suit") != trump:
                                continue
                            rec_uids = set(rec.get("uids", []) or [])
                            if k_uid in rec_uids and q_uid in rec_uids:
                                marriage_pair_in_meld = True
                                break

                    if marriage_pair_in_meld:
                        category = "quinte_trump"
                        points = 250
                    else:
                        await ws.send_json({"type": "info", "msg": "quinte_requires_same_scored_marriage_in_meld"})
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
                            _append_card_to_meld_unique(meld_area, h)
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

    if player_name in (room.get("left_players", []) or []):
        await ws.send_json({"type": "error", "error": "left_game_no_rejoin"})
        await ws.close()
        return

    if (not name_exists) and room.get("phase") not in ("waiting", ""):
        await ws.send_json({"type": "error", "error": "game_started"})
        await ws.close()
        return

    if (not name_exists) and room.get("phase") == "waiting" and len(room.get("players", [])) >= MAX_SEATS:
        await ws.send_json({"type": "error", "error": "room_full"})
        await ws.close()
        return

    if not name_exists:
        room["players"].append({"name": player_name})
        room["scores"].setdefault(player_name, 0)
        room["melds"].setdefault(player_name, [])
        room["scored_melds"].setdefault(player_name, [])
        room["won_tricks"].setdefault(player_name, [])

    room.setdefault("meld_scored_this_trick", False)
    room.setdefault("phase3_melds_picked", False)
    room.setdefault("pending_trick_clear", False)
    room.setdefault("ready_to_start", len(room.get("players", [])) >= MAX_SEATS)

    await _register_single_socket(room_id, player_name, ws)
    _mark_human_connected(room_id, player_name)
    if room.get("awaiting_next_round"):
        _sync_round_control_player(room_id, room)
    await send_state_update_to_player(room_id, player_name)
    await broadcast_state_without_hands(room_id)

    try:
        while True:
            msg = await ws.receive_json()
            await process_action(ws, room_id, player_name, msg)
            await cpu_maybe_act(room_id)
    except WebSocketDisconnect:
        _safe_remove_ws(room_id, player_name, ws)
        try:
            room = ROOMS.get(room_id)
            if room and room.get("phase") not in (None, "", "waiting", "game_over"):
                remaining = ROOM_SOCKETS.get(room_id, {}).get(player_name, []) or []
                if not remaining and not _is_cpu(room, player_name):
                    _mark_human_disconnected(room_id, player_name)
                    if room.get("awaiting_next_round"):
                        _sync_round_control_player(room_id, room)
                    await broadcast_state_without_hands(room_id)
                    asyncio.create_task(_cpu_takeover_after_grace(room_id, player_name))
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
        except:
            pass

    except Exception as e:
        log.exception(f"WS ERROR: {e}")
        _safe_remove_ws(room_id, player_name, ws)
        try:
            room = ROOMS.get(room_id)
            if room and room.get("phase") not in (None, "", "waiting", "game_over"):
                remaining = ROOM_SOCKETS.get(room_id, {}).get(player_name, []) or []
                if not remaining and not _is_cpu(room, player_name):
                    _mark_human_disconnected(room_id, player_name)
                    if room.get("awaiting_next_round"):
                        _sync_round_control_player(room_id, room)
                    await broadcast_state_without_hands(room_id)
                    asyncio.create_task(_cpu_takeover_after_grace(room_id, player_name))
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