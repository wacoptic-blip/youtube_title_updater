#!/usr/bin/env python3
"""
OBS -> YouTube live-broadcast bot.

When OBS starts streaming this script creates a fresh YouTube live broadcast
titled from the weekly service schedule, binds it to the reusable stream key
OBS is actually using, and drives it live. When OBS stops, the broadcast is
completed. If a broadcast is already live it is renamed instead of duplicated.

On days that coptic.io (a free, MIT-licensed public calendar API) reports an
actual feast (not merely a fasting season), its name overrides the weekly
schedule title. Lookup failures are non-fatal; the weekly schedule is used.

Lifecycle control is fully manual (enableAutoStart=False) because:
  * autostart only fires on the ingest stream's inactive -> active edge, which
    has usually already passed by the time we bind (OBS starts pushing first);
  * a broadcast with enableAutoStart=True rejects manual transitions with
    "invalidTransition", leaving no rescue path.

Quota costs (default allowance 10,000 units/day):
    liveBroadcasts.list        1      liveStreams.list           1
    liveBroadcasts.insert     50      liveBroadcasts.bind       50
    liveBroadcasts.update     50      liveBroadcasts.transition 50
    liveBroadcasts.delete     50
A full create -> bind -> live -> complete cycle is ~200 units.

Files created next to this script:
    logs/titlebot.log[.YYYY-MM-DD]   rotating log files
    bot_state.json                   quota ledger + broadcast/stream/title cache
    token.pickle                     cached OAuth credentials
Requires:
    client_secret.json               Google OAuth "desktop app" client
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import pickle
import signal
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from logging.handlers import TimedRotatingFileHandler
from zoneinfo import ZoneInfo

from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from obsws_python import EventClient, ReqClient, Subs


# ===========================================================================
# CONFIGURATION
# ===========================================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# --- LOGGING ---
LOG_DIR = os.path.join(SCRIPT_DIR, 'logs')
LOG_FILE = os.path.join(LOG_DIR, 'titlebot.log')
LOG_RETENTION_DAYS = 30
CONSOLE_LOG_LEVEL = logging.INFO
FILE_LOG_LEVEL = logging.DEBUG

# --- OBS ---
OBS_HOST = 'localhost'
OBS_PORT = 4457
OBS_PASSWORD = '1JiOV6gwOJqGTpGw'          # consider moving out of source control
HEARTBEAT_SECONDS = 30                     # OBS connection watchdog; 0 disables
TRIGGER_IF_ALREADY_STREAMING = True        # act at startup if OBS is mid-stream

# --- YOUTUBE AUTH ---
SCOPES = ['https://www.googleapis.com/auth/youtube.force-ssl']
TOKEN_FILE = os.path.join(SCRIPT_DIR, 'token.pickle')
CLIENT_SECRET_FILE = os.path.join(SCRIPT_DIR, 'client_secret.json')
STATE_FILE = os.path.join(SCRIPT_DIR, 'bot_state.json')

QUOTA_RESET_TZ = ZoneInfo('America/Los_Angeles')   # YouTube quota resets midnight PT

# --- QUOTA ACCOUNTING ---
API_COST_LIST = 1
API_COST_UPDATE = 50
API_COST_INSERT = 50
API_COST_BIND = 50
API_COST_TRANSITION = 50
API_COST_DELETE = 50
DAILY_QUOTA_BUDGET = 9_500                 # headroom under the 10,000 allowance

# --- BROADCAST CREATION ---
CREATE_BROADCAST_IF_MISSING = True
PREFER_MANUAL_SCHEDULED_BROADCAST = True   # use an operator-scheduled broadcast as-is
BROADCAST_PRIVACY = 'public'               # 'public' | 'unlisted' | 'private'
BROADCAST_MADE_FOR_KIDS = False            # required by the API
BROADCAST_DESCRIPTION = (
    'Saint Mary and Archangel Michael Coptic Orthodox Church — live service.'
)
BROADCAST_LATENCY = 'low'                  # 'normal' | 'low' | 'ultraLow'
BROADCAST_ENABLE_DVR = True
BROADCAST_RECORD_FROM_START = True
BROADCAST_ENABLE_AUTO_START = False        # we drive the lifecycle ourselves
BROADCAST_ENABLE_AUTO_STOP = True          # belt and braces; we also complete
BROADCAST_ENABLE_MONITOR = False           # skip the testing/preview stage
BROADCAST_START_OFFSET_SECONDS = 5         # scheduledStartTime = now + this
MAX_BROADCASTS_PER_DAY = 6                 # safety cap against runaway creation
DELETE_UNUSED_CREATED_BROADCAST = True     # bin a broadcast that never went live
COMPLETE_BROADCAST_ON_OBS_STOP = True      # end broadcasts we drove live

# --- STREAM KEY SELECTION ---
MATCH_OBS_STREAM_KEY = True                # bind the stream OBS is really using
PREFERRED_STREAM_ID = ''                   # hard override; blank = auto-detect

# --- GOING LIVE ---
STREAM_ACTIVE_POLL_SECONDS = 6
STREAM_ACTIVE_TIMEOUT_SECONDS = 120        # give up waiting for ingest after this
AUTOSTART_GRACE_SECONDS = 15               # only used if autostart is enabled
DISABLE_AUTOSTART_BEFORE_TRANSITION = True
TRANSITION_RETRY_DELAYS = (5, 10, 20)      # for errorStreamInactive

# --- BEHAVIOUR TUNING ---
OBS_START_DEBOUNCE_SECONDS = 15
LOOKUP_INITIAL_DELAY_SECONDS = 0           # bind ASAP; 0 is fine with manual start
LOOKUP_RETRY_DELAYS = (15, 30, 60)
TITLE_RESUBMIT_GRACE_SECONDS = 6 * 3600
BROADCAST_REUSE_GRACE_SECONDS = 8 * 3600   # reuse a broadcast we made this long ago
RATE_LIMIT_COOLDOWN_SECONDS = 300
MAX_TRIGGER_RETRIES = 3
HTTP_RETRIES = 3
MAX_TITLE_LENGTH = 100                     # YouTube hard limit

# --- BROADCAST STATES ---
LIVE_STATES = {'live', 'testing', 'liveStarting'}
READY_STATES = {'ready', 'created'}
LIFECYCLE_PRIORITY = {'live': 0, 'liveStarting': 1, 'testing': 2, 'ready': 3}
FALLBACK_TO_UPCOMING = False               # rename a scheduled-but-not-live one

# --- API ERROR REASONS ---
HARD_QUOTA_REASONS = {'quotaExceeded', 'dailyLimitExceeded'}
RATE_LIMIT_REASONS = {'rateLimitExceeded', 'userRateLimitExceeded', 'backendError'}
INCOMPATIBLE_PARAM_REASONS = {'incompatibleParameters', 'invalidParameter'}
STREAM_INACTIVE_REASONS = {'errorStreamInactive'}
BENIGN_TRANSITION_REASONS = {'redundantTransition'}
INVALID_TRANSITION_REASONS = {'invalidTransition'}

log = logging.getLogger('titlebot')


# ===========================================================================
# SERVICE SCHEDULE  (weekday: 0=Mon .. 6=Sun; windows are [start, end) local)
# ===========================================================================
DEFAULT_TITLE = '+++ Saint Mary and Archangel Michael Live Service'

SERVICE_SCHEDULE = (
    (1, (4, 30), (5, 15), '+++ Tuesday Liturgy'),                                          # Tuesday 4:30-5:15am
    (2, (8, 30), (9, 15), '+++ Wednesday Liturgy'),                                        # Wednesday 8:30-9:15am
    (3, (9, 30), (10, 15), '+++ Thursday Liturgy'),                                        # Thursday 9:30-10:15am
    (5, (8, 0), (8, 45), '+++ Saturday Liturgy'),                                          # Saturday 8:00-8:45am
    (5, (17, 45), (18, 30), '+++ Saturday Vespers'),                                       # Saturday 5:45-6:30pm
    (5, (20, 15), (21, 0), '+++ Midnight Praises'),                                        # Saturday 8:15-9:00pm
    (6, (7, 30), (8, 15), '+++ Sunday Liturgy'),                                           # Sunday 7:30-8:15am
    (6, (18, 15), (19, 0), '+++ Sunday night Bible Study'),                                # Sunday 6:15-7:00pm
)

# Arabic suffix chosen by keyword, first match wins.
TITLE_SUFFIXES = (
    ('Liturgy', 'القداس الالهي +++'),
    ('Vespers', 'رفع بخور عشية +++'),
    ('Saturday Vespers', 'رفع بخور عشية و اجتماع الانبا موسي +++'),
    ('Midnight Praises', 'تسبيحة نصف الليل +++'),
)
DEFAULT_SUFFIX = '+++'

WEEKDAY_NAMES = ('Monday', 'Tuesday', 'Wednesday', 'Thursday',
                 'Friday', 'Saturday', 'Sunday')


# ===========================================================================
# COPTIC CALENDAR INTEGRATION  (coptic.io)
# ===========================================================================
# Free, open-source (MIT), no API key required: https://github.com/abanobmikaeel/coptic.io
# The API's celebration 'type' values are inconsistent across entries (seen:
# 'feast', 'lordlyFeast', 'majorFeast', 'minorFeast', 'fast', 'commemoration'),
# so we exclude rather than allow-list: anything but a fast/commemoration is
# treated as a title-worthy feast. Continuous fasting seasons (e.g. 'St. Mary
# Fast') are reported for every day within them and are never used as a title.
# 'lordlyFeast'/'majorFeast' identify the 7 major Lord's feasts; their title
# is applied a day early (the eve) when today itself has no feast of its own.
COPTIC_CALENDAR_ENABLED = False
COPTIC_API_BASE_URL = 'https://api.coptic.io/api'
COPTIC_API_TIMEOUT_SECONDS = 8
COPTIC_FEAST_EXCLUDE_TYPES = {'fast', 'commemoration'}
COPTIC_LORDLY_FEAST_TYPES = {'lordlyFeast', 'majorFeast'}
COPTIC_EVE_FEAST_KEYWORDS = ('theophany', 'nativity', 'easter', 'resurrection')


def validate_schedule() -> list[str]:
    """Return a list of human-readable problems with SERVICE_SCHEDULE."""
    problems: list[str] = []
    windows: dict[int, list[tuple[int, int, str]]] = {}

    for weekday, (sh, sm), (eh, em), title in SERVICE_SCHEDULE:
        start, end = sh * 60 + sm, eh * 60 + em
        if not 0 <= weekday <= 6:
            problems.append(f'{title}: weekday {weekday} out of range')
            continue
        if end <= start:
            problems.append(f'{title}: window ends before it starts ({sh:02d}:{sm:02d}'
                            f'-{eh:02d}:{em:02d})')
            continue
        for other_start, other_end, other_title in windows.get(weekday, []):
            if start < other_end and other_start < end:
                problems.append(
                    f'{WEEKDAY_NAMES[weekday]}: "{title}" overlaps "{other_title}"'
                )
        windows.setdefault(weekday, []).append((start, end, title))

    return problems


def _suffix_for(base_title: str) -> str:
    for keyword, suffix in TITLE_SUFFIXES:
        if keyword in base_title:
            return suffix
    return DEFAULT_SUFFIX


def _fit(base: str, date_str: str, suffix: str) -> str:
    """Assemble the title, trimming the *base* if the limit would be exceeded
    so that the date and the Arabic suffix always survive intact."""
    full = f'{base} {date_str} {suffix}'
    if len(full) <= MAX_TITLE_LENGTH:
        return full

    tail = f' {date_str} {suffix}'
    keep = MAX_TITLE_LENGTH - len(tail)
    if keep < 10:                       # pathological config; hard truncate
        return full[:MAX_TITLE_LENGTH]
    return base[:keep].rstrip() + tail


def generate_title(now: datetime | None = None, feast_title: str | None = None) -> str:
    """Generate an API feast title when provided; otherwise use the schedule."""
    now = now or datetime.now()
    minutes = now.hour * 60 + now.minute

    base_title = f'+++ {feast_title}' if feast_title else DEFAULT_TITLE
    if not feast_title:
        for weekday, (sh, sm), (eh, em), title in SERVICE_SCHEDULE:
            if now.weekday() == weekday and sh * 60 + sm <= minutes < eh * 60 + em:
                base_title = title
                break

    return _fit(base_title, now.strftime('%d/%m/%Y'), _suffix_for(base_title))


def fetch_coptic_celebrations(date_str: str) -> list[dict]:
    """Blocking GET against coptic.io; call via asyncio.to_thread."""
    url = f'{COPTIC_API_BASE_URL}/celebrations/{date_str}'
    request = urllib.request.Request(
        url, headers={'User-Agent': 'auto-title-updater (coptic.io client)'}
    )
    with urllib.request.urlopen(request, timeout=COPTIC_API_TIMEOUT_SECONDS) as response:
        return json.loads(response.read().decode('utf-8')) or []


def _feast_display_name(name: str) -> str:
    lowered = name.lower()
    if 'holy thursday' in lowered:
        return 'Covenant Thursday'
    if 'palm sunday' in lowered:
        return 'Palm Sunday'
    if 'thomas sunday' in lowered:
        return 'Thomas Sunday'
    if 'st. mary feast (commemoration of her assumption)' in lowered:
        return 'Feast of St. Mary (Commemoration of Her Assumption)'
    if 'nativity' in lowered:
        feast_name = 'Nativity'
    elif 'theophany' in lowered:
        feast_name = 'Theophany'
    elif 'easter' in lowered or 'resurrection' in lowered:
        feast_name = 'Resurrection'
    else:
        feast_name = name.removeprefix('Feast of the ')
    return f'Feast of the {feast_name}'


def pick_feast_title(celebrations: list[dict]) -> str | None:
    """Return the first API feast title, or None when no feast is reported."""
    for item in celebrations:
        name = item.get('name')
        feast_type = item.get('type')
        if (isinstance(name, str)
                and feast_type
                and feast_type not in COPTIC_FEAST_EXCLUDE_TYPES):
            return _feast_display_name(name)
    return None


def pick_lordly_feast_title(celebrations: list[dict]) -> str | None:
    """Pick only the three feasts whose titles are used on the eve."""
    for item in celebrations:
        name = item.get('name')
        if (isinstance(name, str)
                and item.get('type') in COPTIC_LORDLY_FEAST_TYPES
                and any(keyword in name.lower()
                        for keyword in COPTIC_EVE_FEAST_KEYWORDS)):
            return _feast_display_name(name)
    return None


def resolve_feast_title(date_str: str) -> str | None:
    """Resolve today's title, using the next day's feast after a Paramoun."""
    celebrations = fetch_coptic_celebrations(date_str)
    feast_title = pick_feast_title(celebrations)

    if any(
            'paramoun' in item.get('name', '').lower()
            for item in celebrations
            if isinstance(item.get('name'), str)):
        next_date_str = (datetime.strptime(date_str, '%Y-%m-%d')
                         + timedelta(days=1)).strftime('%Y-%m-%d')
        feast_title = pick_feast_title(fetch_coptic_celebrations(next_date_str))

    if feast_title:
        return feast_title

    next_date_str = (datetime.strptime(date_str, '%Y-%m-%d')
                     + timedelta(days=1)).strftime('%Y-%m-%d')
    return pick_lordly_feast_title(fetch_coptic_celebrations(next_date_str))


# -- CLI preview helpers (see `python auto_title_updater.py preview --help`) --
def _parse_preview_datetime(value: str) -> tuple[datetime, bool]:
    """Returns (datetime, had_time); had_time=False if only a date was given."""
    for fmt in ('%Y-%m-%d %H:%M', '%Y-%m-%dT%H:%M'):
        try:
            return datetime.strptime(value, fmt), True
        except ValueError:
            continue
    return datetime.strptime(value, '%Y-%m-%d'), False


def _fetch_feast_title_safe(date_str: str) -> str | None:
    if not COPTIC_CALENDAR_ENABLED:
        return None
    try:
        return resolve_feast_title(date_str)
    except Exception as exc:
        print(f'  (coptic.io lookup failed for {date_str}: {exc})')
        return None


def preview_titles(values: list[str], use_coptic: bool = True) -> None:
    """Offline CLI helper: print the title(s) that would be generated.
    A bare date (no time) prints every scheduled window for that weekday
    plus the no-service default; a full datetime prints a single title.
    Only the first window/moment of a given date gets the feast override --
    later ones that day fall back to the weekly schedule, mirroring the bot."""
    tracker = _FeastOnceTracker()
    for value in values:
        when, had_time = _parse_preview_datetime(value)
        date_str = when.strftime('%Y-%m-%d')
        raw_feast_title = _fetch_feast_title_safe(date_str) if use_coptic else None
        feast_note = f'  {raw_feast_title}' if raw_feast_title else ''

        if had_time:
            title = generate_title(when, feast_title=tracker.consume(date_str, raw_feast_title))
            print(f'{when:%Y-%m-%d %H:%M} ({WEEKDAY_NAMES[when.weekday()]}): {title}{feast_note}')
            continue

        print(f'{when:%Y-%m-%d} ({WEEKDAY_NAMES[when.weekday()]}){feast_note}')
        windows = [w for w in SERVICE_SCHEDULE if w[0] == when.weekday()]
        if not windows:
            title = generate_title(when.replace(hour=12, minute=0),
                                    feast_title=tracker.consume(date_str, raw_feast_title))
            print(f'    (no scheduled service)     -> {title}')
        for _, (sh, sm), (eh, em), _ in windows:
            moment = when.replace(hour=sh, minute=sm)
            title = generate_title(moment, feast_title=tracker.consume(date_str, raw_feast_title))
            print(f'    {sh:02d}:{sm:02d}-{eh:02d}:{em:02d}          -> {title}')


def preview_range(start_str: str, end_str: str, use_coptic: bool = True) -> None:
    day = datetime.strptime(start_str, '%Y-%m-%d')
    end = datetime.strptime(end_str, '%Y-%m-%d')
    while day <= end:
        preview_titles([day.strftime('%Y-%m-%d')], use_coptic=use_coptic)
        day += timedelta(days=1)


class _FeastOnceTracker:
    """Mirrors StateStore.consume_feast_title for the offline preview CLI:
    a feast title is only handed out once per date within a single run."""

    def __init__(self):
        self._used_dates: set[str] = set()

    def consume(self, date_str: str, feast_title: str | None) -> str | None:
        if not feast_title or date_str in self._used_dates:
            return None
        self._used_dates.add(date_str)
        return feast_title


# ===========================================================================
# ERRORS
# ===========================================================================
class QuotaBlocked(Exception):
    """No API calls may be made right now (daily quota gone or budget spent)."""


class RetryLater(Exception):
    """Transient problem; the trigger should be retried after `delay` seconds."""

    def __init__(self, delay: float, message: str):
        super().__init__(message)
        self.delay = delay


class SetupProblem(Exception):
    """Permanent configuration problem; retrying will not help."""


# ===========================================================================
# HELPERS
# ===========================================================================
def http_status(error: HttpError) -> int | None:
    return getattr(error.resp, 'status', None)


def error_reasons(error: HttpError) -> set[str]:
    """Extract Google API 'reason' strings from an HttpError body."""
    content = getattr(error, 'content', b'') or b''
    if isinstance(content, bytes):
        content = content.decode('utf-8', 'replace')

    reasons: set[str] = set()
    try:
        payload = json.loads(content).get('error', {})
        for item in payload.get('errors', []):
            if item.get('reason'):
                reasons.add(item['reason'])
        if payload.get('status'):
            reasons.add(payload['status'])
    except Exception:
        pass

    if not reasons:
        known = (HARD_QUOTA_REASONS | RATE_LIMIT_REASONS | INCOMPATIBLE_PARAM_REASONS
                 | STREAM_INACTIVE_REASONS | BENIGN_TRANSITION_REASONS
                 | INVALID_TRANSITION_REASONS)
        for candidate in known:
            if candidate in content:
                reasons.add(candidate)
    return reasons


def _invert(text: str) -> tuple:
    """Sort key that orders strings descending (newest timestamp first)."""
    return (text == '', tuple(-ord(c) for c in text))


def acceptable_states() -> set[str]:
    return LIVE_STATES | ({'ready'} if FALLBACK_TO_UPCOMING else set())


def rfc3339(when: datetime) -> str:
    return (when.astimezone(timezone.utc).replace(microsecond=0)
            .isoformat().replace('+00:00', 'Z'))


def mask(secret: str | None) -> str:
    if not secret:
        return '<none>'
    return f'{secret[:4]}…{secret[-4:]}' if len(secret) > 10 else '<short>'


def normalise_key(value: str | None) -> str:
    """OBS keys sometimes carry query params (e.g. '<key>?backup=1')."""
    if not value:
        return ''
    return value.strip().split('?', 1)[0]


# ===========================================================================
# PERSISTED STATE
# ===========================================================================
class StateStore:
    """JSON file holding the quota ledger plus broadcast/stream/title caches."""

    def __init__(self, path: str):
        self.path = path
        self._data: dict = {}
        self._load()
        self.refresh_day()

    # -- persistence
    def _load(self) -> None:
        try:
            with open(self.path, 'r', encoding='utf-8') as fh:
                self._data = json.load(fh)
        except FileNotFoundError:
            self._data = {}
        except Exception as exc:
            log.warning('Could not read state file (%s); starting fresh.', exc)
            self._data = {}

    def _save(self) -> None:
        tmp = f'{self.path}.tmp'
        try:
            with open(tmp, 'w', encoding='utf-8') as fh:
                json.dump(self._data, fh, indent=2, ensure_ascii=False)
            os.replace(tmp, self.path)          # atomic
        except Exception as exc:
            log.warning('Could not write state file: %s', exc)

    # -- quota ledger (Pacific day)
    @staticmethod
    def _quota_day() -> str:
        return datetime.now(QUOTA_RESET_TZ).date().isoformat()

    def refresh_day(self) -> None:
        today = self._quota_day()
        if self._data.get('quota_day') != today:
            self._data['quota_day'] = today
            self._data['units_used'] = 0
            self._data['quota_exhausted'] = False
            self._save()

    @property
    def units_used(self) -> int:
        return int(self._data.get('units_used', 0))

    def can_spend(self, cost: int) -> bool:
        self.refresh_day()
        if self._data.get('quota_exhausted'):
            return False
        return self.units_used + cost <= DAILY_QUOTA_BUDGET

    def charge(self, cost: int) -> None:
        self.refresh_day()
        self._data['units_used'] = self.units_used + cost
        self._save()

    def mark_quota_exhausted(self) -> None:
        self.refresh_day()
        self._data['quota_exhausted'] = True
        self._save()

    def cooldown_remaining(self) -> float:
        return max(0.0, float(self._data.get('cooldown_until', 0)) - time.time())

    def set_cooldown(self, seconds: float) -> None:
        self._data['cooldown_until'] = time.time() + seconds
        self._save()

    # -- creation cap (local day)
    def _roll_creation_day(self) -> None:
        today = datetime.now().date().isoformat()
        if self._data.get('creation_day') != today:
            self._data['creation_day'] = today
            self._data['creations_today'] = 0
            self._save()

    @property
    def creations_today(self) -> int:
        self._roll_creation_day()
        return int(self._data.get('creations_today', 0))

    def note_creation(self) -> None:
        self._roll_creation_day()
        self._data['creations_today'] = self.creations_today + 1
        self._save()

    # -- caches
    @property
    def broadcast_id(self) -> str | None:
        return self._data.get('broadcast_id')

    @broadcast_id.setter
    def broadcast_id(self, value: str | None) -> None:
        if self._data.get('broadcast_id') != value:
            self._data['broadcast_id'] = value
            self._save()

    @property
    def stream_id(self) -> str | None:
        return self._data.get('stream_id')

    @stream_id.setter
    def stream_id(self, value: str | None) -> None:
        if self._data.get('stream_id') != value:
            self._data['stream_id'] = value
            self._save()

    @property
    def list_strategy(self) -> str | None:
        return self._data.get('list_strategy')

    @list_strategy.setter
    def list_strategy(self, value: str | None) -> None:
        if self._data.get('list_strategy') != value:
            self._data['list_strategy'] = value
            self._save()

    # -- broadcast we created ourselves
    @property
    def created_broadcast_id(self) -> str | None:
        return self._data.get('created_broadcast_id')

    def created_broadcast_for(self, title: str) -> str | None:
        if self._data.get('created_title') != title:
            return None
        age = time.time() - float(self._data.get('created_at', 0))
        if not (0 <= age < BROADCAST_REUSE_GRACE_SECONDS):
            return None
        return self._data.get('created_broadcast_id')

    def note_created_broadcast(self, broadcast_id: str, title: str) -> None:
        self._data['created_broadcast_id'] = broadcast_id
        self._data['created_title'] = title
        self._data['created_at'] = time.time()
        self._data['created_went_live'] = False
        self._save()

    @property
    def created_went_live(self) -> bool:
        return bool(self._data.get('created_went_live'))

    @created_went_live.setter
    def created_went_live(self, value: bool) -> None:
        if bool(self._data.get('created_went_live')) != bool(value):
            self._data['created_went_live'] = bool(value)
            self._save()

    def clear_created_broadcast(self) -> None:
        for key in ('created_broadcast_id', 'created_title', 'created_at',
                    'created_went_live'):
            self._data.pop(key, None)
        self._save()

    # -- last applied title
    def title_recently_applied(self, title: str) -> bool:
        if self._data.get('last_title') != title:
            return False
        age = time.time() - float(self._data.get('last_title_at', 0))
        return 0 <= age < TITLE_RESUBMIT_GRACE_SECONDS

    def remember_title(self, broadcast_id: str, title: str) -> None:
        self._data['last_title'] = title
        self._data['last_title_at'] = time.time()
        self._data['last_title_broadcast_id'] = broadcast_id
        self._save()

    # -- feast override (once per day)
    def consume_feast_title(self, date_str: str, feast_title: str | None) -> str | None:
        """Returns feast_title only the first time it's consumed for a given
        date; later calls that day (e.g. a second stream start) return None
        so the weekly schedule takes over. Persisted, so it survives restarts."""
        if not feast_title or self._data.get('feast_used_date') == date_str:
            return None
        self._data['feast_used_date'] = date_str
        self._save()
        return feast_title


# ===========================================================================
# YOUTUBE AUTHENTICATION
# ===========================================================================
def get_youtube_client():
    creds = None
    if os.path.exists(TOKEN_FILE):
        try:
            with open(TOKEN_FILE, 'rb') as token:
                creds = pickle.load(token)
        except Exception as exc:
            log.warning('Could not load cached credentials (%s); re-authorising.', exc)

    if not creds or not creds.valid:
        refreshed = False
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                refreshed = True
            except RefreshError as exc:
                # invalid_grant here almost always means the refresh token died.
                # While the OAuth consent screen is in "Testing" publishing status,
                # Google hard-caps refresh tokens at 7 days regardless of activity;
                # publishing the app (Cloud Console > OAuth consent screen > Publish
                # App) removes that cap and is the only real fix for this recurring.
                log.warning(
                    'Refresh token rejected (%s). If this keeps happening every ~7 '
                    'days, your OAuth consent screen is still in "Testing" status; '
                    'publish it to Production to stop refresh tokens from expiring. '
                    'Falling back to interactive consent now.', exc,
                )
                try:
                    os.remove(TOKEN_FILE)
                except OSError:
                    pass
            except Exception as exc:
                log.warning('Token refresh failed (%s); running consent flow.', exc)
        if not refreshed and (not creds or not creds.valid):
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
            creds = flow.run_local_server(port=0)

        with open(TOKEN_FILE, 'wb') as token:
            pickle.dump(creds, token)

    return build('youtube', 'v3', credentials=creds, cache_discovery=False)


# ===========================================================================
# YOUTUBE BROADCAST MANAGER
# ===========================================================================
class YouTubeBroadcastManager:
    """All YouTube API interaction, with quota accounting and error triage."""

    CONTENT_DETAIL_KEYS = (
        'enableAutoStart', 'enableAutoStop', 'enableClosedCaptions',
        'enableDvr', 'enableEmbed', 'recordFromStart', 'latencyPreference',
        'projection', 'closedCaptionsType',
    )

    def __init__(self, youtube, state: StateStore):
        self.youtube = youtube
        self.state = state
        self.obs_stream_key: str | None = None      # set by TitleBot before use

    # -- low level ----------------------------------------------------------
    async def _execute(self, request, cost: int, what: str):
        if not self.state.can_spend(cost):
            raise QuotaBlocked(
                f'{what} needs {cost} units; '
                f'{self.state.units_used}/{DAILY_QUOTA_BUDGET} already used today '
                f'(resets midnight PT).'
            )

        cooldown = self.state.cooldown_remaining()
        if cooldown > 0:
            raise RetryLater(cooldown, f'rate-limit cooldown active ({cooldown:.0f}s)')

        try:
            # googleapiclient's own exponential backoff for 5xx and rate limits.
            # It deliberately does NOT retry quotaExceeded.
            result = await asyncio.to_thread(request.execute, num_retries=HTTP_RETRIES)
        except HttpError as error:
            self.state.charge(cost)             # assume the call was billed
            reasons = error_reasons(error)
            status = http_status(error)

            if reasons & HARD_QUOTA_REASONS:
                self.state.mark_quota_exhausted()
                raise QuotaBlocked(
                    f'{what}: YouTube daily quota exhausted; '
                    f'no further calls until midnight PT.'
                ) from error

            if (reasons & RATE_LIMIT_REASONS) or status in (429, 500, 503):
                self.state.set_cooldown(RATE_LIMIT_COOLDOWN_SECONDS)
                raise RetryLater(
                    RATE_LIMIT_COOLDOWN_SECONDS,
                    f'{what}: transient error ({status} {sorted(reasons)})',
                ) from error

            raise

        self.state.charge(cost)
        log.debug('%s ok (%d units, %d/%d used today)',
                  what, cost, self.state.units_used, DAILY_QUOTA_BUDGET)
        return result

    # -- broadcast lookups --------------------------------------------------
    # liveBroadcasts.list filters are mutually exclusive: exactly one of `id`,
    # `mine` or `broadcastStatus`. `broadcastType` is only valid with `mine`,
    # and its default ("event") hides persistent broadcasts.

    @staticmethod
    def _pick_broadcast(items: list[dict], allow_upcoming: bool) -> dict | None:
        wanted = set(LIVE_STATES)
        if allow_upcoming:
            wanted |= {'ready'}

        candidates = [
            item for item in items
            if item.get('status', {}).get('lifeCycleStatus') in wanted
        ]
        if not candidates:
            return None

        def sort_key(item: dict):
            status = item.get('status', {}).get('lifeCycleStatus', '')
            snippet = item.get('snippet', {})
            started = (snippet.get('actualStartTime')
                       or snippet.get('scheduledStartTime') or '')
            return (LIFECYCLE_PRIORITY.get(status, 99), _invert(started))

        candidates.sort(key=sort_key)
        return candidates[0]

    async def get_broadcast(self, broadcast_id: str) -> dict | None:
        request = self.youtube.liveBroadcasts().list(
            part='snippet,status,contentDetails', id=broadcast_id, maxResults=1
        )
        try:
            response = await self._execute(
                request, API_COST_LIST, 'liveBroadcasts.list(id)'
            )
        except HttpError as error:
            if http_status(error) == 404:
                return None
            raise
        items = response.get('items', [])
        return items[0] if items else None

    async def _list_by_status(self, status: str) -> list[dict]:
        request = self.youtube.liveBroadcasts().list(
            part='snippet,status,contentDetails', broadcastStatus=status, maxResults=5
        )
        response = await self._execute(
            request, API_COST_LIST, f'liveBroadcasts.list(status={status})'
        )
        return response.get('items', [])

    async def _list_mine(self) -> list[dict]:
        request = self.youtube.liveBroadcasts().list(
            part='snippet,status,contentDetails', mine=True,
            broadcastType='all', maxResults=50
        )
        response = await self._execute(
            request, API_COST_LIST, 'liveBroadcasts.list(mine)'
        )
        return response.get('items', [])

    async def find_active_broadcast(self) -> dict | None:
        allow_upcoming = FALLBACK_TO_UPCOMING

        strategies = ['status', 'mine']
        preferred = self.state.list_strategy
        if preferred in strategies:
            strategies.remove(preferred)
            strategies.insert(0, preferred)

        last_error: HttpError | None = None

        for strategy in strategies:
            try:
                if strategy == 'status':
                    items = await self._list_by_status('active')
                    item = self._pick_broadcast(items, allow_upcoming=False)
                    if item is None and allow_upcoming:
                        items = await self._list_by_status('upcoming')
                        item = self._pick_broadcast(items, allow_upcoming=True)
                else:
                    items = await self._list_mine()
                    item = self._pick_broadcast(items, allow_upcoming)
            except HttpError as error:
                if http_status(error) == 400 and (
                    error_reasons(error) & INCOMPATIBLE_PARAM_REASONS
                ):
                    log.warning(
                        "Strategy '%s' rejected by the API (%s); trying the next one.",
                        strategy, ', '.join(sorted(error_reasons(error))) or '400',
                    )
                    last_error = error
                    continue
                raise

            if item is not None:
                if self.state.list_strategy != strategy:
                    self.state.list_strategy = strategy
                    log.info("Using broadcast lookup strategy '%s'.", strategy)
                return item

            log.debug("Strategy '%s' found no live broadcast.", strategy)

        if last_error is not None and self.state.list_strategy:
            self.state.list_strategy = None
        return None

    async def find_manual_broadcast(self) -> dict | None:
        """Find a broadcast the operator scheduled by hand in YouTube Studio
        (live or upcoming), so it can be driven live untouched instead of
        creating or renaming one. Excludes broadcasts the bot itself created."""
        exclude_id = self.state.created_broadcast_id

        for status in ('active', 'upcoming'):
            items = await self._list_by_status(status)
            candidates = [item for item in items if item.get('id') != exclude_id]
            if not candidates:
                continue

            def sort_key(item: dict):
                status_name = item.get('status', {}).get('lifeCycleStatus', '')
                snippet = item.get('snippet', {})
                return (LIFECYCLE_PRIORITY.get(status_name, 99),
                        snippet.get('scheduledStartTime') or '')

            candidates.sort(key=sort_key)
            return candidates[0]
        return None

    async def use_manual_broadcast(self, broadcast: dict) -> str | None:
        """Bind a manually scheduled broadcast to the active stream if needed,
        without touching its title."""
        broadcast_id = broadcast['id']
        bound = broadcast.get('contentDetails', {}).get('boundStreamId')
        if not bound:
            stream_id = await self.resolve_stream_id()
            await self.bind_broadcast(broadcast_id, stream_id)
            bound = stream_id
        self.state.broadcast_id = broadcast_id
        return bound

    # -- ingest stream selection --------------------------------------------
    async def _list_streams(self) -> list[dict]:
        request = self.youtube.liveStreams().list(
            part='id,snippet,cdn,status', mine=True, maxResults=50
        )
        response = await self._execute(request, API_COST_LIST, 'liveStreams.list')
        return response.get('items', [])

    async def get_stream(self, stream_id: str) -> dict | None:
        request = self.youtube.liveStreams().list(
            part='id,snippet,cdn,status', id=stream_id, maxResults=1
        )
        response = await self._execute(request, API_COST_LIST, 'liveStreams.list(id)')
        items = response.get('items', [])
        return items[0] if items else None

    async def resolve_stream_id(self) -> str:
        """Pick the ingest stream to bind, preferring the key OBS is using."""
        if PREFERRED_STREAM_ID:
            return PREFERRED_STREAM_ID

        streams = await self._list_streams()
        if not streams:
            raise SetupProblem(
                'This channel has no ingest stream keys. Create one in YouTube '
                'Studio (Go Live -> Stream) and point OBS at it.'
            )

        for stream in streams:
            key = stream.get('cdn', {}).get('ingestionInfo', {}).get('streamName')
            log.debug('  stream %s  status=%s  key=%s  title=%s',
                      stream['id'],
                      stream.get('status', {}).get('streamStatus'),
                      mask(key),
                      stream.get('snippet', {}).get('title'))

        # 1. Exact match against the key OBS is configured with -- the only way
        #    to be certain the broadcast we bind will actually receive video.
        if MATCH_OBS_STREAM_KEY and self.obs_stream_key:
            wanted = normalise_key(self.obs_stream_key)
            for stream in streams:
                key = stream.get('cdn', {}).get('ingestionInfo', {}).get('streamName')
                if normalise_key(key) == wanted:
                    log.info('Matched OBS stream key to YouTube stream %s ("%s").',
                             stream['id'], stream.get('snippet', {}).get('title'))
                    self.state.stream_id = stream['id']
                    return stream['id']
            raise SetupProblem(
                f"OBS is streaming with key {mask(self.obs_stream_key)}, which does "
                f"not match any of this channel's {len(streams)} stream key(s). "
                f"Either OBS is pointed at a different channel, or it is using the "
                f"account-linked integration instead of a stream key."
            )

        # 2. A stream we already used successfully.
        cached = self.state.stream_id
        if cached and any(s['id'] == cached for s in streams):
            return cached

        # 3. A stream that is currently receiving data.
        active = [s for s in streams
                  if s.get('status', {}).get('streamStatus') == 'active']
        if len(active) == 1:
            log.info('Binding to the only actively-receiving stream %s.',
                     active[0]['id'])
            self.state.stream_id = active[0]['id']
            return active[0]['id']

        if len(streams) > 1:
            log.warning(
                'Multiple stream keys and no way to tell which OBS uses; picking %s. '
                'Set PREFERRED_STREAM_ID to remove the ambiguity.', streams[0]['id']
            )
        self.state.stream_id = streams[0]['id']
        return streams[0]['id']

    async def wait_for_stream_active(self, stream_id: str, timeout: float) -> bool:
        """Poll until YouTube reports it is receiving data on this key."""
        deadline = time.monotonic() + timeout
        last_state = None

        while True:
            stream = await self.get_stream(stream_id)
            status = (stream or {}).get('status', {})
            state = status.get('streamStatus')
            health = status.get('healthStatus', {}) or {}

            if state != last_state:
                log.info('Ingest stream %s: %s (health=%s)',
                         stream_id, state, health.get('status'))
                for issue in health.get('configurationIssues', []) or []:
                    log.warning('   ingest issue [%s] %s: %s',
                                issue.get('severity'), issue.get('type'),
                                issue.get('reason'))
                last_state = state

            if state == 'active':
                return True
            if time.monotonic() >= deadline:
                log.error(
                    '❌ Ingest stream %s never became active (last state: %s). '
                    'OBS is not sending to this key.', stream_id, state
                )
                return False

            await asyncio.sleep(STREAM_ACTIVE_POLL_SECONDS)

    # -- creation -----------------------------------------------------------
    async def create_broadcast(self, title: str) -> dict:
        if self.state.creations_today >= MAX_BROADCASTS_PER_DAY:
            raise SetupProblem(
                f'Daily broadcast-creation cap reached '
                f'({self.state.creations_today}/{MAX_BROADCASTS_PER_DAY}). '
                f'Raise MAX_BROADCASTS_PER_DAY if this is legitimate.'
            )

        start = datetime.now(timezone.utc) + timedelta(
            seconds=BROADCAST_START_OFFSET_SECONDS
        )
        body = {
            'snippet': {
                'title': title,
                'description': BROADCAST_DESCRIPTION,
                'scheduledStartTime': rfc3339(start),
            },
            'status': {
                'privacyStatus': BROADCAST_PRIVACY,
                'selfDeclaredMadeForKids': BROADCAST_MADE_FOR_KIDS,
            },
            'contentDetails': {
                'enableAutoStart': BROADCAST_ENABLE_AUTO_START,
                'enableAutoStop': BROADCAST_ENABLE_AUTO_STOP,
                'enableDvr': BROADCAST_ENABLE_DVR,
                'recordFromStart': BROADCAST_RECORD_FROM_START,
                'latencyPreference': BROADCAST_LATENCY,
                'monitorStream': {
                    'enableMonitorStream': BROADCAST_ENABLE_MONITOR,
                },
            },
        }

        request = self.youtube.liveBroadcasts().insert(
            part='snippet,status,contentDetails', body=body
        )
        broadcast = await self._execute(
            request, API_COST_INSERT, 'liveBroadcasts.insert'
        )
        self.state.note_creation()

        cd = broadcast.get('contentDetails', {}) or {}
        log.info('🆕 Created broadcast %s (%s, privacy=%s) "%s"',
                 broadcast['id'],
                 broadcast.get('status', {}).get('lifeCycleStatus'),
                 BROADCAST_PRIVACY, title)
        log.info('   autoStart=%s autoStop=%s monitorStream=%s latency=%s',
                 cd.get('enableAutoStart'), cd.get('enableAutoStop'),
                 (cd.get('monitorStream') or {}).get('enableMonitorStream'),
                 cd.get('latencyPreference'))
        log.info('   Watch: https://www.youtube.com/watch?v=%s', broadcast['id'])
        return broadcast

    async def bind_broadcast(self, broadcast_id: str, stream_id: str) -> dict:
        request = self.youtube.liveBroadcasts().bind(
            id=broadcast_id, part='id,contentDetails,status', streamId=stream_id
        )
        result = await self._execute(request, API_COST_BIND, 'liveBroadcasts.bind')
        log.info('🔗 Bound broadcast %s to stream %s.', broadcast_id, stream_id)
        return result

    async def transition(self, broadcast_id: str, status: str) -> dict:
        request = self.youtube.liveBroadcasts().transition(
            id=broadcast_id, part='id,status', broadcastStatus=status
        )
        result = await self._execute(
            request, API_COST_TRANSITION, f'liveBroadcasts.transition({status})'
        )
        log.info('▶️  Transitioned %s -> %s.', broadcast_id, status)
        return result

    async def delete_broadcast(self, broadcast_id: str) -> None:
        request = self.youtube.liveBroadcasts().delete(id=broadcast_id)
        await self._execute(request, API_COST_DELETE, 'liveBroadcasts.delete')
        log.info('🗑️  Deleted unused broadcast %s.', broadcast_id)

    # -- contentDetails updates ---------------------------------------------
    async def set_autostart(self, broadcast: dict, enabled: bool) -> dict:
        """Read-modify-write contentDetails; omitted fields revert to defaults."""
        current = broadcast.get('contentDetails', {}) or {}
        body_cd = {key: current[key] for key in self.CONTENT_DETAIL_KEYS
                   if current.get(key) is not None}
        body_cd['enableAutoStart'] = enabled

        monitor = current.get('monitorStream') or {}
        if monitor:
            body_cd['monitorStream'] = {
                'enableMonitorStream': monitor.get('enableMonitorStream', False),
                'broadcastStreamDelayMs': monitor.get('broadcastStreamDelayMs', 0),
            }

        request = self.youtube.liveBroadcasts().update(
            part='id,contentDetails',
            body={'id': broadcast['id'], 'contentDetails': body_cd},
        )
        result = await self._execute(
            request, API_COST_UPDATE, f'liveBroadcasts.update(autoStart={enabled})'
        )
        log.info('⚙️  Set enableAutoStart=%s on %s.', enabled, broadcast['id'])
        return result

    # -- safe title update (read-modify-write) ------------------------------
    async def write_title(self, broadcast: dict, title: str) -> None:
        snippet = broadcast.get('snippet', {})
        body_snippet = {'title': title}
        # Preserve the other *writable* snippet fields; read-only ones
        # (channelId, thumbnails, liveChatId, actual*Time...) are omitted.
        for field in ('description', 'scheduledStartTime', 'scheduledEndTime'):
            if snippet.get(field) is not None:
                body_snippet[field] = snippet[field]

        request = self.youtube.liveBroadcasts().update(
            part='snippet', body={'id': broadcast['id'], 'snippet': body_snippet}
        )
        await self._execute(request, API_COST_UPDATE, 'liveBroadcasts.update')

    async def _ensure_title(self, broadcast: dict, title: str) -> None:
        current = broadcast.get('snippet', {}).get('title', '')
        if current == title:
            log.info('✅ Title already correct; skipping the 50-unit update.')
        else:
            log.info('Renaming %s: "%s" -> "%s"', broadcast['id'], current, title)
            await self.write_title(broadcast, title)
            log.info('✅ Title updated: %s', title)
        self.state.broadcast_id = broadcast['id']
        self.state.remember_title(broadcast['id'], title)

    # -- transitions ---------------------------------------------------------
    async def _transition_once(self, broadcast_id: str, status: str) -> str:
        """Returns 'ok' | 'redundant' | 'invalid' | 'inactive'."""
        try:
            await self.transition(broadcast_id, status)
            return 'ok'
        except HttpError as error:
            reasons = error_reasons(error)
            if reasons & BENIGN_TRANSITION_REASONS:
                log.info('Broadcast %s is already %s.', broadcast_id, status)
                return 'redundant'
            if reasons & INVALID_TRANSITION_REASONS:
                return 'invalid'
            if reasons & STREAM_INACTIVE_REASONS:
                return 'inactive'
            raise

    async def drive_live(self, broadcast_id: str) -> str | None:
        """Take a bound, ingest-active broadcast from ready to live."""
        broadcast = await self.get_broadcast(broadcast_id)
        if broadcast is None:
            raise SetupProblem(f'Broadcast {broadcast_id} disappeared.')

        state = broadcast.get('status', {}).get('lifeCycleStatus')
        cd = broadcast.get('contentDetails', {}) or {}
        monitor_on = bool((cd.get('monitorStream') or {}).get('enableMonitorStream'))

        if state in LIVE_STATES:
            log.info('✅ Broadcast %s is already %s: '
                     'https://www.youtube.com/watch?v=%s',
                     broadcast_id, state, broadcast_id)
            self.state.created_went_live = True
            return state

        # A broadcast with autostart on cannot be transitioned manually --
        # YouTube answers every attempt with "invalidTransition".
        if cd.get('enableAutoStart') and DISABLE_AUTOSTART_BEFORE_TRANSITION:
            log.info('Broadcast %s has autostart enabled, which blocks manual '
                     'transitions; disabling it first.', broadcast_id)
            await self.set_autostart(broadcast, False)

        # With monitorStream on, ready -> live is illegal; ready -> testing -> live.
        if monitor_on and state == 'ready':
            log.info('Monitor stream is enabled; going via "testing".')
            for delay in (0, *TRANSITION_RETRY_DELAYS):
                if delay:
                    await asyncio.sleep(delay)
                outcome = await self._transition_once(broadcast_id, 'testing')
                if outcome in ('ok', 'redundant'):
                    break
                if outcome == 'inactive':
                    log.warning('Stream still inactive; retrying testing.')
                    continue
                log.warning('Testing transition rejected as invalid; '
                            'attempting live directly.')
                break
            await asyncio.sleep(5)

        for attempt, delay in enumerate((0, *TRANSITION_RETRY_DELAYS)):
            if delay:
                await asyncio.sleep(delay)

            outcome = await self._transition_once(broadcast_id, 'live')

            if outcome in ('ok', 'redundant'):
                broadcast = await self.get_broadcast(broadcast_id)
                state = (broadcast or {}).get('status', {}).get('lifeCycleStatus')
                if state in LIVE_STATES:
                    log.info('✅ Broadcast %s is %s: '
                             'https://www.youtube.com/watch?v=%s',
                             broadcast_id, state, broadcast_id)
                    self.state.created_went_live = True
                    return state
                log.warning('Transition reported success but state is "%s".', state)
                continue

            if outcome == 'inactive':
                log.warning('YouTube still reports the stream inactive '
                            '(attempt %d); retrying.', attempt + 1)
                continue

            # invalid: re-read and adapt rather than hammering the same call.
            broadcast = await self.get_broadcast(broadcast_id)
            state = (broadcast or {}).get('status', {}).get('lifeCycleStatus')
            cd = (broadcast or {}).get('contentDetails', {}) or {}
            log.warning('Transition to live rejected as invalid '
                        '(state=%s, autoStart=%s, monitor=%s).',
                        state, cd.get('enableAutoStart'),
                        (cd.get('monitorStream') or {}).get('enableMonitorStream'))

            if state in LIVE_STATES:
                self.state.created_went_live = True
                return state
            if cd.get('enableAutoStart'):
                await self.set_autostart(broadcast, False)
                continue
            if state == 'ready' and not monitor_on:
                monitor_on = True                 # try the testing route once
                outcome = await self._transition_once(broadcast_id, 'testing')
                if outcome in ('ok', 'redundant'):
                    await asyncio.sleep(5)
                    continue
            break

        log.error('❌ Could not bring %s live. Open https://studio.youtube.com '
                  'and press "Go live" manually, or run unstick.py.', broadcast_id)
        return state

    async def ensure_live(self, broadcast_id: str, stream_id: str) -> str | None:
        if not await self.wait_for_stream_active(
            stream_id, STREAM_ACTIVE_TIMEOUT_SECONDS
        ):
            raise SetupProblem(
                f'Broadcast {broadcast_id} is bound to stream {stream_id}, but '
                f'YouTube never received data on it. Confirm the OBS stream key '
                f'matches that stream.'
            )

        if BROADCAST_ENABLE_AUTO_START and AUTOSTART_GRACE_SECONDS:
            log.info('Ingest is live; giving autostart %ds to fire...',
                     AUTOSTART_GRACE_SECONDS)
            await asyncio.sleep(AUTOSTART_GRACE_SECONDS)

        return await self.drive_live(broadcast_id)

    # -- main entry point ---------------------------------------------------
    async def ensure_broadcast(self, title: str) -> tuple[str, str | None]:
        """Guarantee a broadcast exists carrying `title`.
        Returns (broadcast_id, bound_stream_id_or_None)."""
        title = title[:MAX_TITLE_LENGTH]
        self.state.refresh_day()

        # 1. Did we already create a broadcast for this exact title? Reuse it.
        existing_id = self.state.created_broadcast_for(title)
        if existing_id:
            broadcast = await self.get_broadcast(existing_id)
            state = (broadcast or {}).get('status', {}).get('lifeCycleStatus')
            if broadcast and state in (LIVE_STATES | READY_STATES):
                log.info('Reusing the broadcast we created earlier: %s (%s).',
                         existing_id, state)
                if state in LIVE_STATES:
                    self.state.created_went_live = True
                await self._ensure_title(broadcast, title)

                bound = broadcast.get('contentDetails', {}).get('boundStreamId')
                if not bound and state in READY_STATES:
                    stream_id = await self.resolve_stream_id()
                    await self.bind_broadcast(existing_id, stream_id)
                    bound = stream_id
                return existing_id, bound

            log.info('Previously created broadcast %s is gone/finished (%s).',
                     existing_id, state or 'missing')
            self.state.clear_created_broadcast()

        # 2. Is something already live? Rename it rather than making a duplicate.
        log.info('Checking for an existing live broadcast...')
        broadcast = await self.find_active_broadcast()
        if broadcast is not None:
            log.info('Found live broadcast %s (%s) "%s".',
                     broadcast['id'],
                     broadcast.get('status', {}).get('lifeCycleStatus'),
                     broadcast.get('snippet', {}).get('title', ''))
            await self._ensure_title(broadcast, title)
            return (broadcast['id'],
                    broadcast.get('contentDetails', {}).get('boundStreamId'))

        # 3. Nothing live: create one.
        if not CREATE_BROADCAST_IF_MISSING:
            raise RetryLater(0, 'no active broadcast and creation is disabled')

        log.info('No live broadcast found; creating one.')
        stream_id = await self.resolve_stream_id()
        broadcast = await self.create_broadcast(title)
        self.state.note_created_broadcast(broadcast['id'], title)
        self.state.broadcast_id = broadcast['id']
        self.state.remember_title(broadcast['id'], title)

        try:
            await self.bind_broadcast(broadcast['id'], stream_id)
        except HttpError as error:
            log.error('❌ Bind failed for %s: %s', broadcast['id'], error)
            raise

        return broadcast['id'], stream_id

    # -- ending / cleanup ---------------------------------------------------
    async def complete_broadcast(self) -> None:
        if not COMPLETE_BROADCAST_ON_OBS_STOP:
            return
        broadcast_id = self.state.created_broadcast_id
        if not broadcast_id or not self.state.created_went_live:
            return

        broadcast = await self.get_broadcast(broadcast_id)
        state = (broadcast or {}).get('status', {}).get('lifeCycleStatus')
        if state not in LIVE_STATES:
            log.info('Broadcast %s is already %s; nothing to complete.',
                     broadcast_id, state or 'gone')
            self.state.clear_created_broadcast()
            return

        outcome = await self._transition_once(broadcast_id, 'complete')
        if outcome in ('ok', 'redundant'):
            log.info('🏁 Broadcast %s completed.', broadcast_id)
        else:
            log.warning('Could not complete %s (%s); autostop should handle it.',
                        broadcast_id, outcome)
        self.state.clear_created_broadcast()

    async def cleanup_unused(self) -> None:
        """Delete a broadcast we created that never received any video."""
        if not DELETE_UNUSED_CREATED_BROADCAST:
            return
        broadcast_id = self.state.created_broadcast_id
        if not broadcast_id or self.state.created_went_live:
            return

        broadcast = await self.get_broadcast(broadcast_id)
        state = (broadcast or {}).get('status', {}).get('lifeCycleStatus')
        if state in READY_STATES:
            log.info('Broadcast %s never went live (%s); removing it.',
                     broadcast_id, state)
            await self.delete_broadcast(broadcast_id)
            self.state.clear_created_broadcast()
        elif state in LIVE_STATES:
            self.state.created_went_live = True


# ===========================================================================
# OBS EVENT LISTENER / ASYNC PIPELINE
# ===========================================================================
class TitleBot:
    def __init__(self):
        self.state = StateStore(STATE_FILE)
        self.manager: YouTubeBroadcastManager | None = None
        self.events: EventClient | None = None
        self.requests: ReqClient | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._queue: asyncio.Queue | None = None
        self._stop: asyncio.Event | None = None
        self._pending: set[asyncio.Task] = set()
        self._last_start_monotonic = 0.0
        self._feast_cache: dict[str, str | None] = {}

    # -- OBS callback: runs on the obsws background thread ------------------
    def on_stream_state_changed(self, data) -> None:
        state = getattr(data, 'output_state', '')

        if state == 'OBS_WEBSOCKET_OUTPUT_STOPPED':
            log.info('⏹️  Stream stopped.')
            if self._loop is not None:
                self._loop.call_soon_threadsafe(self._enqueue, ('stop', 0))
            return

        if not (getattr(data, 'output_active', False)
                and state == 'OBS_WEBSOCKET_OUTPUT_STARTED'):
            log.debug('Ignoring stream state %s.', state)
            return

        self.trigger_start()

    def trigger_start(self) -> None:
        now = time.monotonic()
        if now - self._last_start_monotonic < OBS_START_DEBOUNCE_SECONDS:
            log.info('Skipping duplicate stream-start event.')
            return
        self._last_start_monotonic = now

        log.info('🔴 Stream STARTED detected.')
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._enqueue, ('start', 0))

    def _enqueue(self, job: tuple[str, int]) -> None:
        try:
            self._queue.put_nowait(job)
        except asyncio.QueueFull:
            log.warning('Work queue full; dropping %s trigger.', job[0])

    # -- OBS requests (blocking; always call via to_thread) -----------------
    def _read_obs_stream_key(self) -> str | None:
        if self.requests is None:
            return None
        try:
            settings = self.requests.get_stream_service_settings()
            service = getattr(settings, 'stream_service_type', '?')
            values = getattr(settings, 'stream_service_settings', {}) or {}
            key = values.get('key')
            log.info('OBS stream service: %s, server=%s, key=%s',
                     service, values.get('server'), mask(key))
            if not key:
                log.warning(
                    'OBS reports no stream key. If it is using the account-linked '
                    'YouTube integration, switch to "Use Stream Key" so this bot '
                    'controls the broadcast.'
                )
            return key
        except Exception as exc:
            log.warning('Could not read the OBS stream settings: %s', exc)
            return None

    def _obs_is_streaming(self) -> bool:
        if self.requests is None:
            return False
        try:
            return bool(getattr(self.requests.get_stream_status(),
                                'output_active', False))
        except Exception as exc:
            log.warning('Could not read the OBS stream status: %s', exc)
            return False

    def _obs_ping(self) -> bool:
        if self.requests is None:
            return False
        try:
            self.requests.get_version()
            return True
        except Exception:
            return False

    # -- coptic.io feast lookup ----------------------------------------------
    async def _lookup_feast_title(self, now: datetime) -> str | None:
        if not COPTIC_CALENDAR_ENABLED:
            return None

        date_str = now.strftime('%Y-%m-%d')
        if date_str not in self._feast_cache:
            try:
                self._feast_cache = {
                    date_str: await asyncio.to_thread(resolve_feast_title, date_str)
                }
            except Exception as exc:
                log.warning('Coptic calendar lookup failed (%s); using the weekly schedule.', exc)
                return None

        raw_feast_title = self._feast_cache[date_str]
        feast_title = self.state.consume_feast_title(date_str, raw_feast_title)
        if feast_title:
            log.info('📅 Today is a feast day per coptic.io: "%s".', feast_title)
        elif raw_feast_title:
            log.info('📅 Feast title already used for an earlier stream today; '
                     'using the weekly schedule.')
        return feast_title

    # -- async worker -------------------------------------------------------
    async def _worker(self) -> None:
        while True:
            kind, attempt = await self._queue.get()
            try:
                if kind == 'start':
                    await self._handle_start(attempt)
                elif kind == 'stop':
                    await self._handle_stop()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception('Unexpected error handling the %s trigger.', kind)
            finally:
                self._queue.task_done()

    async def _resolve_broadcast(self) -> tuple[str, str | None]:
        """Use an operator-scheduled broadcast untouched if one exists;
        otherwise generate the weekly/feast title and create or rename one."""
        if PREFER_MANUAL_SCHEDULED_BROADCAST:
            manual = await self.manager.find_manual_broadcast()
            if manual is not None:
                log.info('📌 Using manually scheduled broadcast %s ("%s"); '
                         'skipping title generation.',
                         manual['id'], manual.get('snippet', {}).get('title', ''))
                stream_id = await self.manager.use_manual_broadcast(manual)
                return manual['id'], stream_id

        now = datetime.now()
        feast_title = await self._lookup_feast_title(now)
        title = generate_title(now, feast_title=feast_title)
        log.info('Target title: %s', title)
        return await self.manager.ensure_broadcast(title)

    async def _handle_start(self, attempt: int) -> None:
        if attempt == 0 and LOOKUP_INITIAL_DELAY_SECONDS:
            await asyncio.sleep(LOOKUP_INITIAL_DELAY_SECONDS)

        # Refresh the key each time: the operator may have changed it in OBS.
        self.manager.obs_stream_key = await asyncio.to_thread(self._read_obs_stream_key)

        try:
            broadcast_id, stream_id = await self._resolve_broadcast()
        except QuotaBlocked as exc:
            log.error('⏳ %s', exc)
            return
        except SetupProblem as exc:
            log.error('🛠️  %s', exc)
            return
        except RetryLater as exc:
            if attempt >= MAX_TRIGGER_RETRIES:
                log.error('❌ Giving up after %d attempts: %s', attempt + 1, exc)
                return
            delay = exc.delay or LOOKUP_RETRY_DELAYS[
                min(attempt, len(LOOKUP_RETRY_DELAYS) - 1)
            ]
            log.warning('⚠️  %s — retrying in %.0fs.', exc, delay)
            self._schedule(('start', attempt + 1), delay)
            return
        except HttpError as error:
            log.error('❌ YouTube API error (%s): %s', http_status(error), error)
            return

        if not stream_id:
            log.info('Broadcast %s has no bound stream to monitor; '
                     'skipping the go-live step.', broadcast_id)
            return

        try:
            await self.manager.ensure_live(broadcast_id, stream_id)
        except QuotaBlocked as exc:
            log.error('⏳ %s', exc)
        except SetupProblem as exc:
            log.error('🛠️  %s', exc)
        except RetryLater as exc:
            log.warning('⚠️  Go-live deferred: %s', exc)
        except HttpError as error:
            log.error('❌ Go-live failed (%s): %s', http_status(error), error)

    async def _handle_stop(self) -> None:
        try:
            await self.manager.complete_broadcast()
            await self.manager.cleanup_unused()
        except (QuotaBlocked, RetryLater, SetupProblem) as exc:
            log.warning('Stop handling skipped: %s', exc)
        except HttpError as error:
            log.warning('Stop handling failed (%s): %s', http_status(error), error)

    def _schedule(self, job: tuple[str, int], delay: float) -> None:
        async def _later() -> None:
            await asyncio.sleep(delay)
            self._enqueue(job)

        task = asyncio.create_task(_later(), name=f'{job[0]}-retry-{job[1]}')
        self._pending.add(task)
        task.add_done_callback(self._pending.discard)

    # -- OBS connection -----------------------------------------------------
    def _connect_obs(self) -> tuple[EventClient, ReqClient]:
        events = EventClient(
            host=OBS_HOST, port=OBS_PORT, password=OBS_PASSWORD,
            subs=Subs.LOW_VOLUME | Subs.OUTPUTS,
        )
        events.callback.register(self.on_stream_state_changed)
        requests = ReqClient(host=OBS_HOST, port=OBS_PORT, password=OBS_PASSWORD)
        return events, requests

    def _reconnect_obs(self) -> bool:
        for client in (self.events, self.requests):
            if client is not None:
                try:
                    client.disconnect()
                except Exception:
                    pass
        self.events = self.requests = None
        try:
            self.events, self.requests = self._connect_obs()
            return True
        except Exception as exc:
            log.warning('OBS reconnect failed: %s', exc)
            return False

    async def _watchdog(self) -> None:
        """Re-establish the OBS connection if obs-websocket restarts."""
        if not HEARTBEAT_SECONDS:
            return
        connected = True
        while True:
            await asyncio.sleep(HEARTBEAT_SECONDS)
            alive = await asyncio.to_thread(self._obs_ping)
            if alive:
                if not connected:
                    log.info('✅ OBS connection restored.')
                    connected = True
                    if TRIGGER_IF_ALREADY_STREAMING and \
                            await asyncio.to_thread(self._obs_is_streaming):
                        log.info('OBS is already streaming after reconnect.')
                        self.trigger_start()
                continue

            if connected:
                log.warning('⚠️  Lost the OBS connection; reconnecting...')
                connected = False
            if await asyncio.to_thread(self._reconnect_obs):
                log.info('Reconnected to OBS.')
                connected = True
                if TRIGGER_IF_ALREADY_STREAMING and \
                        await asyncio.to_thread(self._obs_is_streaming):
                    self.trigger_start()

    # -- lifecycle ----------------------------------------------------------
    def _request_stop(self) -> None:
        if self._stop and not self._stop.is_set():
            self._stop.set()

    def _install_signal_handlers(self) -> None:
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                self._loop.add_signal_handler(sig, self._request_stop)
            except (NotImplementedError, AttributeError, ValueError):
                try:
                    signal.signal(
                        sig,
                        lambda *_: self._loop.call_soon_threadsafe(self._request_stop),
                    )
                except (ValueError, OSError, AttributeError):
                    pass

    async def run(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._queue = asyncio.Queue(maxsize=8)
        self._stop = asyncio.Event()
        self._install_signal_handlers()

        for problem in validate_schedule():
            log.warning('Schedule problem: %s', problem)

        youtube = await asyncio.to_thread(get_youtube_client)
        self.manager = YouTubeBroadcastManager(youtube, self.state)

        self.events, self.requests = await asyncio.to_thread(self._connect_obs)
        log.info('Connected to OBS WebSocket at %s:%s.', OBS_HOST, OBS_PORT)
        await asyncio.to_thread(self._read_obs_stream_key)

        worker = asyncio.create_task(self._worker(), name='broadcast-worker')
        watchdog = asyncio.create_task(self._watchdog(), name='obs-watchdog')

        log.info('🤖 Bot running. Waiting for stream start...')
        log.info("   Preview title : '%s'", generate_title())
        log.info('   Quota today   : %d/%d units',
                 self.state.units_used, DAILY_QUOTA_BUDGET)
        log.info('   Broadcasts made today: %d/%d',
                 self.state.creations_today, MAX_BROADCASTS_PER_DAY)
        log.info('   Creation mode : %s, privacy=%s, autostart=%s, monitor=%s',
                 'ON' if CREATE_BROADCAST_IF_MISSING else 'OFF',
                 BROADCAST_PRIVACY, BROADCAST_ENABLE_AUTO_START,
                 BROADCAST_ENABLE_MONITOR)

        if TRIGGER_IF_ALREADY_STREAMING:
            if await asyncio.to_thread(self._obs_is_streaming):
                log.info('OBS is already streaming; triggering now.')
                self.trigger_start()

        try:
            while not self._stop.is_set():
                # Short waits keep Ctrl+C responsive on Windows.
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    pass
        finally:
            log.info('Stopping bot...')
            for task in (worker, watchdog, *self._pending):
                task.cancel()
            await asyncio.gather(worker, watchdog, *self._pending,
                                 return_exceptions=True)
            for client, name in ((self.events, 'event'), (self.requests, 'request')):
                if client is not None:
                    try:
                        await asyncio.to_thread(client.disconnect)
                    except Exception as exc:
                        log.warning('Error disconnecting the OBS %s client: %s',
                                    name, exc)
            log.info('Disconnected.')


# ===========================================================================
# LOGGING SETUP
# ===========================================================================
def setup_logging() -> None:
    """Console + daily-rotating file logging under <script dir>/logs."""
    os.makedirs(LOG_DIR, exist_ok=True)

    formatter = logging.Formatter(
        '%(asctime)s %(levelname)-7s [%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    root = logging.getLogger()
    root.setLevel(min(CONSOLE_LOG_LEVEL, FILE_LOG_LEVEL))
    for handler in list(root.handlers):     # avoid duplicate handlers on re-init
        root.removeHandler(handler)
        handler.close()

    file_handler = TimedRotatingFileHandler(
        LOG_FILE, when='midnight', interval=1,
        backupCount=LOG_RETENTION_DAYS, encoding='utf-8', delay=False,
    )
    file_handler.suffix = '%Y-%m-%d'
    file_handler.setLevel(FILE_LOG_LEVEL)
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    # The titles contain Arabic and emoji; force UTF-8 on the console.
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding='utf-8', errors='replace')
        except (AttributeError, ValueError):
            pass

    console = logging.StreamHandler(stream=sys.stdout)
    console.setLevel(CONSOLE_LOG_LEVEL)
    console.setFormatter(formatter)
    root.addHandler(console)

    for noisy, level in (
        ('googleapiclient', logging.ERROR),
        ('googleapiclient.discovery_cache', logging.ERROR),
        ('google_auth_oauthlib', logging.WARNING),
        ('google.auth', logging.WARNING),
        ('urllib3', logging.WARNING),
        ('asyncio', logging.WARNING),
        ('obsws_python', logging.INFO),
    ):
        logging.getLogger(noisy).setLevel(level)

    # Make sure crashes land in the file, including ones on the obsws thread.
    def _excepthook(exc_type, exc, tb):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc, tb)
            return
        logging.getLogger('titlebot').critical(
            'Uncaught exception', exc_info=(exc_type, exc, tb)
        )

    sys.excepthook = _excepthook
    if hasattr(threading, 'excepthook'):
        threading.excepthook = lambda args: _excepthook(
            args.exc_type, args.exc_value, args.exc_traceback
        )


def main() -> None:
    setup_logging()
    log.info('=' * 60)
    log.info('TitleBot starting (pid %d)', os.getpid())
    log.info('Script dir: %s', SCRIPT_DIR)
    log.info('Log file:   %s', LOG_FILE)
    try:
        asyncio.run(TitleBot().run())
    except KeyboardInterrupt:
        log.info('Interrupted by user.')
    except Exception:
        log.exception('Fatal error; exiting.')
        raise
    finally:
        log.info('TitleBot stopped.')
        logging.shutdown()


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='OBS -> YouTube live-broadcast bot.')
    subparsers = parser.add_subparsers(dest='command')

    preview = subparsers.add_parser(
        'preview', help='Print the title(s) that would be generated and exit (no OBS/YouTube).'
    )
    preview.add_argument(
        'dates', nargs='*',
        help="Date (YYYY-MM-DD) or datetime (YYYY-MM-DD HH:MM / YYYY-MM-DDTHH:MM). "
             "Defaults to now."
    )
    preview.add_argument(
        '--range', nargs=2, metavar=('START', 'END'),
        help='Preview every day (YYYY-MM-DD) from START to END inclusive.'
    )
    preview.add_argument(
        '--no-coptic', action='store_true',
        help='Skip the coptic.io feast lookup (offline schedule only).'
    )
    return parser


if __name__ == '__main__':
    args = _build_arg_parser().parse_args()
    if args.command == 'preview':
        use_coptic = not args.no_coptic
        if args.range:
            preview_range(args.range[0], args.range[1], use_coptic=use_coptic)
        dates = args.dates or ([] if args.range else [datetime.now().strftime('%Y-%m-%d %H:%M')])
        if dates:
            preview_titles(dates, use_coptic=use_coptic)
    else:
        main()