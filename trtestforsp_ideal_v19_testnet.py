import sqlite3
import telebot
from flask import Flask, request
import threading
import json
from binance.um_futures import UMFutures
import hmac
import hashlib
import os
import requests
from requests.adapters import HTTPAdapter
from urllib.parse import urlencode
import math
import time
import random
import logging
from binance.exceptions import BinanceAPIException
from contextlib import contextmanager
import re
import websocket
import uuid

# ---------------------------------------------------------------------------
# Binance environment: mainnet vs futures testnet
# Set BINANCE_TESTNET=1 (or BINANCE_ENV=testnet) to switch ALL REST/WS endpoints.
# You MUST use separate API keys created on the Futures Testnet.
# ---------------------------------------------------------------------------

def _env_truthy(name: str, default: bool = False) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on", "testnet")

_BINANCE_ENV = (os.environ.get("BINANCE_ENV") or "").strip().lower()
BINANCE_TESTNET = _env_truthy("BINANCE_TESTNET") or (_BINANCE_ENV in ("testnet", "demo", "paper", "sandbox"))

_DEFAULT_FAPI_REST_BASE = "https://testnet.binancefuture.com" if BINANCE_TESTNET else "https://fapi.binance.com"
_DEFAULT_FAPI_WS_BASE = "wss://stream.binancefuture.com" if BINANCE_TESTNET else "wss://fstream.binance.com"
_DEFAULT_WS_TRADING_URL = "wss://testnet.binancefuture.com/ws-fapi/v1" if BINANCE_TESTNET else "wss://ws-fapi.binance.com/ws-fapi/v1"

# Allow overrides via env (useful if Binance changes endpoints)
_BINANCE_FAPI_REST_BASE = (os.environ.get("BINANCE_FAPI_BASE_URL") or _DEFAULT_FAPI_REST_BASE).rstrip("/")
_BINANCE_FAPI_WS_BASE = (os.environ.get("BINANCE_FAPI_WS_BASE_URL") or _DEFAULT_FAPI_WS_BASE).rstrip("/")

_WS_TRADING_URL = os.environ.get("BINANCE_WS_TRADING_URL", _DEFAULT_WS_TRADING_URL)
_WS_TRADING_RECV_WINDOW = int(os.environ.get("BINANCE_WS_RECV_WINDOW", "5000"))

def _gen_client_order_id(prefix: str, symbol: str) -> str:
    try:
        prefix = (prefix or 'o')[:3]
        symbol = (symbol or '').upper()
        base = f"{prefix}_{symbol}_".replace(':','_').replace('/','_')
        suf = uuid.uuid4().hex[:16]
        oid = (base + suf)[:36]
        oid = re.sub(r'[^\.A-Z\:/a-z0-9_-]', '_', oid)
        if not oid:
            oid = uuid.uuid4().hex[:32]
        return oid[:36]
    except Exception:
        return uuid.uuid4().hex[:36]
class WsAPIException(Exception):
    def __init__(self, status: int, code, msg: str, raw=None):
        super().__init__(f"WS_API_ERROR status={status} code={code} msg={msg}")
        self.status = status
        self.code = code
        self.msg = msg
        self.raw = raw or {}
def _ws_bool(v):
    if isinstance(v, bool):
        return "true" if v else "false"
    return v
def _ws_upper_bool(v):
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    return v
def _ws_clean_params(params: dict) -> dict:
    out = {}
    for k, v in params.items():
        if v is None:
            continue
        if k in ("closePosition", "reduceOnly"):
            out[k] = _ws_bool(v)
        elif k in ("priceProtect",):
            out[k] = _ws_upper_bool(v)
        else:
            out[k] = v
    for k in ("quantity","price","stopPrice","activationPrice","callbackRate","triggerPrice","triggerprice"):
        if k in out and isinstance(out[k], (int, float)):
            out[k] = str(out[k])
    return out

class FuturesWSTradingSession:
    def __init__(self, api_key: str, secret_key: str, url: str = _WS_TRADING_URL, recv_window: int = _WS_TRADING_RECV_WINDOW):
        self.api_key = api_key
        self.secret_key = secret_key
        self.url = url
        self.recv_window = recv_window
        self._wsapp = None
        self._thread = None
        self._watchdog_thread = None
        self._connected = threading.Event()
        self._send_lock = threading.Lock()
        self._pending = {}
        self._pending_lock = threading.Lock()
        self._stop = threading.Event()
        self._conn_open_ts = 0.0
        self._last_frame_ts = 0.0
        self._reconnect_req = threading.Event()
    def _sign(self, params: dict) -> str:
        payload_items = [(k, params[k]) for k in sorted(params.keys()) if k != "signature"]
        query = urlencode(payload_items, doseq=True)
        return hmac.new(self.secret_key.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
    def _mark_frame(self):
        now = time.time()
        self._last_frame_ts = now
        if not self._conn_open_ts:
            self._conn_open_ts = now
    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._reconnect_req.clear()
        self._connected.clear()
        self._conn_open_ts = 0.0
        self._last_frame_ts = 0.0
        def run():
            while not self._stop.is_set():
                self._connected.clear()
                self._conn_open_ts = 0.0
                self._last_frame_ts = 0.0
                def on_open(ws):
                    self._conn_open_ts = time.time()
                    self._last_frame_ts = self._conn_open_ts
                    self._connected.set()
                def on_message(ws, message):
                    self._mark_frame()
                    try:
                        data = json.loads(message)
                    except Exception:
                        return
                    req_id = data.get("id")
                    if not req_id:
                        return
                    with self._pending_lock:
                        entry = self._pending.get(req_id)
                    if entry:
                        ev, holder = entry
                        holder["data"] = data
                        ev.set()
                def on_error(ws, error):
                    self._mark_frame()
                def on_close(ws, status_code, msg):
                    self._connected.clear()
                def on_ping(ws, payload):
                    self._mark_frame()
                    try:
                        ws.send(payload, opcode=websocket.ABNF.OPCODE_PONG)
                    except Exception:
                        pass
                def on_pong(ws, payload):
                    self._mark_frame()
                self._wsapp = websocket.WebSocketApp(
                    self.url,
                    on_open=on_open,
                    on_message=on_message,
                    on_error=on_error,
                    on_close=on_close,
                    on_ping=on_ping,
                    on_pong=on_pong,
                )
                try:
                    self._wsapp.run_forever(ping_interval=30, ping_timeout=10)
                except Exception:
                    pass
                self._connected.clear()
                if self._stop.is_set():
                    break
                time.sleep(1.0)
            self._connected.clear()
        self._thread = threading.Thread(target=run, daemon=True, name=f"WS_TRADE_{self.api_key[:6]}")
        self._thread.start()
        def watchdog():
            max_age = 23 * 60 * 60
            max_silence = 10 * 60
            while not self._stop.is_set():
                time.sleep(5.0)
                if not self.is_connected():
                    continue
                now = time.time()
                age = now - float(self._conn_open_ts or now)
                silence = now - float(self._last_frame_ts or now)
                if age > max_age or silence > max_silence or self._reconnect_req.is_set():
                    self._reconnect_req.clear()
                    try:
                        if self._wsapp:
                            self._wsapp.close()
                    except Exception:
                        pass
        self._watchdog_thread = threading.Thread(target=watchdog, daemon=True, name=f"WS_TRADE_WD_{self.api_key[:6]}")
        self._watchdog_thread.start()
        self._connected.wait(timeout=3.0)
    def request_reconnect(self):
        self._reconnect_req.set()
    def stop(self):
        self._stop.set()
        try:
            if self._wsapp:
                self._wsapp.close()
        except Exception:
            pass
        self._connected.clear()
    def is_connected(self) -> bool:
        return self._connected.is_set()
    def request(self, method: str, params: dict, timeout: float = 4.0) -> dict:
        self.start()
        if not self.is_connected():
            raise WsAPIException(status=0, code=None, msg="WS not connected", raw={})
        req_id = str(uuid.uuid4())
        params = _ws_clean_params(params)
        params["apiKey"] = self.api_key
        params["timestamp"] = binance_now_ms()
        params.setdefault("recvWindow", self.recv_window)
        params["signature"] = self._sign(params)
        payload = {"id": req_id, "method": method, "params": params}
        ev = threading.Event()
        holder = {}
        with self._pending_lock:
            self._pending[req_id] = (ev, holder)
        try:
            with self._send_lock:
                self._wsapp.send(json.dumps(payload, separators=(",", ":")))
        except Exception as e:
            with self._pending_lock:
                self._pending.pop(req_id, None)
            raise WsAPIException(status=0, code=None, msg=f"send failed: {e}", raw={})
        if not ev.wait(timeout=timeout):
            with self._pending_lock:
                self._pending.pop(req_id, None)
            raise WsAPIException(status=0, code=None, msg="timeout waiting WS response", raw={})
        with self._pending_lock:
            self._pending.pop(req_id, None)
        data = holder.get("data") or {}
        status = int(data.get("status") or 0)
        if status != 200:
            err = data.get("error") or {}
            raise WsAPIException(status=status, code=err.get("code"), msg=str(err.get("msg") or data), raw=data)
        return data.get("result") or data

_WS_TRADING_SESSIONS = {}
_WS_TRADING_SESSIONS_LOCK = threading.Lock()
def get_ws_trading_session(api_key: str, secret_key: str) -> FuturesWSTradingSession:
    k = (api_key, secret_key, _WS_TRADING_URL, _WS_TRADING_RECV_WINDOW)
    with _WS_TRADING_SESSIONS_LOCK:
        s = _WS_TRADING_SESSIONS.get(k)
        if s is None:
            s = FuturesWSTradingSession(api_key, secret_key)
            _WS_TRADING_SESSIONS[k] = s
    return s
_REST_CLIENT_CACHE = {}
_REST_CLIENT_LOCK = threading.Lock()
def get_rest_client(api_key: str, secret_key: str):
    base_url = _BINANCE_FAPI_REST_BASE
    # python-binance futures connector can accept base_url (testnet/mainnet)
    try:
        if api_key and secret_key:
            return UMFutures(key=api_key, secret=secret_key, base_url=base_url)
        return UMFutures(base_url=base_url)
    except TypeError:
        # Older connector versions may not support base_url; fall back to default
        if api_key and secret_key:
            return UMFutures(key=api_key, secret=secret_key)
        return UMFutures()

class TradingClient:
    def __init__(self, api_key: str, secret_key: str):
        self.api_key = api_key
        self.secret_key = secret_key
        self.rest = get_rest_client(api_key, secret_key)
        self.ws = get_ws_trading_session(api_key, secret_key)
    def _should_recover(self, e: Exception) -> bool:
        msg = str(e).lower()
        if isinstance(e, WsAPIException):
            return e.status == 0 or "timeout waiting ws response" in msg or "ws not connected" in msg
        if isinstance(e, BinanceAPIException):
            try:
                sc = int(getattr(e, "status_code", 0) or 0)
            except Exception:
                sc = 0
            if sc in (502, 503, 504):
                return True
            try:
                code = int(getattr(e, "code", 0) or 0)
            except Exception:
                code = 0
            if code in (-1007, -1008, -1001, -1021):
                return True
        if "service unavailable" in msg or "temporarily unavailable" in msg or "execution status unknown" in msg or "unknown error" in msg:
            return True
        if "timed out" in msg or "timeout" in msg or "connection reset" in msg:
            return True
        return False
    def new_order(self, **kwargs):
        if "newClientOrderId" not in kwargs or not kwargs.get("newClientOrderId"):
            try:
                sym = (kwargs.get("symbol") or "").upper()
                pfx = "o"
                ot = (kwargs.get("type") or "").upper()
                if ot in ("MARKET",):
                    pfx = "e"
                elif ot in ("LIMIT",):
                    pfx = "l"
                kwargs["newClientOrderId"] = _gen_client_order_id(pfx, sym)
            except Exception:
                pass
        coid = kwargs.get("newClientOrderId")
        symbol = kwargs.get("symbol")
        try:
            return self.ws.request("order.place", kwargs, timeout=4.0)
        except WsAPIException as e:
            if e.status and e.code is not None:
                raise
            if coid and symbol and self._should_recover(e):
                try:
                    time.sleep(0.05)
                    q = self.query_order(symbol=symbol, origClientOrderId=coid)
                    if isinstance(q, dict) and (q.get("orderId") or q.get("clientOrderId")):
                        return q
                except Exception:
                    pass
        except Exception as e:
            if coid and symbol and self._should_recover(e):
                try:
                    time.sleep(0.05)
                    q = self.query_order(symbol=symbol, origClientOrderId=coid)
                    if isinstance(q, dict) and (q.get("orderId") or q.get("clientOrderId")):
                        return q
                except Exception:
                    pass
        try:
            kwargs.setdefault("recvWindow", 60000)
            kwargs.setdefault("timestamp", binance_now_ms())
            return self.rest.new_order(**kwargs)
        except Exception as e:
            if coid and symbol and self._should_recover(e):
                try:
                    time.sleep(0.1)
                    q = self.rest.query_order(symbol=symbol, origClientOrderId=coid, recvWindow=60000, timestamp=binance_now_ms())
                    if isinstance(q, dict) and (q.get("orderId") or q.get("clientOrderId")):
                        return q
                except Exception:
                    pass
            raise
    def cancel_order(self, **kwargs):
        try:
            return self.ws.request("order.cancel", kwargs, timeout=4.0)
        except WsAPIException as e:
            if e.status and e.code is not None:
                raise
        except Exception:
            pass
        kwargs.setdefault("recvWindow", 60000)
        kwargs.setdefault("timestamp", binance_now_ms())
        return self.rest.cancel_order(**kwargs)
    def query_order(self, **kwargs):
        try:
            return self.ws.request("order.status", kwargs, timeout=4.0)
        except WsAPIException as e:
            if e.status and e.code is not None:
                raise
        except Exception:
            pass
        kwargs.setdefault("recvWindow", 60000)
        kwargs.setdefault("timestamp", binance_now_ms())
        return self.rest.query_order(**kwargs)
    def modify_order(self, **kwargs):
        try:
            return self.ws.request("order.modify", kwargs, timeout=4.0)
        except WsAPIException as e:
            if e.status and e.code is not None:
                raise
        except Exception:
            pass
        kwargs.setdefault("recvWindow", 60000)
        kwargs.setdefault("timestamp", binance_now_ms())
        return self.rest.modify_order(**kwargs)
    def algo_order_place(self, **kwargs):
        try:
            return self.ws.request("algoOrder.place", kwargs, timeout=4.0)
        except WsAPIException as e:
            if e.status and e.code is not None:
                raise
        except Exception:
            return None
    def algo_order_cancel(self, **kwargs):
        try:
            return self.ws.request("algoOrder.cancel", kwargs, timeout=4.0)
        except WsAPIException as e:
            if e.status and e.code is not None:
                raise
        except Exception:
            return None
    def __getattr__(self, item):
        return getattr(self.rest, item)
_TRADING_CLIENT_CACHE = {}
_TRADING_CLIENT_LOCK = threading.Lock()
def get_trading_client(api_key: str, secret_key: str) -> TradingClient:
    k = (api_key, secret_key)
    with _TRADING_CLIENT_LOCK:
        c = _TRADING_CLIENT_CACHE.get(k)
        if c is None:
            c = TradingClient(api_key, secret_key)
            _TRADING_CLIENT_CACHE[k] = c
    return c

_thread_local = threading.local()
def _get_http_session() -> requests.Session:
    """Per-thread requests.Session with connection pooling."""
    s = getattr(_thread_local, "session", None)
    if s is None:
        s = requests.Session()
        adapter = HTTPAdapter(pool_connections=50, pool_maxsize=50, max_retries=0, pool_block=False)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        _thread_local.session = s
    return s
def _http_request(method: str, url: str, **kwargs):
    """Thin wrapper to ensure session reuse."""
    s = _get_http_session()
    return s.request(method=method, url=url, **kwargs)
# _BINANCE_FAPI_REST_BASE is defined near the top (mainnet/testnet switch)
class BinanceTimeSync:
    def __init__(self, base_rest: str):
        self.base_rest = (base_rest or _BINANCE_FAPI_REST_BASE).rstrip("/")
        self._lock = threading.Lock()
        self._offset_ms = 0
        self._last_sync_wall = 0.0
    def now_ms(self) -> int:
        self.maybe_sync()
        return int(time.time() * 1000) + int(self._offset_ms)
    def maybe_sync(self, max_age_sec: float = 300.0):
        try:
            if (time.time() - float(self._last_sync_wall or 0.0)) < float(max_age_sec):
                return
        except Exception:
            pass
        self.sync()
    def sync(self):
        with self._lock:
            try:
                if (time.time() - float(self._last_sync_wall or 0.0)) < 2.0:
                    return
            except Exception:
                pass
            try:
                t0 = time.time()
                r = _http_request("GET", self.base_rest + "/fapi/v1/time", timeout=5)
                t1 = time.time()
                try:
                    j = r.json()
                except Exception:
                    j = {}
                st = j.get("serverTime")
                if st is None:
                    return
                mid_local_ms = int(((t0 + t1) / 2.0) * 1000)
                self._offset_ms = int(st) - int(mid_local_ms)
                self._last_sync_wall = time.time()
            except Exception:
                pass
_TIME_SYNC = BinanceTimeSync(_BINANCE_FAPI_REST_BASE)
def binance_now_ms() -> int:
    return _TIME_SYNC.now_ms()
def force_time_sync():
    try:
        _TIME_SYNC.sync()
    except Exception:
        pass


def _now_iso():
    try:
        return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
    except Exception:
        try:
            return str(int(time.time()))
        except Exception:
            return "0"
def safe_binance_call(func, *args, retries=3, delay=1, **kwargs):
    for attempt in range(retries):
        try:
            return func(*args, **kwargs)
        except BinanceAPIException as e:
            if e.code == -1007:
                print(f'[WARN] Binance timeout, retry {attempt + 1}/{retries}')
                time.sleep(delay)
                continue
            raise
        except Exception as e:
            if 'Read timed out' in str(e) or 'timed out' in str(e).lower():
                print(f'[WARN] Network timeout, retry {attempt + 1}/{retries}')
                time.sleep(delay)
                continue
            raise
    raise Exception('Binance API timeout after retries')
def _extract_order_id(obj):
    if obj is None:
        return None
    if isinstance(obj, dict):
        for key in ('orderId', 'order_id', 'id'):
            if key in obj:
                return obj.get(key)
        return None
    if isinstance(obj, (int, float)):
        return obj
    if isinstance(obj, str):
        try:
            parsed = json.loads(obj)
            if isinstance(parsed, dict):
                for key in ('orderId', 'order_id', 'id'):
                    if key in parsed:
                        return parsed.get(key)
        except Exception:
            pass
        return obj
    return None
app = Flask(__name__)
bot = telebot.TeleBot(os.environ.get('TELEGRAM_BOT_TOKEN', '7680871680:AAHPx1Mf6viK9z0ByqUuzrHtF2htUOYeqhQ'))
secret_key = []
api_key = []
conn = sqlite3.connect('users1.db', check_same_thread=False)
try:
    conn.execute('PRAGMA journal_mode=WAL;')
    conn.execute('PRAGMA synchronous=NORMAL;')
    conn.execute('PRAGMA temp_store=MEMORY;')
    conn.execute('PRAGMA foreign_keys=ON;')
    conn.execute('PRAGMA busy_timeout=5000;')
except Exception:
    pass
cursor = conn.cursor()
cursor.execute('\nCREATE TABLE IF NOT EXISTS users (\n    user_id INTEGER PRIMARY KEY,\n    api_key TEXT,\n    secret_key TEXT,\n    balance REAL,\n    last_balance REAL,\n    initial_balance REAL,\n    is_active BOOLEAN DEFAULT 1,\n    last_deactivation INTEGER\n)\n')
cursor.execute('\nCREATE TABLE IF NOT EXISTS positions_history (\n    id INTEGER PRIMARY KEY AUTOINCREMENT,\n    symbol TEXT,\n    user_id INTEGER,\n    direction TEXT,\n    entry_price REAL,\n    exit_price REAL,\n    pnl REAL,\n    closed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n    details TEXT \n)\n')
cursor.execute('\nCREATE TABLE IF NOT EXISTS trading_pairs (\n    symbol TEXT PRIMARY KEY,\n    is_active BOOLEAN DEFAULT 1,\n    current_loss REAL DEFAULT 0.0\n)\n')
cursor.execute('\nCREATE TABLE IF NOT EXISTS balance_history (\n    id INTEGER PRIMARY KEY AUTOINCREMENT,\n    user_id INTEGER NOT NULL,\n    balance REAL NOT NULL,\n    timestamp INTEGER NOT NULL\n)\n')
cursor.execute('\nCREATE TABLE IF NOT EXISTS global_loss (\n    id INTEGER PRIMARY KEY,\n    total_loss REAL DEFAULT 0.0\n)\n')
cursor.execute('\nCREATE TABLE IF NOT EXISTS positions (\n    symbol TEXT PRIMARY KEY,\n    user_id INTEGER NOT NULL,\n    direction TEXT NOT NULL,\n    entry_price REAL NOT NULL,\n    current_entry_price REAL NOT NULL,\n    quantity REAL NOT NULL,\n    initial_quantity REAL,\n    stop_price REAL,\n    stop_order_id TEXT,\n    entry_order_id TEXT,\n    tp_order_ids TEXT,            \n    tp_data TEXT,           \n    metadata TEXT,           \n    stop_loss_moved INTEGER DEFAULT 0,\n    first_tp_filled INTEGER DEFAULT 0,\n    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n    recovery_order_id TEXT DEFAULT NULL\n)\n')
cursor.execute('\nCREATE TABLE IF NOT EXISTS signals (\n    id INTEGER PRIMARY KEY AUTOINCREMENT,\n    symbol TEXT,\n    direction TEXT,\n    entry_price REAL,\n    stop_price REAL,\n    tp_levels TEXT,\n    user_id INTEGER,\n    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,\n    raw_json TEXT\n)\n')
cursor.execute('\nCREATE TABLE IF NOT EXISTS symbol_cooldowns (\n    symbol TEXT PRIMARY KEY,\n    signals_missed INTEGER DEFAULT 0,\n    consecutive_losses INTEGER DEFAULT 0\n);')
cursor.execute('PRAGMA table_info(symbol_cooldowns)')
cols_info = cursor.fetchall()
cols = [c[1] for c in cols_info]
if 'consecutive_losses' not in cols:
    try:
        cursor.execute('ALTER TABLE symbol_cooldowns ADD COLUMN consecutive_losses INTEGER DEFAULT 0')
        conn.commit()
    except Exception as e:
        print(f'migration error: {e}')
def _ensure_columns(table, col_defs):
    try:
        with DB_LOCK:
            cursor.execute(f"PRAGMA table_info({table})")
            existing = {row[1] for row in cursor.fetchall()}
            for col, ddl in col_defs.items():
                if col not in existing:
                    try:
                        cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl}")
                    except Exception as e:
                        print(f"[migration] failed to add {table}.{col}: {e}")
            conn.commit()
    except Exception as e:
        print(f"[migration] error ensuring columns for {table}: {e}")
_ensure_columns("positions", {
    "add_count": "INTEGER DEFAULT 0",
    "signals_missed_after_stop": "INTEGER DEFAULT 0"
})
try:
    cursor.execute('''CREATE TABLE IF NOT EXISTS signal_dedup (
        symbol TEXT NOT NULL,
        fp TEXT NOT NULL,
        last_ts INTEGER NOT NULL,
        PRIMARY KEY(symbol, fp)
    )''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_signal_dedup_last_ts ON signal_dedup(last_ts);')
    conn.commit()
except Exception as e:
    print(f"[migration] signal_dedup create failed: {e}")
cursor.execute('CREATE INDEX IF NOT EXISTS idx_positions_user ON positions(user_id);')
cursor.execute('SELECT COUNT(*) FROM global_loss')
if cursor.fetchone()[0] == 0:
    cursor.execute('INSERT INTO global_loss (id, total_loss) VALUES (1, 0.0)')
conn.commit()
initial_pairs = ['KAITOUSDT', 'APTUSDT', 'AVAXUSDT', 'ATOMUSDT', 'ENAUSDT', 'SOLUSDT', 'DOGEUSDT', 'UNIUSDT', 'LTCUSDT', 'DOTUSDT', 'XRPUSDT', 'BTCUSDT', 'ALGOUSDT']
for pair in initial_pairs:
    cursor.execute('INSERT OR IGNORE INTO trading_pairs (symbol) VALUES (?)', (pair,))
conn.commit()
LEVERAGE = 20
VOLUME_PERCENT = 0.03
active_orders = {}
active_trailing_threads = {}
BTC_VOLUME_PERCENT = 0.06
ADMINS = [1901059519, 6189545928]
active_trailing_threads_lock = threading.Lock()
DB_LOCK = threading.Lock()
@contextmanager
def db_tx():
    with DB_LOCK:
        try:
            conn.execute("BEGIN")
        except Exception:
            pass
        try:
            yield conn
            try:
                conn.commit()
            except Exception:
                pass
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
USER_STREAMS = {}
USER_STREAMS_LOCK = threading.Lock()
class UserDataStreamManager(threading.Thread):
    def __init__(self, user_id, api_key, secret_key, base_rest=_BINANCE_FAPI_REST_BASE, base_ws=None):
        super().__init__(daemon=True)
        self.user_id = int(user_id)
        self.api_key = api_key
        self.secret_key = secret_key
        self.base_rest = base_rest
        base_ws = (base_ws or _BINANCE_FAPI_WS_BASE)
        self.base_ws = str(base_ws).rstrip('/')
        self.listen_key = None
        self._stop = threading.Event()
        self._ws_app = None
        self._keepalive_thread = None
        self._watchdog_thread = None
        self.is_connected = False
        self.last_msg_ts = 0.0
        self._conn_open_ts = 0.0
        self.last_error = None
    def stop(self):
        self._stop.set()
        try:
            self.is_connected = False
        except Exception:
            pass
        try:
            if self._ws_app:
                self._ws_app.close()
        except Exception:
            pass
    def _create_listen_key(self):
        url = self.base_rest.rstrip('/') + '/fapi/v1/listenKey'
        headers = {'X-MBX-APIKEY': self.api_key}
        r = _http_request('POST', url, headers=headers, timeout=10)
        r.raise_for_status()
        j = r.json()
        return j.get('listenKey')
    def _keepalive_loop(self):
        url = self.base_rest.rstrip('/') + '/fapi/v1/listenKey'
        headers = {'X-MBX-APIKEY': self.api_key}
        while not self._stop.is_set():
            for _ in range(25 * 60):
                if self._stop.is_set():
                    return
                time.sleep(1)
            try:
                if not self.listen_key:
                    continue
                r = _http_request('PUT', url, headers=headers, params={'listenKey': self.listen_key}, timeout=10)
                if r.status_code != 200:
                    try:
                        j = r.json()
                    except Exception:
                        j = {}
                    if isinstance(j, dict) and j.get('code') == -1125:
                        self.listen_key = self._create_listen_key()
                        try:
                            if self._ws_app:
                                self._ws_app.close()
                        except Exception:
                            pass
            except Exception:
                pass
    def _dispatch(self, event):
        try:
            if not isinstance(event, dict):
                return
            et = event.get('e')
            sym = None
            if et in ('ORDER_TRADE_UPDATE', 'ALGO_UPDATE') and isinstance(event.get('o'), dict):
                sym = event['o'].get('s')
            elif et == 'TRADE_LITE':
                sym = event.get('s')
            with active_trailing_threads_lock:
                if et == 'ACCOUNT_UPDATE':
                    for th in list(active_trailing_threads.values()):
                        try:
                            if getattr(th, 'user_id', None) == self.user_id:
                                th.notify_user_stream_event(event)
                        except Exception:
                            pass
                else:
                    if sym:
                        th = active_trailing_threads.get(sym)
                        if th and getattr(th, 'user_id', None) == self.user_id:
                            th.notify_user_stream_event(event)
        except Exception:
            pass
    def _watchdog_loop(self):
        max_age = 23 * 60 * 60
        max_silence = 10 * 60
        while not self._stop.is_set():
            time.sleep(5.0)
            try:
                if not self.is_connected:
                    continue
                now = time.time()
                age = now - float(self._conn_open_ts or now)
                silence = now - float(self.last_msg_ts or now)
                if age > max_age or silence > max_silence:
                    try:
                        if self._ws_app:
                            self._ws_app.close()
                    except Exception:
                        pass
            except Exception:
                pass
    def run(self):
        if not self._watchdog_thread or not self._watchdog_thread.is_alive():
            self._watchdog_thread = threading.Thread(target=self._watchdog_loop, daemon=True)
            self._watchdog_thread.start()
        while not self._stop.is_set():
            try:
                if not self.listen_key:
                    self.listen_key = self._create_listen_key()
                if not self.listen_key:
                    time.sleep(5)
                    continue
                if not self._keepalive_thread or not self._keepalive_thread.is_alive():
                    self._keepalive_thread = threading.Thread(target=self._keepalive_loop, daemon=True)
                    self._keepalive_thread.start()
                ws_url = f"{self.base_ws.rstrip('/')}/ws/{self.listen_key}"
                def on_message(ws, message):
                    try:
                        data = json.loads(message)
                    except Exception:
                        return
                    try:
                        self.last_msg_ts = time.time()
                    except Exception:
                        pass
                    self._dispatch(data)
                def on_error(ws, error):
                    try:
                        self.last_error = str(error)
                        self.is_connected = False
                    except Exception:
                        pass
                def on_close(ws, status_code, msg):
                    try:
                        self.is_connected = False
                    except Exception:
                        pass
                def on_open(ws):
                    try:
                        self.is_connected = True
                        self._conn_open_ts = time.time()
                        self.last_msg_ts = self._conn_open_ts
                    except Exception:
                        pass
                def on_ping(ws, message):
                    try:
                        self.last_msg_ts = time.time()
                    except Exception:
                        pass
                def on_pong(ws, message):
                    self.last_ws_message_ts = time.time()
                self._ws_app = websocket.WebSocketApp(
                    ws_url,
                    on_message=on_message,
                    on_error=on_error,
                    on_close=on_close,
                    on_open=on_open,
                    on_ping=on_ping,
                    on_pong=on_pong,
                )
                self._ws_app.run_forever(ping_interval=30, ping_timeout=20)
            except Exception:
                pass
            time.sleep(3)

def ensure_user_stream(user_id, api_key, secret_key):
    user_id = int(user_id)
    with USER_STREAMS_LOCK:
        mgr = USER_STREAMS.get(user_id)
        if mgr and mgr.is_alive():
            try:
                if getattr(mgr, 'is_connected', False) and (time.time() - float(getattr(mgr, 'last_msg_ts', 0.0) or 0.0) < 300.0):
                    return mgr
                mgr.stop()
            except Exception:
                return mgr
        mgr = UserDataStreamManager(user_id, api_key, secret_key)
        USER_STREAMS[user_id] = mgr
        mgr.start()
        return mgr
def send_trade_notification(user_id, message):
    try:
        bot.send_message(user_id, message)
    except Exception as e:
        print(f'Ошибка отправки уведомления: {str(e)}')
@app.route('/webhook/<symbol>', methods=['POST'])
def webhook(symbol):
    data = request.json
    data['symbol'] = symbol.upper()
    process_signal(data)
    return ('OK', 200)
SIGNAL_DEDUP_WINDOW_SEC = 4                                                                                   
def _signal_fingerprint(signal: dict) -> str:
    try:
        if not isinstance(signal, dict):
            return "not-a-dict"
        sid = (
            signal.get("signal_id")
            or signal.get("id")
            or signal.get("alert_id")
            or signal.get("uuid")
            or signal.get("msg_id")
        )
        if sid is not None and str(sid).strip() != "":
            return f"id:{sid}"
        raw = dict(signal)
        for k in ("timestamp", "time", "timenow", "ts", "sent_at", "received_at"):
            raw.pop(k, None)
        base = {
            "type": raw.get("type"),
            "symbol": (raw.get("symbol") or "").upper(),
            "direction": raw.get("direction") or raw.get("side"),
            "entry_price": raw.get("entry_price") or raw.get("entry"),
            "stop_price": raw.get("stop_price") or raw.get("stop"),
            "tp_levels": raw.get("tp_levels") or raw.get("tps") or raw.get("tp"),
            "raw": raw,
        }
        payload = json.dumps(base, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()
    except Exception:
        return str(time.time())
def is_duplicate_signal(signal: dict) -> bool:
    """Return True if this exact signal appears to be a webhook retry.

    IMPORTANT: We only dedup within a small time window, so normal repeated signals
    (used for averaging) will still work.
    """
    try:
        sym = (signal.get("symbol") or "").upper()
        fp = _signal_fingerprint(signal)
        now_ms = int(time.time() * 1000)
        window_ms = int(SIGNAL_DEDUP_WINDOW_SEC * 1000)
        with DB_LOCK:
            cursor.execute("SELECT last_ts FROM signal_dedup WHERE symbol = ? AND fp = ?", (sym, fp))
            row = cursor.fetchone()
            if row and row[0] is not None:
                try:
                    last_ts = int(row[0])
                    if (now_ms - last_ts) < window_ms:
                        return True
                except Exception:
                    pass
            cursor.execute(
                "INSERT OR REPLACE INTO signal_dedup(symbol, fp, last_ts) VALUES (?,?,?)",
                (sym, fp, now_ms),
            )
            if random.random() < 0.01:
                cutoff = now_ms - 3600_000          
                cursor.execute("DELETE FROM signal_dedup WHERE last_ts < ?", (cutoff,))
            conn.commit()
    except Exception:
        return False
    return False
def process_signal(signal):
    try:
        if is_duplicate_signal(signal):
            try:
                print(f"[dedup] ignored duplicate signal for {signal.get('symbol')}")
            except Exception:
                pass
            return
    except Exception:
        pass
    signal_type = signal.get('type')
    if signal_type == 'main':
        handle_main_signal(signal)
def _binance_min_notional(symbol_info):
    try:
        for f in symbol_info.get('filters', []):
            if f.get('filterType') in ('NOTIONAL', 'MIN_NOTIONAL'):
                v = f.get('notional') or f.get('minNotional') or f.get('minNotional')
                if v is not None:
                    return float(v)
    except Exception:
        pass
    return None
_EXCHANGE_INFO_CACHE = {"ts": 0.0, "by_symbol": {}}
_EXCHANGE_INFO_TTL_SEC = 60 * 60          
def get_symbol_info_cached(client, symbol: str):
    """Return symbol_info from exchangeInfo with 1h in-process cache."""
    try:
        sym = (symbol or "").upper().replace("/", "")
        if not sym:
            return None
        now = time.time()
        try:
            ts = float(_EXCHANGE_INFO_CACHE.get("ts") or 0.0)
        except Exception:
            ts = 0.0
        by_symbol = _EXCHANGE_INFO_CACHE.get("by_symbol") or {}
        if by_symbol and (now - ts) < _EXCHANGE_INFO_TTL_SEC and sym in by_symbol:
            return by_symbol.get(sym)
        info = safe_api_call(client.exchange_info, retries=3)
        if not info or not isinstance(info, dict):
            return None
        m = {}
        for s in info.get("symbols", []) or []:
            try:
                k = s.get("symbol")
                if k:
                    m[str(k)] = s
            except Exception:
                continue
        _EXCHANGE_INFO_CACHE["by_symbol"] = m
        _EXCHANGE_INFO_CACHE["ts"] = now
        return m.get(sym)
    except Exception:
        return None


def _floor_to_step(value: float, step: float) -> float:
    """Floor `value` down to the nearest multiple of `step`."""
    if step <= 0:
        return value
    return math.floor(value / step) * step


def allocate_tp_volumes(total_volume: float, weights, step_size: float = 0.001, min_qty: float = 0.0, max_slices: int = None):
    """Split total volume by TP weights with step/minQty constraints.

    Returns a list aligned with `weights` where each element:
    - is non-negative,
    - is a multiple of `step_size` (if step_size > 0),
    - keeps the total sum <= total_volume.
    """
    try:
        total_volume = float(total_volume)
    except Exception:
        return []
    if (not math.isfinite(total_volume)) or total_volume <= 0:
        return []

    try:
        raw_weights = list(weights) if weights is not None else []
    except Exception:
        raw_weights = []

    n = len(raw_weights)
    if max_slices is not None:
        try:
            n = min(n, max(0, int(max_slices)))
        except Exception:
            pass
    if n <= 0:
        return []

    safe_weights = []
    for w in raw_weights[:n]:
        try:
            fw = float(w)
            safe_weights.append(max(0.0, fw) if math.isfinite(fw) else 0.0)
        except Exception:
            safe_weights.append(0.0)

    weight_sum = sum(safe_weights)
    if weight_sum <= 0:
        safe_weights = [1.0 / n] * n
    else:
        safe_weights = [w / weight_sum for w in safe_weights]

    try:
        step = float(step_size)
        step = max(step, 0.0) if math.isfinite(step) else 0.0
    except Exception:
        step = 0.0
    try:
        min_qty = float(min_qty)
        min_qty = max(min_qty, 0.0) if math.isfinite(min_qty) else 0.0
    except Exception:
        min_qty = 0.0

    def _round_out(values, step_val):
        if not values:
            return values
        if step_val <= 0:
            return [float(v) for v in values]
        try:
            p = max(0, min(12, int(round(-math.log10(step_val)))))
        except Exception:
            p = 8
        return [round(float(v), p) for v in values]

    if step <= 0:
        vols = [total_volume * w for w in safe_weights]
        if min_qty > 0:
            dust = 0.0
            for i, v in enumerate(vols):
                if 0 < v < min_qty:
                    dust += v
                    vols[i] = 0.0
            if dust > 0:
                target_idx = next((i for i in range(len(vols) - 1, -1, -1) if vols[i] > 0), len(vols) - 1)
                vols[target_idx] += dust
                if 0 < vols[target_idx] < min_qty:
                    return [0.0] * len(vols)
        return _round_out(vols, step)

    # Integer-step allocation avoids floating drift and keeps deterministic totals.
    total_steps = int(math.floor(total_volume / step + 1e-12))
    if total_steps <= 0:
        return [0.0] * n

    raw_steps = [total_steps * w for w in safe_weights]
    step_alloc = [int(math.floor(v + 1e-12)) for v in raw_steps]
    leftovers = total_steps - sum(step_alloc)
    if leftovers > 0:
        order = sorted(range(n), key=lambda i: (raw_steps[i] - step_alloc[i], safe_weights[i]), reverse=True)
        for i in range(leftovers):
            step_alloc[order[i % n]] += 1

    if min_qty > 0:
        min_steps = int(math.ceil(min_qty / step - 1e-12))
        if min_steps > 1:
            tiny_idx = [i for i, st in enumerate(step_alloc) if 0 < st < min_steps]
            if tiny_idx:
                moved_steps = sum(step_alloc[i] for i in tiny_idx)
                for i in tiny_idx:
                    step_alloc[i] = 0
                target_idx = next((i for i in range(n - 1, -1, -1) if step_alloc[i] >= min_steps), None)
                if target_idx is None:
                    if total_steps >= min_steps:
                        step_alloc = [0] * (n - 1) + [total_steps]
                    else:
                        step_alloc = [0] * n
                else:
                    step_alloc[target_idx] += moved_steps

    vols = [steps * step for steps in step_alloc]

    # Final safety clamp: due to numeric precision the sum can drift above stepped max.
    max_sum = total_steps * step
    cur_sum = sum(vols)
    if cur_sum > max_sum + 1e-12:
        overflow_steps = int(math.ceil((cur_sum - max_sum) / step - 1e-12))
        for i in range(n - 1, -1, -1):
            if overflow_steps <= 0:
                break
            take = min(step_alloc[i], overflow_steps)
            step_alloc[i] -= take
            overflow_steps -= take
        vols = [steps * step for steps in step_alloc]

    return _round_out(vols, step)


def _pick_first_tp_order_id(entry_price: float, direction: str, tp_data_for_db: dict, fallback_ids=None):
    """Pick the TP order id closest to entry price on the profit side."""
    items = []
    if isinstance(tp_data_for_db, dict):
        for v in tp_data_for_db.values():
            try:
                p = float(v.get('price'))
                oid = v.get('order_id')
                if oid:
                    items.append((p, str(oid)))
            except Exception:
                continue

    direction = (direction or '').upper().strip()
    try:
        entry_price = float(entry_price)
    except Exception:
        entry_price = None

    if entry_price is not None and items:
        if direction == 'LONG':
            profit_side = [it for it in items if it[0] > entry_price]
            if profit_side:
                return min(profit_side, key=lambda it: it[0])[1]
        elif direction == 'SHORT':
            profit_side = [it for it in items if it[0] < entry_price]
            if profit_side:
                return max(profit_side, key=lambda it: it[0])[1]
        return min(items, key=lambda it: abs(it[0] - entry_price))[1]

    if fallback_ids:
        try:
            return str(list(fallback_ids)[0])
        except Exception:
            return None
    return None

def safe_api_call(func, *args, retries=6, initial_backoff=0.5, max_backoff=30.0, backoff_factor=2.0, jitter=0.3,
                  allowed_exceptions=(Exception,), **kwargs):
    def _is_retryable(e: Exception) -> tuple[bool, float]:
        msg = str(e).lower()
        non_retryable_codes = {
            -1100, -1101, -1102, -1103, -1104, -1105, -1106, -1108,
            -1111, -1112, -1113, -1114, -1115, -1116, -1117, -1118, -1119,
            -1120, -1121, -1122, -1123, -1124,
            -2010, -2011, -2013, -2014, -2021, -2022,
        }
        code = None
        status = None
        if isinstance(e, BinanceAPIException):
            try:
                code = int(getattr(e, "code", None))
            except Exception:
                code = None
            try:
                status = int(getattr(e, "status_code", None))
            except Exception:
                status = None
        elif isinstance(e, WsAPIException):
            code = e.code
            status = e.status
        if code in non_retryable_codes:
            return (False, 0.0)
        if "unknown order sent" in msg or "order does not exist" in msg:
            return (False, 0.0)
        if status in (418, 429) or "too many requests" in msg or "rate limit" in msg:
            return (True, 60.0 if status == 418 else 10.0)
        if status in (500, 502, 503, 504):
            return (True, 0.8)
        if code == -1021:
            try:
                force_time_sync()
            except Exception:
                pass
            return (True, 0.2)
        if code in (-1007, -1008, -1001):
            return (True, 0.8)
        if "service unavailable" in msg or "temporarily unavailable" in msg or "system busy" in msg or "overload" in msg:
            return (True, 0.8)
        if "timed out" in msg or "timeout" in msg or "connection reset" in msg or "connection aborted" in msg:
            return (True, 0.6)
        if "ws not connected" in msg or "timeout waiting ws response" in msg:
            return (True, 0.6)
        return (False, 0.0)
    backoff = float(initial_backoff)
    for attempt in range(1, retries + 1):
        try:
            return func(*args, **kwargs)
        except allowed_exceptions as e:
            retryable, min_sleep = _is_retryable(e)
            if not retryable or attempt == retries:
                try:
                    print(f"[ERROR] safe_api_call: не ретраю ({attempt}/{retries}) для {getattr(func, '__name__', str(func))}: {e}")
                except Exception:
                    pass
                return None
            backoff = min(max_backoff, max(backoff * backoff_factor, min_sleep))
            sleep_for = min(max_backoff, backoff) + random.random() * jitter
            try:
                print(f"[WARN] safe_api_call: retry {attempt}/{retries} для {getattr(func,'__name__',str(func))}: {e} | sleep {sleep_for:.2f}s")
            except Exception:
                pass
            time.sleep(sleep_for)
        except Exception as e:
            try:
                print(f"[ERROR] safe_api_call: неожиданное исключение: {e}")
            except Exception:
                pass
            return None
    return None

def update_position_in_db(symbol, entry_price=None, current_entry_price=None, quantity=None, initial_quantity=None, add_tp_items=None, replace_tp_data=None, stop_price=None, stop_order_id=None, stop_loss_moved=None, first_tp_filled=None, metadata=None):
    with DB_LOCK:
        cursor.execute('SELECT tp_data FROM positions WHERE symbol = ?', (symbol,))
        row = cursor.fetchone()
        existing_tp = []
        if row and row[0]:
            try:
                existing_tp = json.loads(row[0])
            except Exception:
                existing_tp = []
        if add_tp_items:
            for it in add_tp_items:
                if not any((it.get('orderId') == ex.get('orderId') for ex in existing_tp)):
                    existing_tp.append(it)
        if replace_tp_data is not None:
            new_tp = replace_tp_data
        else:
            new_tp = existing_tp
        sets = []
        vals = []
        if entry_price is not None:
            sets.append('entry_price = ?')
            vals.append(entry_price)
        if current_entry_price is not None:
            sets.append('current_entry_price = ?')
            vals.append(current_entry_price)
        if quantity is not None:
            sets.append('quantity = ?')
            vals.append(quantity)
        if initial_quantity is not None:
            sets.append('initial_quantity = ?')
            vals.append(initial_quantity)
        if stop_price is not None:
            sets.append('stop_price = ?')
            vals.append(stop_price)
        if stop_order_id is not None:
            sets.append('stop_order_id = ?')
            vals.append(str(stop_order_id))
        if stop_loss_moved is not None:
            sets.append('stop_loss_moved = ?')
            vals.append(1 if stop_loss_moved else 0)
        if first_tp_filled is not None:
            sets.append('first_tp_filled = ?')
            vals.append(1 if first_tp_filled else 0)
        if metadata is not None:
            sets.append('metadata = ?')
            vals.append(json.dumps(metadata))
        sets.append('tp_data = ?')
        vals.append(json.dumps(new_tp))
        try:
            tp_order_ids = [p.get('orderId') for p in new_tp if p.get('orderId')]
        except Exception:
            tp_order_ids = []
        sets.append('tp_order_ids = ?')
        vals.append(json.dumps(tp_order_ids))
        sets.append('updated_at = ?')
        vals.append(int(time.time()))
        if not sets:
            return
        vals.append(symbol)
        query = f"UPDATE positions SET {', '.join(sets)} WHERE symbol = ?"
        cursor.execute(query, tuple(vals))
        conn.commit()
def get_position_from_db(symbol):
    with DB_LOCK:
        cursor.execute('SELECT symbol, user_id, direction, entry_price, current_entry_price, quantity, initial_quantity, stop_price, stop_order_id, entry_order_id, tp_order_ids, tp_data, metadata, stop_loss_moved, first_tp_filled FROM positions WHERE symbol = ?', (symbol,))
        row = cursor.fetchone()
        if not row:
            return None
        try:
            tp_data = json.loads(row[11]) if row[11] else []
        except Exception:
            tp_data = []
        try:
            metadata = json.loads(row[12]) if row[12] else {}
        except Exception:
            metadata = {}
        return {'symbol': row[0], 'user_id': row[1], 'direction': row[2], 'entry_price': row[3], 'current_entry_price': row[4] or row[3], 'quantity': float(row[5]) if row[5] else 0.0, 'initial_quantity': float(row[6]) if row[6] else None, 'stop_price': row[7], 'stop_order_id': row[8], 'entry_order_id': row[9], 'tp_order_ids': json.loads(row[10]) if row[10] else [], 'tp_data': tp_data, 'metadata': metadata, 'stop_loss_moved': bool(row[13]), 'first_tp_filled': bool(row[14])}
def delete_position_from_db(symbol):
    with DB_LOCK:
        cursor.execute('SELECT * FROM positions WHERE symbol = ?', (symbol,))
        row = cursor.fetchone()
        details = {}
        if row:
            try:
                cols = [d[0] for d in cursor.description]
                details = dict(zip(cols, row))
            except Exception:
                try:
                    details = {'symbol': row[0], 'user_id': row[1], 'direction': row[2], 'entry_price': row[3], 'current_entry_price': row[4], 'quantity': row[5], 'initial_quantity': row[6], 'stop_price': row[7], 'stop_order_id': row[8], 'entry_order_id': row[9], 'tp_order_ids': row[10], 'tp_data': row[11], 'metadata': row[12], 'stop_loss_moved': row[13], 'first_tp_filled': row[14]}
                except Exception:
                    details = {}
            try:
                details['tp_data'] = json.loads(details.get('tp_data') or '[]')
            except Exception:
                details['tp_data'] = []
            try:
                details['metadata'] = json.loads(details.get('metadata') or '{}')
            except Exception:
                details['metadata'] = {}
            try:
                cursor.execute('INSERT INTO positions_history(symbol,user_id,direction,entry_price,exit_price,pnl,details) VALUES(?,?,?,?,?,?,?)', (details.get('symbol'), details.get('user_id'), details.get('direction'), details.get('entry_price'), None, None, json.dumps(details)))
            except Exception:
                logging.exception('delete_position_from_db: не удалось вставить в history для %s', symbol)
        cursor.execute('DELETE FROM positions WHERE symbol = ?', (symbol,))
        conn.commit()
def update_user_balance(user_id):
    try:
        with DB_LOCK:
            cursor.execute('SELECT api_key, secret_key FROM users WHERE user_id = ?', (user_id,))
            keys = cursor.fetchone()
        if not keys or not keys[0] or not keys[1]:
            return None
        api_key, secret_key = keys[0], keys[1]
        client = get_trading_client(api_key, secret_key)
        account = client.account()
        total_wallet_balance = None
        try:
            assets = account.get('assets') or []
            for a in assets:
                if a.get('asset') == 'USDT':
                    wb = a.get('walletBalance')
                    if wb is not None:
                        total_wallet_balance = float(wb)
                        break
        except Exception:
            total_wallet_balance = None
        if total_wallet_balance is None:
            for k in ('totalWalletBalance', 'walletBalance', 'balance'):
                try:
                    v = account.get(k)
                    if v is not None:
                        total_wallet_balance = float(v)
                        break
                except Exception:
                    continue
        if total_wallet_balance is None:
            return None
        ts = int(time.time())
        with DB_LOCK:
            cursor.execute('UPDATE users SET balance = ? WHERE user_id = ?', (total_wallet_balance, user_id))
            cursor.execute('INSERT INTO balance_history (user_id, balance, timestamp) VALUES (?, ?, ?)', (user_id, total_wallet_balance, ts))
            conn.commit()
        return total_wallet_balance
    except Exception as e:
        try:
            print(f'Ошибка обновления баланса: {str(e)}')
        except Exception:
            pass
        return None


def get_available_balance(user_id):
    try:
        with DB_LOCK:
            cursor.execute('SELECT api_key, secret_key FROM users WHERE user_id = ?', (user_id,))
            keys = cursor.fetchone()
        if not keys or not keys[0] or not keys[1]:
            return None
        api_key, secret_key = keys[0], keys[1]
        client = get_trading_client(api_key, secret_key)
        account = client.account()
        for k in ('availableBalance', 'available_balance', 'available'):
            try:
                v = account.get(k)
                if v is not None:
                    return float(v)
            except Exception:
                pass
        try:
            assets = account.get('assets') or []
            for a in assets:
                if a.get('asset') == 'USDT':
                    ab = a.get('availableBalance') or a.get('available')
                    if ab is not None:
                        return float(ab)
        except Exception:
            pass
        return None
    except Exception as e:
        try:
            print(f'Ошибка получения доступного баланса: {str(e)}')
        except Exception:
            pass
        return None
def recalculate_losses():
    try:
        cursor.execute('SELECT total_loss FROM global_loss LIMIT 1')
        total_loss = cursor.fetchone()[0]
        cursor.execute('SELECT symbol FROM trading_pairs WHERE is_active = 1')
        active_pairs = cursor.fetchall()
        num_active_pairs = len(active_pairs)
        if num_active_pairs > 0:
            loss_per_pair = total_loss / num_active_pairs
            for pair in active_pairs:
                cursor.execute('UPDATE trading_pairs SET current_loss = ? WHERE symbol = ?', (loss_per_pair, pair[0]))
            conn.commit()
            message = f'🔄 Перерасчет убытков после изменения количества активных пар\n📊 Новый общий убыток: {total_loss:.2f} USDT\n🔄 Убыток на пару: {loss_per_pair:.2f} USDT'
            send_trade_notification(1901059519, message)
            send_trade_notification(6189545928, message)
    except Exception as e:
        print(f'Ошибка перерасчета убытков: {str(e)}')
class BalanceMonitor(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.running = True
    def run(self):
        while self.running:
            try:
                cursor.execute('SELECT user_id, last_deactivation FROM users WHERE is_active = 0')
                inactive_users = cursor.fetchall()
                current_time = int(time.time())
                for user in inactive_users:
                    user_id, last_deactivation = user
                    if last_deactivation is not None:
                        elapsed_time = current_time - last_deactivation
                        if elapsed_time >= 86400:
                            cursor.execute('UPDATE users SET is_active = 1, last_deactivation = NULL WHERE user_id = ?', (user_id,))
                            conn.commit()
                            send_trade_notification(1901059519, f'✅ Торговля для пользователя {user_id} автоматически включена после 24 часов безactivité')
                            send_trade_notification(6189545928, f'✅ Торговля для пользователя {user_id} автоматически включена после 24 часов безactivité')
                cursor.execute('SELECT user_id, balance FROM users WHERE is_active = 1')
                active_users = cursor.fetchall()
                for user in active_users:
                    user_id, current_balance = user
                    time_24h_ago = current_time - 86400
                    cursor.execute('SELECT balance FROM balance_history WHERE user_id = ? AND timestamp >= ? ORDER BY timestamp ASC LIMIT 1', (user_id, time_24h_ago))
                    history_record = cursor.fetchone()
                    if history_record:
                        balance_24h_ago = history_record[0]
                        if current_balance < balance_24h_ago * 0.8:
                            cursor.execute('UPDATE users SET is_active = 0, last_deactivation = ? WHERE user_id = ?', (current_time, user_id))
                            conn.commit()
                            msg = f'⚠️ Баланс упал на 20% за 24 часа! Было: {balance_24h_ago:.2f} USD, Сейчас: {current_balance:.2f} USD. Торговля остановлена.'
                            send_trade_notification(1901059519, msg)
                            send_trade_notification(6189545928, msg)
                old_timestamp = current_time - 172800
                cursor.execute('DELETE FROM balance_history WHERE timestamp < ?', (old_timestamp,))
                conn.commit()
                time.sleep(180)
            except Exception as e:
                print(f'Ошибка мониторинга: {str(e)}')
                time.sleep(60)
class TrailingStopThread(threading.Thread):
    def __init__(self, user_id, symbol, direction, entry_price, client, price_precision, quantity_precision, first_tp_order_id, stop_order_id, initial_stop_loss, api_key, secret_key, tp_order_ids=None, entry_order_id=None):
        super().__init__()
        self.user_id = user_id
        self.symbol = symbol
        self.direction = direction
        self.initial_entry_price = float(entry_price)
        self.current_entry_price = float(entry_price)
        self.client = client
        self.price_precision = price_precision
        self.quantity_precision = quantity_precision
        self.first_tp_order_id = first_tp_order_id
        self.entry_order_id = entry_order_id
        self.stop_order_id = str(stop_order_id) if stop_order_id is not None else None
        self.initial_stop_loss = initial_stop_loss
        self.api_key = api_key
        self.secret_key = secret_key
        try:
            normalized = []
            for o in tp_order_ids or []:
                try:
                    if not isinstance(o, str) and hasattr(o, 'get'):
                        normalized.append(str(_extract_order_id(o)))
                    else:
                        normalized.append(str(o))
                except Exception:
                    try:
                        normalized.append(str(o))
                    except Exception:
                        pass
            self.tp_order_ids = normalized
        except Exception:
            self.tp_order_ids = []
        self.active = True
        self.position_closed = False
        self.lock = threading.Lock()
        self.stop_loss_moved = False
        self.dynamic_tp_order_ids = []
        self.filled_tp_count = 0
        self.first_tp_filled = False
        self.last_realized_pnl = None
        self._cv = threading.Condition(self.lock)
        self._ws_flags = {
            'tp_filled': False,
            'first_tp_filled': False,
            'pos_zero': False,
            'stop_fired': False,
            'tp_trade': False,
            'tp_trade_oid': None,
        }
        self.last_position_amt = None
        self._last_position_amt_ts = 0.0
        self._last_tp_status_check_ts = 0.0
        self._last_ws_event_ts = 0.0
        self._last_emergency_poll_ts = 0.0
    def _sync_stop_order_id_from_db(self):
        try:
            if 'get_current_stop_order_id_from_db' in globals():
                db_id = get_current_stop_order_id_from_db(self.symbol)
                if db_id:
                    return str(db_id)
        except Exception:
            pass
        try:
            with db_tx() as conn:
                cur = conn.cursor()
                cur.execute('SELECT stop_order_id FROM positions WHERE symbol = ?', (self.symbol,))
                r = cur.fetchone()
                if r and r[0] is not None:
                    return str(r[0])
        except Exception:
            pass
        return None
    def _cancel_stop_order_by_id(self, order_id):
        if not order_id:
            return (True, 'no-order-id')
        try:
            ok, res = _http_cancel_order(self.api_key, self.secret_key, self.symbol, order_id) if '_http_cancel_order' in globals() else (False, 'no_helper')
            if ok:
                return (True, res)
            return (False, res)
        except Exception as e:
            return (False, e)
    def _is_algo_stop_id(self, stop_id: str) -> bool:
        try:
            return isinstance(stop_id, str) and stop_id.startswith('algo:')
        except Exception:
            return False
    def _signed_get(self, url: str, params: dict, timeout: int = 15):
        """Signed GET helper for endpoints not covered by binance-connector."""
        try:
            params = dict(params or {})
            qs = urlencode(params)
            signature = hmac.new((self.secret_key or '').encode('utf-8'), qs.encode('utf-8'), hashlib.sha256).hexdigest()
            params['signature'] = signature
            headers = {'X-MBX-APIKEY': self.api_key or ''}
            r = _http_request('GET', url, params=params, headers=headers, timeout=timeout)
            try:
                j = r.json()
            except Exception:
                j = {'http_status': r.status_code, 'text': r.text}
            return (r.status_code, j)
        except Exception as e:
            return (0, {'error': str(e)})
    def _query_order_flexible(self, order_id):
        """Query either a normal orderId or an algoId in form 'algo:<id>'"""
        if not order_id:
            return None
        oid = str(order_id)
        if self._is_algo_stop_id(oid):
            algo_id = oid.split(':', 1)[1]
            url = f"{_BINANCE_FAPI_REST_BASE}/fapi/v1/algoOrder"
            params = {
                'symbol': self.symbol,
                'algoId': algo_id,
                'timestamp': binance_now_ms(),
                'recvWindow': 60000,
            }
            status, j = self._signed_get(url, params=params, timeout=15)
            return j
        return self.client.query_order(symbol=self.symbol, orderId=oid)
    def _cancel_order_flexible(self, order_id):
        """Cancel either a normal orderId or an algoId in form 'algo:<id>'"""
        if not order_id:
            return (True, 'no-order-id')
        oid = str(order_id)
        if self._is_algo_stop_id(oid):
            algo_id = oid.split(':', 1)[1]
            try:
                return _cancel_algo_order(self.api_key, self.secret_key, self.symbol, algo_id)
            except Exception as e:
                return (False, str(e))
        return self._cancel_stop_order_by_id(oid)
    def _cleanup_duplicate_protective_orders(self, keep_stop_id: str):
        """Best-effort cleanup if cancel failed and we might have multiple protective stops."""
        try:
            keep_stop_id = str(keep_stop_id) if keep_stop_id is not None else None
            keep_is_algo = self._is_algo_stop_id(keep_stop_id or '')
            keep_algo_id = keep_stop_id.split(':', 1)[1] if (keep_is_algo and keep_stop_id) else None
            stop_types = {'STOP', 'STOP_MARKET', 'TAKE_PROFIT', 'TAKE_PROFIT_MARKET', 'TRAILING_STOP_MARKET'}
            try:
                open_orders = self.client.get_open_orders(symbol=self.symbol)
                if isinstance(open_orders, dict):
                    open_orders = open_orders.get('orders') or open_orders.get('data') or open_orders.get('result') or []
            except Exception:
                open_orders = []
            if isinstance(open_orders, (list, tuple)):
                for o in open_orders:
                    try:
                        otype = (o.get('type') if isinstance(o, dict) else None)
                        if otype not in stop_types:
                            continue
                        reduce_only = False
                        close_pos = False
                        try:
                            reduce_only = bool(o.get('reduceOnly')) if isinstance(o, dict) else False
                            close_pos = bool(o.get('closePosition')) if isinstance(o, dict) else False
                        except Exception:
                            pass
                        if not (reduce_only or close_pos):
                            continue
                        oid = None
                        try:
                            oid = str(o.get('orderId'))
                        except Exception:
                            oid = None
                        if not oid:
                            continue
                        if (not keep_is_algo) and keep_stop_id and oid == keep_stop_id:
                            continue
                        self._cancel_stop_order_by_id(oid)
                    except Exception:
                        pass
            try:
                aok, aresp = _get_algo_open_orders(self.api_key, self.secret_key, self.symbol)
            except Exception:
                aok, aresp = (False, None)
            if aok:
                aorders = aresp
                if isinstance(aresp, dict):
                    aorders = aresp.get('orders') or aresp.get('data') or aresp.get('result') or []
                if isinstance(aorders, (list, tuple)):
                    for ao in aorders:
                        try:
                            order_type = ao.get('orderType') or ao.get('type')
                            if order_type not in stop_types:
                                continue
                            algo_id = str(ao.get('algoId') or ao.get('aid') or ao.get('id') or '')
                            if not algo_id:
                                continue
                            if keep_is_algo and keep_algo_id and algo_id == str(keep_algo_id):
                                continue
                            _cancel_algo_order(self.api_key, self.secret_key, self.symbol, algo_id)
                        except Exception:
                            pass
        except Exception:
            pass
    def _write_stop_order_id_to_db(self, order_id, stop_price=None):
        try:
            update_kwargs = {'stop_order_id': order_id}
            if stop_price is not None:
                update_kwargs['stop_price'] = stop_price
            try:
                update_position_in_db(self.symbol, **update_kwargs)
                return True
            except Exception:
                with db_tx() as conn:
                    cur = conn.cursor()
                    if 'stop_price' in update_kwargs:
                        cur.execute('UPDATE positions SET stop_order_id = ?, stop_price = ? WHERE symbol = ?', (str(order_id), update_kwargs['stop_price'], self.symbol))
                    else:
                        cur.execute('UPDATE positions SET stop_order_id = ? WHERE symbol = ?', (str(order_id), self.symbol))
                    conn.commit()
                return True
        except Exception:
            return False
    def run(self):
        try:
            while self.active:
                now_ts = time.time()
                with self.lock:
                    ws_tp = bool(self._ws_flags.get('tp_filled'))
                    ws_first = bool(self._ws_flags.get('first_tp_filled'))
                    ws_pos_zero = bool(self._ws_flags.get('pos_zero'))
                    ws_stop_fired = bool(self._ws_flags.get('stop_fired'))
                    ws_tp_trade = bool(self._ws_flags.get('tp_trade'))
                    ws_tp_trade_oid = self._ws_flags.get('tp_trade_oid')
                    self._ws_flags['tp_filled'] = False
                    self._ws_flags['first_tp_filled'] = False
                    self._ws_flags['stop_fired'] = False
                    self._ws_flags['pos_zero'] = False
                    self._ws_flags['tp_trade'] = False
                    self._ws_flags['tp_trade_oid'] = None
                stream_ok = False
                try:
                    with USER_STREAMS_LOCK:
                        mgr = USER_STREAMS.get(int(self.user_id))
                    stream_ok = bool(mgr and mgr.is_alive() and getattr(mgr, 'is_connected', False) and (now_ts - float(getattr(mgr, 'last_msg_ts', 0.0) or 0.0) < 120.0))
                except Exception:
                    stream_ok = False
                do_poll = (not stream_ok) and ((now_ts - self._last_emergency_poll_ts) > 60.0)
                if ws_pos_zero or ws_stop_fired:
                    self._handle_position_closed()
                    break
                if (not ws_tp) and ws_tp_trade and ws_tp_trade_oid:
                    should_check = False
                    with self.lock:
                        if (now_ts - float(getattr(self, '_last_tp_status_check_ts', 0.0))) > 0.5:
                            self._last_tp_status_check_ts = now_ts
                            should_check = True
                    if should_check:
                        try:
                            res = self.client.query_order(symbol=self.symbol, orderId=ws_tp_trade_oid)
                            status = (res or {}).get('status')
                            if status == 'FILLED':
                                ws_tp = True
                                if self.first_tp_order_id and ws_tp_trade_oid == str(self.first_tp_order_id):
                                    ws_first = True
                        except Exception:
                            pass
                if not self.stop_loss_moved and (ws_tp or (do_poll and self._is_any_tp_filled())):
                    active = self.is_position_active() if do_poll else True
                    if active is True:
                        try:
                            self.update_stop_loss()
                            with self.lock:
                                self.stop_loss_moved = True
                                self.filled_tp_count = 0
                            try:
                                update_position_in_db(self.symbol, stop_loss_moved=True)
                            except Exception:
                                pass
                            try:
                                send_trade_notification(1901059519, f'[STOP_MOVED] {self.symbol}: stop moved to avg-entry due to TP.')
                            except Exception:
                                pass
                        except Exception as e:
                            print(f'[TrailingStop] Ошибка при обновлении стопа для {self.symbol}: {e}')
                    elif active is False:
                        self._handle_position_closed()
                        break
                if do_poll:
                    active = self.is_position_active()
                    if active is False:
                        self._handle_position_closed()
                        break
                if (not self.first_tp_filled) and self.first_tp_order_id and (ws_first or (do_poll and self.check_first_tp_filled())):
                    try:
                        _ = self.get_realized_pnl_by_order()
                    except Exception:
                        pass
                    with self.lock:
                        self.first_tp_filled = True
                    try:
                        update_position_in_db(self.symbol, first_tp_filled=True)
                    except Exception:
                        pass
                    try:
                        send_trade_notification(1901059519, f'✅ Первый TP взят для {self.symbol}, orderId={self.first_tp_order_id}')
                    except Exception:
                        pass
                    try:
                        send_trade_notification(self.user_id, f'✅ Первый TP взят для {self.symbol}')
                    except Exception:
                        pass
                if do_poll:
                    self._last_emergency_poll_ts = now_ts
                try:
                    with self._cv:
                        self._cv.wait(timeout=30)
                except Exception:
                    time.sleep(5)
        except Exception as e:
            print(f'Ошибка в потоке TrailingStop: {str(e)}')
        finally:
            self.active = False
    def update_current_entry_price(self, new_entry_price):
        with self.lock:
            try:
                self.current_entry_price = float(new_entry_price)
            except Exception:
                pass
    def add_dynamic_tp_orders(self, tp_order_ids):
        with self.lock:
            for o in tp_order_ids or []:
                try:
                    self.dynamic_tp_order_ids.append(str(o))
                except Exception:
                    pass
    def notify_user_stream_event(self, event):
        try:
            if not isinstance(event, dict):
                return
            et = event.get('e')
            with self.lock:
                try:
                    self._last_ws_event_ts = time.time()
                except Exception:
                    pass
                if et == 'ORDER_TRADE_UPDATE':
                    o = event.get('o') or {}
                    oid = o.get('i') or o.get('orderId')
                    status = o.get('X')
                    exec_type = o.get('x')
                    oid_str = str(oid) if oid is not None else None
                    if oid_str and status == 'FILLED' and exec_type == 'TRADE':
                        if oid_str in (self.tp_order_ids or []) or oid_str in (self.dynamic_tp_order_ids or []):
                            self._ws_flags['tp_filled'] = True
                        if self.first_tp_order_id and oid_str == str(self.first_tp_order_id):
                            self._ws_flags['first_tp_filled'] = True
                        if self.stop_order_id and oid_str == str(self.stop_order_id):
                            self._ws_flags['stop_fired'] = True
                elif et == 'ACCOUNT_UPDATE':
                    a = event.get('a') or {}
                    P = a.get('P') or []
                    for p in P:
                        if p.get('s') == self.symbol:
                            try:
                                pa = float(p.get('pa', 0))
                            except Exception:
                                pa = None
                            try:
                                ep = float(p.get('ep', 0))
                            except Exception:
                                ep = None
                            if ep and ep > 0:
                                self.current_entry_price = ep
                            if pa is not None:
                                self.last_position_amt = pa
                                try:
                                    self._last_position_amt_ts = time.time()
                                except Exception:
                                    pass
                                if abs(pa) < 1e-12:
                                    self._ws_flags['pos_zero'] = True
                            break
                elif et == 'ALGO_UPDATE':
                    o = event.get('o') or {}
                    aid = o.get('aid') or o.get('algoId') or o.get('a')
                    st = o.get('X') or o.get('st') or o.get('s') or o.get('S') or o.get('status')
                    if self.stop_order_id and str(self.stop_order_id).startswith('algo:'):
                        try:
                            target_aid = str(self.stop_order_id).split(':', 1)[1]
                        except Exception:
                            target_aid = None
                        if target_aid is not None and str(aid) == target_aid:
                            if str(st).upper() in ('TRIGGERED', 'FINISHED'):
                                self._ws_flags['stop_fired'] = True
                elif et == 'TRADE_LITE':
                    oid = event.get('i')
                    oid_str = str(oid) if oid is not None else None
                    if oid_str:
                        if self.stop_order_id and isinstance(self.stop_order_id, str) and not self.stop_order_id.startswith('algo:'):
                            if oid_str == str(self.stop_order_id):
                                self._ws_flags['stop_fired'] = True
                        if oid_str in (self.tp_order_ids or []) or oid_str in (self.dynamic_tp_order_ids or []) or (self.first_tp_order_id and oid_str == str(self.first_tp_order_id)):
                            self._ws_flags['tp_trade'] = True
                            self._ws_flags['tp_trade_oid'] = oid_str
                try:
                    self._cv.notify_all()
                except Exception:
                    pass
        except Exception:
            pass
    def move_stop_loss_to_initial_entry(self):
        try:
            db_id = self._sync_stop_order_id_from_db()
            if db_id and str(getattr(self, 'stop_order_id', None)) != str(db_id):
                self.stop_order_id = str(db_id)
            old_stop_id = str(self.stop_order_id) if getattr(self, 'stop_order_id', None) else None
            quantity = self._get_current_quantity()
            if quantity <= 0:
                raise ValueError('Нет открытой позиции для переноса стопа')
            new_stop_price = float(self.initial_entry_price)
            qprec = int(self.quantity_precision) if self.quantity_precision is not None else 8
            pprec = int(self.price_precision) if self.price_precision is not None else 2
            qty_str = f'{abs(quantity):.{qprec}f}'
            stop_price_str = f'{new_stop_price:.{pprec}f}'
            kind, new_id, raw = create_stop_order(
                self.api_key,
                self.secret_key,
                self.client,
                symbol=self.symbol,
                side='SELL' if self.direction == 'LONG' else 'BUY',
                quantity=qty_str,
                stop_price=stop_price_str,
                priceProtect=True,
                reduceOnly=True,
                closePosition=False,
                workingType='CONTRACT_PRICE'
            )
            if not new_id:
                raise ValueError(f'Не удалось создать новый стоп: {raw}')
            self.stop_order_id = str(new_id)
            try:
                self._write_stop_order_id_to_db(self.stop_order_id, stop_price=new_stop_price)
            except Exception as exc:
                print(f'[warn] DB update failed for initial-entry stop move {self.symbol}: {exc}')
            if old_stop_id and old_stop_id != str(new_id):
                ok, res = self._cancel_order_flexible(old_stop_id)
                if not ok:
                    print(f'[warn] move_stop_loss_to_initial_entry: cancel returned not-ok: {res}')
            message = f'✅ Стоп-лосс пары {self.symbol} перемещён в ТВХ: {new_stop_price:.{pprec}f}'
            try:
                send_trade_notification(1901059519, message)
                send_trade_notification(self.user_id, message)
            except Exception:
                pass
        except Exception as e:
            try:
                send_trade_notification(1901059519, f'❗ Ошибка переноса стопа в ТВХ ({self.symbol}): {e}')
            except Exception:
                pass
    def update_stop_loss(self):
        try:
            db_id = self._sync_stop_order_id_from_db()
            if db_id and str(getattr(self, 'stop_order_id', None)) != str(db_id):
                self.stop_order_id = str(db_id)
            old_stop_id = str(self.stop_order_id) if getattr(self, 'stop_order_id', None) else None
            quantity = self._get_current_quantity()
            if quantity is None or quantity <= 0:
                raise ValueError(f'Некорректный quantity: {quantity}')
            new_stop_price = float(self.current_entry_price)
            qprec = int(self.quantity_precision) if self.quantity_precision is not None else 8
            pprec = int(self.price_precision) if self.price_precision is not None else 2
            qty_str = f'{abs(quantity):.{qprec}f}'
            stop_price_str = f'{new_stop_price:.{pprec}f}'
            kind, new_id, raw = create_stop_order(
                self.api_key,
                self.secret_key,
                self.client,
                symbol=self.symbol,
                side='SELL' if self.direction == 'LONG' else 'BUY',
                quantity=qty_str,
                stop_price=stop_price_str,
                priceProtect=True,
                reduceOnly=True,
                closePosition=False,
                workingType='CONTRACT_PRICE'
            )
            if not new_id:
                raise ValueError(f'Не удалось создать новый стоп ордер: {raw}')
            self.stop_order_id = str(new_id)
            try:
                self._write_stop_order_id_to_db(self.stop_order_id, stop_price=new_stop_price)
                try:
                    update_position_in_db(self.symbol, stop_loss_moved=True)
                except Exception:
                    pass
            except Exception as exc_upd:
                print(f'[warn] DB update failed for stop move {self.symbol}: {exc_upd}')
            if old_stop_id and old_stop_id != str(new_id):
                ok, res = self._cancel_order_flexible(old_stop_id)
                if ok:
                    print(f'[info] Cancelled old stop order {old_stop_id} for {self.symbol}')
                else:
                    print(f'[warn] Cancel old stop returned not-ok: {res}')
                    try:
                        self._cleanup_duplicate_protective_orders(keep_stop_id=str(new_id))
                    except Exception:
                        pass
            message = f'✅ Стоп-лосс пары {self.symbol} перемещён в avg-entry: {new_stop_price:.{pprec}f}'
            try:
                send_trade_notification(1901059519, message)
                send_trade_notification(self.user_id, message)
            except Exception:
                pass
        except Exception as e:
            error_msg = f'❗ Ошибка при обновлении стоп-лосса: {str(e)}'
            try:
                send_trade_notification(1901059519, error_msg)
            except Exception:
                pass
    def _get_current_quantity(self):
        try:
            pa = getattr(self, 'last_position_amt', None)
            if pa is not None:
                try:
                    q = abs(float(pa))
                    return 0 if q < 1e-12 else q
                except Exception:
                    pass
        except Exception:
            pass
        try:
            position = self.client.get_position_risk(symbol=self.symbol)
            position_data = next((p for p in position if float(p.get('positionAmt', 0)) != 0), {})
            quantity = float(position_data.get('positionAmt', 0))
            return abs(quantity) if abs(quantity) >= 1e-12 else 0
        except Exception:
            return 0
    def update_stop_loss_with_new_quantity(self):
        try:
            db_id = self._sync_stop_order_id_from_db()
            if db_id and str(getattr(self, 'stop_order_id', None)) != str(db_id):
                self.stop_order_id = str(db_id)
            old_stop_id = str(self.stop_order_id) if getattr(self, 'stop_order_id', None) else None
            current_stop_price = None
            try:
                if getattr(self, 'stop_order_id', None):
                    try:
                        current_order = self._query_order_flexible(self.stop_order_id)
                    except Exception:
                        current_order = None
                    if isinstance(current_order, dict):
                        sp = current_order.get('stopPrice') or current_order.get('triggerPrice')
                        if sp is not None:
                            current_stop_price = float(sp)
            except Exception:
                current_stop_price = None
            if current_stop_price is None:
                current_stop_price = float(self.current_entry_price)
            quantity = self._get_current_quantity()
            if quantity is None or quantity <= 0:
                raise ValueError(f'Некорректный quantity: {quantity}')
            qprec = int(self.quantity_precision) if self.quantity_precision is not None else 8
            pprec = int(self.price_precision) if self.price_precision is not None else 2
            kind, new_id, raw = create_stop_order(
                self.api_key,
                self.secret_key,
                self.client,
                symbol=self.symbol,
                side='SELL' if self.direction == 'LONG' else 'BUY',
                quantity=f'{abs(quantity):.{qprec}f}',
                stop_price=f'{current_stop_price:.{pprec}f}',
                priceProtect=True,
                reduceOnly=True,
                closePosition=False,
                workingType='CONTRACT_PRICE'
            )
            if not new_id:
                raise ValueError(f'Не удалось создать новый стоп ордер: {raw}')
            self.stop_order_id = str(new_id)
            try:
                self._write_stop_order_id_to_db(self.stop_order_id, stop_price=float(current_stop_price))
            except Exception:
                pass
            if old_stop_id and old_stop_id != str(new_id):
                ok, res = self._cancel_order_flexible(old_stop_id)
                if not ok:
                    try:
                        self._cleanup_duplicate_protective_orders(keep_stop_id=str(new_id))
                    except Exception:
                        pass
            message = f'✅ Стоп-лосс для {self.symbol} обновлён: Новый объем: {quantity:.{qprec}f}, Уровень стопа: {current_stop_price:.{pprec}f}'
            try:
                send_trade_notification(1901059519, message)
                send_trade_notification(self.user_id, message)
            except Exception:
                pass
        except Exception as e:
            error_msg = f'❗ Ошибка обновления стоп-лосса: {str(e)}'
            try:
                send_trade_notification(1901059519, error_msg)
            except Exception:
                pass
    def _is_any_tp_filled(self):
        all_order_ids = list(self.tp_order_ids) + list(self.dynamic_tp_order_ids)
        valid_order_ids = [oid for oid in all_order_ids if oid is not None and str(oid).strip() != '']
        for order_id in valid_order_ids:
            try:
                order = self.client.query_order(symbol=self.symbol, orderId=order_id)
                status = order.get('status')
                if status == 'FILLED':
                    avg_entry = float(self.current_entry_price)
                    executed_price = float(order.get('avgPrice', 0) or 0)
                    is_profitable = executed_price > avg_entry if self.direction == 'LONG' else executed_price < avg_entry
                    if is_profitable:
                        self.filled_tp_count += 1
                        try:
                            if not self.first_tp_filled and (self.first_tp_order_id is None or str(self.first_tp_order_id) == str(order_id)):
                                self.first_tp_order_id = str(order_id)
                        except Exception:
                            pass
                        return True
            except Exception as e:
                print(f'[TrailingStop] Error querying TP order {order_id} for {self.symbol}: {e}')
                continue
        return False
    def is_position_active(self):
        try:
            position = self.client.get_position_risk(symbol=self.symbol)
            pos_data = next((p for p in position if float(p.get('positionAmt', 0)) != 0), {})
            return float(pos_data.get('positionAmt', 0)) != 0
        except Exception as e:
            print(f'[WARN] Ошибка проверки позиции {self.symbol}: {e}, помечаем как unknown')
            return None
    def _handle_position_closed(self, realized_pnl=None):
        if self.position_closed:
            return
        try:
            rp_val = None
            if realized_pnl is not None:
                rp_val = float(realized_pnl)
            elif self.last_realized_pnl is not None:
                rp_val = float(self.last_realized_pnl)
            else:
                try:
                    rp_val = float(self.get_realized_pnl_by_order() or 0.0)
                except Exception:
                    rp_val = 0.0
        except Exception:
            rp_val = 0.0
        try:
            with db_tx() as conn:
                cur = conn.cursor()
                cur.execute('INSERT OR IGNORE INTO symbol_cooldowns(symbol, signals_missed, consecutive_losses) VALUES (?,0,0)', (self.symbol,))
                if rp_val < 0:
                    cur.execute('SELECT signals_missed, consecutive_losses FROM symbol_cooldowns WHERE symbol = ?', (self.symbol,))
                    row = cur.fetchone()
                    curr_losses = int(row[1]) if row and row[1] is not None else 0
                    curr_losses += 1
                    cur.execute('UPDATE symbol_cooldowns SET consecutive_losses = ? WHERE symbol = ?', (curr_losses, self.symbol))
                    if curr_losses >= 2:
                        cur.execute('UPDATE symbol_cooldowns SET signals_missed = 0 WHERE symbol = ?', (self.symbol,))
                        try:
                            send_trade_notification(1901059519, f'[COOLDOWN_START] {self.symbol}: закрытие в убытке {rp_val:.2f} — 2 подряд убытка, начинаем пропускать 2 сигнала')
                        except Exception:
                            pass
                    else:
                        try:
                            send_trade_notification(1901059519, f'[LOSS_COUNT] {self.symbol}: подряд убытков {curr_losses}/2 — cooldown не активирован')
                        except Exception:
                            pass
                else:
                    cur.execute('DELETE FROM symbol_cooldowns WHERE symbol = ?', (self.symbol,))
                    try:
                        send_trade_notification(1901059519, f'[COOLDOWN_CLEAR] {self.symbol}: закрытие в плюсе {rp_val:.2f} — сигналы разрешены, счётчик убытков сброшен')
                    except Exception:
                        pass
                try:
                    cur.execute('DELETE FROM positions WHERE symbol = ?', (self.symbol,))
                except Exception:
                    try:
                        cur.execute('UPDATE positions SET quantity = 0, add_count = 0 WHERE symbol = ?', (self.symbol,))
                    except Exception:
                        pass
                try:
                    conn.commit()
                except Exception:
                    pass
        except Exception:
            pass
        try:
            with active_trailing_threads_lock:
                active_trailing_threads.pop(self.symbol, None)
        except Exception:
            pass
        try:
            self.cleanup_after_stop_loss()
        except Exception:
            pass
        self.position_closed = True
        self.active = False
        try:
            if hasattr(self, 'update_losses_after_position_closed'):
                try:
                    self.update_losses_after_position_closed(rp_val)
                except Exception:
                    pass
        except Exception:
            pass
        try:
            delete_position_from_db(self.symbol)
        except Exception:
            try:
                update_position_in_db(self.symbol, stop_loss_moved=self.stop_loss_moved, first_tp_filled=self.first_tp_filled)
            except Exception:
                pass
    def check_first_tp_filled(self):
        try:
            if not self.first_tp_order_id:
                return False
            order = self.client.query_order(symbol=self.symbol, orderId=self.first_tp_order_id)
            if order and order.get('status') == 'FILLED':
                if not self.first_tp_filled:
                    self.first_tp_filled = True
                    try:
                        update_position_in_db(self.symbol, first_tp_filled=True)
                    except Exception:
                        pass
                    try:
                        send_trade_notification(1901059519, f'✅ Первый TP подтверждён (thread) для {self.symbol}, orderId={self.first_tp_order_id}')
                    except Exception:
                        pass
                    try:
                        send_trade_notification(self.user_id, f'✅ Первый TP подтверждён для {self.symbol}')
                    except Exception:
                        pass
                return True
        except Exception as e:
            print(f'Ошибка проверки первого TP для {self.symbol}: {str(e)}')
        return False
    def get_realized_pnl_by_order(self):
        try:
            client = get_trading_client(self.api_key, self.secret_key)
            current_time = int(time.time() * 1000)
            start_time = current_time - 3 * 24 * 60 * 60 * 1000
            trades = safe_binance_call(client.get_income_history, symbol=self.symbol, startTime=start_time, endTime=current_time, limit=1)
            pnl_trades = [float(t.get('income', 0)) for t in trades or [] if t.get('incomeType') == 'REALIZED_PNL' and t.get('symbol') == self.symbol]
            total_pnl = sum(pnl_trades)
            if abs(total_pnl) < 1e-08:
                trades = safe_binance_call(client.get_income_history, startTime=start_time, endTime=current_time, limit=500)
                pnl_trades = [float(t.get('income', 0)) for t in trades or [] if t.get('incomeType') == 'REALIZED_PNL' and t.get('symbol') == self.symbol]
                total_pnl = sum(pnl_trades)
            try:
                print(f'[DEBUG] get_realized_pnl {self.symbol}: {total_pnl}')
            except Exception:
                pass
            return total_pnl
        except Exception as e:
            print(f'Ошибка получения realized PNL для {self.symbol}: {str(e)}')
            return 0.0
    def update_losses_after_position_closed(self, realized_pnl):
        try:
            if realized_pnl is None:
                rp = 0.0
            else:
                try:
                    rp = float(realized_pnl)
                except Exception:
                    rp = 0.0
            reduction = 0.0
            new_total_loss = None
            current_total = 0.0
            try:
                with db_tx() as conn:
                    cur = conn.cursor()
                    cur.execute('SELECT total_loss FROM global_loss WHERE id = 1')
                    row = cur.fetchone()
                    current_total = float(row[0] or 0.0) if row and row[0] is not None else 0.0
                    if rp < 0:
                        add_amount = abs(rp)
                        new_total_loss = current_total + float(add_amount)
                        cur.execute('INSERT OR REPLACE INTO global_loss (id, total_loss) VALUES (1, ?)', (new_total_loss,))
                        conn.commit()
                        try:
                            send_trade_notification(1901059519, f'[GLOBAL_LOSS_ADDED] Позиция {self.symbol} закрыта с убытком {add_amount:.2f}. New total_loss={new_total_loss:.2f}')
                        except Exception:
                            pass
                        reduction = 0.0
                    else:
                        reduce_amount = float(rp)
                        reduction = min(reduce_amount, current_total)
                        new_total_loss = max(0.0, current_total - reduce_amount)
                        cur.execute('INSERT OR REPLACE INTO global_loss (id, total_loss) VALUES (1, ?)', (new_total_loss,))
                        conn.commit()
                        try:
                            send_trade_notification(1901059519, f'[GLOBAL_LOSS_REDUCED] Позиция {self.symbol} закрыта с профитом {reduce_amount:.2f}. New total_loss={new_total_loss:.2f}')
                        except Exception:
                            pass
            except Exception as e:
                try:
                    print(f'[WARN] update_losses_after_position_closed DB error: {e}')
                except Exception:
                    pass
            try:
                if 'recalculate_losses' in globals():
                    recalculate_losses()
            except Exception:
                pass
            try:
                with db_tx() as conn:
                    cur = conn.cursor()
                    cur.execute('SELECT total_loss FROM global_loss WHERE id = 1')
                    row = cur.fetchone()
                    new_total_loss = float(row[0] or 0.0) if row and row[0] is not None else new_total_loss if new_total_loss is not None else current_total
                    cur.execute('SELECT COUNT(*) FROM trading_pairs WHERE is_active = 1')
                    active_pairs_count = cur.fetchone()[0] or 0
                    loss_per_pair = new_total_loss / active_pairs_count if active_pairs_count > 0 else 0.0
            except Exception:
                new_total_loss = float(new_total_loss) if new_total_loss is not None else float(current_total)
                loss_per_pair = 0.0
            try:
                msg = f'📈 Прибыль по сделке: {rp:.2f} USDT\n📉 Отбито из общего убытка: {reduction:.2f} USDT\n📊 Новый общий убыток: {new_total_loss:.2f} USDT\n🔄 Текущий убыток на пару: {loss_per_pair:.2f} USDT'
                try:
                    send_trade_notification(1901059519, msg)
                except Exception:
                    pass
                try:
                    send_trade_notification(self.user_id, msg)
                except Exception:
                    pass
            except Exception:
                pass
        except Exception as e:
            try:
                print(f'[ERROR] update_losses_after_position_closed fatal: {e}')
            except Exception:
                pass
    def get_realized_pnl_by_order_DEPRECATED(self):
        try:
            client = get_trading_client(self.api_key, self.secret_key)
            current_time = int(time.time() * 1000)
            start_time = current_time - 24 * 60 * 60 * 1000
            trades = safe_binance_call(client.get_income_history, symbol=self.symbol, startTime=start_time, endTime=current_time, limit=1)
            pnl_trades = [float(t.get('income', 0)) for t in trades or [] if t.get('incomeType') == 'REALIZED_PNL' and t.get('symbol') == self.symbol]
            total_pnl = sum(pnl_trades)
            return total_pnl
        except Exception:
            return 0.0
    def cleanup_after_stop_loss(self):
        try:
            cancel_all_orders(self.client, self.symbol, self.api_key, self.secret_key)
        except Exception as e:
            print(f'Ошибка очистки ордеров: {str(e)}')
def get_current_stop_order_id_from_db(symbol):
    try:
        with DB_LOCK:
            cursor.execute('SELECT stop_order_id FROM positions WHERE symbol = ?', (symbol,))
            row = cursor.fetchone()
            if row and row[0] is not None:
                return str(row[0])
    except Exception as e:
        print(f'[get_current_stop_order_id_from_db] DB error for {symbol}: {e}')
    return None
def cancel_all_orders(client, symbol, api_key, secret_key):
    """Cancel all open normal orders AND all open algo/conditional orders for a symbol.
    Returns True if the normal allOpenOrders call succeeded; algo cancellation is best-effort.
    """
    ok_normal = False
    try:
        base_url = _BINANCE_FAPI_REST_BASE
        endpoint = '/fapi/v1/allOpenOrders'
        params = {'symbol': symbol, 'timestamp': binance_now_ms(), 'recvWindow': 60000}
        query_string = urlencode(params)
        signature = hmac.new(secret_key.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()
        params['signature'] = signature
        headers = {'X-MBX-APIKEY': api_key}
        response = _http_request('DELETE', base_url + endpoint, params=params, headers=headers, timeout=15)
        if response.status_code == 200:
            ok_normal = True
            print(f'[INFO] Все ордера (normal) для {symbol} отменены')
            if symbol in active_orders:
                active_orders[symbol] = []
        else:
            print(f'[ERROR] Не удалось отменить все normal ордера: {response.text}')
            ok_normal = False
    except Exception as e:
        print(f'[ERROR] Неизвестная ошибка при отмене normal ордеров: {str(e)}')
        ok_normal = False
    try:
        ok2, res2 = _cancel_all_algo_open_orders(api_key, secret_key, symbol)
        if ok2:
            print(f'[INFO] Algo ордера для {symbol} отменены')
        else:
            print(f'[WARN] Не удалось отменить algo ордера для {symbol}: {res2}')
    except Exception as _e:
        print(f'[WARN] Ошибка при отмене algo ордеров для {symbol}: {_e}')
    return ok_normal
def ensure_cancel_all_orders(client, symbol, api_key=None, secret_key=None, verify_retries=3, verify_delay=1.0, **kwargs):
    """Cancel + verify no open orders left. Checks normal open orders, and (if creds provided) algo open orders too.
    Keeps checks minimal: algo verification is only done when normal open orders are already empty.
    """
    symbol = (symbol or '').upper().replace('/', '')
    retries = int(kwargs.get('retries', verify_retries))
    delay = float(kwargs.get('delay', verify_delay))
    for attempt in range(1, retries + 1):
        try:
            ok = cancel_all_orders(client, symbol, api_key, secret_key)
        except Exception:
            ok = False
        try:
            time.sleep(delay)
        except Exception:
            pass
        open_orders = None
        try:
            methods_to_try = [
                ('get_open_orders', {'symbol': symbol}),
                ('open_orders', {'symbol': symbol}),
                ('get_open_orders', {'symbol': symbol, 'limit': 100}),
            ]
            for mname, params in methods_to_try:
                fn = getattr(client, mname, None)
                if callable(fn):
                    try:
                        try:
                            open_orders = safe_api_call(fn, **params, retries=2)
                        except TypeError:
                            open_orders = safe_api_call(fn, retries=2)
                        break
                    except Exception:
                        open_orders = None
                        continue
        except Exception:
            open_orders = None
        normal_empty = False
        if open_orders is None:
            if ok:
                try:
                    send_trade_notification(1901059519, f'[ENSURE_CANCEL] assumed success (no open_orders response) symbol={symbol} attempt={attempt}')
                except Exception:
                    pass
                return True
        else:
            if isinstance(open_orders, dict):
                open_orders = open_orders.get('orders') or open_orders.get('data') or open_orders.get('result') or []
            if isinstance(open_orders, (list, tuple)) and len(open_orders) == 0:
                normal_empty = True
        if not normal_empty:
            try:
                send_trade_notification(1901059519, f"[ENSURE_CANCEL_RETRY] symbol={symbol} attempt={attempt} ok={ok} open_orders_count={(len(open_orders) if isinstance(open_orders, (list, tuple)) else 'unknown')}")
            except Exception:
                pass
            delay = delay * 2
            continue
        if api_key and secret_key:
            try:
                aok, aresp = _get_algo_open_orders(api_key, secret_key, symbol)
                if aok:
                    aorders = aresp
                    if isinstance(aresp, dict):
                        aorders = aresp.get('orders') or aresp.get('data') or aresp.get('result') or []
                    if isinstance(aorders, (list, tuple)) and len(aorders) == 0:
                        try:
                            send_trade_notification(1901059519, f'[ENSURE_CANCEL] confirmed no normal+algo open orders for {symbol} attempt={attempt}')
                        except Exception:
                            pass
                        return True
                    try:
                        _cancel_all_algo_open_orders(api_key, secret_key, symbol)
                    except Exception:
                        pass
            except Exception:
                pass
        if not (api_key and secret_key):
            try:
                send_trade_notification(1901059519, f'[ENSURE_CANCEL] confirmed no open orders for {symbol} attempt={attempt}')
            except Exception:
                pass
            return True
        delay = delay * 2
    try:
        send_trade_notification(1901059519, f'[ENSURE_CANCEL_FAIL] failed to confirm cancellation for {symbol} after {retries} attempts')
    except Exception:
        pass
    return False
def _signed_params(params, secret_key):
    p = params.copy()
    items = []
    for k in sorted(p.keys()):
        v = p[k]
        if isinstance(v, bool):
            v = "true" if v else "false"
        items.append((k, str(v)))
    qs = urlencode(items, doseq=True)
    signature = hmac.new(secret_key.encode("utf-8"), qs.encode("utf-8"), hashlib.sha256).hexdigest()
    p["signature"] = signature
    return p
def _get_signed(url, api_key, secret_key, params, timeout=15):
    params = _signed_params(params, secret_key)
    headers = {"X-MBX-APIKEY": api_key}
    r = _http_request("GET", url, params=params, headers=headers, timeout=timeout)
    try:
        j = r.json()
    except Exception:
        j = {"http_status": r.status_code, "text": r.text}
    try:
        ra = r.headers.get('Retry-After')
        if ra is not None and isinstance(j, dict):
            j.setdefault('retryAfter', ra)
    except Exception:
        pass
    return (r.status_code, j)
def _post_signed(url, api_key, secret_key, params, timeout=15):
    params = params.copy()
    items = []
    for k in sorted(params.keys()):
        v = params[k]
        if isinstance(v, bool):
            v = 'true' if v else 'false'
        items.append((k, str(v)))
    qs = urlencode(items, doseq=True)
    signature = hmac.new(secret_key.encode('utf-8'), qs.encode('utf-8'), hashlib.sha256).hexdigest()
    params['signature'] = signature
    headers = {'X-MBX-APIKEY': api_key}
    r = _http_request('POST', url, params=params, headers=headers, timeout=timeout)
    try:
        j = r.json()
    except Exception:
        j = {'http_status': r.status_code, 'text': r.text}
    try:
        ra = r.headers.get('Retry-After')
        if ra is not None and isinstance(j, dict):
            j.setdefault('retryAfter', ra)
    except Exception:
        pass
    return (r.status_code, j)
def _delete_signed(url, api_key, secret_key, params, timeout=15):
    params = params.copy()
    items = []
    for k in sorted(params.keys()):
        v = params[k]
        if isinstance(v, bool):
            v = 'true' if v else 'false'
        items.append((k, str(v)))
    qs = urlencode(items, doseq=True)
    signature = hmac.new(secret_key.encode('utf-8'), qs.encode('utf-8'), hashlib.sha256).hexdigest()
    params['signature'] = signature
    headers = {'X-MBX-APIKEY': api_key}
    r = _http_request('DELETE', url, params=params, headers=headers, timeout=timeout)
    try:
        j = r.json()
    except Exception:
        j = {'http_status': r.status_code, 'text': r.text}
    try:
        ra = r.headers.get('Retry-After')
        if ra is not None and isinstance(j, dict):
            j.setdefault('retryAfter', ra)
    except Exception:
        pass
    return (r.status_code, j)
def _http_cancel_order(api_key, secret_key, symbol, order_identifier, timeout=15):
    try:
        kind, resp = cancel_order_flexible(api_key, secret_key, symbol, order_identifier, timeout=timeout)
        return (True, resp)
    except Exception as e:
        return (False, str(e))
def place_multiple_orders(api_key, secret_key, orders, base_rest=None, recvWindow=5000, timeout=15):
    base_rest = (base_rest or _BINANCE_FAPI_REST_BASE).rstrip('/')
    """Place up to 5 orders via POST /fapi/v1/batchOrders.
    Returns list of results (each element is success object or error object).
    """
    if not orders:
        return []
    if len(orders) > 5:
        raise ValueError("batchOrders max 5 orders per request")
    url = base_rest.rstrip('/') + '/fapi/v1/batchOrders'
    params = {
        'batchOrders': json.dumps(orders, separators=(',', ':')),
        'recvWindow': int(recvWindow),
        'timestamp': binance_now_ms()
    }
    status, payload = _post_signed(url, api_key, secret_key, params, timeout=timeout)
    if status >= 400:
        raise Exception(f"batchOrders HTTP {status}: {payload}")
    if not isinstance(payload, list):
        raise Exception(f"batchOrders unexpected response: {payload}")
    return payload
def _chunked(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i+n]
def _place_algo_order(api_key, secret_key, symbol, side, order_type, trigger_price, quantity=None, workingType='CONTRACT_PRICE', priceProtect=False, reduceOnly=False, closePosition=False, clientAlgoId=None, recvWindow=60000, timeout=15):
    try:
        tc = get_trading_client(api_key, secret_key)
        ws_params = {
            "algoType": "CONDITIONAL",
            "symbol": symbol,
            "side": side,
            "type": order_type,
            "triggerPrice": str(trigger_price),
            "workingType": workingType,
            "priceProtect": ("TRUE" if priceProtect else "FALSE"),
            "newOrderRespType": "RESULT",
        }
        if closePosition:
            ws_params["closePosition"] = "true"
        else:
            ws_params["closePosition"] = "false"
            ws_params["reduceOnly"] = ("true" if reduceOnly else "false")
            if quantity is not None:
                ws_params["quantity"] = str(quantity)
        if clientAlgoId:
            ws_params["clientAlgoId"] = clientAlgoId
        ws_resp = tc.algo_order_place(**ws_params)
        if ws_resp:
            return ws_resp
    except Exception:
        pass
    base = _BINANCE_FAPI_REST_BASE
    endpoint = '/fapi/v1/algoOrder'
    url = base + endpoint
    params = {
        'algoType': 'CONDITIONAL',
        'symbol': symbol,
        'side': side,
        'type': order_type,
        'triggerPrice': str(trigger_price),
        'workingType': workingType,
        'priceProtect': 'TRUE' if priceProtect else 'FALSE',
        'timestamp': binance_now_ms(),
        'recvWindow': recvWindow,
    }
    if closePosition:
        params['closePosition'] = 'true'
    else:
        params['closePosition'] = 'false'
        params['reduceOnly'] = 'true' if reduceOnly else 'false'
        if quantity is not None:
            params['quantity'] = str(quantity)
    if clientAlgoId:
        params['clientAlgoId'] = clientAlgoId
    for _attempt in range(1, 4):
        code, resp = _post_signed(url, api_key, secret_key, params, timeout=timeout)
        if code == 200:
            return resp
        ra = None
        try:
            if isinstance(resp, dict):
                ra = resp.get('retryAfter')
                if ra is None:
                    ra = resp.get('retry-after')
        except Exception:
            ra = None
        if code in (418, 429):
            try:
                s = float(ra) if ra is not None else (60.0 if code == 418 else 10.0)
            except Exception:
                s = 60.0 if code == 418 else 10.0
            time.sleep(max(1.0, min(180.0, s)))
            continue
        try:
            if isinstance(resp, dict) and int(resp.get('code') or 0) == -1021:
                force_time_sync()
                time.sleep(0.2)
                continue
        except Exception:
            pass
        break
    raise Exception(f'AlgoOrder placement failed HTTP {code}: {resp}')
def _cancel_algo_order(api_key, secret_key, algoId=None, clientAlgoId=None, recvWindow=60000, timeout=15):
    try:
        tc = get_trading_client(api_key, secret_key)
        ws_params = {}
        if algoId is not None:
            ws_params["algoId"] = int(algoId) if str(algoId).isdigit() else algoId
        if clientAlgoId is not None:
            ws_params["clientAlgoId"] = clientAlgoId
        if ws_params:
            ws_resp = tc.algo_order_cancel(**ws_params)
            if ws_resp:
                return ws_resp
    except Exception:
        pass
    base = _BINANCE_FAPI_REST_BASE
    endpoint = '/fapi/v1/algoOrder'
    url = base + endpoint
    params = {'timestamp': binance_now_ms(), 'recvWindow': recvWindow}
    if algoId is not None:
        try:
            params['algoId'] = int(algoId)
        except Exception:
            params['algoId'] = algoId
    elif clientAlgoId is not None:
        params['clientAlgoId'] = clientAlgoId
    else:
        raise ValueError('algoId or clientAlgoId required for cancel')
    for _attempt in range(1, 4):
        code, resp = _delete_signed(url, api_key, secret_key, params, timeout=timeout)
        if code == 200:
            return resp
        ra = None
        try:
            if isinstance(resp, dict):
                ra = resp.get('retryAfter')
                if ra is None:
                    ra = resp.get('retry-after')
        except Exception:
            ra = None
        if code in (418, 429):
            try:
                s = float(ra) if ra is not None else (60.0 if code == 418 else 10.0)
            except Exception:
                s = 60.0 if code == 418 else 10.0
            time.sleep(max(1.0, min(180.0, s)))
            continue
        try:
            if isinstance(resp, dict) and int(resp.get('code') or 0) == -1021:
                force_time_sync()
                time.sleep(0.2)
                continue
        except Exception:
            pass
        break
    raise Exception(f'AlgoOrder cancel failed HTTP {code}: {resp}')
def _cancel_all_algo_open_orders(api_key, secret_key, symbol, recvWindow=60000, timeout=15):
    base = _BINANCE_FAPI_REST_BASE
    endpoint = '/fapi/v1/algoOpenOrders'
    url = base + endpoint
    params = {'symbol': symbol, 'timestamp': binance_now_ms(), 'recvWindow': recvWindow}
    code, resp = _delete_signed(url, api_key, secret_key, params, timeout=timeout)
    if code == 200:
        return (True, resp)
    if isinstance(resp, dict) and resp.get('code') in (-2011, -2013):
        return (True, resp)
    return (False, resp)
def _get_algo_open_orders(api_key, secret_key, symbol, recvWindow=60000, timeout=15):
    """Get current open algo/conditional orders for a symbol (best-effort)."""
    base = _BINANCE_FAPI_REST_BASE
    endpoint = '/fapi/v1/openAlgoOrders'
    url = base + endpoint
    params = {'symbol': symbol, 'timestamp': binance_now_ms(), 'recvWindow': recvWindow}
    code, resp = _get_signed(url, api_key, secret_key, params, timeout=timeout)
    if code != 200:
        return (False, resp)
    return (True, resp)
def create_stop_order(api_key, secret_key, client, symbol, side, quantity, stop_price, priceProtect=True, reduceOnly=True, closePosition=False, workingType='CONTRACT_PRICE'):
    try:
        kwargs = dict(
            symbol=symbol,
            side=side,
            type='STOP_MARKET',
            stopPrice=str(stop_price),
            priceProtect=priceProtect,
            workingType=workingType,
        )
        if closePosition:
            kwargs['closePosition'] = True
        else:
            if quantity is not None:
                kwargs['quantity'] = str(quantity)
            kwargs['reduceOnly'] = reduceOnly
        tc = get_trading_client(api_key, secret_key)
        res = tc.new_order(**kwargs)
        oid = None
        try:
            oid = _extract_order_id(res)
        except Exception:
            try:
                oid = res.get('orderId')
            except Exception:
                oid = None
        return ('order', str(oid) if oid is not None else None, res)
    except Exception as e:
        text = str(e)
        if 'Algo Order' in text or 'Please use the Algo Order API' in text or '-4120' in text or ('Order type not supported' in text):
            algo_resp = _place_algo_order(api_key, secret_key, symbol=symbol, side=side, order_type='STOP_MARKET', trigger_price=stop_price, quantity=None if closePosition else quantity, priceProtect=priceProtect, reduceOnly=reduceOnly, closePosition=closePosition, workingType=workingType)
            algo_id = algo_resp.get('algoId') or algo_resp.get('algoID') or algo_resp.get('algo_id')
            if algo_id is None:
                raise Exception(f'No algoId in response: {algo_resp}')
            return ('algo', f'algo:{algo_id}', algo_resp)
        else:
            raise
def cancel_order_flexible(api_key, secret_key, symbol, order_identifier, timeout=15):
    tc = get_trading_client(api_key, secret_key)
    if isinstance(order_identifier, str) and order_identifier.startswith('algo:'):
        algoid = order_identifier.split(':', 1)[1]
        resp = _cancel_algo_order(api_key, secret_key, algoId=algoid, timeout=timeout)
        return ('algo', resp)
    params = {'symbol': symbol}
    try:
        if isinstance(order_identifier, (int, float)) or (isinstance(order_identifier, str) and str(order_identifier).isdigit()):
            params['orderId'] = int(order_identifier)
        else:
            params['origClientOrderId'] = str(order_identifier)
        resp = tc.cancel_order(**params)
        return ('order', resp)
    except Exception as e:
        msg = str(e)
        if '-4120' in msg or 'STOP_ORDER_SWITCH_ALGO' in msg or 'Algo Order' in msg or 'Please use the Algo Order API' in msg:
            try:
                resp = _cancel_algo_order(api_key, secret_key, algoId=order_identifier, timeout=timeout)
                return ('algo', resp)
            except Exception as e2:
                raise Exception(f'Cancel order failed: {e}; algo cancel failed: {e2}')
        raise
def handle_main_signal(signal):
    logging.info(f'[handle_main_signal] Received signal: {signal}')
    try:
        user_id = int(signal.get('user_id', 6189545928))
        symbol = str(signal.get('symbol', '')).upper().replace('/', '')
        direction = str(signal.get('direction', 'LONG')).upper()
        entry_price = float(signal.get('entry_price', 0) or 0)
        new_stop = None
        tp_in = signal.get('take_profit') or {}
        tp_levels = {}
        if isinstance(tp_in, dict):
            for k, v in tp_in.items():
                try:
                    tp_levels[k] = float(v)
                except Exception:
                    pass
        elif isinstance(tp_in, (list, tuple)):
            for i, v in enumerate(tp_in):
                try:
                    tp_levels[f'tp{i + 1}'] = float(v)
                except Exception:
                    pass
        price_precision = 2
        qty_precision = 3
        symbol_info = None
        api_key = None
        secret_key = None
        try:
            send_trade_notification(1901059519, f'[PARSE] user_id={user_id} symbol={symbol} dir={direction} entry={entry_price} stop={new_stop} tps={tp_levels}')
        except Exception:
            pass
    except Exception as e:
        logging.exception('handle_main_signal: invalid payload')
        try:
            send_trade_notification(1901059519, f'[ERROR_PARSE] {e}')
        except Exception:
            pass
        return
    try:
        with db_tx() as conn:
            cur = conn.cursor()
            cur.execute('INSERT OR IGNORE INTO symbol_cooldowns(symbol, signals_missed, consecutive_losses) VALUES (?,0,0)', (symbol,))
            cur.execute('SELECT signals_missed, consecutive_losses FROM symbol_cooldowns WHERE symbol = ?', (symbol,))
            row = cur.fetchone()
            signals_missed = int(row[0]) if row and row[0] is not None else 0
            consecutive_losses = int(row[1]) if row and len(row) > 1 and (row[1] is not None) else 0
            if consecutive_losses >= 2:
                if signals_missed < 2:
                    cur.execute('UPDATE symbol_cooldowns SET signals_missed = signals_missed + 1 WHERE symbol = ?', (symbol,))
                    conn.commit()
                    try:
                        if 'send_trade_notification' in globals():
                            send_trade_notification(1901059519, f'SKIP_SIGNAL {symbol} ({signals_missed + 1}/2)')
                    except Exception:
                        pass
                    return
                else:
                    cur.execute('UPDATE symbol_cooldowns SET consecutive_losses = 0, signals_missed = 0 WHERE symbol = ?', (symbol,))
                    conn.commit()
    except Exception as e:
        logging.exception(f'[COOLDOWN_CHECK_ERR] {e}')
    def _get_current_stop_order_id_db(symb):
        try:
            with db_tx() as conn:
                cur = conn.cursor()
                cur.execute('SELECT stop_order_id FROM positions WHERE symbol = ?', (symb,))
                r = cur.fetchone()
                if r and r[0] is not None:
                    return str(r[0])
        except Exception as e:
            logging.debug(f'[get_stop_db_err] {e}')
        return None
    def _parse_qty_from_thread_message(thread_msg):
        if not thread_msg:
            return None
        try:
            m = re.search('\\|\\s*Qty:\\s*([0-9]+(?:\\.[0-9]+)?)', str(thread_msg))
            if m:
                return float(m.group(1))
        except Exception:
            pass
        return None
    try:
        with db_tx() as conn:
            cur = conn.cursor()
            cur.execute('SELECT quantity, current_entry_price, direction, add_count, entry_order_id, stop_order_id, tp_order_ids, recovery_order_id FROM positions WHERE symbol = ?', (symbol,))
            existing_pos_row = cur.fetchone()
    except Exception as e:
        logging.exception(f'[POSITIONS_READ_ERR] {e}')
        existing_pos_row = None
    existing_qty = 0.0
    existing_direction = None
    existing_add_count = 0
    existing_entry_id = None
    existing_stop_id = None
    existing_tp_ids = []
    existing_recovery_id = None
    is_existing_pos_in_db = existing_pos_row is not None
    existing_current_entry = None
    if is_existing_pos_in_db:
        try:
            existing_qty = float(existing_pos_row[0] or 0.0)
        except Exception:
            existing_qty = 0.0
        existing_current_entry = existing_pos_row[1]
        existing_direction = existing_pos_row[2]
        existing_add_count = existing_pos_row[3] or 0
        existing_entry_id = existing_pos_row[4]
        existing_stop_id = existing_pos_row[5]
        try:
            existing_tp_ids = json.loads(existing_pos_row[6]) if existing_pos_row[6] else []
        except Exception:
            existing_tp_ids = []
        existing_recovery_id = existing_pos_row[7]
    pos_db = None
    try:
        pos_db = get_position_from_db(symbol)
    except Exception:
        pos_db = None
    try:
        if is_existing_pos_in_db:
            try:
                cursor.execute('SELECT stop_loss_moved, first_tp_filled, user_id FROM positions WHERE symbol = ?', (symbol,))
                flag_row = cursor.fetchone()
                stop_moved_flag = bool(flag_row[0]) if flag_row and flag_row[0] is not None else False
                first_tp_flag = bool(flag_row[1]) if flag_row and flag_row[1] is not None else False
                pos_user_for_check = flag_row[2] if flag_row and len(flag_row) > 2 else user_id
            except Exception:
                stop_moved_flag = False
                first_tp_flag = False
                pos_user_for_check = user_id
            if stop_moved_flag or first_tp_flag:
                active_on_exchange = None
                try:
                    if not (api_key and secret_key):
                        cursor.execute('SELECT api_key, secret_key FROM users WHERE user_id = ?', (pos_user_for_check,))
                        keys = cursor.fetchone()
                        if keys and keys[0] and keys[1]:
                            api_key, secret_key = (keys[0], keys[1])
                    if api_key and secret_key:
                        client_check = get_trading_client(api_key, secret_key)
                        pr = safe_api_call(client_check.get_position_risk, symbol=symbol)
                        if pr:
                            p = next((p for p in pr if p.get('symbol') == symbol), None)
                            if p:
                                try:
                                    pos_amt_raw = p.get('positionAmt')
                                    pos_amt = float(pos_amt_raw or 0)
                                    active_on_exchange = abs(pos_amt) > 1e-08
                                except Exception:
                                    active_on_exchange = True
                    else:
                        active_on_exchange = True
                except Exception as e:
                    print(f'[BLOCK_CHECK_ERR] exchange check error for {symbol}: {e}')
                    active_on_exchange = None
                if active_on_exchange is True:
                    send_trade_notification(1901059519, f'[SKIP_NEW_POSITION] {symbol}: новая позиция заблокирована (stop_loss_moved={stop_moved_flag}, first_tp_filled={first_tp_flag})')
                    return
                if active_on_exchange is None:
                    send_trade_notification(1901059519, f'[SKIP_NEW_POSITION_UNSURE] {symbol}: блокировка по локальным флагам (stop_loss_moved={stop_moved_flag}, first_tp_filled={first_tp_flag}), exchange-check не подтвердил/ошибка')
                    return
    except Exception as e:
        print(f'[BLOCK_NEW_ENTRY_CHECK] unexpected error for {symbol}: {e}')
    try:
        s_in = signal.get('stop_loss')
        stop_levels = []
        if s_in is None:
            stop_levels = []
        elif isinstance(s_in, dict):
            for v in s_in.values():
                try:
                    stop_levels.append(float(v))
                except Exception:
                    continue
        elif isinstance(s_in, (list, tuple)):
            for v in s_in:
                try:
                    stop_levels.append(float(v))
                except Exception:
                    continue
        else:
            try:
                stop_levels = [float(s_in)]
            except Exception:
                stop_levels = []
        if stop_levels:
            try:
                new_stop = max(stop_levels, key=lambda s: abs(float(s) - float(entry_price)))
                send_trade_notification(1901059519, f'[STOP_CHOSEN] {symbol}: выбран самый удалённый стоп {new_stop}')
            except Exception as e:
                new_stop = stop_levels[0]
                print(f'[STOP_CHOOSE_ERR] {symbol}: {e} — взяли первый из списка')
        else:
            new_stop = None
    except Exception as e:
        new_stop = None
        print(f'[STOP_PARSE_ERR] error parsing stop for {symbol}: {e}')
    is_position_active_on_exchange = False
    if is_existing_pos_in_db:
        with db_tx() as conn:
            cur = conn.cursor()
            cursor.execute('SELECT api_key, secret_key FROM users WHERE user_id = ?', (user_id,))
            keys = cursor.fetchone()
        if keys and keys[0] and keys[1]:
            api_key, secret_key = (keys[0], keys[1])
            try:
                client_for_check = get_trading_client(api_key, secret_key)
                pos_risk = safe_api_call(client_for_check.get_position_risk, symbol=symbol)
                if pos_risk:
                    current_pos = next((p for p in pos_risk if p.get('symbol') == symbol), None)
                    if current_pos:
                        pos_amt_raw = current_pos.get('positionAmt')
                        if pos_amt_raw is not None:
                            try:
                                pos_amt = float(pos_amt_raw)
                                if abs(pos_amt) > 1e-08:
                                    is_position_active_on_exchange = True
                            except ValueError:
                                logging.warning(f'[handle_main_signal] Could not parse positionAmt for {symbol}: {pos_amt_raw}')
            except Exception as e:
                logging.error(f'[handle_main_signal] Error checking position on exchange for {symbol}: {e}')
        else:
            logging.warning(f'[handle_main_signal] No API keys found for user {user_id}, cannot verify position on exchange for {symbol}. Assuming DB state is correct.')
    is_new_entry = not is_position_active_on_exchange or existing_direction != direction
    is_add_position = is_position_active_on_exchange and existing_direction == direction
    is_flip = is_position_active_on_exchange and existing_direction != direction
    if is_flip:
        send_trade_notification(1901059519, f'[FLIP_DETECTED] Flipping position for {symbol}. Existing direction: {existing_direction}, New direction: {direction}.')
        with active_trailing_threads_lock:
            existing_thread = active_trailing_threads.get(symbol)
        try:
            with db_tx() as conn:
                cur = conn.cursor()
                cur.execute('SELECT signals_missed, consecutive_losses FROM symbol_cooldowns WHERE symbol = ?', (symbol,))
                row = cur.fetchone()
                current_signals_missed = int(row[0]) if row and row[0] is not None else 2
                current_consecutive_losses = int(row[1]) if row and len(row) > 1 and (row[1] is not None) else 0
        except Exception as e:
            print(f'[COOLDOWN_READ_ERR] {e}')
            current_signals_missed = 2
            current_consecutive_losses = 0
        if current_signals_missed < 2:
            try:
                new_signals_missed = current_signals_missed + 1
                with db_tx() as conn:
                    cur = conn.cursor()
                    cur.execute('INSERT OR REPLACE INTO symbol_cooldowns (symbol, signals_missed, consecutive_losses) VALUES (?, ?, ?)', (symbol, new_signals_missed, current_consecutive_losses))
                send_trade_notification(1901059519, f'[SKIP_SIGNAL] Skipping signal for {symbol} due to loss-cooldown. Missed: {new_signals_missed}/2 (DB); consecutive_losses={current_consecutive_losses}')
            except Exception as e:
                print(f'[COOLDOWN_WRITE_ERR] {e}')
            return
        else:
            send_trade_notification(1901059519, f'[COOLDOWN_PASSED] Cooldown period passed for {symbol}, proceeding with signal. consecutive_losses={current_consecutive_losses}')
        pos_amt_on_exchange = None
        client_for_close = None
        try:
            if not (api_key and secret_key):
                with db_tx() as conn:
                    cur = conn.cursor()
                    cursor.execute('SELECT api_key, secret_key FROM users WHERE user_id = ?', (user_id,))
                    keys = cursor.fetchone()
                if keys and keys[0] and keys[1]:
                    api_key, secret_key = (keys[0], keys[1])
            if api_key and secret_key:
                client_for_close = get_trading_client(api_key, secret_key)
                pos_risk = safe_api_call(client_for_close.get_position_risk, symbol=symbol)
                current_pos = next((p for p in pos_risk or [] if p.get('symbol') == symbol), None)
                if current_pos:
                    pos_amt_raw = current_pos.get('positionAmt')
                    if pos_amt_raw is not None:
                        try:
                            pos_amt_on_exchange = float(pos_amt_raw)
                        except Exception:
                            pos_amt_on_exchange = None
            else:
                send_trade_notification(1901059519, f'[FLIP_ERR] No API keys for user {user_id}, cannot actively close on exchange.')
        except Exception as e:
            logging.exception(f'[handle_main_signal] Error while fetching position for flip-close: {e}')
            pos_amt_on_exchange = None
        if pos_amt_on_exchange is not None and abs(pos_amt_on_exchange) > 1e-08:
            close_side = 'BUY' if pos_amt_on_exchange < 0 else 'SELL'
            try:
                close_qty = round(abs(pos_amt_on_exchange), int(qty_precision))
            except Exception:
                close_qty = abs(pos_amt_on_exchange)
            if close_qty <= 0:
                send_trade_notification(1901059519, f'[FLIP_CLOSE_SKIP] Computed close qty <= 0 ({close_qty}) for {symbol}. Aborting flip to avoid partial netting.')
                return
            qty_str = f'{close_qty:.{qty_precision}f}'
            try:
                send_trade_notification(1901059519, f'[FLIP_CLOSE_ATTEMPT] Closing on exchange before flip: side={close_side}, qty={qty_str}')
            except Exception:
                pass
            try:
                close_order = safe_api_call(client_for_close.new_order, symbol=symbol, side=close_side, type='MARKET', quantity=qty_str, reduceOnly=True)
                send_trade_notification(1901059519, f'[FLIP_CLOSE_ORDER] {close_order}')
            except Exception as e:
                err_txt = str(e).lower()
                if 'notional' in err_txt and 'smaller' in err_txt or '-4164' in err_txt or 'must be no smaller' in err_txt:
                    try:
                        send_trade_notification(1901059519, f'[FLIP_CLOSE_FALLBACK] Retrying close with reduceOnly=True to bypass min notional for {symbol}')
                    except Exception:
                        pass
                    try:
                        close_order = safe_api_call(client_for_close.new_order, symbol=symbol, side=close_side, type='MARKET', quantity=qty_str, reduceOnly=True)
                        send_trade_notification(1901059519, f'[FLIP_CLOSE_ORDER_FALLBACK] {close_order}')
                    except Exception as e2:
                        logging.exception(f'[handle_main_signal] Error placing fallback close order during flip: {e2}')
                        send_trade_notification(1901059519, f'[FLIP_CLOSE_ORDER_ERR] fallback err={e2}')
                        return
                else:
                    logging.exception(f'[handle_main_signal] Error placing close order during flip: {e}')
                    send_trade_notification(1901059519, f'[FLIP_CLOSE_ORDER_ERR] {e}')
                    return
            closed = False
            try:
                retries = 10
                delay = 1
                last_amt = None
                for attempt in range(retries):
                    time.sleep(delay)
                    try:
                        pos_risk = safe_api_call(client_for_close.get_position_risk, symbol=symbol)
                        current_pos = next((p for p in pos_risk or [] if p.get('symbol') == symbol), None)
                        current_amt = float(current_pos.get('positionAmt', 0) or 0) if current_pos else 0.0
                    except Exception:
                        current_amt = None
                    last_amt = current_amt
                    if current_amt is None or abs(current_amt) < 1e-08:
                        closed = True
                        break
                    else:
                        send_trade_notification(1901059519, f'[FLIP_CLOSE_WAIT] positionAmt still {current_amt:.{qty_precision}f}, attempt {attempt + 1}/{retries}')
                if not closed:
                    send_trade_notification(1901059519, f"[FLIP_CLOSE_TIMEOUT] Couldn't confirm full close on exchange for {symbol} after {retries} attempts. Current pos: {last_amt}")
                    return
            except Exception as e:
                logging.exception(f'[handle_main_signal] Exception while polling position after flip-close: {e}')
                send_trade_notification(1901059519, f'[FLIP_CLOSE_POLL_ERR] {e}')
                return
        else:
            send_trade_notification(1901059519, f'[FLIP_CLOSE_SKIP] No open net position detected on exchange for {symbol} (pos_amt={pos_amt_on_exchange}). Proceeding with local cleanup.')
        if existing_thread:
            try:
                existing_thread._handle_position_closed()
                send_trade_notification(1901059519, f'[FLIP_CLOSE] Closed existing position for {symbol} via thread cleanup.')
            except Exception as e:
                logging.exception(f'[handle_main_signal] Error calling existing_thread._handle_position_closed() for {symbol}: {e}')
                send_trade_notification(1901059519, f'[FLIP_CLOSE_THREAD_ERR] {e}')
                return
        else:
            send_trade_notification(1901059519, f'[FLIP_CLOSE] No active trailing thread for {symbol}; exchange-close (if any) performed, proceeding to reset local state.')
        existing_qty = 0.0
        existing_add_count = 0
        existing_current_entry = None
        is_existing_pos_in_db = False
    if is_add_position and existing_qty > 0:
        if existing_add_count >= 3:
            send_trade_notification(1901059519, f'[MAX_ADDS] Max adds (3) reached for {symbol}, skipping signal.')
            with db_tx() as conn:
                cur = conn.cursor()
                new_missed_count = min(2, current_signals_missed + 1)
                cursor.execute('INSERT OR REPLACE INTO symbol_cooldowns (symbol, signals_missed, consecutive_losses) VALUES (?, ?, ?)', (symbol, new_missed_count, current_consecutive_losses))
            return
        with db_tx() as conn:
            cur = conn.cursor()
            cursor.execute('SELECT api_key, secret_key FROM users WHERE user_id = ?', (user_id,))
            keys = cursor.fetchone()
        if not (keys and keys[0] and keys[1]):
            send_trade_notification(1901059519, f'[ADD_ERROR] No API keys for user {user_id}')
            return
        api_key, secret_key = (keys[0], keys[1])
        client = get_trading_client(api_key, secret_key)
        target_qty_to_add = None
        if signal.get('quantity'):
            try:
                target_qty_to_add = float(signal.get('quantity'))
            except Exception:
                target_qty_to_add = None
        try:
            try:
                add_count_val = int(existing_add_count or 0)
            except Exception:
                add_count_val = 0
            initial_qty = None
            try:
                if is_existing_pos_in_db and float(existing_qty) > 0:
                    initial_qty = float(existing_qty) / (1 + max(add_count_val, 0))
            except Exception:
                initial_qty = None
            if target_qty_to_add is None and initial_qty and (initial_qty > 0):
                try:
                    target_qty_to_add = round(float(initial_qty), int(qty_precision))
                except Exception:
                    target_qty_to_add = float(initial_qty)
                send_trade_notification(1901059519, f'[ADD_QTY_INIT] Using initial qty for add: {target_qty_to_add:.{qty_precision}f}')
        except Exception:
            target_qty_to_add = None
        try:
            if pos_db:
                try:
                    db_initial = pos_db.get('initial_quantity') if isinstance(pos_db, dict) else None
                    if db_initial is None:
                        try:
                            db_initial = pos_db.get('initial_qty')
                        except Exception:
                            db_initial = None
                    if db_initial is not None:
                        db_initial_f = float(db_initial)
                        if db_initial_f > 0:
                            target_qty_to_add = round(db_initial_f, int(qty_precision))
                            send_trade_notification(1901059519, f'[ADD_QTY_INIT_DB] Using initial_quantity from DB for add: {target_qty_to_add:.{qty_precision}f}')
                except Exception:
                    pass
        except Exception:
            pass
        if target_qty_to_add is None:
            thread_qty = None
            try:
                with active_trailing_threads_lock:
                    th = active_trailing_threads.get(symbol)
                if th:
                    for a in ('last_thread_msg', 'last_msg', 'thread_msg', 'last_message', 'msg', 'message'):
                        v = getattr(th, a, None)
                        if v:
                            thread_qty = _parse_qty_from_thread_message(v)
                            if thread_qty:
                                break
                    if thread_qty is None:
                        for a in ('total_qty', 'position_qty', 'quantity', 'current_qty'):
                            v = getattr(th, a, None)
                            if v is not None:
                                try:
                                    thread_qty = float(v)
                                    break
                                except Exception:
                                    pass
            except Exception:
                thread_qty = None
            if thread_qty is None and pos_db:
                try:
                    v = pos_db.get('last_thread_msg')
                    thread_qty = _parse_qty_from_thread_message(v)
                except Exception:
                    thread_qty = None
            if thread_qty is None and pos_db:
                for k in ('total_qty', 'total_new_qty', 'initial_quantity', 'quantity'):
                    try:
                        v = pos_db.get(k)
                        if v is not None:
                            thread_qty = float(v)
                            break
                    except Exception:
                        pass
            if thread_qty is not None and thread_qty > 0:
                try:
                    target_qty_to_add = round(float(thread_qty), int(qty_precision))
                except Exception:
                    target_qty_to_add = float(thread_qty)
                send_trade_notification(1901059519, f'[ADD_QTY_FROM_THREAD] Using qty from thread/message for add: {target_qty_to_add:.{qty_precision}f}')
        if target_qty_to_add is None:
            try:
                target_qty_to_add = max(existing_qty * 0.5, 0.0)
                target_qty_to_add = round(target_qty_to_add, int(qty_precision))
            except Exception:
                target_qty_to_add = 0.0
        pos_db = None
        try:
            pos_db = get_position_from_db(symbol)
        except Exception:
            pos_db = None
        thread_qty = None
        try:
            with active_trailing_threads_lock:
                th = active_trailing_threads.get(symbol)
            if th:
                for a in ('last_thread_msg', 'last_msg', 'thread_msg', 'last_message', 'msg', 'message'):
                    v = getattr(th, a, None)
                    if v:
                        thread_qty = _parse_qty_from_thread_message(v)
                        if thread_qty:
                            break
                if thread_qty is None:
                    for a in ('total_qty', 'position_qty', 'quantity', 'current_qty'):
                        v = getattr(th, a, None)
                        if v is not None:
                            try:
                                thread_qty = float(v)
                                break
                            except Exception:
                                pass
        except Exception:
            thread_qty = None
        if thread_qty is None and pos_db:
            try:
                v = pos_db.get('last_thread_msg')
                thread_qty = _parse_qty_from_thread_message(v)
            except Exception:
                thread_qty = None
        if thread_qty is None and pos_db:
            for k in ('total_qty', 'total_new_qty', 'initial_quantity', 'quantity'):
                try:
                    v = pos_db.get(k)
                    if v is not None:
                        thread_qty = float(v)
                        break
                except Exception:
                    pass
        if target_qty_to_add is None:
            if thread_qty is not None and thread_qty > 0:
                try:
                    target_qty_to_add = round(float(thread_qty), int(qty_precision))
                except Exception:
                    target_qty_to_add = float(thread_qty)
                send_trade_notification(1901059519, f'[ADD_QTY_FROM_THREAD] Using qty from thread/message for add: {target_qty_to_add:.{qty_precision}f}')
        if target_qty_to_add is None:
            try:
                target_qty_to_add = max(existing_qty * 0.5, 0.0)
                target_qty_to_add = round(target_qty_to_add, int(qty_precision))
            except Exception:
                target_qty_to_add = 0.0
        if target_qty_to_add <= 0:
            send_trade_notification(1901059519, f'[ADD_QTY_ZERO] computed add qty <= 0 for {symbol}, skipping add.')
            return
        try:
            side = 'BUY' if direction == 'LONG' else 'SELL'
            send_trade_notification(1901059519, f'[ENTRY_ADD_ATTEMPT] {side} {target_qty_to_add}')
            entry_order = safe_api_call(client.new_order, symbol=symbol, side=side, type='MARKET', quantity=f'{target_qty_to_add:.{qty_precision}f}')
            send_trade_notification(1901059519, f'[ENTRY_ADD_RESULT] {entry_order}')
            fill_qty = 0.0
            fill_price = float(entry_price)
            try:
                if entry_order and isinstance(entry_order, dict):
                    if entry_order.get('fills'):
                        total_filled = 0.0
                        weighted = 0.0
                        for f in entry_order.get('fills', []):
                            q = float(f.get('qty') or 0)
                            p = float(f.get('price') or 0)
                            total_filled += q
                            weighted += q * p
                        if total_filled > 0:
                            fill_qty = total_filled
                            fill_price = weighted / total_filled
                    else:
                        try:
                            avg_candidate = entry_order.get('avgPrice')
                            avg_val = float(avg_candidate) if avg_candidate is not None else 0.0
                            if avg_val > 0:
                                fill_price = avg_val
                        except Exception:
                            pass
                        try:
                            fill_qty = float(entry_order.get('executedQty') or entry_order.get('origQty') or 0)
                        except Exception:
                            pass
            except Exception:
                pass
            if fill_qty <= 0:
                try:
                    entry_oid = None
                    entry_coid = None
                    try:
                        if isinstance(entry_order, dict):
                            entry_oid = _extract_order_id(entry_order) or entry_order.get('orderId') or entry_order.get('i')
                            entry_coid = entry_order.get('clientOrderId') or entry_order.get('c')
                    except Exception:
                        pass
                    for attempt in range(5):
                        time.sleep(0.15 * (attempt + 1))
                        try:
                            q = None
                            if entry_oid is not None:
                                q = client.query_order(symbol=symbol, orderId=entry_oid)
                            elif entry_coid:
                                q = client.query_order(symbol=symbol, origClientOrderId=entry_coid)
                            if isinstance(q, dict):
                                st = q.get('status') or q.get('X')
                                if st and str(st).upper() not in ('FILLED', 'PARTIALLY_FILLED', 'PARTIALLYFILLED'):
                                    continue
                                try:
                                    if fill_qty <= 0:
                                        fill_qty = float(q.get('executedQty') or q.get('origQty') or 0)
                                except Exception:
                                    pass
                                try:
                                    if not (fill_price and float(fill_price) > 0):
                                        ap = q.get('avgPrice') or q.get('avg_price') or q.get('avg') or q.get('price')
                                        apv = float(ap) if ap is not None else 0.0
                                        if apv > 0:
                                            fill_price = apv
                                except Exception:
                                    pass
                                if fill_qty > 0 and (fill_price and float(fill_price) > 0):
                                    break
                        except Exception:
                            pass
                    if fill_qty <= 0:
                        try:
                            pos_risk = safe_api_call(client.get_position_risk, symbol=symbol)
                            current_pos = next((p for p in pos_risk or [] if p.get('symbol') == symbol), None)
                            if current_pos:
                                pos_amt = float(current_pos.get('positionAmt', 0) or 0)
                                if abs(pos_amt) > 0:
                                    fill_qty = abs(pos_amt)
                                    ep = current_pos.get('entryPrice') or current_pos.get('avgPrice') or current_pos.get('positionAvgPrice')
                                    try:
                                        epv = float(ep) if ep is not None else 0.0
                                        if epv > 0:
                                            fill_price = epv
                                    except Exception:
                                        pass
                        except Exception:
                            pass
                    if fill_qty <= 0:
                        send_trade_notification(1901059519, f'[ENTRY_ERR_ADD] Could not verify order fill for {symbol}. Aborting add.')
                        return
                except Exception as e:
                    logging.exception(f'[ENTRY_WAIT_ERR] {e}')
            if not (fill_price and float(fill_price) > 0):
                fill_price = float(entry_price)
                send_trade_notification(1901059519, f'[WARN_FILL_PRICE_FALLBACK] Used signal entry_price as fill_price for {symbol}: {fill_price}')
            send_trade_notification(1901059519, f'[ENTRY_ADD_FILL] Price: {fill_price:.{price_precision}f}, Quantity: {fill_qty:.{qty_precision}f}')
        except Exception as e:
            logging.exception(f'[handle_main_signal] Entry add order placement error: {e}')
            send_trade_notification(1901059519, f'[ENTRY_ADD_ERR] {e}')
            return
        try:
            new_add_count = int(existing_add_count) + 1
            added_qty = float(fill_qty)
            prev_qty = float(existing_qty)
            total_new_qty = prev_qty + added_qty
            prev_avg = float(existing_current_entry) if existing_current_entry else float(entry_price)
            if total_new_qty > 0:
                new_avg_price = (prev_avg * prev_qty + added_qty * float(fill_price)) / total_new_qty
            else:
                new_avg_price = float(fill_price)
            try:
                new_avg_price = float(round(new_avg_price, int(price_precision)))
            except Exception:
                pass
            send_trade_notification(1901059519, f'[ADD_APPLIED] symbol={symbol} fill_qty={fill_qty} fill_price={fill_price} added_qty={added_qty} total_new_qty={total_new_qty} new_add_count={new_add_count} new_avg_price={new_avg_price}')
        except Exception as e:
            logging.exception(f'[ADD_APPLY_ERR] error applying add for {symbol}: {e}')
            return
        try:
            current_db_stop = _get_current_stop_order_id_db(symbol)
            if current_db_stop:
                ok_cancel, cancel_res = _http_cancel_order(api_key, secret_key, symbol, current_db_stop) if '_http_cancel_order' in globals() else (False, 'no_helper')
                if not ok_cancel:
                    logging.warning(f'[handle_main_signal] Cancel old stop failed: {cancel_res}')
        except Exception as e:
            logging.exception(f'[handle_main_signal] error while cancelling old stop before add: {e}')
        chosen_stop = new_stop
        stop_oid = None
        if chosen_stop and chosen_stop != 0:
            stop_side = 'BUY' if direction == 'SHORT' else 'SELL'
            stop_qty_str = f'{total_new_qty:.{qty_precision}f}'
            for attempt in range(3):
                try:
                    stop_order = safe_api_call(client.new_order, symbol=symbol, side=stop_side, type='STOP_MARKET', quantity=stop_qty_str, stopPrice=chosen_stop, reduceOnly=True, workingType='CONTRACT_PRICE')
                    stop_oid = _extract_order_id(stop_order) if stop_order else None
                    if stop_oid:
                        send_trade_notification(1901059519, f'[STOP_CREATE_ADD] success attempt={attempt + 1} Price: {chosen_stop:.{price_precision}f}, Qty: {stop_qty_str}, ID: {stop_oid}')
                        break
                    else:
                        send_trade_notification(1901059519, f'[STOP_CREATE_ADD] attempt={attempt + 1} returned no orderId, retrying...')
                except Exception as e:
                    logging.exception(f'[handle_main_signal] Stop order placement error during add: attempt {attempt + 1}: {e}')
                    send_trade_notification(1901059519, f'[STOP_CREATE_ERR_ADD] attempt={attempt + 1} err={e}; SL={chosen_stop:.{price_precision}f}')
                time.sleep(1)
            if not stop_oid:
                send_trade_notification(1901059519, f'[STOP_CREATE_ADD_FAIL] Не удалось создать стоп для {symbol} после 3 попыток. Проверьте API/логи.')
        else:
            send_trade_notification(1901059519, '[STOP_PLAN_ADD] Chosen stop price was None or 0, skipping stop order.')
        signal_prices = sorted(tp_levels.values(), reverse=direction == 'LONG')
        if symbol == 'BTCUSDT':
            if len(signal_prices) >= 5:
                default_weights = [0.05, 0.05, 0.1, 0.4, 0.4]
            else:
                default_weights = [0.05, 0.1, 0.4, 0.4]
        elif direction == 'LONG':
            default_weights = [0.05, 0.1, 0.4, 0.4]
        elif direction == 'SHORT':
            default_weights = [0.05, 0.1, 0.4, 0.4]
        else:
            default_weights = [0.05, 0.1, 0.4, 0.4]
        weights = default_weights[:len(signal_prices)]
        if len(weights) > 0:
            total_weight = sum(weights)
            if total_weight != 0:
                weights = [w / total_weight for w in weights]
            else:
                weights = [1.0 / len(weights)] * len(weights)
        else:
            weights = []
        if len(weights) < len(signal_prices):
            weights.extend([0.0] * (len(signal_prices) - len(weights)))
        tp_volumes = allocate_tp_volumes(target_qty_to_add, weights, step_size=10 ** (-qty_precision), min_qty=10 ** (-qty_precision), max_slices=len(signal_prices))
        tp_data_for_db = {}
        tp_ids = []
        tp_order_results = []
        tp_side = 'BUY' if direction == 'SHORT' else 'SELL'
        order_payloads = []
        order_meta = []                            
        for i, (price, vol) in enumerate(zip(signal_prices, tp_volumes)):
            if vol <= 0:
                continue
            vol_str = f'{vol:.{qty_precision}f}'
            order_payloads.append({
                'symbol': symbol,
                'side': tp_side,
                'type': 'LIMIT',
                'timeInForce': 'GTC',
                'quantity': vol_str,
                'price': f'{price:.{price_precision}f}',
                'reduceOnly': 'true',
                'newClientOrderId': _gen_client_order_id(f"tp{i+1}", symbol),
                'newOrderRespType': 'ACK'
            })
            order_meta.append((i, price, vol, vol_str))
        batch_ok = False
        if order_payloads and api_key and secret_key:
            try:
                all_results = []
                for ch_orders in _chunked(order_payloads, 5):
                    res = place_multiple_orders(api_key, secret_key, ch_orders)
                    all_results.extend(res or [])
                for meta, res in zip(order_meta, all_results):
                    i, price, vol, vol_str = meta
                    if isinstance(res, dict) and res.get('orderId'):
                        tp_oid = res.get('orderId')
                        tp_ids.append(str(tp_oid))
                        tp_order_results.append(res)
                        tp_data_for_db[f'tp{i + 1}'] = {'price': price, 'volume': vol, 'order_id': tp_oid}
                        send_trade_notification(1901059519, f'[TP_CREATE_BATCH] idx={i} price={price:.{price_precision}f} qty={vol_str} id={tp_oid}')
                    else:
                        send_trade_notification(1901059519, f"[TP_CREATE_BATCH_ERR] idx={i} price={price:.{price_precision}f} qty={vol_str} err={res}")
                        try:
                            tp_order = safe_api_call(client.new_order, symbol=symbol, side=tp_side, type='LIMIT',
                                                    quantity=vol_str, price=price, timeInForce='GTC', reduceOnly=True)
                            if tp_order is not None:
                                tp_oid2 = _extract_order_id(tp_order)
                                tp_ids.append(str(tp_oid2))
                                tp_order_results.append(tp_order)
                                tp_data_for_db[f'tp{i + 1}'] = {'price': price, 'volume': vol, 'order_id': tp_oid2}
                                send_trade_notification(1901059519, f"[TP_CREATE_FALLBACK] idx={i} price={price:.{price_precision}f} qty={vol_str} id={tp_oid2}")
                        except Exception as e2:
                            send_trade_notification(1901059519, f"[TP_CREATE_FALLBACK_ERR] idx={i} err={e2}")
                batch_ok = True
            except Exception as e:
                logging.exception(f'[handle_main_signal] batchOrders placement error: {e}')
                try:
                    send_trade_notification(1901059519, f'[TP_BATCH_ERR] {e}')
                except Exception:
                    pass
                batch_ok = False
        if not batch_ok:
            for i, (price, vol) in enumerate(zip(signal_prices, tp_volumes)):
                if vol <= 0:
                    continue
                vol_str = f'{vol:.{qty_precision}f}'
                try:
                    tp_order = safe_api_call(client.new_order, symbol=symbol, side=tp_side, type='LIMIT',
                                            quantity=vol_str, price=price, timeInForce='GTC', reduceOnly=True)
                    if tp_order is None:
                        logging.error(f"[TP_CREATE_ERR] idx={i} price={price} qty={vol_str} err='API call failed after retries, returned None'")
                        continue
                    tp_oid = _extract_order_id(tp_order)
                    tp_ids.append(str(tp_oid))
                    tp_order_results.append(tp_order)
                    tp_data_for_db[f'tp{i + 1}'] = {'price': price, 'volume': vol, 'order_id': tp_oid}
                    send_trade_notification(1901059519, f'[TP_CREATE] idx={i} price={price:.{price_precision}f} qty={vol_str} id={tp_oid}')
                except Exception as e:
                    logging.exception(f'[handle_main_signal] TP {i} order placement error: {e}')
                    send_trade_notification(1901059519, f'[TP_CREATE_ERR] idx={i} price={price:.{price_precision}f} qty={vol_str} err={e}')
        if tp_ids:
            tp_msg = ', '.join([f'{p:.{price_precision}f}' for p in signal_prices])
            send_trade_notification(1901059519, f'[TP_PLACED] {len(tp_ids)} orders placed. Prices: {tp_msg}')
        else:
            send_trade_notification(1901059519, '[TP_PLACED] No TP orders were placed (all volumes were zero or placement failed).')
        first_tp_order_id = _pick_first_tp_order_id(entry_price, direction, tp_data_for_db, tp_ids)
        try:
            replace_tp_data = []
            for k, v in sorted(tp_data_for_db.items()):
                replace_tp_data.append({'orderId': v.get('order_id'), 'price': v.get('price'), 'volume': v.get('volume')})
            update_position_in_db(symbol, current_entry_price=new_avg_price, quantity=total_new_qty, stop_price=chosen_stop, stop_order_id=stop_oid, metadata={'last_add_ts': _now_iso(), 'price_precision': price_precision, 'qty_precision': qty_precision})
            with DB_LOCK:
                cursor.execute('UPDATE positions SET add_count = COALESCE(add_count,0) + 1, entry_order_id = ? WHERE symbol = ?', (str(entry_order.get('orderId') if isinstance(entry_order, dict) else entry_order), symbol))
                conn.commit()
            send_trade_notification(1901059519, f'[DB_UPDATE_ADD] Position for {symbol} updated with new orders after add.')
        except Exception as e:
            logging.exception(f'[handle_main_signal] DB update error after add: {e}')
            send_trade_notification(1901059519, f'[DB_UPDATE_ERR_ADD] {e}')
            return
        try:
            signal_prices_add = sorted(tp_levels.values(), reverse=direction == 'LONG')
        except Exception:
            signal_prices_add = []
        try:
            tp_msg_add = ', '.join([f'{p:.{price_precision}f}' for p in signal_prices_add]) if signal_prices_add else ''
            add_msg = f"[THREAD_START_ADD] 📈 Позиция {symbol} добавлена! (Add #{new_add_count})\n| Entry: {entry_price:.{price_precision}f}\n| Avg Entry: {new_avg_price:.{price_precision}f}\n| Stop: {chosen_stop:.{price_precision}f}\n| TP: {tp_msg_add}\n| Qty Added (requested): {target_qty_to_add:.{qty_precision}f}\n| Qty Added (applied): {added_qty:.{qty_precision}f}\n| Total Qty: {total_new_qty:.{qty_precision}f}\n| AddCount: {new_add_count}\n| entry_id={(entry_order.get('orderId') if isinstance(entry_order, dict) else entry_order)} stop_id={stop_oid} recovery_id={existing_recovery_id}"
            send_trade_notification(1901059519, add_msg)
            try:
                send_trade_notification(user_id, add_msg)
            except Exception:
                pass
        except Exception as _e:
            logging.exception(f'[ADD_NOTIFY_ERR] {_e}')
        with active_trailing_threads_lock:
            th = active_trailing_threads.get(symbol)
        if th:
            try:
                th.update_current_entry_price(new_avg_price)
                try:
                    setattr(th, 'total_qty', total_new_qty)
                    setattr(th, 'position_qty', total_new_qty)
                except Exception:
                    pass
                if tp_ids:
                    th.add_dynamic_tp_orders(tp_ids)
                if stop_oid:
                    try:
                        setattr(th, 'stop_order_id', str(stop_oid))
                    except Exception:
                        pass
                try:
                    th.update_stop_loss_with_new_quantity()
                except Exception:
                    pass
                send_trade_notification(1901059519, f'[ADD_HANDLED_IN_THREAD] Updated existing thread for {symbol} with new avg={new_avg_price} qty={total_new_qty}')
            except Exception as e:
                logging.exception(f'[handle_main_signal] Failed to update existing thread for {symbol}: {e}')
                try:
                    client_for_thread = get_trading_client(api_key, secret_key)
                    new_thread = TrailingStopThread(user_id=user_id, symbol=symbol, direction=direction, entry_price=new_avg_price, client=client_for_thread, price_precision=price_precision, quantity_precision=qty_precision, first_tp_order_id=first_tp_order_id, stop_order_id=stop_oid, initial_stop_loss=round(chosen_stop, price_precision) if chosen_stop is not None else None, api_key=api_key, secret_key=secret_key, tp_order_ids=tp_ids, entry_order_id=entry_order.get('orderId') if isinstance(entry_order, dict) else entry_order)
                    new_thread.start()
                    with active_trailing_threads_lock:
                        active_trailing_threads[symbol] = new_thread
                    try:
                        ensure_user_stream(user_id, api_key, secret_key)
                    except Exception:
                        pass
                    send_trade_notification(1901059519, f'[ADD_FALLBACK_THREAD_CREATED] for {symbol}')
                except Exception as ee:
                    logging.exception(f'[handle_main_signal] Fallback thread create failed for {symbol}: {ee}')
        else:
            try:
                client_for_thread = get_trading_client(api_key, secret_key)
                new_thread = TrailingStopThread(user_id=user_id, symbol=symbol, direction=direction, entry_price=new_avg_price, client=client_for_thread, price_precision=price_precision, quantity_precision=qty_precision, first_tp_order_id=first_tp_order_id, stop_order_id=stop_oid, initial_stop_loss=round(chosen_stop, price_precision) if chosen_stop is not None else None, api_key=api_key, secret_key=secret_key, tp_order_ids=tp_ids, entry_order_id=entry_order.get('orderId') if isinstance(entry_order, dict) else entry_order)
                new_thread.start()
                with active_trailing_threads_lock:
                    active_trailing_threads[symbol] = new_thread
                    try:
                        ensure_user_stream(user_id, api_key, secret_key)
                    except Exception:
                        pass
                send_trade_notification(1901059519, f'[ADD_THREAD_CREATED] for {symbol}')
            except Exception as e:
                logging.exception(f'[handle_main_signal] Creating trailing thread after add failed for {symbol}: {e}')
        return
    try:
        with db_tx() as conn:
            cur = conn.cursor()
            cursor.execute('SELECT api_key, secret_key FROM users WHERE user_id = ?', (user_id,))
            keys = cursor.fetchone()
        if not keys or not keys[0] or (not keys[1]):
            send_trade_notification(1901059519, f'[AUTH_ERROR] No API key/secret found for user {user_id}')
            return
        api_key, secret_key = (keys[0], keys[1])
        client = get_trading_client(api_key, secret_key)
        try:
            ensure_user_stream(user_id, api_key, secret_key)
        except Exception:
            pass
        with db_tx() as conn:
            cur = conn.cursor()
            cursor.execute('SELECT is_active FROM users WHERE user_id = ?', (user_id,))
            user_row = cursor.fetchone()
        if user_row and (not user_row[0]):
            send_trade_notification(1901059519, f'[USER_DISABLED] {user_id}')
            return
        with db_tx() as conn:
            cur = conn.cursor()
            cursor.execute('SELECT is_active FROM trading_pairs WHERE symbol = ?', (symbol,))
            r = cursor.fetchone()
        if not r or not r[0]:
            send_trade_notification(1901059519, f'[PAIR_DISABLED] {symbol}')
            return
        symbol_info = get_symbol_info_cached(client, symbol)
        if not symbol_info:
            send_trade_notification(1901059519, f'[SYMBOL_ERROR] Symbol {symbol} not found on exchange')
            return
        price_precision = 2
        qty_precision = 3
        for f in symbol_info.get('filters', []):
            if f.get('filterType') == 'PRICE_FILTER':
                tick = float(f.get('tickSize', 10 ** (-price_precision)))
                try:
                    price_precision = min(8, max(0, int(-math.log10(tick))))
                except Exception:
                    pass
            elif f.get('filterType') == 'LOT_SIZE':
                step = float(f.get('stepSize', 10 ** (-qty_precision)))
                try:
                    qty_precision = min(8, max(0, int(-math.log10(step))))
                except Exception:
                    pass
    except Exception as e:
        logging.exception(f'[handle_main_signal] Auth/Setup error: {e}')
        return
    update_user_balance(user_id)
    available_balance = get_available_balance(user_id)
    send_trade_notification(1901059519, f'[BALANCE] {available_balance}')
    if available_balance is None or available_balance <= 0:
        send_trade_notification(1901059519, '[BALANCE_ERROR] insufficient')
        return
    total_balance = available_balance  # reuse, avoid extra API call
    target_balance = total_balance * VOLUME_PERCENT
    leverage = LEVERAGE
    target_qty_calc = abs(target_balance * leverage / entry_price) if entry_price > 0 else 0
    try:
        target_qty_calc = round(target_qty_calc, int(qty_precision))
    except Exception:
        try:
            target_qty_calc = round(float(target_qty_calc), int(qty_precision))
        except Exception:
            pass
    min_notional = _binance_min_notional(symbol_info)
    if min_notional:
        min_qty_for_notional = min_notional / entry_price if entry_price > 0 else 0
        target_qty_calc = max(target_qty_calc, min_qty_for_notional)
        try:
            target_qty_calc = round(target_qty_calc, int(qty_precision))
        except Exception:
            pass
    if target_qty_calc <= 0:
        send_trade_notification(1901059519, f'[SIZE_ERROR] Calculated qty <= 0 for {symbol}')
        return
    send_trade_notification(1901059519, f'[SIZE_CALC] balance_part={target_balance:.4f}, lev={leverage}, entry={entry_price:.4f}, raw_qty={target_qty_calc:.8f}')
    total_new_qty = target_qty_calc
    try:
        res = safe_api_call(client.change_leverage, symbol=symbol, leverage=leverage)
        send_trade_notification(1901059519, f'[LEVERAGE] set {leverage} for {symbol}')
    except Exception as e:
        send_trade_notification(1901059519, f'[LEVERAGE_ERR] {e}')
    try:
        ok_cancel = ensure_cancel_all_orders(client, symbol, api_key, secret_key)
        send_trade_notification(1901059519, f'[ENSURE_CANCEL] {symbol} ok={ok_cancel}')
    except Exception as e:
        logging.exception(f'[handle_main_signal] Cancel orders error: {e}')
    try:
        side = 'BUY' if direction == 'LONG' else 'SELL'
        entry_order = safe_api_call(client.new_order, symbol=symbol, side=side, type='MARKET', quantity=total_new_qty)
        fill_qty = 0.0
        fill_price = float(entry_price)
        try:
            if entry_order and isinstance(entry_order, dict):
                if entry_order.get('fills'):
                    total_filled = 0.0
                    weighted = 0.0
                    for f in entry_order.get('fills', []):
                        q = float(f.get('qty') or 0)
                        p = float(f.get('price') or 0)
                        total_filled += q
                        weighted += q * p
                    if total_filled > 0:
                        fill_qty = total_filled
                        fill_price = weighted / total_filled
                else:
                    try:
                        avg_candidate = entry_order.get('avgPrice')
                        avg_val = float(avg_candidate) if avg_candidate is not None else 0.0
                        if avg_val > 0:
                            fill_price = avg_val
                    except Exception:
                        pass
                    try:
                        fill_qty = float(entry_order.get('executedQty') or entry_order.get('origQty') or 0)
                    except Exception:
                        pass
        except Exception:
            pass
        if fill_qty <= 0:
            try:
                entry_oid = None
                entry_coid = None
                try:
                    if isinstance(entry_order, dict):
                        entry_oid = _extract_order_id(entry_order) or entry_order.get('orderId') or entry_order.get('i')
                        entry_coid = entry_order.get('clientOrderId') or entry_order.get('c')
                except Exception:
                    pass
                for attempt in range(5):
                    time.sleep(0.15 * (attempt + 1))
                    try:
                        q = None
                        if entry_oid is not None:
                            q = client.query_order(symbol=symbol, orderId=entry_oid)
                        elif entry_coid:
                            q = client.query_order(symbol=symbol, origClientOrderId=entry_coid)
                        if isinstance(q, dict):
                            st = q.get('status') or q.get('X')
                            if st and str(st).upper() not in ('FILLED', 'PARTIALLY_FILLED', 'PARTIALLYFILLED'):
                                continue
                            try:
                                if fill_qty <= 0:
                                    fill_qty = float(q.get('executedQty') or q.get('origQty') or 0)
                            except Exception:
                                pass
                            try:
                                if not (fill_price and float(fill_price) > 0):
                                    ap = q.get('avgPrice') or q.get('avg_price') or q.get('avg') or q.get('price')
                                    apv = float(ap) if ap is not None else 0.0
                                    if apv > 0:
                                        fill_price = apv
                            except Exception:
                                pass
                            if fill_qty > 0 and (fill_price and float(fill_price) > 0):
                                break
                    except Exception:
                        pass
                if fill_qty <= 0:
                    try:
                        pos_risk = safe_api_call(client.get_position_risk, symbol=symbol)
                        current_pos = next((p for p in pos_risk or [] if p.get('symbol') == symbol), None)
                        if current_pos:
                            pos_amt = float(current_pos.get('positionAmt', 0) or 0)
                            if abs(pos_amt) > 0:
                                fill_qty = abs(pos_amt)
                                ep = current_pos.get('entryPrice') or current_pos.get('avgPrice') or current_pos.get('positionAvgPrice')
                                try:
                                    epv = float(ep) if ep is not None else 0.0
                                    if epv > 0:
                                        fill_price = epv
                                except Exception:
                                    pass
                    except Exception:
                        pass
                if fill_qty <= 0:
                    send_trade_notification(1901059519, f'[ENTRY_ERR] Could not verify order fill for {symbol} after polling. Aborting.')
                    return
            except Exception as e:
                logging.exception(f'[ENTRY_WAIT_ERR] {e}')
                return
        if not (fill_price and float(fill_price) > 0):
            fill_price = float(entry_price)
            send_trade_notification(1901059519, f'[WARN_FILL_PRICE_FALLBACK] Used signal entry_price as fill_price for {symbol}: {fill_price}')
        send_trade_notification(1901059519, f'[ENTRY_FINAL_FILL] Price: {fill_price:.{price_precision}f}, Quantity: {fill_qty:.{qty_precision}f}')
    except Exception as e:
        logging.exception(f'[handle_main_signal] Entry order placement or verification error: {e}')
        send_trade_notification(1901059519, f'[ENTRY_ERR] {e}')
        return
    if fill_qty <= 0 or fill_price <= 0:
        logging.error(f'[handle_main_signal] Invalid fill_qty ({fill_qty}) or fill_price ({fill_price}) for {symbol}.')
        send_trade_notification(1901059519, f'[ENTRY_ERR] Invalid fill confirmation for {symbol}: qty={fill_qty}, price={fill_price}')
        return
    new_avg_price = fill_price
    total_new_qty = fill_qty
    try:
        current_db_stop = _get_current_stop_order_id_db(symbol)
        if current_db_stop:
            ok_cancel, cancel_res = _http_cancel_order(api_key, secret_key, symbol, current_db_stop) if '_http_cancel_order' in globals() else (False, 'no_helper')
            if not ok_cancel:
                logging.warning(f'[handle_main_signal] Cancel old stop failed: {cancel_res}')
    except Exception as e:
        logging.exception(f'[handle_main_signal] Error cancelling existing stop: {e}')
    chosen_stop = new_stop
    stop_oid = None
    if chosen_stop and chosen_stop != 0:
        stop_side = 'BUY' if direction == 'SHORT' else 'SELL'
        stop_qty_str = f'{total_new_qty:.{qty_precision}f}'
        try:
            stop_order = safe_api_call(client.new_order, symbol=symbol, side=stop_side, type='STOP_MARKET', quantity=stop_qty_str, stopPrice=chosen_stop, reduceOnly=True, workingType='CONTRACT_PRICE')
            stop_oid = _extract_order_id(stop_order) if stop_order else None
            send_trade_notification(1901059519, f'[STOP_CREATE] Price: {chosen_stop:.{price_precision}f}, Qty: {stop_qty_str}, ID: {stop_oid}')
        except Exception as e:
            logging.exception(f'[handle_main_signal] Stop order placement error: {e}')
            send_trade_notification(1901059519, f'[STOP_CREATE_ERR] {e}; SL={chosen_stop:.{price_precision}f}')
    else:
        send_trade_notification(1901059519, '[STOP_PLAN] Chosen stop price was None or 0, skipping stop order.')
    signal_prices = sorted(tp_levels.values(), reverse=direction == 'LONG')
    if symbol == 'BTCUSDT':
        if len(signal_prices) >= 5:
            default_weights = [0.05, 0.05, 0.1, 0.4, 0.4]
        else:
            default_weights = [0.05, 0.1, 0.4, 0.4]
    elif direction == 'LONG':
        default_weights = [0.05, 0.1, 0.4, 0.4]
    elif direction == 'SHORT':
        default_weights = [0.05, 0.1, 0.4, 0.4]
    else:
        default_weights = [0.05, 0.1, 0.4, 0.4]
    weights = default_weights[:len(signal_prices)]
    if len(weights) > 0:
        total_weight = sum(weights)
        if total_weight != 0:
            weights = [w / total_weight for w in weights]
        else:
            weights = [1.0 / len(weights)] * len(weights)
    else:
        weights = []
    if len(weights) < len(signal_prices):
        weights.extend([0.0] * (len(signal_prices) - len(weights)))
    tp_volumes = allocate_tp_volumes(total_new_qty, weights, step_size=10 ** (-qty_precision), min_qty=10 ** (-qty_precision), max_slices=len(signal_prices))
    tp_data_for_db = {}
    tp_ids = []
    tp_order_results = []
    for i, (price, vol) in enumerate(zip(signal_prices, tp_volumes)):
        if vol <= 0:
            continue
        vol_str = f'{vol:.{qty_precision}f}'
        tp_side = 'BUY' if direction == 'SHORT' else 'SELL'
        try:
            tp_order = safe_api_call(client.new_order, symbol=symbol, side=tp_side, type='LIMIT', quantity=vol_str, price=price, timeInForce='GTC', reduceOnly=True)
            if tp_order is None:
                logging.error(f"[TP_CREATE_ERR] idx={i} price={price} qty={vol_str} err='API call failed after retries, returned None'")
                continue
            tp_oid = _extract_order_id(tp_order)
            tp_ids.append(str(tp_oid))
            tp_order_results.append(tp_order)
            tp_data_for_db[f'tp{i + 1}'] = {'price': price, 'volume': vol, 'order_id': tp_oid}
            send_trade_notification(1901059519, f'[TP_CREATE] idx={i} price={price:.{price_precision}f} qty={vol_str} id={tp_oid}')
        except Exception as e:
            logging.exception(f'[handle_main_signal] TP {i} order placement error: {e}')
            send_trade_notification(1901059519, f'[TP_CREATE_ERR] idx={i} price={price:.{price_precision}f} qty={vol_str} err={e}')
    if tp_ids:
        tp_msg = ', '.join([f'{p:.{price_precision}f}' for p in signal_prices])
        send_trade_notification(1901059519, f'[TP_PLACED] {len(tp_ids)} orders placed. Prices: {tp_msg}')
    else:
        send_trade_notification(1901059519, '[TP_PLACED] No TP orders were placed (all volumes were zero or placement failed).')
    first_tp_order_id = _pick_first_tp_order_id(entry_price, direction, tp_data_for_db, tp_ids)
    try:
        tp_ids_json = json.dumps(tp_ids) if tp_ids else '[]'
        metadata_json = json.dumps({'price_precision': price_precision, 'qty_precision': qty_precision, 'timestamp': _now_iso()})
        with db_tx() as conn:
            cur = conn.cursor()
            cursor.execute('\n                INSERT OR REPLACE INTO positions\n                (symbol, user_id, direction, quantity, entry_price, current_entry_price, stop_price, tp_data, entry_order_id, stop_order_id, recovery_order_id, metadata, add_count, signals_missed_after_stop)\n                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)\n            ', (symbol, user_id, direction, total_new_qty, new_avg_price, fill_price, chosen_stop, json.dumps(tp_data_for_db), entry_order.get('orderId') if isinstance(entry_order, dict) else entry_order, stop_oid, None, metadata_json, 0, 0))
        send_trade_notification(1901059519, f'[DB_UPDATE] Position for {symbol} updated successfully.')
        try:
            with db_tx() as conn:
                cur = conn.cursor()
                cur.execute('UPDATE positions SET initial_quantity = ? WHERE symbol = ?', (total_new_qty, symbol))
                conn.commit()
        except Exception:
            pass
    except Exception as e:
        logging.exception(f'[handle_main_signal] DB update error: {e}')
        send_trade_notification(1901059519, f'[DB_UPDATE_ERR] {e}')
        return
    try:
        if tp_order_results:
            _tp_order_ids = [str(_extract_order_id(o)) for o in tp_order_results]
        else:
            _tp_order_ids = [str(o) for o in tp_ids]
        trailing_thread = TrailingStopThread(user_id=user_id, symbol=symbol, direction=direction, entry_price=round(entry_price, price_precision), client=client, price_precision=price_precision, quantity_precision=qty_precision, first_tp_order_id=first_tp_order_id, stop_order_id=stop_oid, initial_stop_loss=round(chosen_stop, price_precision) if chosen_stop is not None else None, api_key=api_key, secret_key=secret_key, tp_order_ids=_tp_order_ids, entry_order_id=entry_order.get('orderId') if isinstance(entry_order, dict) else entry_order)
        trailing_thread.start()
        with active_trailing_threads_lock:
            active_trailing_threads[symbol] = trailing_thread
        send_trade_notification(1901059519, f'[THREAD_ENSURED] Trailing thread for {symbol} ensured.')
    except Exception as e:
        logging.exception(f'[handle_main_signal] Error ensuring trailing thread for {symbol}: {e}')
        send_trade_notification(1901059519, f'[THREAD_ENSURE_ERR] {e}')
    tp_msg = ', '.join([f'{p:.{price_precision}f}' for p in signal_prices])
    msg = f"✅ Сделка открыта: {symbol} {direction}\n| Entry: {fill_price:.{price_precision}f}\n| Stop: {chosen_stop:.{price_precision}f}\n| TP: {tp_msg}\n| TP_ids: {tp_ids}\n| Qty: {total_new_qty:.{qty_precision}f}\n| AddCount: {0}\n| entry_id={(entry_order.get('orderId') if isinstance(entry_order, dict) else entry_order)} stop_id={stop_oid} recovery_id=None"
    send_trade_notification(1901059519, f'[THREAD_START] {msg}')
    try:
        send_trade_notification(user_id, msg)
    except Exception as e:
        logging.warning(f'[handle_main_signal] Notification to {user_id} failed: {e}')
    logging.info(f'[handle_main_signal] Completed processing signal for {symbol}')
def run_telegram_bot():
    bot.polling(none_stop=True)
@bot.message_handler(commands=['start'])
def start(message):
    user_id = message.from_user.id
    if user_id not in ADMINS:
        bot.send_message(user_id, 'У вас нет доступа к управлению.')
        return
    cursor.execute('SELECT api_key, secret_key FROM users WHERE user_id = ?', (user_id,))
    current_api, current_secret = cursor.fetchone() or (None, None)
    if current_api and current_secret:
        bot.send_message(user_id, 'Ключи уже сохранены. Используйте /reset_keys для перезаписи.')
    else:
        bot.send_message(user_id, 'Введите ваш Binance Futures API-ключ:')
@bot.message_handler(commands=['add_pair'])
def add_pair(message):
    user_id = message.from_user.id
    if user_id not in ADMINS:
        bot.send_message(user_id, 'Нет доступа')
        return
    try:
        pair = message.text.split(maxsplit=1)[1].strip().upper()
        if not pair.endswith('USDT'):
            bot.send_message(user_id, '❗ Некорректный формат пары. Пример: BTCUSDT')
            return
        cursor.execute('SELECT * FROM trading_pairs WHERE symbol = ?', (pair,))
        existing = cursor.fetchone()
        if existing:
            bot.send_message(user_id, f'❗ Пара {pair} уже существует')
            return
        cursor.execute('INSERT INTO trading_pairs (symbol, is_active, current_loss) VALUES (?, ?, ?)', (pair, True, 0.0))
        conn.commit()
        bot.send_message(user_id, f'✅ Пара {pair} добавлена')
    except IndexError:
        bot.send_message(user_id, '❗ Укажите пару после команды, например: /add_pair BTCUSDT')
    except Exception as e:
        bot.send_message(user_id, f'❗ Ошибка: {str(e)}')
@bot.message_handler(commands=['show_settings'])
def show_settings(message):
    user_id = message.from_user.id
    if user_id not in ADMINS:
        return
    try:
        cursor.execute('SELECT SUM(balance) FROM users WHERE is_active = 1')
        total_balance = cursor.fetchone()[0] or 0
        cursor.execute('SELECT user_id, balance FROM users WHERE is_active = 1')
        active_users = cursor.fetchall()
        message_text = '📊 Текущие настройки и балансы:\n'
        message_text += f'💰 Общий баланс активных пользователей (с позициями): {total_balance:.2f} USDT\n'
        message_text += '👥 Активные пользователи:\n'
        for user in active_users:
            user_id, balance = user
            message_text += f'- Пользователь {user_id}: {balance:.2f} USDT\n'
        message_text += '\n⚙️ Настройки объемов:\n'
        message_text += f'- BTC: {BTC_VOLUME_PERCENT * 100:.1f}% от баланса\n'
        message_text += f'- Другие пары: {VOLUME_PERCENT * 100:.1f}% от баланса\n'
        bot.send_message(user_id, message_text)
    except Exception as e:
        bot.send_message(user_id, f'❌ Ошибка при получении данных: {str(e)}')
@bot.message_handler(commands=['reset_keys'])
def reset_keys(message):
    user_id = message.from_user.id
    if user_id not in ADMINS:
        return
    cursor.execute('UPDATE users SET api_key = NULL, secret_key = NULL WHERE user_id = ?', (user_id,))
    conn.commit()
    bot.send_message(user_id, 'Ключи обнулены. Введите API-ключ:')
@bot.message_handler(commands=['babduk'])
def admin_panel(message):
    user_id = message.from_user.id
    if user_id in ADMINS:
        cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
        user = cursor.fetchone()
        if user:
            is_active = 'Включен ✅' if user[6] else 'Остановлен ❌'
            markup = telebot.types.InlineKeyboardMarkup()
            btn_stop_text = '🔴 Остановить торговлю' if user[6] else '🟢 Включить торговлю'
            btn_stop = telebot.types.InlineKeyboardButton(btn_stop_text, callback_data='stop_trading')
            btn_pairs = telebot.types.InlineKeyboardButton('📋 Список торговых пар', callback_data='show_pairs')
            markup.add(btn_stop, btn_pairs)
            bot.send_message(user_id, f'Баланс: {user[3]} USD\nСтатус: {is_active}', reply_markup=markup)
    else:
        bot.send_message(user_id, 'Нет доступа')
@bot.message_handler(commands=['set_volume_percent'])
def set_volume_percent(message):
    user_id = message.from_user.id
    if user_id not in ADMINS:
        return
    try:
        percent = float(message.text.split()[1]) / 100
        if 0 < percent <= 0.1:
            global VOLUME_PERCENT
            VOLUME_PERCENT = percent
            bot.send_message(user_id, f'✅ Процент установлен: {percent * 100}%')
        else:
            bot.send_message(user_id, '❗ Укажите процент от 0.1 до 5 (например: /set_volume_percent 2)')
    except Exception as e:
        bot.send_message(user_id, '❗ Неверный формат команды. Используйте /set_volume_percent 2')
@bot.callback_query_handler(func=lambda call: call.data.startswith('toggle_pair_'))
def toggle_pair_status(call):
    user_id = call.from_user.id
    if user_id not in ADMINS:
        return
    symbol = call.data.split('_')[-1]
    cursor.execute('SELECT is_active FROM trading_pairs WHERE symbol = ?', (symbol,))
    is_active = cursor.fetchone()[0]
    new_status = not is_active
    cursor.execute('UPDATE trading_pairs SET is_active = ? WHERE symbol = ?', (new_status, symbol))
    conn.commit()
    recalculate_losses()
    show_pairs(call)
@bot.callback_query_handler(func=lambda call: call.data == 'show_pairs')
def show_pairs(call):
    user_id = call.from_user.id
    if user_id not in ADMINS:
        return
    cursor.execute('SELECT symbol, is_active FROM trading_pairs')
    pairs = cursor.fetchall()
    markup = telebot.types.InlineKeyboardMarkup()
    for symbol, is_active in pairs:
        status = '✅' if is_active else '❌'
        btn = telebot.types.InlineKeyboardButton(f'{status} {symbol}', callback_data=f'toggle_pair_{symbol}')
        markup.add(btn)
    bot.edit_message_text(chat_id=user_id, message_id=call.message.message_id, text='Выберите пару для изменения статуса:', reply_markup=markup)
@bot.callback_query_handler(func=lambda call: True)
def button_handler(call):
    if call.data == 'stop_trading':
        user_id = call.from_user.id
        cursor.execute('UPDATE users SET is_active = NOT is_active WHERE user_id = ?', (user_id,))
        conn.commit()
        cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
        user = cursor.fetchone()
        is_active = 'Включен ✅' if user[6] else 'Остановлен ❌'
        markup = telebot.types.InlineKeyboardMarkup()
        btn_stop_text = '🔴 Остановить торговлю' if user[6] else '🟢 Включить торговлю'
        btn_stop = telebot.types.InlineKeyboardButton(btn_stop_text, callback_data='stop_trading')
        btn_pairs = telebot.types.InlineKeyboardButton('📋 Список торговых пар', callback_data='show_pairs')
        markup.add(btn_stop, btn_pairs)
        bot.edit_message_text(chat_id=user_id, message_id=call.message.message_id, text=f'Баланс: {user[3]} USD\nСтатус: {is_active}', reply_markup=markup)
        bot.answer_callback_query(call.id, 'Статус изменён')
@bot.message_handler(func=lambda m: True)
def handle_input(message):
    if message.text.startswith('/'):
        return
    user_id = message.from_user.id
    text = message.text
    if user_id not in ADMINS:
        return
    cursor.execute('INSERT OR IGNORE INTO users (user_id) VALUES (?)', (user_id,))
    conn.commit()
    cursor.execute('SELECT api_key, secret_key FROM users WHERE user_id = ?', (user_id,))
    current_api, current_secret = cursor.fetchone() or (None, None)
    if current_api is None:
        cursor.execute('UPDATE users SET api_key = ? WHERE user_id = ?', (text, user_id))
        conn.commit()
        bot.send_message(user_id, 'Теперь введите секретный ключ:')
    elif current_secret is None:
        cursor.execute('UPDATE users SET secret_key = ? WHERE user_id = ?', (text, user_id))
        conn.commit()
        try:
            cursor.execute('SELECT api_key, secret_key FROM users WHERE user_id = ?', (user_id,))
            api_key, secret_key = cursor.fetchone()
            client = get_trading_client(api_key, secret_key)
            account = client.account()
            usdt_balance = next((a for a in account['assets'] if a['asset'] == 'USDT'), None)
            initial_balance = float(usdt_balance['walletBalance']) if usdt_balance else 0
            cursor.execute('UPDATE users SET initial_balance = ?, is_active = 1 WHERE user_id = ?', (initial_balance, user_id))
            conn.commit()
            bot.send_message(user_id, f'✅ Ключи сохранены. Начальный депозит: {initial_balance:.2f} USDT')
        except Exception as e:
            bot.send_message(user_id, f'❌ Ошибка авторизации: {str(e)}. Проверьте ключи.')
            cursor.execute('UPDATE users SET api_key = NULL, secret_key = NULL WHERE user_id = ?', (user_id,))
            conn.commit()
    else:
        bot.send_message(user_id, '❌ Ключи уже введены. Используйте /start для перезаписи.')
if __name__ == '__main__':
    recalculate_losses()
    telegram_thread = threading.Thread(target=run_telegram_bot)
    telegram_thread.start()
    balance_monitor = BalanceMonitor()
    balance_monitor.start()
    app.run(port=5001, debug=False)
