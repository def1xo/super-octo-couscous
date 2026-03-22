import hashlib
import hmac
import json
import logging
import math
import os
import queue
import sqlite3
import threading
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

import requests
import telebot
from flask import Flask, jsonify, request

APP_PORT = int(os.environ.get('BYBIT_BOT_PORT', '6001'))
DB_PATH = os.environ.get('BYBIT_DB_PATH', 'bybit_testnet_users.db')
BYBIT_TESTNET = str(os.environ.get('BYBIT_TESTNET', '1')).strip().lower() not in ('0', 'false', 'off', 'no')
BYBIT_BASE_URL = (os.environ.get('BYBIT_BASE_URL') or ('https://api-testnet.bybit.com' if BYBIT_TESTNET else 'https://api.bybit.com')).rstrip('/')
BYBIT_CATEGORY = os.environ.get('BYBIT_CATEGORY', 'linear')
BYBIT_RECV_WINDOW = str(os.environ.get('BYBIT_RECV_WINDOW', '5000'))
BYBIT_SIGNAL_MODE = os.environ.get('BYBIT_SIGNAL_MODE', 'sync').strip().lower()
BYBIT_SIGNAL_WORKERS = max(1, int(os.environ.get('BYBIT_SIGNAL_WORKERS', '1')))
BYBIT_DEFAULT_USER_ID = int(os.environ.get('BYBIT_DEFAULT_USER_ID', '0') or '0')
BYBIT_WEBHOOK_SECRET = os.environ.get('BYBIT_WEBHOOK_SECRET') or os.environ.get('WEBHOOK_SECRET')
BYBIT_TELEGRAM_BOT_TOKEN = os.environ.get('BYBIT_TELEGRAM_BOT_TOKEN') or os.environ.get('TELEGRAM_BOT_TOKEN', '7680871680:AAHPx1Mf6viK9z0ByqUuzrHtF2htUOYeqhQ')
LEVERAGE = int(os.environ.get('BYBIT_LEVERAGE', '20'))
VOLUME_PERCENT = float(os.environ.get('BYBIT_VOLUME_PERCENT', '0.03'))
BTC_VOLUME_PERCENT = float(os.environ.get('BYBIT_BTC_VOLUME_PERCENT', '0.06'))
SIGNAL_QUEUE_MAXSIZE = max(1, int(os.environ.get('BYBIT_SIGNAL_QUEUE_MAXSIZE', '1000')))
ADMIN_IDS = [int(x) for x in os.environ.get('BYBIT_ADMIN_IDS', '').split(',') if x.strip().isdigit()]

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
app = Flask(__name__)
bot = telebot.TeleBot(BYBIT_TELEGRAM_BOT_TOKEN)
DB_LOCK = threading.Lock()
SIGNAL_QUEUE: "queue.Queue[Tuple[str, Dict[str, Any]]]" = queue.Queue(maxsize=SIGNAL_QUEUE_MAXSIZE)
SIGNAL_THREADS: List[threading.Thread] = []
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
cursor = conn.cursor()


def init_db() -> None:
    with DB_LOCK:
        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                api_key TEXT,
                secret_key TEXT,
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )
        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                direction TEXT NOT NULL,
                payload TEXT NOT NULL,
                status TEXT NOT NULL,
                details TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )
        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS positions (
                symbol TEXT PRIMARY KEY,
                side TEXT NOT NULL,
                qty TEXT NOT NULL,
                entry_order_id TEXT,
                stop_order_id TEXT,
                tp_order_ids TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )
        conn.commit()


class BybitAPIError(Exception):
    pass


class BybitV5Client:
    def __init__(self, api_key: str, secret_key: str, base_url: str = BYBIT_BASE_URL):
        self.api_key = api_key
        self.secret_key = secret_key
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()

    def _timestamp(self) -> str:
        return str(int(time.time() * 1000))

    def _sign(self, timestamp: str, payload: str) -> str:
        raw = f'{timestamp}{self.api_key}{BYBIT_RECV_WINDOW}{payload}'
        return hmac.new(self.secret_key.encode('utf-8'), raw.encode('utf-8'), hashlib.sha256).hexdigest()

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        payload: Optional[Dict[str, Any]] = None,
        auth: bool = True,
    ) -> Dict[str, Any]:
        method = method.upper()
        params = params or {}
        payload = payload or {}
        body = json.dumps(payload, separators=(',', ':')) if payload else ''
        ordered_query = '&'.join(f'{k}={params[k]}' for k in sorted(params))
        sign_payload = ordered_query if method == 'GET' else body
        headers = {'Content-Type': 'application/json'}
        if auth:
            ts = self._timestamp()
            headers.update(
                {
                    'X-BAPI-API-KEY': self.api_key,
                    'X-BAPI-TIMESTAMP': ts,
                    'X-BAPI-RECV-WINDOW': BYBIT_RECV_WINDOW,
                    'X-BAPI-SIGN': self._sign(ts, sign_payload),
                }
            )
        response = self.session.request(
            method=method,
            url=f'{self.base_url}{path}',
            params=params or None,
            data=body or None,
            headers=headers,
            timeout=20,
        )
        response.raise_for_status()
        data = response.json()
        if data.get('retCode') != 0:
            raise BybitAPIError(f"{data.get('retCode')}: {data.get('retMsg')}")
        return data.get('result') or {}

    def get_wallet_summary(self) -> Dict[str, float]:
        result = self._request('GET', '/v5/account/wallet-balance', params={'accountType': 'UNIFIED'})
        rows = result.get('list') or []
        if not rows:
            return {'total_equity': 0.0, 'available_balance': 0.0, 'usdt_equity': 0.0}
        row = rows[0]
        usdt_coin = next((coin for coin in row.get('coin') or [] if coin.get('coin') == 'USDT'), {})
        return {
            'total_equity': float(row.get('totalEquity') or 0.0),
            'available_balance': float(row.get('totalAvailableBalance') or 0.0),
            'usdt_equity': float(usdt_coin.get('equity') or 0.0),
        }

    def get_instrument_info(self, symbol: str) -> Dict[str, Any]:
        result = self._request('GET', '/v5/market/instruments-info', params={'category': BYBIT_CATEGORY, 'symbol': symbol}, auth=False)
        items = result.get('list') or []
        if not items:
            raise BybitAPIError(f'Instrument not found: {symbol}')
        return items[0]

    def get_ticker(self, symbol: str) -> Dict[str, Any]:
        result = self._request('GET', '/v5/market/tickers', params={'category': BYBIT_CATEGORY, 'symbol': symbol}, auth=False)
        items = result.get('list') or []
        if not items:
            raise BybitAPIError(f'Ticker not found: {symbol}')
        return items[0]

    def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        return self._request(
            'POST',
            '/v5/position/set-leverage',
            payload={'category': BYBIT_CATEGORY, 'symbol': symbol, 'buyLeverage': str(leverage), 'sellLeverage': str(leverage)},
        )

    def get_positions(self, symbol: str) -> List[Dict[str, Any]]:
        result = self._request('GET', '/v5/position/list', params={'category': BYBIT_CATEGORY, 'symbol': symbol})
        return result.get('list') or []

    def get_open_orders(self, symbol: str, open_only: int = 0) -> List[Dict[str, Any]]:
        result = self._request('GET', '/v5/order/realtime', params={'category': BYBIT_CATEGORY, 'symbol': symbol, 'openOnly': open_only})
        return result.get('list') or []

    def get_order(self, symbol: str, *, order_id: Optional[str] = None, order_link_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        params: Dict[str, Any] = {'category': BYBIT_CATEGORY, 'symbol': symbol, 'openOnly': 1}
        if order_id:
            params['orderId'] = order_id
        if order_link_id:
            params['orderLinkId'] = order_link_id
        result = self._request('GET', '/v5/order/realtime', params=params)
        items = result.get('list') or []
        return items[0] if items else None

    def cancel_order(self, symbol: str, *, order_id: Optional[str] = None, order_link_id: Optional[str] = None) -> Dict[str, Any]:
        payload: Dict[str, Any] = {'category': BYBIT_CATEGORY, 'symbol': symbol}
        if order_id:
            payload['orderId'] = order_id
        if order_link_id:
            payload['orderLinkId'] = order_link_id
        return self._request('POST', '/v5/order/cancel', payload=payload)

    def cancel_all_orders(self, symbol: str) -> Dict[str, Any]:
        return self._request('POST', '/v5/order/cancel-all', payload={'category': BYBIT_CATEGORY, 'symbol': symbol})

    def get_execution_list(self, symbol: str, limit: int = 50) -> List[Dict[str, Any]]:
        result = self._request('GET', '/v5/execution/list', params={'category': BYBIT_CATEGORY, 'symbol': symbol, 'limit': limit})
        return result.get('list') or []

    def create_order(
        self,
        *,
        symbol: str,
        side: str,
        order_type: str,
        qty: str,
        reduce_only: bool = False,
        price: Optional[str] = None,
        trigger_price: Optional[str] = None,
        trigger_direction: Optional[int] = None,
        close_on_trigger: bool = False,
        order_link_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            'category': BYBIT_CATEGORY,
            'symbol': symbol,
            'side': side,
            'orderType': order_type,
            'qty': qty,
            'positionIdx': 0,
            'reduceOnly': reduce_only,
            'orderLinkId': order_link_id or uuid.uuid4().hex[:36],
        }
        if order_type == 'Limit':
            payload['timeInForce'] = 'GTC'
        if price is not None:
            payload['price'] = price
        if trigger_price is not None:
            payload['triggerPrice'] = trigger_price
            payload['triggerBy'] = 'LastPrice'
        if trigger_direction is not None:
            payload['triggerDirection'] = trigger_direction
        if close_on_trigger:
            payload['closeOnTrigger'] = True
        return self._request('POST', '/v5/order/create', payload=payload)


init_db()


def db_execute(sql: str, params: Tuple[Any, ...] = ()) -> None:
    with DB_LOCK:
        cursor.execute(sql, params)
        conn.commit()


def db_fetchone(sql: str, params: Tuple[Any, ...] = ()) -> Optional[Tuple[Any, ...]]:
    with DB_LOCK:
        cursor.execute(sql, params)
        return cursor.fetchone()


def get_user_keys(user_id: int) -> Tuple[Optional[str], Optional[str]]:
    row = db_fetchone('SELECT api_key, secret_key FROM users WHERE user_id = ?', (user_id,))
    return row if row else (None, None)


def save_user_keys(user_id: int, api_key: str, secret_key: str) -> None:
    db_execute(
        '''
        INSERT INTO users (user_id, api_key, secret_key, updated_at)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(user_id) DO UPDATE SET
            api_key = excluded.api_key,
            secret_key = excluded.secret_key,
            is_active = 1,
            updated_at = CURRENT_TIMESTAMP
        ''',
        (user_id, api_key, secret_key),
    )


def reset_user_keys(user_id: int) -> None:
    db_execute('UPDATE users SET api_key = NULL, secret_key = NULL, updated_at = CURRENT_TIMESTAMP WHERE user_id = ?', (user_id,))


def store_signal(symbol: str, direction: str, payload: Dict[str, Any], status: str, details: str = '') -> None:
    db_execute(
        'INSERT INTO signals (symbol, direction, payload, status, details) VALUES (?, ?, ?, ?, ?)',
        (symbol, direction, json.dumps(payload, ensure_ascii=False), status, details),
    )


def update_position_record(symbol: str, side: str, qty: str, entry_order_id: str, stop_order_id: str, tp_order_ids: List[str]) -> None:
    db_execute(
        '''
        INSERT INTO positions (symbol, side, qty, entry_order_id, stop_order_id, tp_order_ids, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(symbol) DO UPDATE SET
            side = excluded.side,
            qty = excluded.qty,
            entry_order_id = excluded.entry_order_id,
            stop_order_id = excluded.stop_order_id,
            tp_order_ids = excluded.tp_order_ids,
            updated_at = CURRENT_TIMESTAMP
        ''',
        (symbol, side, qty, entry_order_id, stop_order_id, json.dumps(tp_order_ids)),
    )


def normalize_symbol(symbol: str) -> str:
    return str(symbol or '').strip().upper().replace('/', '').replace(':', '')


def decimal_places(value: str) -> int:
    value = str(value)
    return len(value.rstrip('0').split('.')[-1]) if '.' in value else 0


def floor_to_step(value: float, step: float) -> float:
    if step <= 0:
        return value
    return math.floor(value / step) * step


def trigger_direction(side: str, stop_price: float, current_price: float) -> int:
    if side == 'Buy':
        return 2 if stop_price < current_price else 1
    return 1 if stop_price > current_price else 2


def parse_direction(value: Any) -> Tuple[str, str]:
    text = str(value or '').strip().lower()
    if text in ('buy', 'long'):
        return 'Buy', 'Sell'
    if text in ('sell', 'short'):
        return 'Sell', 'Buy'
    raise ValueError('direction must be one of: buy, long, sell, short')


def parse_tp_levels(payload: Dict[str, Any]) -> List[float]:
    tp_levels = payload.get('tp_levels') or payload.get('take_profits') or payload.get('targets') or []
    if isinstance(tp_levels, str):
        tp_levels = [x.strip() for x in tp_levels.split(',') if x.strip()]
    return [float(x) for x in tp_levels]


def split_tp_volumes(total_qty: float, tp_count: int, qty_step: float) -> List[float]:
    if tp_count <= 0:
        return []
    raw_part = total_qty / tp_count
    result: List[float] = []
    allocated = 0.0
    for idx in range(tp_count):
        if idx == tp_count - 1:
            chunk = max(floor_to_step(total_qty - allocated, qty_step), qty_step)
        else:
            chunk = max(floor_to_step(raw_part, qty_step), qty_step)
        allocated += chunk
        result.append(chunk)
    return result


def extract_signal(payload: Dict[str, Any], symbol: str) -> Dict[str, Any]:
    side, close_side = parse_direction(payload.get('direction') or payload.get('side') or payload.get('signal'))
    stop_price = payload.get('stop_price') or payload.get('stop') or payload.get('sl')
    if stop_price is None:
        raise ValueError('stop_price is required')
    return {
        'symbol': normalize_symbol(symbol or payload.get('symbol')),
        'side': side,
        'close_side': close_side,
        'entry_price': float(payload['entry_price']) if payload.get('entry_price') is not None else None,
        'stop_price': float(stop_price),
        'tp_levels': parse_tp_levels(payload),
        'user_id': int(payload.get('user_id') or BYBIT_DEFAULT_USER_ID or 0),
    }


def calculate_order_qty(client: BybitV5Client, symbol: str) -> Tuple[str, Dict[str, Any], float]:
    instrument = client.get_instrument_info(symbol)
    ticker = client.get_ticker(symbol)
    summary = client.get_wallet_summary()
    mark_price = float(ticker.get('lastPrice') or 0.0)
    if mark_price <= 0:
        raise BybitAPIError(f'Invalid market price for {symbol}')
    lot = instrument.get('lotSizeFilter') or {}
    qty_step = float(lot.get('qtyStep') or 0.001)
    min_qty = float(lot.get('minOrderQty') or qty_step)
    qty_precision = decimal_places(lot.get('qtyStep') or '0.001')
    percent = BTC_VOLUME_PERCENT if symbol == 'BTCUSDT' else VOLUME_PERCENT
    notional = float(summary['available_balance']) * percent * LEVERAGE
    raw_qty = notional / mark_price
    qty = max(floor_to_step(raw_qty, qty_step), min_qty)
    return f'{qty:.{qty_precision}f}', instrument, mark_price


def notify_admins(text: str) -> None:
    for admin_id in ADMIN_IDS:
        try:
            bot.send_message(admin_id, text)
        except Exception:
            logging.exception('failed to notify admin %s', admin_id)


def authorize_webhook(req: request) -> None:
    if not BYBIT_WEBHOOK_SECRET:
        return
    secret = req.headers.get('X-Webhook-Secret', '')
    if secret != BYBIT_WEBHOOK_SECRET:
        raise PermissionError('invalid X-Webhook-Secret')


def process_bybit_signal(symbol: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    signal = extract_signal(payload, symbol)
    if not signal['user_id']:
        raise ValueError('user_id is required in payload or BYBIT_DEFAULT_USER_ID env')

    api_key, secret_key = get_user_keys(signal['user_id'])
    if not api_key or not secret_key:
        raise ValueError(f'Bybit API keys are not configured for user_id={signal["user_id"]}')

    client = BybitV5Client(api_key, secret_key)
    qty, instrument, market_price = calculate_order_qty(client, signal['symbol'])
    qty_float = float(qty)
    qty_step = float((instrument.get('lotSizeFilter') or {}).get('qtyStep') or 0.001)
    qty_precision = decimal_places((instrument.get('lotSizeFilter') or {}).get('qtyStep') or '0.001')

    try:
        client.set_leverage(signal['symbol'], LEVERAGE)
    except Exception as exc:
        logging.warning('set_leverage warning for %s: %s', signal['symbol'], exc)

    try:
        client.cancel_all_orders(signal['symbol'])
    except Exception as exc:
        logging.warning('cancel_all_orders warning for %s: %s', signal['symbol'], exc)

    entry_order = client.create_order(
        symbol=signal['symbol'],
        side=signal['side'],
        order_type='Market',
        qty=qty,
        order_link_id=f'entry-{signal["symbol"]}-{uuid.uuid4().hex[:12]}',
    )

    stop_order = client.create_order(
        symbol=signal['symbol'],
        side=signal['close_side'],
        order_type='Market',
        qty=qty,
        reduce_only=True,
        close_on_trigger=True,
        trigger_price=str(signal['stop_price']),
        trigger_direction=trigger_direction(signal['side'], signal['stop_price'], market_price),
        order_link_id=f'sl-{signal["symbol"]}-{uuid.uuid4().hex[:12]}',
    )

    tp_orders: List[Dict[str, Any]] = []
    tp_order_ids: List[str] = []
    if signal['tp_levels']:
        tp_sizes = split_tp_volumes(qty_float, len(signal['tp_levels']), qty_step)
        for index, (tp_price, tp_size) in enumerate(zip(signal['tp_levels'], tp_sizes), start=1):
            order = client.create_order(
                symbol=signal['symbol'],
                side=signal['close_side'],
                order_type='Limit',
                qty=f'{tp_size:.{qty_precision}f}',
                price=str(tp_price),
                reduce_only=True,
                order_link_id=f'tp{index}-{signal["symbol"]}-{uuid.uuid4().hex[:12]}',
            )
            tp_orders.append(order)
            if order.get('orderId'):
                tp_order_ids.append(order['orderId'])

    update_position_record(
        signal['symbol'],
        signal['side'],
        qty,
        entry_order.get('orderId', ''),
        stop_order.get('orderId', ''),
        tp_order_ids,
    )

    result = {
        'status': 'ok',
        'exchange': 'bybit',
        'testnet': BYBIT_TESTNET,
        'base_url': BYBIT_BASE_URL,
        'symbol': signal['symbol'],
        'side': signal['side'],
        'qty': qty,
        'market_price': market_price,
        'entry_order': entry_order,
        'stop_order': stop_order,
        'tp_orders': tp_orders,
    }
    store_signal(signal['symbol'], signal['side'], payload, 'processed', json.dumps(result, ensure_ascii=False))
    return result


def enqueue_signal(symbol: str, payload: Dict[str, Any]) -> str:
    try:
        SIGNAL_QUEUE.put_nowait((symbol, payload))
    except queue.Full as exc:
        raise RuntimeError('signal queue is full') from exc
    return str(SIGNAL_QUEUE.qsize())


def signal_worker_loop(worker_id: int) -> None:
    while True:
        symbol, payload = SIGNAL_QUEUE.get()
        try:
            process_bybit_signal(symbol, payload)
        except Exception as exc:
            logging.exception('signal worker %s failed for %s', worker_id, symbol)
            try:
                store_signal(normalize_symbol(symbol), str(payload.get('direction') or payload.get('side') or 'unknown'), payload, 'error', str(exc))
            except Exception:
                logging.exception('failed to store worker error')
            notify_admins(f'❌ Bybit worker error for {symbol}: {exc}')
        finally:
            SIGNAL_QUEUE.task_done()


def start_signal_workers() -> None:
    if SIGNAL_THREADS:
        return
    for idx in range(BYBIT_SIGNAL_WORKERS):
        thread = threading.Thread(target=signal_worker_loop, args=(idx + 1,), daemon=True, name=f'bybit-signal-worker-{idx + 1}')
        thread.start()
        SIGNAL_THREADS.append(thread)


@app.route('/health', methods=['GET'])
def health():
    return jsonify(
        {
            'status': 'ok',
            'exchange': 'bybit',
            'testnet': BYBIT_TESTNET,
            'base_url': BYBIT_BASE_URL,
            'port': APP_PORT,
            'db_path': DB_PATH,
            'signal_mode': BYBIT_SIGNAL_MODE,
            'signal_queue_size': SIGNAL_QUEUE.qsize(),
            'signal_workers': BYBIT_SIGNAL_WORKERS,
        }
    )


@app.route('/webhook/<symbol>', methods=['POST'])
def webhook(symbol: str):
    try:
        authorize_webhook(request)
        payload = request.get_json(force=True, silent=False) or {}
        if BYBIT_SIGNAL_MODE == 'async':
            qsize = enqueue_signal(symbol, payload)
            return jsonify({'status': 'accepted', 'exchange': 'bybit', 'queue_size': qsize}), 202
        result = process_bybit_signal(symbol, payload)
        return jsonify(result), 200
    except Exception as exc:
        logging.exception('webhook failed for %s', symbol)
        payload = request.get_json(silent=True) or {}
        try:
            store_signal(normalize_symbol(symbol), str(payload.get('direction') or payload.get('side') or 'unknown'), payload, 'error', str(exc))
        except Exception:
            logging.exception('failed to persist webhook error')
        return jsonify({'status': 'error', 'exchange': 'bybit', 'message': str(exc)}), 400


@bot.message_handler(commands=['start'])
def start_message(message):
    bot.send_message(
        message.chat.id,
        'Bybit testnet бот готов. Отправь API key и Secret key одним сообщением в формате:\nAPI_KEY:SECRET_KEY\n\n'
        f'Сейчас бот будет слушать /webhook/<symbol> на порту {APP_PORT} и слать реальные Bybit V5 testnet HTTP-запросы.',
    )


@bot.message_handler(commands=['balance'])
def balance_message(message):
    api_key, secret_key = get_user_keys(message.chat.id)
    if not api_key or not secret_key:
        bot.send_message(message.chat.id, '❌ Сначала сохрани Bybit API key/secret через /start')
        return
    try:
        summary = BybitV5Client(api_key, secret_key).get_wallet_summary()
        bot.send_message(
            message.chat.id,
            '💰 Bybit testnet balance\n'
            f"Total Equity: {summary['total_equity']:.2f} USDT\n"
            f"Available Balance: {summary['available_balance']:.2f} USDT\n"
            f"USDT Equity: {summary['usdt_equity']:.2f} USDT",
        )
    except Exception as exc:
        bot.send_message(message.chat.id, f'❌ Ошибка запроса баланса Bybit: {exc}')


@bot.message_handler(commands=['reset_keys'])
def reset_keys_message(message):
    reset_user_keys(message.chat.id)
    bot.send_message(message.chat.id, '✅ Bybit API key и secret удалены. Отправь новые через /start')


@bot.message_handler(commands=['health'])
def health_message(message):
    status = health().json
    bot.send_message(message.chat.id, json.dumps(status, ensure_ascii=False, indent=2))


@bot.message_handler(func=lambda message: ':' in (message.text or ''))
def save_credentials(message):
    text = (message.text or '').strip()
    if ':' not in text:
        return
    api_key, secret_key = [part.strip() for part in text.split(':', 1)]
    if not api_key or not secret_key:
        bot.send_message(message.chat.id, '❌ Формат неверный. Используй API_KEY:SECRET_KEY')
        return
    try:
        summary = BybitV5Client(api_key, secret_key).get_wallet_summary()
        save_user_keys(message.chat.id, api_key, secret_key)
        bot.send_message(
            message.chat.id,
            '✅ Bybit testnet ключи сохранены.\n'
            f"Total Equity: {summary['total_equity']:.2f} USDT\n"
            f"Available Balance: {summary['available_balance']:.2f} USDT\n"
            f"USDT Equity: {summary['usdt_equity']:.2f} USDT",
        )
    except Exception as exc:
        bot.send_message(message.chat.id, f'❌ Не удалось проверить Bybit testnet ключи: {exc}')


def run_telegram_bot() -> None:
    while True:
        try:
            bot.infinity_polling(timeout=30, long_polling_timeout=30)
        except Exception:
            logging.exception('telegram polling crashed, restarting in 5s')
            time.sleep(5)


if __name__ == '__main__':
    start_signal_workers()
    logging.info('Starting Bybit bot on %s', APP_PORT)
    logging.info('Bybit testnet=%s base_url=%s category=%s', BYBIT_TESTNET, BYBIT_BASE_URL, BYBIT_CATEGORY)
    logging.info('signal_mode=%s signal_workers=%s db_path=%s', BYBIT_SIGNAL_MODE, BYBIT_SIGNAL_WORKERS, DB_PATH)
    telegram_thread = threading.Thread(target=run_telegram_bot, daemon=True, name='bybit-telegram-thread')
    telegram_thread.start()
    app.run(host='0.0.0.0', port=APP_PORT, debug=False)
