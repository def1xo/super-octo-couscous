# Запуск Binance + Bybit testnet ботов и Nginx

## Что добавлено
- `trtestforsp_ideal_v19_testnet.py` продолжает работать на `5001` как раньше.
- `trtestforsp_ideal_v19_bybit_testnet.py` — отдельный Bybit testnet бот на `6001` с собственной БД и отдельными HTTP-запросами к Bybit V5 API.
- `nginx.conf` слушает `80` порт, основной трафик проксирует на `5001`, а через `mirror` дублирует те же запросы на `6001`.

## 1. Установка системных пакетов
```bash
sudo apt update
sudo apt install -y nginx python3 python3-pip python3-venv
```

## 2. Установка Python зависимостей
В каталоге проекта:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install flask pytelegrambotapi requests python-binance websocket-client
```

## 3. Переменные окружения
Минимальный набор:
```bash
export TELEGRAM_BOT_TOKEN='ваш_telegram_bot_token'
export BINANCE_TESTNET=1
export BYBIT_TESTNET=1
export BYBIT_BOT_PORT=6001
export BINANCE_DB_PATH=binance_testnet_users.db
export BYBIT_DB_PATH=bybit_testnet_users.db
```

Новый файл теперь работает именно с Bybit V5 API testnet (`api-testnet.bybit.com`) и использует свои переменные `BYBIT_TESTNET` / `BYBIT_BOT_PORT` / `BYBIT_DB_PATH`.

## 4. Запуск двух ботов
В двух отдельных терминалах или через process manager:
```bash
source .venv/bin/activate
python trtestforsp_ideal_v19_testnet.py
```

```bash
source .venv/bin/activate
python trtestforsp_ideal_v19_bybit_testnet.py
```

Ожидаемо:
- Binance бот слушает `127.0.0.1:5001`.
- Bybit testnet бот слушает `0.0.0.0:6001`.

## 5. Установка nginx.conf
Скопируйте конфиг:
```bash
sudo cp nginx.conf /etc/nginx/sites-available/trading-bots
sudo ln -sf /etc/nginx/sites-available/trading-bots /etc/nginx/sites-enabled/trading-bots
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t
sudo systemctl reload nginx
```

## 6. Как это работает сейчас
- Любой запрос на `http://SERVER_IP/...` попадает в `nginx` на `80` порт.
- `nginx` отправляет основной запрос на `5001`.
- Тот же самый запрос зеркалируется на `6001`.
- То есть пока без дополнительных префиксов и отдельных route оба приложения получают одинаковый входящий HTTP трафик.

## 7. Пример webhook для Bybit testnet на 6001
```bash
curl -X POST http://127.0.0.1/webhook/BTCUSDT \
  -H 'Content-Type: application/json' \
  -H 'X-Webhook-Secret: секрет_для_заголовка_X-Webhook-Secret' \
  -d '{
    "user_id": 123456789,
    "direction": "buy",
    "entry_price": 84000,
    "stop_price": 83000,
    "tp_levels": [85000, 86000, 87000]
  }'
```

## 8. Telegram команды Bybit testnet бота на 6001
- `/start` — сохранить `API_KEY:SECRET_KEY`.
- `/balance` — проверить баланс testnet аккаунта.

## 9. Что важно
- Основной бот использует отдельную БД `binance_testnet_users.db`.
- Bybit бот использует отдельную БД `bybit_testnet_users.db`.
- Bybit бот работает через отдельные HTTP-запросы к Bybit V5 API testnet.
- Следующим шагом уже можно будет нормально разводить Telegram-ботов и отдельные команды запуска для каждого процесса.
- Если захотите позже развести маршрутизацию по домену/path — можно будет изменить только `nginx.conf`, не меняя код ботов.
