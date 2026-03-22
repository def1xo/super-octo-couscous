# Запуск Binance + Bybit testnet ботов и Nginx

## Что добавлено
- `trtestforsp_ideal_v19_testnet.py` продолжает работать на `5001` как раньше.
- `trtestforsp_ideal_v19_bybit_testnet.py` — полный логический клон основного бота, вынесенный в отдельный файл/БД/порт `6001` для дальнейшей миграции на Bybit.
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
export BYBIT_BOT_PORT=6001
export BINANCE_DB_PATH=binance_testnet_users.db
export BYBIT_DB_PATH=bybit_testnet_users.db
```

Новый файл сейчас повторяет логику существующего бота почти дословно, поэтому для его запуска используются те же runtime-зависимости и те же exchange-переменные, что и у исходного файла, плюс отдельные `BYBIT_BOT_PORT`/`BYBIT_DB_PATH`.

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
- Клон бота слушает `0.0.0.0:6001`.

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

## 7. Пример webhook для клона на 6001
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

## 8. Telegram команды клона на 6001
- `/start` — сохранить `API_KEY:SECRET_KEY`.
- `/balance` — проверить баланс testnet аккаунта.

## 9. Что важно
- Основной бот использует отдельную БД `binance_testnet_users.db`.
- Клон использует отдельную БД `bybit_testnet_users.db`.
- Сейчас это именно полный логический клон основного 4000+ строчного бота, а не урезанная реализация.
- Если захотите позже уже поэтапно менять интеграцию на Bybit, удобнее будет делать это в этом отдельном файле, не ломая основной бот на `5001`.
- Если захотите позже развести маршрутизацию по домену/path — можно будет изменить только `nginx.conf`, не меняя код ботов.
