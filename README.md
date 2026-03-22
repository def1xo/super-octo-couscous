# Быстрый запуск из `/home/def1xo/super-octo-couscous/`

Ниже — максимально короткий запуск под твой сценарий: **оба файла лежат в одной папке**, тебе нужно только подставить свой `TELEGRAM_BOT_TOKEN`, после чего открыть 2 терминала для ботов и 1 команду для `nginx`.

> Важно: `trtestforsp_ideal_v19_bybit_testnet.py` сейчас сделан как **полный логический клон** основного бота, но вынесен в отдельный файл, отдельную БД и отдельный порт `6001`, чтобы дальше уже поэтапно менять интеграцию на Bybit, не ломая основной процесс.

## 0. Один раз установить зависимости
Скопируй и выполни:

```bash
cd /home/def1xo/super-octo-couscous/ && \
python3 -m venv .venv && \
source .venv/bin/activate && \
pip install --upgrade pip && \
pip install flask pytelegrambotapi requests python-binance websocket-client
```

---

## 1. Терминал №1 — основной бот на `5001`
Скопируй и выполни:

```bash
cd /home/def1xo/super-octo-couscous/ && \
source .venv/bin/activate && \
export TELEGRAM_BOT_TOKEN='ВСТАВЬ_СЮДА_СВОЙ_TG_BOT_TOKEN' && \
export BINANCE_TESTNET=1 && \
export BINANCE_DB_PATH=binance_testnet_users.db && \
python3 trtestforsp_ideal_v19_testnet.py
```

---

## 2. Терминал №2 — клон бота на `6001`
Скопируй и выполни:

```bash
cd /home/def1xo/super-octo-couscous/ && \
source .venv/bin/activate && \
export TELEGRAM_BOT_TOKEN='ВСТАВЬ_СЮДА_СВОЙ_TG_BOT_TOKEN' && \
export BINANCE_TESTNET=1 && \
export BYBIT_BOT_PORT=6001 && \
export BYBIT_DB_PATH=bybit_testnet_users.db && \
python3 trtestforsp_ideal_v19_bybit_testnet.py
```

---

## 3. Установка `nginx` + включение + запуск
Скопируй и выполни одной командой:

```bash
sudo apt update && \
sudo apt install -y nginx && \
sudo cp /home/def1xo/super-octo-couscous/nginx.conf /etc/nginx/sites-available/trading-bots && \
sudo ln -sf /etc/nginx/sites-available/trading-bots /etc/nginx/sites-enabled/trading-bots && \
sudo rm -f /etc/nginx/sites-enabled/default && \
sudo nginx -t && \
sudo systemctl enable --now nginx && \
sudo systemctl reload nginx
```

---

## 4. Быстрая проверка, что всё поднялось
### Проверка основного бота
```bash
curl http://127.0.0.1:5001/health
```

### Проверка клона на `6001`
```bash
curl http://127.0.0.1:6001/health
```

### Проверка `nginx` на `80`
```bash
curl http://127.0.0.1/health
```

---

## 5. Что куда идёт сейчас
- `5001` — основной бот.
- `6001` — второй файл-клон под дальнейшую Bybit-адаптацию.
- `80` — `nginx`.
- `nginx` отправляет основной запрос на `5001` и зеркалирует этот же запрос на `6001`.

---

## 6. Если остановить
### Остановить ботов
В каждом терминале нажми:

```bash
Ctrl+C
```

### Остановить nginx
```bash
sudo systemctl stop nginx
```

---

## 7. Что тебе реально нужно поменять прямо сейчас
Минимум тебе нужно поменять TG token, а базы уже разнесены по разным именам:

```bash
export TELEGRAM_BOT_TOKEN='ВСТАВЬ_СЮДА_СВОЙ_TG_BOT_TOKEN'
```

Имена БД по умолчанию:

```bash
binance_testnet_users.db
bybit_testnet_users.db
```

После этого оба процесса будут работать из одной и той же папки:

```bash
/home/def1xo/super-octo-couscous/
```
