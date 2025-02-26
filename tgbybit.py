import requests
import telebot
from telebot import types
from datetime import datetime, timedelta
from time import sleep
import threading
import matplotlib.pyplot as plt
from openpyxl import Workbook, load_workbook
import logging
import time
import json
import os
import matplotlib.dates as mdates
from matplotlib.ticker import MaxNLocator

logging.basicConfig(level=logging.ERROR)
plt.switch_backend('Agg')

CONFIG_FILE = 'config.json'

def load_config():
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

def save_config(config):
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=4, ensure_ascii=False)

config = load_config()

TOKEN = config.get('TOKEN', '')
cookies = config.get('cookies', '')
admins = config.get('admins', [])
db_update_interval = config.get('db_update_interval', 30)
balance_send_interval = config.get('balance_send_interval', 30)
chat_id = config.get('chat_id', '')

REQUEST_TIMEOUT = 60
MAX_RETRIES = 5
EXCEL_FILE = 'balance_data.xlsx'
WAITING_FOR_RENEW = False

BOT_LIST_URL = 'https://api2.bybit.com/s1/bot/tradingbot/v1/list-all-bots'
BALANCE_URL = 'https://api2.bybit.com/v3/private/cht/asset-common/total-balance?quoteCoin=USDT&balanceType=1'

bot = telebot.TeleBot(TOKEN)

keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
keyboard.add(types.KeyboardButton('/balance'), types.KeyboardButton('/graph'))

last_balance = None

db_update_thread = None
balance_send_thread = None
stop_threads = False

def setup_excel():
    try:
        workbook = load_workbook(EXCEL_FILE)
        worksheet = workbook.active
    except FileNotFoundError:
        workbook = Workbook()
        worksheet = workbook.active
        worksheet.append(['Дата', 'Баланс USDT', 'Баланс RUB', 'Изменение (%)'])
        workbook.save(EXCEL_FILE)
    return workbook, worksheet

workbook, worksheet = setup_excel()

def expire_mode_notify():
    global WAITING_FOR_RENEW
    WAITING_FOR_RENEW = True
    for admin_id in admins:
        try:
            bot.send_message(admin_id, "Срок действия данных истёк или возникла ошибка соединения. Обновите данные и перезапустите бота через панель админа.")
        except:
            pass

def retry_request(url, method='GET', headers=None, params=None, cookies_arg=None, timeout=REQUEST_TIMEOUT):
    attempts = 0
    while attempts < MAX_RETRIES:
        try:
            if method == 'GET':
                response = requests.get(url, headers=headers, params=params, cookies=cookies_arg, timeout=timeout)
            else:
                response = requests.post(url, headers=headers, data=params, cookies=cookies_arg, timeout=timeout)

            response.raise_for_status()
            return response
        except requests.RequestException as e:
            logging.error(f"Ошибка запроса: {e}")
            attempts += 1
            sleep(2 ** attempts)

    expire_mode_notify()
    return None

def get_usdt_to_rub():
    if WAITING_FOR_RENEW:
        return None
    response = retry_request('https://api.coingecko.com/api/v3/simple/price?ids=tether&vs_currencies=rub')
    if response and not WAITING_FOR_RENEW:
        data = response.json()
        return float(data['tether']['rub'])
    return None

def fetch_bot_list_data():
    if WAITING_FOR_RENEW:
        return []
    response = retry_request(BOT_LIST_URL, method='POST', cookies_arg={'secure-token': cookies})
    if response and not WAITING_FOR_RENEW:
        data = response.json()
        if data.get("ret_code") == 0:
            bots = data.get("result", {}).get("bots", [])
            return bots
    return []

def fetch_balance_cookies(add_to_db=True):
    global last_balance
    if WAITING_FOR_RENEW:
        return "Бот в режиме ожидания обновления данных."

    response = retry_request(
        BALANCE_URL,
        cookies_arg={'secure-token': cookies})
    if response and not WAITING_FOR_RENEW:
        data = response.json()
        if 'result' in data and 'totalBalanceItems' in data['result']:
            for item in data['result']['totalBalanceItems']:
                if item['accountType'] == 'ACCOUNT_TYPE_BOT':
                    current_balance = float(item['originBalance'])
                    usdt_to_rub = get_usdt_to_rub()
                    if usdt_to_rub:
                        rub_balance = current_balance * usdt_to_rub
                    else:
                        rub_balance = "Ошибка курса"

                    now = datetime.now()

                    rows = list(worksheet.iter_rows(values_only=True))[1:]
                    closest_balance_24h_ago = None
                    closest_time_diff = float('inf')
                    twenty_four_hours_ago_ts = time.time() - 24*3600

                    for row in rows:
                        timestamp_row = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S')
                        time_diff = abs(timestamp_row.timestamp() - twenty_four_hours_ago_ts)
                        if time_diff < closest_time_diff:
                            closest_time_diff = time_diff
                            closest_balance_24h_ago = float(row[1])

                    change_percent = 0
                    if closest_balance_24h_ago is not None:
                        change_percent = ((current_balance - closest_balance_24h_ago) / closest_balance_24h_ago) * 100

                    if add_to_db:
                        last_balance = current_balance
                        worksheet.append([now.strftime('%Y-%m-%d %H:%M:%S'), current_balance, rub_balance, change_percent])
                        workbook.save(EXCEL_FILE)

                    sign = '🟢 +' if change_percent >= 0 else '🔴 '
                    arrow = "📈" if change_percent >= 0 else "📉"
                    change_str = f"{arrow} Изменение за 24ч: {sign}{change_percent:.2f}%"

                    rows = list(worksheet.iter_rows(values_only=True))[1:]
                    now_ts = time.time()
                    count_24h = sum(
                        1 for r in rows if (now_ts - datetime.strptime(r[0], '%Y-%m-%d %H:%M:%S').timestamp()) <= 24 * 3600)
                    diff_str = ""
                    if len(rows) > 1:
                        last_balance_val = rows[-1][1]
                        prev_balance_val = rows[-2][1]
                        diff_val = last_balance_val - prev_balance_val
                        diff_sign = '🟢 +' if diff_val >= 0 else '🔴 '
                        diff_str = f"Изменение с последнего замера: {diff_sign}{diff_val:.2f} USDT"
                    else:
                        diff_str = "Недостаточно данных для расчёта изменения с последнего замера."

                    balance_info = (f"📅 Дата: {now.strftime('%Y-%m-%d %H:%M:%S')}\n"
                                    f"💰 Баланс: {current_balance:.2f} USDT = {rub_balance:.2f} RUB\n"
                                    f"{change_str}\n"
                                    f"📊 Записей за последние 24ч: {count_24h}\n"
                                    f"{diff_str}")

                    return balance_info
        else:
            expire_mode_notify()
            return "Срок действия cookies истёк. Бот в ожидании."
    return "Ошибка соединения или данные недоступны"

def fetch_balance(add_to_db=True):
    balance_info = fetch_balance_cookies(add_to_db=add_to_db)
    return balance_info

@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    bot.send_message(message.chat.id, "Бот запущен", reply_markup=keyboard)

@bot.message_handler(commands=['balance'])
def balance_cmd(message):
    try:
        balance_info = fetch_balance(add_to_db=False)
        bot.send_message(message.chat.id, balance_info)
    except Exception as e:
        logging.error(f"Ошибка при отправке баланса: {e}")
        bot.send_message(message.chat.id, "Произошла ошибка при отправке баланса")


def format_duration(sec_str):
    seconds = int(sec_str)
    days = seconds // 86400
    seconds %= 86400
    hours = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60
    return f"{days}D {hours}h {minutes}m"

def get_all_dates():
    rows = list(worksheet.iter_rows(values_only=True))[1:]
    dates = sorted(list(set([datetime.strptime(r[0], '%Y-%m-%d %H:%M:%S').date() for r in rows])))
    return dates

def get_months_from_dates(dates):
    ym_set = set((d.year, d.month) for d in dates)
    ym_list = sorted(ym_set)
    return ym_list

def dates_in_month(dates, year, month):
    return [d for d in dates if d.year == year and d.month == month]

def month_name(year, month):
    return datetime(year, month, 1).strftime('%B %Y')

def generate_calendar_markup(selected_year, selected_month):
    dates = get_all_dates()
    if not dates:
        return None, (selected_year, selected_month)
    months = get_months_from_dates(dates)
    if (selected_year, selected_month) not in months:
        selected_year, selected_month = months[-1]

    current_month_dates = dates_in_month(dates, selected_year, selected_month)

    markup = types.InlineKeyboardMarkup(row_width=7)
    day_buttons = []
    for d in current_month_dates:
        day_str = f"{d.day:02d}"
        cb_data = f"graph_day_{d.strftime('%Y%m%d')}"
        day_buttons.append(types.InlineKeyboardButton(day_str, callback_data=cb_data))
    if day_buttons:
        markup.add(*day_buttons)

    idx = months.index((selected_year, selected_month))
    prev_month_cb = None
    next_month_cb = None
    if idx > 0:
        py, pm = months[idx-1]
        prev_month_cb = f"graph_month_{py}{pm:02d}"
    if idx < len(months)-1:
        ny, nm = months[idx+1]
        next_month_cb = f"graph_month_{ny}{nm:02d}"

    nav_buttons = []
    if prev_month_cb:
        nav_buttons.append(types.InlineKeyboardButton("<", callback_data=f"graph_monthnav_prev_{selected_year}{selected_month:02d}"))
    nav_buttons.append(types.InlineKeyboardButton(month_name(selected_year, selected_month), callback_data=f"graph_month_{selected_year}{selected_month:02d}"))
    if next_month_cb:
        nav_buttons.append(types.InlineKeyboardButton(">", callback_data=f"graph_monthnav_next_{selected_year}{selected_month:02d}"))

    markup.add(*nav_buttons)
    return markup, (selected_year, selected_month)

def get_default_month():
    dates = get_all_dates()
    if not dates:
        return None
    months = get_months_from_dates(dates)
    return months[-1] if months else None

def generate_graph_for_date(selected_date=None):
    rows = list(worksheet.iter_rows(values_only=True))[1:]
    if len(rows) < 2:
        return None, "Недостаточно данных для построения графика"

    if selected_date is None:
        all_dates = sorted(list(set([datetime.strptime(r[0], '%Y-%m-%d %H:%M:%S').date() for r in rows])))
        if not all_dates:
            return None, "Нет данных."
        selected_date = all_dates[-1]

    day_rows = [(datetime.strptime(r[0], '%Y-%m-%d %H:%M:%S'), r[1]) for r in rows if datetime.strptime(r[0], '%Y-%m-%d %H:%M:%S').date() == selected_date]
    if len(day_rows) < 2:
        return None, "Недостаточно данных для выбранного дня"

    # Сортируем и используем как ранее, без пропусков, просто как было
    day_rows.sort(key=lambda x: x[0])
    times = [x[0] for x in day_rows]
    balances_usdt = [x[1] for x in day_rows]

    daily_balances = {}
    for row in rows:
        timestamp = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S')
        date = timestamp.date()
        balance_usdt = row[1]
        if isinstance(balance_usdt, (int, float)):
            if date not in daily_balances:
                daily_balances[date] = []
            daily_balances[date].append(balance_usdt)

    all_dates_sorted = sorted(daily_balances.keys())
    if len(all_dates_sorted) > 30:
        last_30_days = all_dates_sorted[-30:]
    else:
        last_30_days = all_dates_sorted

    avg_30, max_30, min_30, dates_30 = [], [], [], []
    for d in last_30_days:
        vals = daily_balances[d]
        avg_30.append(sum(vals)/len(vals))
        max_30.append(max(vals))
        min_30.append(min(vals))
        dates_30.append(d)

    now_date = datetime.now().date()
    one_year_ago = now_date - timedelta(days=365)
    monthly_balances = {}
    for d, vals in daily_balances.items():
        if d >= one_year_ago:
            month = d.replace(day=1)
            if month not in monthly_balances:
                monthly_balances[month] = []
            monthly_balances[month].extend(vals)

    avg_year, max_year, min_year, dates_year = [], [], [], []
    if monthly_balances:
        for m in sorted(monthly_balances.keys()):
            vs = monthly_balances[m]
            avg_m = sum(vs)/len(vs)
            avg_year.append(avg_m)
            max_year.append(max(vs))
            min_year.append(min(vs))
            dates_year.append(m)

    bots_data = fetch_bot_list_data()
    bots_data = bots_data[:6]

    fig = plt.figure(figsize=(16,10), constrained_layout=True)
    gs = fig.add_gridspec(3, 3, width_ratios=[4,1,1], height_ratios=[1,1,1])

    ax_day = fig.add_subplot(gs[0,0])
    ax_30 = fig.add_subplot(gs[1,0])
    ax_year = fig.add_subplot(gs[2,0])

    ax_bot = []
    ax_bot.append(fig.add_subplot(gs[0,1]))
    ax_bot.append(fig.add_subplot(gs[0,2]))
    ax_bot.append(fig.add_subplot(gs[1,1]))
    ax_bot.append(fig.add_subplot(gs[1,2]))
    ax_bot.append(fig.add_subplot(gs[2,1]))
    ax_bot.append(fig.add_subplot(gs[2,2]))

    for a in ax_bot:
        a.axis('off')
        a.set_facecolor('white')

    y_locator = MaxNLocator(nbins=5)
    for ax in [ax_day, ax_30, ax_year]:
        ax.yaxis.set_major_locator(y_locator)
        ax.grid(True, which='both', linestyle='--', linewidth=0.5)

    # График за день (как было в начале - с точками и аннотациями)
    ax_day.plot(times, balances_usdt, marker='o', linestyle='-', color='tab:red', label='Баланс (текущий день)')
    ax_day.set_ylabel('Баланс (USDT)', fontsize=9)
    ax_day.set_title(f'Баланс за {selected_date.strftime("%Y-%m-%d")}', fontsize=10)
    ax_day.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax_day.tick_params(axis='x', rotation=45, labelsize=8)
    ax_day.tick_params(axis='y', labelsize=8)
    step = max(1, len(times)//10)
    for i in range(0, len(times), step):
        ax_day.annotate(f'{balances_usdt[i]:.2f}', (times[i], balances_usdt[i]),
                        textcoords="offset points", xytext=(0,10), ha='center', fontsize=7, color='black')
    ax_day.legend(fontsize=7)

    # 30 дней с аннотацией Max, Avg, Min (как было)
    if dates_30:
        ax_30.plot(dates_30, avg_30, marker='o', linestyle='-', color='tab:blue', label='Средний баланс (30 дней)')
        ax_30.set_ylabel('Средний баланс (USDT)', fontsize=9)
        ax_30.set_title('Средний баланс за последние 30 дней', fontsize=10)
        ax_30.xaxis.set_major_formatter(mdates.DateFormatter('%d\n%b'))
        ax_30.tick_params(axis='x', rotation=45, labelsize=8)
        ax_30.tick_params(axis='y', labelsize=8)
        step_30 = max(1, len(dates_30)//5)
        # Аннотация с цветными значениями и чёрным текстом "Max/Avg/Min"
        for i in range(0, len(dates_30), step_30):
            x, y = dates_30[i], avg_30[i]
            # Max
            ax_30.annotate("Max:", (x,y), xytext=(-20,40), textcoords="offset points", ha='right', fontsize=7, color='black')
            ax_30.annotate(f"{max_30[i]:.2f}", (x,y), xytext=(0,40), textcoords="offset points", ha='left', fontsize=7, color='green')
            # Avg
            ax_30.annotate("Avg:", (x,y), xytext=(-20,25), textcoords="offset points", ha='right', fontsize=7, color='black')
            ax_30.annotate(f"{avg_30[i]:.2f}", (x,y), xytext=(0,25), textcoords="offset points", ha='left', fontsize=7, color='orange')
            # Min
            ax_30.annotate("Min:", (x,y), xytext=(-20,10), textcoords="offset points", ha='right', fontsize=7, color='black')
            ax_30.annotate(f"{min_30[i]:.2f}", (x,y), xytext=(0,10), textcoords="offset points", ha='left', fontsize=7, color='red')
        ax_30.legend(fontsize=7)
    else:
        ax_30.text(0.5, 0.5, 'Нет данных за последние 30 дней', ha='center', va='center', transform=ax_30.transAxes, fontsize=9)

    # Год с аннотацией Max, Avg, Min (как было)
    if dates_year:
        ax_year.plot(dates_year, avg_year, marker='o', linestyle='-', color='tab:green', label='Средний баланс (год)')
        ax_year.set_ylabel('Средний баланс (USDT)', fontsize=9)
        ax_year.set_title('Средний баланс за последний год', fontsize=10)
        ax_year.xaxis.set_major_formatter(mdates.DateFormatter('%b\n%Y'))
        ax_year.tick_params(axis='x', rotation=45, labelsize=8)
        ax_year.tick_params(axis='y', labelsize=8)
        step_year = max(1, len(dates_year)//5)
        for i in range(0, len(dates_year), step_year):
            x, y = dates_year[i], avg_year[i]
            # Max
            ax_year.annotate("Max:", (x,y), xytext=(-20,40), textcoords="offset points", ha='right', fontsize=7, color='black')
            ax_year.annotate(f"{max_year[i]:.2f}", (x,y), xytext=(0,40), textcoords="offset points", ha='left', fontsize=7, color='green')
            # Avg
            ax_year.annotate("Avg:", (x,y), xytext=(-20,25), textcoords="offset points", ha='right', fontsize=7, color='black')
            ax_year.annotate(f"{avg_year[i]:.2f}", (x,y), xytext=(0,25), textcoords="offset points", ha='left', fontsize=7, color='orange')
            # Min
            ax_year.annotate("Min:", (x,y), xytext=(-20,10), textcoords="offset points", ha='right', fontsize=7, color='black')
            ax_year.annotate(f"{min_year[i]:.2f}", (x,y), xytext=(0,10), textcoords="offset points", ha='left', fontsize=7, color='red')
        ax_year.legend(fontsize=7)
    else:
        ax_year.text(0.5, 0.5, 'Нет данных за последний год', ha='center', va='center', transform=ax_year.transAxes, fontsize=9)

    # Меню ботов
    for i, b in enumerate(bots_data):
        axx = ax_bot[i]
        axx.set_facecolor('white')
        axx.axis([0,1,0,1])
        symbol = 'N/A'
        invested = 'N/A'
        pnl = 'N/A'
        pnl_per = '0.00%'
        price_range = 'N/A'
        price_drop = 'N/A'
        cell_num = 'N/A'
        add_pos_per = 'N/A'
        mark_price = 'N/A'
        liq_price = 'N/A'
        bot_name_type = ''
        bot_tag = ''
        runtime = 'N/A'

        b_type = b.get('type','N/A')
        fg = b.get('future_grid')
        fm = b.get('fmart')

        if b_type == 'GRID_FUTURES' and fg:
            symbol = fg.get('symbol','N/A')
            invested = fg.get('total_investment','N/A')
            pnl = fg.get('pnl','N/A')
            try:
                pp = float(fg.get('pnl_per','0'))*100
                pnl_per = f"{pp:.2f}%"
            except:
                pnl_per = '0.00%'
            mark_price = fg.get('mark_price','N/A')
            liq_price = fg.get('liq_price','N/A')
            price_range = f"{fg.get('min_price','N/A')} - {fg.get('max_price','N/A')}"
            cell_num = fg.get('cell_num','N/A')
            success_trades = fg.get('arbitrage_num','N/A')
            duration = fg.get('running_duration','0')
            runtime = format_duration(duration)
            mode = fg.get('grid_mode','').lower()
            lev = fg.get('leverage','N/A')
            bot_name_type = "Фьючерсный grid-бот"
            if 'neutral' in mode:
                bot_tag = f"Нейтральный {lev}x"
            elif 'long' in mode:
                bot_tag = f"Лонг {lev}x"
            elif 'short' in mode:
                bot_tag = f"Шорт {lev}x"

            text = (
                f"*{symbol}*\n"
                f"Тип: {bot_name_type}\n"
                f"{bot_tag}\n"
                f"Активен: {runtime}\n"
                f"Инвестиции (USDT): {invested}\n"
                f"Общий P&L (USDT): {pnl}\n"
                f"% P&L: {pnl_per}\n"
                f"Ценовой диапазон: {price_range}\n"
                f"Кол-во сеток: {cell_num}\n"
                f"Успешные сделки: {success_trades}\n"
                f"Цена маркировки: {mark_price} USDT\n"
                f"Цена ликвидации: {liq_price} USDT\n"
            )

        elif b_type == 'MART_FUTURES' and fm:
            symbol = fm.get('symbol','N/A')
            invested = fm.get('total_margin','N/A')
            pnl = fm.get('total_profit','N/A')
            try:
                pp = float(fm.get('total_profit_per','0'))*100
                pnl_per = f"{pp:.2f}%"
            except:
                pnl_per = '0.00%'
            mark_price = fm.get('mark_price','N/A')
            liq_price = fm.get('liq_price','N/A')
            add_pos_per = fm.get('add_pos_per','N/A')
            price_f = fm.get('price_float_per','0')
            if price_f:
                price_drop = f"{float(price_f)*100:.1f}%"
            else:
                price_drop = 'N/A'
            duration = fm.get('running_duration','0')
            runtime = format_duration(duration)
            bot_name_type = "Фьючерсный Мартингейл"
            mode = fm.get('fmart_mode','').lower()
            lev = fm.get('leverage','N/A')
            if 'neutral' in mode:
                bot_tag = f"Нейтральный {lev}x"
            elif 'long' in mode:
                bot_tag = f"Лонг {lev}x"
            elif 'short' in mode:
                bot_tag = f"Шорт {lev}x"

            text = (
                f"*{symbol}*\n"
                f"Тип: {bot_name_type}\n"
                f"{bot_tag}\n"
                f"Активен: {runtime}\n"
                f"Инвестиции (USDT): {invested}\n"
                f"Общий P&L (USDT): {pnl}\n"
                f"% P&L: {pnl_per}\n"
                f"Снижение цены: {price_drop}\n"
                f"Множитель позиции: {add_pos_per}\n"
                f"Цена маркировки: {mark_price} USDT\n"
                f"Цена ликвидации: {liq_price} USDT\n"
            )

        elif b_type == 'GRID_SPOT' and b.get('grid', {}).get('info'):
            gr = b['grid']['info']
            profit = b['grid']['profit']
            symbol = gr.get('symbol','N/A')
            invested = gr.get('total_investment','N/A')
            pnl = profit.get('total_profit','N/A')
            try:
                pp = float(profit.get('total_apr','0'))*100
                pnl_per = f"{pp:.2f}%"
            except:
                pnl_per = '0.00%'
            price_range = f"{gr.get('min_price','N/A')} - {gr.get('max_price','N/A')}"
            cell_num = gr.get('cell_number','N/A')
            duration = gr.get('operation_time','0')[:-3]
            runtime = format_duration(duration)
            bot_name_type = "Спотовый grid-бот"
            mode = gr.get('grid_mode','').lower()
            if 'neutral' in mode:
                bot_tag = f"Нейтральный"
            elif 'long' in mode:
                bot_tag = f"Лонг"
            elif 'short' in mode:
                bot_tag = f"Шорт"

            text = (
                f"*{symbol}*\n"
                f"Тип: {bot_name_type}\n"
                f"{bot_tag}\n"
                f"Активен: {runtime}\n"
                f"Инвестиции (USDT): {invested}\n"
                f"Общий P&L (USDT): {pnl}\n"
                f"% P&L: {pnl_per}\n"
                f"Ценовой диапазон: {price_range}\n"
                f"Кол-во сеток: {cell_num}\n"
            )
        else:
            text = "Нет данных о боте."

        axx.text(0.05, 0.95, text, ha='left', va='top', wrap=True, fontsize=8, color='black', transform=axx.transAxes)

    graph_filename = f'graph_{selected_date.strftime("%Y%m%d")}.png'
    plt.savefig(graph_filename, dpi=300)
    plt.close()
    return graph_filename, None

@bot.message_handler(commands=['graph'])
def send_graph(message):
    try:
        ym = get_default_month()
        if ym is None:
            bot.send_message(message.chat.id, "Нет данных для построения графиков.")
            return
        y,m = ym
        markup, (sy, sm) = generate_calendar_markup(y, m)
        if not markup:
            bot.send_message(message.chat.id, "Нет данных.")
            return
        dates = get_all_dates()
        selected_date = dates[-1]
        filename, error = generate_graph_for_date(selected_date)
        if error:
            bot.send_message(message.chat.id, error)
            return

        with open(filename, 'rb') as photo:
            bot.send_photo(message.chat.id, photo, caption="Выберите дату:", reply_markup=markup)
    except Exception as e:
        logging.error(f"Ошибка генерации графика: {e}")
        bot.send_message(message.chat.id, "Произошла ошибка при генерации графика.")

@bot.callback_query_handler(func=lambda call: call.data.startswith("graph_"))
def callback_graph(call):
    bot.answer_callback_query(call.id)

    if call.data.startswith("graph_day_"):
        date_str = call.data.replace("graph_day_", "")
        selected_date = datetime.strptime(date_str, "%Y%m%d").date()
        filename, error = generate_graph_for_date(selected_date)
        if error:
            with open(filename, 'rb') as photo:
                bot.edit_message_media(types.InputMediaPhoto(photo), chat_id=call.message.chat.id,
                                       message_id=call.message.message_id)
                bot.edit_message_caption(chat_id=call.message.chat.id, message_id=call.message.message_id,
                                         caption=error)
            return
        y, m = selected_date.year, selected_date.month
        markup, _ = generate_calendar_markup(y, m)
        with open(filename, 'rb') as photo:
            bot.edit_message_media(types.InputMediaPhoto(photo), chat_id=call.message.chat.id,
                                   message_id=call.message.message_id)
            bot.edit_message_caption(chat_id=call.message.chat.id, message_id=call.message.message_id,
                                     caption=f"График за {selected_date.strftime('%Y-%m-%d')}\nВыберите дату:",
                                     reply_markup=markup)

    elif call.data.startswith("graph_month_"):
        ym_str = call.data.replace("graph_month_", "")
        year = int(ym_str[:4])
        month = int(ym_str[4:])
        dates = dates_in_month(get_all_dates(), year, month)
        if not dates:
            return
        selected_date = dates[-1]
        filename, error = generate_graph_for_date(selected_date)
        if error:
            with open(filename, 'rb') as photo:
                bot.edit_message_media(types.InputMediaPhoto(photo), chat_id=call.message.chat.id,
                                       message_id=call.message.message_id)
                bot.edit_message_caption(chat_id=call.message.chat.id, message_id=call.message.message_id,
                                         caption=error)
            return
        markup, _ = generate_calendar_markup(year, month)
        with open(filename, 'rb') as photo:
            bot.edit_message_media(types.InputMediaPhoto(photo), chat_id=call.message.chat.id,
                                   message_id=call.message.message_id)
            bot.edit_message_caption(chat_id=call.message.chat.id, message_id=call.message.message_id,
                                     caption=f"График за {selected_date.strftime('%Y-%m-%d')}\nВыберите дату:",
                                     reply_markup=markup)

    elif call.data.startswith("graph_monthnav_prev_") or call.data.startswith("graph_monthnav_next_"):
        ym_str = call.data[-6:]
        year = int(ym_str[:4])
        month = int(ym_str[4:])
        dates = get_all_dates()
        months = get_months_from_dates(dates)
        idx = months.index((year, month))
        if call.data.startswith("graph_monthnav_prev_") and idx > 0:
            year, month = months[idx-1]
        elif call.data.startswith("graph_monthnav_next_") and idx < len(months)-1:
            year, month = months[idx+1]

        md = dates_in_month(dates, year, month)
        if not md:
            return
        selected_date = md[-1]
        filename, error = generate_graph_for_date(selected_date)
        if error:
            with open(filename, 'rb') as photo:
                bot.edit_message_media(types.InputMediaPhoto(photo), chat_id=call.message.chat.id,
                                       message_id=call.message.message_id)
                bot.edit_message_caption(chat_id=call.message.chat.id, message_id=call.message.message_id,
                                         caption=error)
            return
        markup, _ = generate_calendar_markup(year, month)
        with open(filename, 'rb') as photo:
            bot.edit_message_media(types.InputMediaPhoto(photo), chat_id=call.message.chat.id,
                                   message_id=call.message.message_id)
            bot.edit_message_caption(chat_id=call.message.chat.id, message_id=call.message.message_id,
                                     caption=f"График за {selected_date.strftime('%Y-%m-%d')}\nВыберите дату:",
                                     reply_markup=markup)

def wait_until_next_interval(minutes):
    now = datetime.now()
    minute = (now.minute // minutes + 1) * minutes
    hour = now.hour
    if minute >= 60:
        minute = 0
        hour = (hour + 1) % 24
    target = datetime(now.year, now.month, now.day, hour, minute, 0)
    delta = (target - now).total_seconds()
    if delta < 0:
        target += timedelta(days=1)
        delta = (target - now).total_seconds()
    sleep(delta)

def db_update_loop():
    while not stop_threads:
        if not WAITING_FOR_RENEW:
            fetch_balance()
        wait_until_next_interval(db_update_interval)

def balance_send_loop():
    while not stop_threads:
        if not WAITING_FOR_RENEW:
            balance_info = fetch_balance(add_to_db=False)
            if isinstance(balance_info, str) and chat_id:
                try:
                    bot.send_message(chat_id, balance_info)
                except:
                    pass
        wait_until_next_interval(balance_send_interval)

threads_started = False

def start_threads():
    global db_update_thread, balance_send_thread, stop_threads, threads_started
    if threads_started:
        return
    stop_threads = False
    db_update_thread = threading.Thread(target=db_update_loop, daemon=True)
    balance_send_thread = threading.Thread(target=balance_send_loop, daemon=True)
    db_update_thread.start()
    balance_send_thread.start()
    threads_started = True

def stop_all_threads():
    global stop_threads, threads_started
    stop_threads = True
    threads_started = False

def is_admin(user_id):
    return user_id in admins

@bot.message_handler(commands=['admin'])
def admin_panel(message):
    if message.chat.type != 'private':
        return
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "У вас нет прав доступа.")
        return

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("Изменить TOKEN", callback_data="change_token"))
    markup.add(types.InlineKeyboardButton("Изменить cookies", callback_data="change_cookies"))
    markup.add(types.InlineKeyboardButton("Скачать базу данных", callback_data="download_db"))
    markup.add(types.InlineKeyboardButton("Изменить интервал обновления БД", callback_data="change_db_interval"))
    markup.add(types.InlineKeyboardButton("Изменить интервал отправки баланса", callback_data="change_balance_interval"))
    markup.add(types.InlineKeyboardButton("Добавить админа", callback_data="add_admin"))
    markup.add(types.InlineKeyboardButton("Удалить админа", callback_data="remove_admin"))
    markup.add(types.InlineKeyboardButton("Показать текущие настройки", callback_data="show_config"))
    markup.add(types.InlineKeyboardButton("Перезапустить бота", callback_data="reload_bot"))
    markup.add(types.InlineKeyboardButton("Снять режим ожидания", callback_data="resume_bot" if WAITING_FOR_RENEW else "no_wait_mode"))

    bot.send_message(message.chat.id, "Панель админа:", reply_markup=markup)

pending_actions = {}

@bot.callback_query_handler(func=lambda call: call.data in ["change_token","change_cookies",
                                                            "change_db_interval","change_balance_interval",
                                                            "add_admin","remove_admin","download_db",
                                                            "show_config","reload_bot","resume_bot","no_wait_mode"])
def callback_admin(call):
    bot.answer_callback_query(call.id)
    user_id = call.from_user.id
    chat_type = call.message.chat.type if call.message else None
    if chat_type != 'private':
        return
    if not is_admin(user_id):
        return

    if call.data in ["change_token", "change_cookies",
                     "change_db_interval", "change_balance_interval", "add_admin", "remove_admin"]:
        pending_actions[user_id] = (call.data,)
        field_name = {
            "change_token": "TOKEN",
            "change_cookies": "cookies",
            "change_db_interval": "интервал обновления БД (мин)",
            "change_balance_interval": "интервал отправки баланса (мин)",
            "add_admin": "ID нового админа",
            "remove_admin": "ID админа для удаления"
        }[call.data]
        bot.send_message(user_id, f"Отправьте новое значение для: {field_name}")

    elif call.data == "download_db":
        if os.path.exists(EXCEL_FILE):
            with open(EXCEL_FILE, 'rb') as f:
                bot.send_document(user_id, f)
        else:
            bot.send_message(user_id, "Файл базы данных не найден.")
    elif call.data == "show_config":
        conf_text = (
            f"Текущие настройки:\n\n"
            f"TOKEN: {config.get('TOKEN', '')}\n"
            f"cookies: {config.get('cookies', '')}\n"
            f"admins: {config.get('admins', [])}\n"
            f"db_update_interval: {config.get('db_update_interval', 30)} минут\n"
            f"balance_send_interval: {config.get('balance_send_interval', 30)} минут\n"
            f"chat_id: {config.get('chat_id', '')}"
        )
        bot.send_message(user_id, conf_text)
    elif call.data == "reload_bot":
        reload_config()
        bot.send_message(user_id, "Конфиг перезагружен, бот работает с новыми параметрами.")
    elif call.data == "resume_bot":
        global WAITING_FOR_RENEW
        WAITING_FOR_RENEW = False
        bot.send_message(user_id, "Режим ожидания снят, бот продолжит работу.")
    elif call.data == "no_wait_mode":
        bot.send_message(user_id, "Бот не в режиме ожидания.")

def reload_config():
    global config, TOKEN, cookies, admins, db_update_interval, balance_send_interval, chat_id
    global stop_threads

    config = load_config()
    TOKEN = config.get('TOKEN', '')
    cookies = config.get('cookies', '')
    admins = config.get('admins', [])
    db_update_interval = config.get('db_update_interval', 30)
    balance_send_interval = config.get('balance_send_interval', 30)
    chat_id = config.get('chat_id', '')

    stop_all_threads()
    sleep(1)
    start_threads()

@bot.message_handler(func=lambda message: message.from_user.id in pending_actions)
def admin_input_handler(message):
    user_id = message.from_user.id
    action = pending_actions[user_id][0]

    try:
        if action == "change_token":
            config['TOKEN'] = message.text.strip()
            bot.send_message(user_id, "TOKEN обновлён.")
        elif action == "change_cookies":
            config['cookies'] = message.text.strip()
            bot.send_message(user_id, "cookies обновлены.")
        elif action == "change_db_interval":
            interval = int(message.text.strip())
            config['db_update_interval'] = interval
            bot.send_message(user_id, f"Интервал обновления БД теперь {interval} минут.")
        elif action == "change_balance_interval":
            interval = int(message.text.strip())
            config['balance_send_interval'] = interval
            bot.send_message(user_id, f"Интервал отправки баланса теперь {interval} минут.")
        elif action == "add_admin":
            new_admin = int(message.text.strip())
            if new_admin not in config['admins']:
                config['admins'].append(new_admin)
                bot.send_message(user_id, f"Админ {new_admin} добавлен.")
            else:
                bot.send_message(user_id, f"{new_admin} уже админ.")
        elif action == "remove_admin":
            remove_id = int(message.text.strip())
            if remove_id in config['admins']:
                config['admins'].remove(remove_id)
                bot.send_message(user_id, f"Админ {remove_id} удалён.")
            else:
                bot.send_message(user_id, f"{remove_id} не найден в списке админов.")

        save_config(config)

    except ValueError:
        bot.send_message(user_id, "Некорректный ввод.")

    del pending_actions[user_id]

if __name__ == '__main__':
    start_threads()
    bot.polling(non_stop=True)
