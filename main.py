import os
import pytz
import asyncio
import logging
import calendar
import aiohttp

from datetime import datetime
from pathlib import Path

from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup,
    FSInputFile
)
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from apscheduler.schedulers.asyncio import AsyncIOScheduler

import database as db
from analytic_report import (
    create_user_spreadsheet,
    schedule_sheet_deletion,
    fill_pnl_report
)
from wb_api import get_supplier_name
from token_daily_refresh import refresh_token


import redis.asyncio as redis
from aiogram.fsm.storage.redis import RedisStorage

from dotenv import load_dotenv

# Загружаем .env файл
load_dotenv()

# --- Настройки ---
#BOT_TOKEN = "7569142757:AAH_qYkixvV-4mSrcQfnagpIlJJc2YMLCW0"
#PHOTO_PATH = Path("start_image.jpeg")

# --- Логирование ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- FSM States ---

class ShopStates(StatesGroup):
    add_shop = State()
    get_api_key = State()
    main_menu = State()
    settings_menu = State()
    change_api_key = State()
    set_tax_rate = State()
    select_start_date = State()
    select_end_date = State()

BOT_TOKEN = os.getenv("BOT_TOKEN")
PHOTO_PATH = Path("start_image.jpeg")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# --- Инициализация ---
bot = Bot(token=BOT_TOKEN)
redis_client = redis.from_url(REDIS_URL)
storage = RedisStorage(redis=redis_client)
dp = Dispatcher(storage=storage)
scheduler = AsyncIOScheduler()

MOSCOW_TZ = pytz.timezone('Europe/Moscow')

# --- Вспомогательные функции ---
def generate_calendar(year: int, month: int):
    keyboard = []

    month_name = calendar.month_name[month]
    header = [InlineKeyboardButton(
        text=f"{month_name} {year}", callback_data="ignore")]
    keyboard.append(header)

    days_of_week = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    week_days = [InlineKeyboardButton(
        text=day, callback_data="ignore") for day in days_of_week]
    keyboard.append(week_days)

    month_calendar = calendar.monthcalendar(year, month)
    for week in month_calendar:
        week_buttons = []
        for day in week:
            if day == 0:
                week_buttons.append(InlineKeyboardButton(
                    text=" ", callback_data="ignore"))
            else:
                week_buttons.append(InlineKeyboardButton(
                    text=str(day),
                    callback_data=f"day_{year}_{month}_{day}"
                ))
        keyboard.append(week_buttons)

    prev_year, prev_month = (year, month - 1) if month > 1 else (year - 1, 12)
    next_year, next_month = (year, month + 1) if month < 12 else (year + 1, 1)

    navigation = [
        InlineKeyboardButton(
            text="⬅️", callback_data=f"nav_{prev_year}_{prev_month}"),
        InlineKeyboardButton(text="❌ Отмена", callback_data="cancel"),
        InlineKeyboardButton(
            text="➡️", callback_data=f"nav_{next_year}_{next_month}")
    ]
    keyboard.append(navigation)

    return keyboard


async def validate_wb_api_key(api_key: str) -> bool:
    url_stat = "https://seller-analytics-api.wildberries.ru/api/v1/supplier/stocks"
    url_ads = "https://advert-api.wildberries.ru/adv/v0/adverts"
    headers = {"Authorization": api_key}

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url_stat, headers=headers, timeout=30) as resp:
                if resp.status == 401:
                    return False
                if resp.status != 200:
                    async with session.get("https://seller-analytics-api.wildberries.ru/ping", headers=headers, timeout=10) as ping:
                        if ping.status != 200:
                            return False
        except Exception as e:
            logger.error(f"Stat API error: {e}")
            return False

        try:
            async with session.get(url_ads, headers=headers, timeout=10) as resp:
                if resp.status == 401:
                    return False
                if resp.status != 200:
                    async with session.get("https://advert-api.wildberries.ru/ping", headers=headers, timeout=10) as ping:
                        if ping.status != 200:
                            return False
        except Exception as e:
            logger.error(f"Ads API error: {e}")
            return False

    return True


async def send_main_menu(message_or_query, text: str = ""):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Фин. отчёт",
                              callback_data="fin_report")],
        [InlineKeyboardButton(text="⚙️ Настройки магазина",
                              callback_data="settings")],
        [InlineKeyboardButton(text="⭐️ Подписка",
                              callback_data="subscription")],
    ])

    caption = f"{text}\n\nВыберите действие:"

    if isinstance(message_or_query, Message):
        if PHOTO_PATH.exists():
            await message_or_query.answer_photo(
                photo=FSInputFile(PHOTO_PATH),
                caption=caption,
                reply_markup=keyboard
            )
        else:
            await message_or_query.answer(text=caption, reply_markup=keyboard)
    else:  # CallbackQuery
        try:
            await message_or_query.message.delete()
        except:
            pass
        if PHOTO_PATH.exists():
            await bot.send_photo(
                chat_id=message_or_query.from_user.id,
                photo=FSInputFile(PHOTO_PATH),
                caption=caption,
                reply_markup=keyboard
            )
        else:
            await bot.send_message(
                chat_id=message_or_query.from_user.id,
                text=caption,
                reply_markup=keyboard
            )


async def send_settings_menu(user_id: int):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔑 Изменить API-ключ",
                              callback_data="change_api")],
        [InlineKeyboardButton(text="🧾 Налоговая система",
                              callback_data="set_tax")],
        [InlineKeyboardButton(
            text="📦 Себестоимость артикулов", callback_data="cost_price")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")],
    ])
    await bot.send_message(chat_id=user_id, text="⚙️ Настройки магазина", reply_markup=keyboard)


# --- Обработчики ---

@dp.message(Command("start"))
async def start(message: Message, state: FSMContext):
    user = message.from_user
    db.add_user(user.id)
    api_key, _, _, shop_name = db.get_user_data(user.id)

    if not api_key:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Добавить магазин",
                                  callback_data="add_shop")]
        ])
        caption = f"👋 Привет, {user.first_name}!\n\nЭто бот для аналитики продаж на Wildberries. Для начала работы добавьте ваш магазин."

        if PHOTO_PATH.exists():
            await message.answer_photo(photo=FSInputFile(PHOTO_PATH), caption=caption, reply_markup=keyboard)
        else:
            await message.answer(text=caption, reply_markup=keyboard)
        await state.set_state(ShopStates.add_shop)
    else:
        # Формируем сообщение с названием магазина
        shop_display = shop_name or "Магазин"
        await send_main_menu(message, f"Ваш кабинет:\nМагазин: {shop_display}")
        await state.set_state(ShopStates.main_menu)


@dp.callback_query(F.data == "add_shop", StateFilter(ShopStates.add_shop))
async def prompt_add_shop(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    try:
        await callback.message.delete()
    except:
        pass
    await callback.message.answer("Пожалуйста, отправьте ваш API-ключ Wildberries")
    await state.set_state(ShopStates.get_api_key)


@dp.message(StateFilter(ShopStates.get_api_key))
async def save_api_key(message: Message, state: FSMContext):
    api_key = message.text.strip()
    user_id = message.from_user.id

    await message.answer("🔍 Проверяю API-ключ...")
    is_valid = await validate_wb_api_key(api_key)

    if not is_valid:
        await message.answer(
            "❌ Неверный API-ключ. Пожалуйста, проверьте ключ и попробуйте снова.\n\n"
            "Убедитесь, что используете ключ от статистики (x64) и он имеет доступ к:\n"
            "• Seller Analytics API\n• Advert API"
        )
        return

    # Получаем название магазина
    shop_name = await get_supplier_name(api_key)

    logger.info(f"User {user_id} added shop: {shop_name}")
    # Сохраняем всё в БД
    db.update_api_key(user_id, api_key)
    db.update_shop_name(user_id, shop_name)
    try:
        await message.delete()
    except:
        pass
    _, _, sheet_link, _ = db.get_user_data(user_id)
    if not sheet_link:
        await message.answer("📊 Создаю вашу постоянную таблицу...")
        sheet_link = await create_user_spreadsheet(user_id, shop_name)
        if sheet_link:
            db.update_google_sheet_link(user_id, sheet_link)
            await message.answer(f"✅ Ваша постоянная таблица создана: {sheet_link}")
        else:
            await message.answer("⚠️ Не удалось создать таблицу.")

    await message.answer("✅ API-ключ успешно сохранен и проверен!")
    await send_main_menu(message)
    await state.set_state(ShopStates.main_menu)


@dp.callback_query(F.data == "fin_report", StateFilter(ShopStates.main_menu))
async def prompt_fin_report(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    try:
        await callback.message.delete()
    except:
        pass
    now = datetime.now()
    keyboard = generate_calendar(now.year, now.month)
    await callback.message.answer(
        "📅 Выберите начальную дату отчета:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard)
    )
    await state.set_state(ShopStates.select_start_date)


@dp.callback_query(StateFilter(ShopStates.select_start_date, ShopStates.select_end_date))
async def handle_calendar_selection(callback: CallbackQuery, state: FSMContext):
    data = callback.data
    await callback.answer()

    if data.startswith("nav_"):
        # Навигация по месяцам — просто обновляем календарь
        _, year, month = data.split("_")
        keyboard = generate_calendar(int(year), int(month))
        await callback.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))
        return

    elif data == "cancel":
        # Отмена выбора даты — возврат в главное меню
        try:
            await callback.message.delete()
        except:
            pass
        await send_main_menu(callback)
        await state.set_state(ShopStates.main_menu)
        return

    elif data.startswith("day_"):
        _, year, month, day = data.split("_")
        selected_date = datetime(int(year), int(month), int(day))
        date_str = selected_date.isoformat()  # ← сериализуем в строку

        current_state = await state.get_state()
        user_data = await state.get_data()

        if current_state == ShopStates.select_start_date.state:
            # Сохраняем начальную дату как строку
            await state.update_data(start_date=date_str)
            # Показываем календарь для конечной даты
            keyboard = generate_calendar(
                selected_date.year, selected_date.month)
            await callback.message.edit_text(
                "📅 Выберите конечную дату отчета:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard)
            )
            await state.set_state(ShopStates.select_end_date)

        elif current_state == ShopStates.select_end_date.state:
            # Сохраняем конечную дату как строку
            await state.update_data(end_date=date_str)
            await generate_and_send_report(callback, state)
            await state.set_state(ShopStates.main_menu)

    else:
        # Игнорируем всё остальное (например, "ignore")
        await callback.answer()


async def generate_and_send_report(callback: CallbackQuery, state: FSMContext):
    try:
        await callback.message.delete()
    except:
        pass

    user_id = callback.from_user.id
    user_data = await state.get_data()
    start_date_str = user_data.get("start_date")
    end_date_str = user_data.get("end_date")

    # Очищаем данные в FSM
    await state.update_data(start_date=None, end_date=None)

    # Проверка наличия дат
    if not start_date_str or not end_date_str:
        await bot.send_message(user_id, "❌ Ошибка: не удалось определить даты отчёта.")
        await send_main_menu(callback)
        return

    try:
        start_date = datetime.fromisoformat(start_date_str)
        end_date = datetime.fromisoformat(end_date_str)
    except Exception as e:
        logger.error(f"Ошибка парсинга дат: {e}")
        await bot.send_message(user_id, "❌ Ошибка: некорректный формат дат.")
        await send_main_menu(callback)
        return

    msg = await bot.send_message(
        user_id,
        f"📊 Генерирую отчет с {start_date.strftime('%d.%m.%Y')} по {end_date.strftime('%d.%m.%Y')}..."
    )

    ### ИЗМЕНЕНИЕ ###
    # Старый код:
    # report_result, report_url = await generate_financial_report(user_id, start_date, end_date)

    # Новый код:
    # Получаем sheet_id из базы. В исходной логике он не использовался,
    # но мы должны его передать в функцию.
    _, _, sheet_id, _ = db.get_user_data(user_id)

    # Вызываем нашу обновленную функцию
    report_url = await fill_pnl_report(sheet_id, user_id, start_date, end_date)

    # Запланировать удаление. ID таблицы берем из URL
    if report_url:
        spreadsheet_id_to_delete = report_url.split('/d/')[1].split('/')[0]
        asyncio.create_task(schedule_sheet_deletion(spreadsheet_id_to_delete))

    # Формируем ответ пользователю
    if report_url:
        report_result = "📊 Отчет успешно создан!\nОтчет будет доступен 12 часов."
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Открыть отчёт", url=report_url)]
        ])
        await msg.edit_text(report_result, reply_markup=keyboard)
    else:
        report_result = "❌ Ошибка: не удалось сгенерировать отчет. Пожалуйста, попробуйте позже."
        await msg.edit_text(report_result)

    await send_main_menu(callback)


async def generate_financial_report(user_id, start_date, end_date):
    api_key, _, sheet_id, _ = db.get_user_data(user_id)
    if not api_key:
        return "❌ Ошибка: API ключ не найден. Добавьте магазин в настройках.", None

    report_url = await fill_pnl_report(sheet_id, user_id, start_date, end_date)

    asyncio.create_task(schedule_sheet_deletion(sheet_id))
    return "📊 Отчет успешно создан!\nОтчет будет доступен 12 часов.",  report_url


@dp.callback_query(F.data == "settings", StateFilter(ShopStates.main_menu))
async def show_settings_menu_handler(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await send_settings_menu(callback.from_user.id)
    await state.set_state(ShopStates.settings_menu)


@dp.callback_query(F.data == "change_api", StateFilter(ShopStates.settings_menu))
async def prompt_change_api(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    try:
        await callback.message.delete()
    except:
        pass
    back_kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(
        text="⬅️ Назад", callback_data="back_to_settings")]])
    await callback.message.answer("Отправьте новый API-ключ Wildberries (статистика x64).", reply_markup=back_kb)
    await state.set_state(ShopStates.change_api_key)


@dp.message(StateFilter(ShopStates.change_api_key))
async def save_new_api_key(message: Message, state: FSMContext):
    if message.text.startswith("⬅️"):
        await send_settings_menu(message.from_user.id)
        await state.set_state(ShopStates.settings_menu)
        return

    api_key = message.text.strip()
    user_id = message.from_user.id

    await message.answer("🔍 Проверяю новый API-ключ...")
    is_valid = await validate_wb_api_key(api_key)

    if not is_valid:
        back_kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(
            text="⬅️ Назад", callback_data="back_to_settings")]])
        await message.answer("❌ Неверный API-ключ.", reply_markup=back_kb)
        return

    shop_name = await get_supplier_name(api_key)
    db.update_api_key(user_id, api_key)
    db.update_shop_name(user_id, shop_name)
    try:
        await message.delete()
    except:
        pass
    await message.answer("✅ API-ключ успешно обновлен!")
    await send_settings_menu(user_id)
    await state.set_state(ShopStates.settings_menu)


@dp.callback_query(F.data == "set_tax", StateFilter(ShopStates.settings_menu))
async def prompt_set_tax_rate(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    back_kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(
        text="⬅️ Назад", callback_data="back_to_settings")]])
    await callback.message.edit_text("Введите вашу налоговую ставку в процентах (например, 6 для УСН 6%).", reply_markup=back_kb)
    await state.set_state(ShopStates.set_tax_rate)


@dp.message(StateFilter(ShopStates.set_tax_rate))
async def save_tax_rate(message: Message, state: FSMContext):
    if message.text.startswith("⬅️"):
        await send_settings_menu(message.from_user.id)
        await state.set_state(ShopStates.settings_menu)
        return

    try:
        rate = float(message.text.replace(",", "."))
        if 0 <= rate <= 100:
            db.update_tax_rate(message.from_user.id, rate)
            await message.answer(f"✅ Налоговая ставка установлена: {rate} %")
            await send_settings_menu(message.from_user.id)
            await state.set_state(ShopStates.settings_menu)
        else:
            raise ValueError
    except ValueError:
        back_kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(
            text="⬅️ Назад", callback_data="back_to_settings")]])
        await message.answer("❌ Введите число от 0 до 100.", reply_markup=back_kb)


@dp.callback_query(F.data == "cost_price", StateFilter(ShopStates.settings_menu))
async def get_cost_price_sheet(callback: CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    _, _, sheet_link, _ = db.get_user_data(user_id)

    if not sheet_link:
        await callback.message.answer("📊 Создаю вашу постоянную таблицу...")
        sheet_link = await create_user_spreadsheet(user_id, f"Магазин_{user_id}")
        if sheet_link:
            db.update_google_sheet_link(user_id, sheet_link)
        else:
            await callback.message.edit_text("❌ Не удалось создать таблицу.")
            return

    back_kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(
        text="⬅️ Назад", callback_data="back_to_settings")]])
    await callback.message.edit_text(
        f"📊 Ваша постоянная таблица для себестоимости:\n\n{sheet_link}\n\nЗаполните её данными по артикулам.",
        reply_markup=back_kb
    )


@dp.callback_query(F.data == "subscription", StateFilter(ShopStates.main_menu))
async def show_subscription_info(callback: CallbackQuery):
    await callback.answer()
    try:
        await callback.message.delete()
    except:
        pass
    back_kb = InlineKeyboardMarkup(inline_keyboard=[
                                   [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")]])
    await callback.message.answer(
        "⭐️ Ваша подписка активна до 31.12.2025.\n\nВам доступны все функции бота.",
        reply_markup=back_kb
    )


@dp.callback_query(F.data == "main_menu")
async def back_to_main_menu(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await send_main_menu(callback)
    await state.set_state(ShopStates.main_menu)


# @dp.callback_query(F.data == "back_to_settings", StateFilter(ShopStates.change_api_key, ShopStates.set_tax_rate))
@dp.callback_query(F.data == "back_to_settings")
async def back_to_settings_from_input(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await send_settings_menu(callback.from_user.id)
    await state.set_state(ShopStates.settings_menu)


# --- Запуск ---
async def main():
    db.init_db()

    try:
        scheduler.remove_job("daily_refresh_token")
    except:
        pass

    scheduler.add_job(
        refresh_token,
        'cron',
        hour=0,
        minute=1,
        timezone=MOSCOW_TZ,
        id="daily_refresh_token"
    )

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
