from collections import defaultdict
import os
import pickle
import gspread
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.exceptions import RefreshError
import logging
from datetime import datetime, timedelta
import asyncio
import database as db
from wb_api import get_wb_orders, get_wb_weekly_report, get_wb_paid_storage_report
from unit_economics_report import create_unit_economics_sheet, fill_unit_economics_sheet
from wb_advert import get_aggregated_ad_costs




logger = logging.getLogger(__name__)

# Области доступа для Google Sheets API
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']


# ========================================
# АУТЕНТИФИКАЦИЯ И СОЗДАНИЕ ТАБЛИЦ
# ========================================


async def get_gspread_client():
    """
    Получает аутентифицированный клиент gspread для работы с Google Sheets.
    Использует файлы credentials.json и token.pickle.
    """
    creds = None
    # Файл token.pickle хранит токены доступа и обновления пользователя.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)

    # Если учетные данные недействительны, обновляем или запрашиваем новые.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except RefreshError:
                logger.error(
                    "Не удалось обновить токен. Требуется повторная аутентификация.")
                os.remove('token.pickle')  # Удаляем старый токен
                return None  # Возвращаем None, чтобы обработать ошибку
        else:
            try:
                flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
                # Запуск локального сервера для аутентификации
                creds = flow.run_local_server(port=0)
            except FileNotFoundError:
                logger.error("Файл credentials.json не найден. Пожалуйста, скачайте его из Google Cloud Console.")
                return None

        # Сохраняем учетные данные для следующего запуска
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    try:
        return gspread.authorize(creds)
    except Exception as e:
        logger.error(f"Ошибка при авторизации в gspread: {e}")
        return None


async def create_user_spreadsheet(user_id: int, shop_name: str) -> str:
    """
    Создает постоянную таблицу для пользователя при регистрации.
    Возвращает ссылку на таблицу.
    """
    try:
        gc = await get_gspread_client()
        if not gc:
            return None

        spreadsheet_title = f"Магазин: {shop_name} (User ID: {user_id})"
        spreadsheet = gc.create(spreadsheet_title)

        headers = ['Артикул', 'Себестоимость']
        table = spreadsheet.get_worksheet(0)
        table.update("A1", [headers])
        # Настраиваем доступ
        spreadsheet.share(None, perm_type='anyone', role='writer')

        logger.info(
            f"Создана постоянная таблица для пользователя {user_id}: {spreadsheet.url}")
        return spreadsheet.url

    except Exception as e:
        logger.error(f"Ошибка при создании таблицы пользователя: {e}")
        return None


async def create_temporary_report(shop_id: int, shop_api_token: str, start_date: datetime, end_date: datetime, shop_name: str, full_data=None) -> tuple:
    """
    Создает временный отчет на 12 часов.
    Возвращает (ссылка, spreadsheet_id)
    """
    try:
        gc = await get_gspread_client()
        if not gc:
            return None, None

        period_text = f"{start_date.strftime('%d.%m.%Y')}-{end_date.strftime('%d.%m.%Y')}"
        spreadsheet_title = f"Отчет: {shop_name} ({period_text})"

        spreadsheet = gc.create(spreadsheet_title)

        # Настраиваем доступ
        spreadsheet.share(None, perm_type='anyone', role='reader')

        logger.info(f"Создан временный отчет для магазина {shop_id}: {spreadsheet.url}")
        return spreadsheet.url, spreadsheet.id

    except Exception as e:
        logger.error(f"Ошибка при создании временного отчета: {e}")
        return None, None


# ========================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ========================================

def get_current_week_range(today: datetime) -> tuple[datetime, datetime]:
    """Возвращает (monday 00:00, sunday 23:59:59.999999) текущей недели."""
    monday = today - timedelta(days=today.weekday())
    monday = monday.replace(hour=0, minute=0, second=0, microsecond=0)
    sunday = monday + timedelta(days=6, hours=23, minutes=59, seconds=59, microseconds=999999)
    return monday, sunday


# ========================================
# ЗАПОЛНЕНИЕ ЛИСТА "P&L недельный" (по дням)
# ========================================


async def fill_pnl_weekly_sheet(spreadsheet, weekly_data: list, daily_data, start_date: datetime, end_date: datetime):
    """Заполняет лист 'P&L недельный' на основе данных из reportDetailByPeriod.
    
        Дата                   -      rr_dt
        Количество заказов     -      количество строк из daily_data 
        Заказы                 -      totalPrice * (1 - discountPercent/100) из daily_data
        Выкупили               -      quantity (для продаж)
        Продажи до СПП         -      retail_amount * quantity (для продаж)
        Себестоймость продаж   -      
        Потери от брака        -      
        Комиссия               -      sales_before_spp - ppvz_for_pay
        Возвраты               -      retail_amount * quantity (для возвратов)
        Реклама                -      deduction
        Прямая логистика       -      delivery_rub - rebill_logistic_cost
        Обратная логистика     -      rebill_logistic_cost
        Хранение               -      storage_fee
        Приемка                -      acceptance
        Корректировки          -      additional_payment + cashback_discount + cashback_amount + cashback_commission_change
        Штрафы                 -      penalty
        Итого к оплате         -      ppvz_for_pay - Корректировки - penalty - delivery_rub - storage_fee - acceptance - deduction - Возвраты
        Опер затраты           -      
        Ebitda/%               -      
        Налоги                 -      
        Кредит                 -      
        Чистая прибыль/ROI     -      
    """

    try:
        # 1. Создание листа (если его еще нет)
        try:
            ws = spreadsheet.worksheet("P&L недельный")
        except gspread.WorksheetNotFound:
            ws = spreadsheet.add_worksheet(title="P&L недельный", rows=500, cols=30)

        headers = [
            "Дата", "Количество заказов", "Заказы", "Выкупили", "Продажи до СПП",
            "Себестоймость продаж", "Потери от брака", "Комиссия", "Возвраты", "Реклама",
            "Прямая логистика", "Обратная логистика", "Хранение", "Приемка",
            "Корректировки", "Штрафы", "Итого к оплате", "Опер затраты",
            "Ebitda/%", "", "Налоги", "Кредит", "Чистая прибыль/ROI", ""
        ]

        # 2. ЕДИНАЯ И ПРАВИЛЬНАЯ АГРЕГАЦИЯ
        daily_aggr = defaultdict(lambda: defaultdict(float))

        # Сначала агрегируем заказы по дням
        for order in daily_data:
            date_str = order.get("date", "")[:10]
            if not date_str: continue
            daily_aggr[date_str]["orders_count"] += 1
            daily_aggr[date_str]["orders"] += order.get("totalPrice", 0) * (1 - order.get("discountPercent", 0) / 100)

        # Затем агрегируем все финансовые показатели из детализированного отчета
        for row in weekly_data:
            # ---> КЛЮЧЕВОЙ ФИЛЬТР <---
            # Учитываем только строки, где есть и дата, и артикул
            date_str = row.get("rr_dt", "")[:10]
            nm_id = row.get("nm_id")
            if not date_str or not nm_id:
                continue

            # Суммируем все необходимые метрики по дням
            daily_aggr[date_str]["commission_rub"] += row.get("ppvz_vw", 0) + row.get("ppvz_vw_nds", 0)
            daily_aggr[date_str]["advertising"] += row.get("deduction", 0)
            daily_aggr[date_str]["forward_logistics"] += row.get("delivery_rub", 0) - row.get("rebill_logistic_cost", 0)
            daily_aggr[date_str]["reverse_logistics"] += row.get("rebill_logistic_cost", 0)
            daily_aggr[date_str]["storage"] += row.get("storage_fee", 0)
            daily_aggr[date_str]["acceptance"] += row.get("acceptance", 0)
            daily_aggr[date_str]["penalties"] += row.get("penalty", 0)
            daily_aggr[date_str]["adjustments"] += row.get("additional_payment", 0)
            daily_aggr[date_str]["to_pay"] += row.get("ppvz_for_pay", 0)  # Для расчета "Итого к оплате"

            doc_type = (row.get("doc_type_name") or "").lower()
            quantity = row.get("quantity", 0)
            if "продажа" in doc_type:
                daily_aggr[date_str]["sales_quantity"] += quantity
                daily_aggr[date_str]["sales_before_spp"] += row.get("retail_amount", 0)
            elif "возврат" in doc_type:
                daily_aggr[date_str]["returns"] += row.get("retail_amount", 0)

        # 3. Формирование строк для вывода в таблицу
        rows = [headers]
        total_row = defaultdict(float)  # Используем defaultdict для удобства суммирования
        total_row['label'] = "Факт"

        current = start_date
        while current <= end_date:
            date_str = current.strftime("%Y-%m-%d")
            day_data = daily_aggr[date_str]  # Получаем данные за день

            # Рассчитываем "Итого к оплате" для этого дня
            total_to_pay_day = (day_data["to_pay"] - day_data["adjustments"] - day_data["penalties"] -
                                (day_data["forward_logistics"] + day_data["reverse_logistics"]) -
                                day_data["storage"] - day_data["acceptance"] - day_data["advertising"] -
                                day_data["returns"])

            row = [
                current.strftime("%d.%m.%Y"),
                int(day_data["orders_count"]),
                day_data["orders"],
                int(day_data["sales_quantity"]),
                day_data["sales_before_spp"],
                0, 0,  # Себестоимость, Потери
                day_data["commission_rub"],  # Выводим как положительный расход
                day_data["returns"],
                day_data["advertising"],
                day_data["forward_logistics"],
                day_data["reverse_logistics"],
                day_data["storage"],
                day_data["acceptance"],
                day_data["adjustments"],
                day_data["penalties"],
                total_to_pay_day,
                0, 0, 0, 0, 0, 0, 0  # Пустые колонки
            ]
            rows.append(row)

            # Суммируем в итоговую строку
            for i, header in enumerate(headers):
                if i > 0 and isinstance(row[i], (int, float)):
                    total_row[header] += row[i]

            current += timedelta(days=1)

        # Преобразуем total_row в список для вывода
        total_row_list = [total_row.get('label', "Факт")] + [total_row.get(h, 0) for h in headers[1:]]

        # 4. Вставка итогов и обновление таблицы
        rows.insert(1, total_row_list)
        # Рассчитываем процентные соотношения для третьей строки
        percentage_row = ["%"]
        # База для расчета - "Продажи до СПП" из итоговой строки
        sales_before_spp_total = total_row.get("Продажи до СПП", 0)

        for i in range(1, len(headers)):
            header = headers[i]
            total_value = total_row.get(header, 0)

            # Не считаем процент для самих продаж и пустых колонок
            if header == "Продажи до СПП" or header == "" or sales_before_spp_total == 0:
                percentage_row.append("")
            else:
                percentage = total_value / sales_before_spp_total
                percentage_row.append(percentage)

        rows.insert(2, percentage_row)

        ws.update("A1", rows, value_input_option='USER_ENTERED')

        # ========================================
        # ФОРМАТИРОВАНИЕ ЧЕРЕЗ batch_format
        # ========================================

        format_requests = []

        # 10. Закрепление первого столбца и первой строки
        ws.freeze(rows=1, cols=1)

        # 11. Шрифт Verdana, 11 на всю таблицу
        format_requests.append({
            "range": "A1:X",
            "format": {
                "textFormat": {
                    "fontFamily": "Verdana",
                    "fontSize": 11
                }
            }
        })

        # 12. Первая строка: жирный, белый текст, заливка #3a6f95
        format_requests.append({
            "range": "A1:X1",
            "format": {
                "textFormat": {
                    "bold": True,
                    "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}  # Белый
                },
                "backgroundColor": {
                    "red": 58/255, "green": 111/255, "blue": 149/255  # #3a6f95
                },
                "horizontalAlignment": "CENTER"
            }
        })

        format_requests.append({
            "range": "B3:X3",
            "format": {
                "numberFormat": {
                    "type": "PERCENT",
                    "pattern": "0.00%"
                }
            }
        })

        # 14. Форматирование строки 2 (Факт)
        # Применяем формат "Число" к "Количество заказов" (B2) и "Выкупили" (D2)
        format_requests.append({
            "range": "B2",
            "format": {"numberFormat": {"type": "NUMBER", "pattern": "0"}}
        })
        format_requests.append({
            "range": "D2",
            "format": {"numberFormat": {"type": "NUMBER", "pattern": "0"}}
        })

        # Применяем формат "Валюта" к остальным нужным диапазонам
        # C2 (Заказы) и E2:X2 (Продажи до СПП и далее)
        format_requests.append({
            "range": "C2",
            "format": {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00\" ₽\""}}
        })
        format_requests.append({
            "range": "E2:X2",
            "format": {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00\" ₽\""}}
        })
        # 14.1. Формат "Валюта" для данных по дням (строки с 4-й и ниже)
        # Нам нужно отформатировать все числовые столбцы
        # Индекс "Комиссии" = 7. A=0, B=1... H=7
        commission_col_letter = "H"
        format_requests.append({
            # Применяем формат ко всему столбцу, начиная с 4-й строки
            "range": f"{commission_col_letter}4:X",
            "format": {
                "numberFormat": {
                    "type": "CURRENCY",
                    "pattern": "#,##0.00\" ₽\""
                }
            }
        })
        # 15. Строка 3: нижняя граница темно-серый
        format_requests.append({
            "range": "A3:X3",
            "format": {
                "borders": {
                    "bottom": {
                        "style": "SOLID",
                        "width": 1,
                        "color": {"red": 0.4, "green": 0.4, "blue": 0.4}  # Темно-серый
                    }
                }
            }
        })

        # 16. Центрируем текст в объединенных ячейках
        format_requests.append({
            "range": "S1:T3",
            "format": {
                "horizontalAlignment": "CENTER"
            }
        })

        format_requests.append({
            "range": "W1:X3",
            "format": {
                "horizontalAlignment": "CENTER"
            }
        })

        # Применяем все форматирования одним запросом
        if format_requests:
            ws.batch_format(format_requests)

        # Объединение ячеек для заголовков Ebitda/% и Чистая прибыль/ROI
        # Объединяем T1:U1 (Ebitda и %)
        ws.merge_cells("S1:T1")
        # Объединяем V1:W1 (Чистая прибыль и ROI) 
        ws.merge_cells("W1:X1")

        # Объединяем соответствующие ячейки в строках 2 и 3
        ws.merge_cells("S2:T2")
        ws.merge_cells("W2:x2")
        ws.merge_cells("S3:T3") 
        ws.merge_cells("W3:X3")

    except Exception as e:
        logger.error(f"Ошибка при заполнении 'P&L недельный': {e}")
        raise


# ========================================
# ЗАПОЛНЕНИЕ ЛИСТА "Товарная аналитика (недельная)"
# ========================================

async def fill_product_analytics_weekly_sheet(spreadsheet, weekly_data: list, daily_data):
    """
    Заполняет лист 'Товарная аналитика (недельная)' по артикулам.
        Артикул (nmId)         -      nm_id
        Заказы, руб            -      totalPrice * (1 - discountPercent/100) из daily_data
        Выкупы, руб            -      retail_amount * quantity (для продаж)
        Возвраты по браку, руб -      retail_amount * quantity (для возвратов)
        Заказы, шт             -      количество заказов из daily_data
        Выкупы, шт             -      quantity (для продаж)
        Возвраты по браку, шт  -      
        % выкупа               -      
        Комиссия               -      sales_before_spp - ppvz_for_pay
        Логистика прямая       -      delivery_rub - rebill_logistic_cost
        Логистика обратная     -      rebill_logistic_cost
        Хранение               -      storage_fee
        Приемка                -      acceptance
        Реклама                -      deduction
        Штрафы                 -      penalty
        Корректировки          -      additional_payment + cashback_discount + cashback_amount + cashback_commission_change
    """

    try:
        try:
            ws = spreadsheet.worksheet("Товарная аналитика (недельная)")
        except:
            ws = spreadsheet.add_worksheet(
                title="Товарная аналитика (недельная)", rows=1000, cols=20)

        headers = [
            "Артикул (nmId)",
            "Заказы, руб",
            "Выкупы, руб",
            "Возвраты по браку, руб",
            "Заказы, шт",
            "Выкупы, шт",
            "Возвраты по браку, шт",
            "% выкупа",
            "Комиссия",
            "Логистика прямая",
            "Логистика обратная",
            "Хранение",
            "Приемка",
            "Реклама",
            "Штрафы",
            "Корректировки"
        ]

        products = defaultdict(lambda: {
            "orders_count": 0,
            "orders": 0,
            "sales_quantity": 0,
            "sales_before_spp": 0,
            "cost": 0,
            "commission": 0,
            "returns": 0,
            "advertising": 0,
            "forward_logistics": 0,
            "reverse_logistics": 0,
            "storage": 0,
            "acceptance": 0,
            "adjustments": 0,
            "penalties": 0,
            "oper_expenses": 0,
            "to_pay": 0,
            "total_to_pay": 0
        })

        for row in daily_data:
            nm_id = row.get("nmId")
            if not nm_id:
                continue

            products[nm_id]["orders_count"] += 1
            products[nm_id]["orders"] += row.get("totalPrice", 0) * (1 - row.get("discountPercent", 0) / 100)
            

        for row in weekly_data:
            nm_id = row.get("nm_id")
            if not nm_id:
                #logger.warning(f"Пропущена строка без nm_id: {row}")
                continue

            doc_type = (row.get("doc_type_name") or "").lower()
            price_with_disc = row.get("retail_price_withdisc_rub", 0)
            quantity = row.get("quantity", 0)

            is_sale = "продажа" in doc_type
            is_return = "возврат" in doc_type

            if is_sale:
                products[nm_id]["sales_quantity"] += quantity
                products[nm_id]["sales_before_spp"] += row.get("retail_amount", 0) * quantity
            returns = 0
            if is_return:
                returns = row.get("retail_amount", 0) * quantity
                products[nm_id]["returns"] += returns

            products[nm_id]["commission"] += row.get("ppvz_vw", 0) + row.get("ppvz_vw_nds", 0)
            
            products[nm_id]["advertising"] += row.get("deduction", 0)
            products[nm_id]["forward_logistics"] += row.get("delivery_rub", 0) - row.get("rebill_logistic_cost", 0)
            products[nm_id]["reverse_logistics"] += row.get("rebill_logistic_cost", 0)
            products[nm_id]["storage"] += row.get("storage_fee", 0)
            products[nm_id]["acceptance"] += row.get("acceptance", 0)

            # products[nm_id]["acquiring"] += row.get("acquiring_fee", 0) 
            # products[nm_id]["acquiring_2"] += row.get("acquiring_fee", 0) * (1 - row.get("acquiring_percent", 0) / 100)
            
            products[nm_id]["penalties"] += row.get("penalty", 0)
            products[nm_id]["to_pay"] += row.get("ppvz_for_pay", 0)

            additional_payment = row.get("additional_payment", 0)
            installment_cofinancing = row.get("installment_cofinancing_amount", 0)
            cashback_discount = row.get("cashback_discount", 0)
            cashback_amount = row.get("cashback_amount", 0)
            cashback_commission_change = row.get("cashback_commission_change", 0)
            adjustments = additional_payment + cashback_discount + cashback_amount + cashback_commission_change
            products[nm_id]["adjustments"] += adjustments
            
            products[nm_id]["total_to_pay"] += row.get("ppvz_for_pay", 0) - adjustments - row.get("penalty", 0) - row.get("delivery_rub", 0) \
                                                 - row.get("storage_fee", 0) - row.get("acceptance", 0) - row.get("deduction", 0) - returns 

        data = [headers]
        for nm_id in sorted(products.keys()):
            p = products[nm_id]
            row = [
                nm_id,
                p["orders"],
                p["sales_before_spp"],
                p["returns"],
                p["orders_count"],
                p["sales_quantity"],
                0,
                0, # % выкупа
                p["commission"],
                p["forward_logistics"],
                p["reverse_logistics"],
                p["storage"],
                p["acceptance"],
                p["advertising"],
                p["penalties"],
                p["adjustments"],
            ]
            data.append(row)

        ws.update("A1", data)

        format_requests = []

        # 1. Закрепление первого столбца и первой строки
        ws.freeze(rows=1, cols=1)

        # 2. Шрифт Verdana, 11 на всю таблицу
        format_requests.append({
            "range": "A1:X",
            "format": {
                "textFormat": {
                    "fontFamily": "Verdana",
                    "fontSize": 11
                }
            }
        })

        # 3. Первая строка: жирный, белый текст, заливка #3a6f95
        format_requests.append({
            "range": "A1:P1",
            "format": {
                "textFormat": {
                    "bold": True,
                    "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}  # Белый
                },
                "backgroundColor": {
                    "red": 58/255, "green": 111/255, "blue": 149/255  # #3a6f95
                },
                "horizontalAlignment": "CENTER"
            }
        })

        if format_requests:
            ws.batch_format(format_requests)

    except Exception as e:
        logger.error(
            f"Ошибка при заполнении 'Товарная аналитика (недельная)': {e}")
        raise


# ========================================
# ЕЖЕДНЕВНЫЕ ФУНКЦИИ (В итоге не используются)
# ========================================

async def fill_pnl_daily_sheet(spreadsheet, daily_data, acceptance_by_day, storage_by_day, start_date, end_date):
    """Заполняет лист 'P&L ежедневный'."""
    try:
        try:
            ws = spreadsheet.worksheet("P&L ежедневный")
        except:
            ws = spreadsheet.add_worksheet(
                title="P&L ежедневный", rows=100, cols=10)

        headers = [
            "Дата",
            "Сумма заказов",
            "Кол-во заказов",
            "Сумма продаж",
            "Кол-во продаж",
            "Платная приёмка",
            "Хранение"
        ]
        rows = [headers]

        # orders_amt, orders_cnt, sales_amt, sales_cnt, acceptance, storage
        totals = [0, 0, 0, 0, 0.0, 0.0]

        current = start_date
        while current <= end_date:
            date_str = current.strftime("%Y-%m-%d")
            day = daily_data.get(date_str, {})
            acc = acceptance_by_day.get(date_str, 0.0)
            stor = storage_by_day.get(date_str, 0.0)

            row = [
                current.strftime("%d.%m.%Y"),
                day.get("orders_amount", 0),
                day.get("orders_count", 0),
                day.get("sales_amount", 0),
                day.get("sales_count", 0),
                acc,
                stor
            ]
            rows.append(row)

            totals[0] += row[1]
            totals[1] += row[2]
            totals[2] += row[3]
            totals[3] += row[4]
            totals[4] += row[5]
            totals[5] += row[6]

            current += timedelta(days=1)

        total_row = ["ИТОГО за период"] + totals
        rows.insert(1, total_row)
        ws.update("A1", rows)
        ws.format("A1:G1", {"textFormat": {"bold": True}})
        ws.format("A2:G2", {"textFormat": {"bold": True}})
    except Exception as e:
        logger.error(f"Ошибка при заполнении 'P&L ежедневный': {e}")
        raise


async def fill_product_analytics_daily_sheet(spreadsheet, products, acceptance_by_nm, storage_by_nm):
    """Заполняет 'Товарная аналитика (ежедневная)'."""
    try:
        try:
            ws = spreadsheet.worksheet("Товарная аналитика (ежедневная)")
        except:
            ws = spreadsheet.add_worksheet(
                title="Товарная аналитика (ежедневная)", rows=1000, cols=10)

        headers = [
            "Артикул (nmId)",
            "Сумма заказов",
            "Количество заказов",
            "Сумма продаж",
            "Количество продаж",
            "Платная приёмка",
            "Хранение"
        ]
        data = [headers]

        all_nm = set(products.keys()) | set(
            acceptance_by_nm.keys()) | set(storage_by_nm.keys())
        for nm in sorted(all_nm):
            p = products.get(
                nm, {"orders_amount": 0, "orders_count": 0, "sales_amount": 0, "sales_count": 0})
            row = [
                nm,
                p["orders_amount"],
                p["orders_count"],
                p["sales_amount"],
                p["sales_count"],
                acceptance_by_nm.get(nm, 0.0),
                storage_by_nm.get(nm, 0.0)
            ]
            data.append(row)

        ws.update("A1", data)
        ws.format("A1:G1", {"textFormat": {"bold": True}})
    except Exception as e:
        logger.error(
            f"Ошибка при заполнении 'Товарная аналитика (ежедневная)': {e}")
        raise


# ========================================
# ОСНОВНАЯ ФУНКЦИЯ
# ========================================

# --- analytic_report.py ---

async def fill_pnl_report(
        spreadsheet_id: str,
        shop_id: int,
        start_date: datetime,
        end_date: datetime,
        full_data=None
) -> str:
    """
    Создает ОДНУ Google Таблицу и заполняет ее всеми отчетами из единого набора данных,
    полученного за указанный пользователем период.
    """
    # Инициализируем переменные для доступа в блоке except
    spreadsheet = None
    gc = None
    try:
        # === 1. Получение API ключа и создание Google Таблицы ===
        gc = await get_gspread_client()
        if not gc: return None

        api_key, _, _, shop_name = db.get_user_data(shop_id)
        if not api_key:
            logger.error(f"API ключ не найден для shop_id {shop_id}")
            return None

        shop_display_name = shop_name or f"Магазин {shop_id}"
        spreadsheet_title = f"Фин. отчет: {shop_display_name} ({start_date.strftime('%d.%m')}-{end_date.strftime('%d.%m.%Y')})"
        spreadsheet = gc.create(spreadsheet_title)
        spreadsheet.share(None, perm_type='anyone', role='reader')

        logger.info(f"Создана таблица: {spreadsheet.url}")
        default_sheet = spreadsheet.get_worksheet(0)

        # === 2. ОПТИМИЗИРОВАННЫЙ И ПОСЛЕДОВАТЕЛЬНЫЙ ЗАПРОС ДАННЫХ ===
        logger.info("Запрашиваю данные от WB API (фаза 1: заказы и хранение)...")

        # 1. Формируем и параллельно запускаем "медленные" задачи к РАЗНЫМ доменам
        orders_task = get_wb_orders(api_key, start_date, end_date)
        storage_report_task = get_wb_paid_storage_report(api_key, start_date, end_date)

        orders_data, storage_data = await asyncio.gather(
            orders_task, storage_report_task
        )

        # 2. Формируем и параллельно запускаем вторую фазу запросов
        logger.info("Запрашиваю данные от WB API (фаза 2: детализация и реклама)...")

        # Сначала готовим target_nm_ids, так как он нужен для одной из задач
        target_nm_ids = {order['nmId'] for order in (orders_data or []) if 'nmId' in order}
        logger.info(f"Найдено {len(target_nm_ids)} уникальных nmId для запроса расходов на рекламу.")

        # Создаем задачи
        report_data_task = get_wb_weekly_report(api_key, start_date, end_date, period="daily")
        ad_costs_task = get_aggregated_ad_costs(api_key, start_date, end_date, target_nm_ids)

        # 3. Выполняем их одновременно
        report_data, ad_costs = await asyncio.gather(
            report_data_task, ad_costs_task
        )

        # 4. Агрегируем данные по хранению (теперь это просто обработка уже полученных данных)
        storage_costs = defaultdict(float)
        if storage_data:
            for row in storage_data:
                key = (row.get("date"), row.get("nmId"))
                if all(key):
                    storage_costs[key] += row.get("warehousePrice", 0)

        # 5. Проверяем на критическую ошибку API
        if report_data is None or orders_data is None:
            logger.error("Критическая ошибка API: не удалось получить основные данные (детализация или заказы).")
            raise Exception("API data fetch failed")

        # === 3. Заполнение всех листов из единого набора данных ===
        logger.info("Заполняю листы отчетов...")

        # Все функции вызываются с едиными данными и датами, которые выбрал пользователь
        await fill_pnl_weekly_sheet(spreadsheet, report_data, orders_data, start_date, end_date)
        await fill_product_analytics_weekly_sheet(spreadsheet, report_data, orders_data)
        await create_unit_economics_sheet(spreadsheet)
        await fill_unit_economics_sheet(spreadsheet, report_data, orders_data, ad_costs, storage_costs)

        # Удаляем стандартный лист, созданный по умолчанию
        spreadsheet.del_worksheet(default_sheet)

        return spreadsheet.url

    except Exception as e:
        logger.error(f"Критическая ошибка в fill_pnl_report: {e}", exc_info=True)
        # Если что-то пошло не так, и таблица была создана, пытаемся ее удалить
        if spreadsheet and gc:
            try:
                gc.del_spreadsheet(spreadsheet.id)
            except Exception as del_e:
                logger.error(f"Не удалось удалить частично созданную таблицу: {del_e}")
        return None
############################################################################################################################


async def schedule_sheet_deletion(sheet_id: str, delay_hours: int = 12):
    """
    Планирует удаление таблицы через указанное количество часов
    """
    await asyncio.sleep(delay_hours * 3600)
    try:
        gc = await get_gspread_client()
        if gc:
            spreadsheet = gc.open_by_key(sheet_id)
            gc.del_spreadsheet(spreadsheet.id)
            logger.info(f"Таблица {sheet_id} удалена")
    except Exception as e:
        logger.error(f"Ошибка удаления таблицы: {e}")


###########################################################################################################################
# НОВЫЙ БЛОК ДЛЯ ОТЧЕТА "ЮНИТ ЭКОНОМИКА"
# ###########################################################################################################################

async def generate_daily_unit_economics_report(user_id: int, start_date: datetime, end_date: datetime):
    """
    Полный цикл генерации отчета "Юнит экономика" по дням и артикулам.
    """
    # Импортируем bot здесь, чтобы избежать циклических зависимостей
    from main import bot

    # 1. Проверка периода (не более 31 дня)
    if (end_date - start_date).days > 30:
        logger.warning(f"Пользователь {user_id} запросил слишком большой период. Отклонено.")
        return "❌ Ошибка: Период отчета не должен превышать 31 день.", None

    # 2. Получение данных
    api_key, _, _, shop_name = db.get_user_data(user_id)
    if not api_key:
        return "❌ Ошибка: API-ключ не найден. Пожалуйста, добавьте магазин в настройках.", None

    date_from_str = start_date.strftime("%Y-%m-%d")
    date_to_str = end_date.strftime("%Y-%m-%d")

    msg_status = await bot.send_message(user_id, "⏳ Запрашиваю данные из Wildberries API...")

    # Вызываем обновленную функцию с period="daily"
    report_task = get_wb_weekly_report(api_key, date_from_str, date_to_str, period="daily")
    orders_task = get_wb_orders(api_key, date_from_str, date_to_str)
    daily_report_data, orders_data = await asyncio.gather(report_task, orders_task)

    if daily_report_data is None or orders_data is None:
        return "❌ Ошибка: Не удалось получить данные от Wildberries. Попробуйте позже.", None

    await msg_status.edit_text("⚙️ Создаю и форматирую Google Таблицу...")

    # 3. Создание и форматирование таблицы
    gc = await get_gspread_client()
    if not gc:
        return "❌ Ошибка: Не удалось подключиться к Google API.", None

    shop_display_name = shop_name or f"Магазин {user_id}"
    spreadsheet_title = f"Юнит-экономика: {shop_display_name} ({start_date.strftime('%d.%m')}-{end_date.strftime('%d.%m.%Y')})"
    spreadsheet = gc.create(spreadsheet_title)
    spreadsheet.share(None, perm_type='anyone', role='reader')

    default_sheet = spreadsheet.get_worksheet(0)

    await create_unit_economics_sheet(spreadsheet)

    await msg_status.edit_text("📝 Заполняю отчет данными...")

    # 4. Наполнение данными
    await fill_unit_economics_sheet(spreadsheet, daily_report_data, orders_data)


    spreadsheet.del_worksheet(default_sheet)

    return "✅ Отчет успешно создан!", spreadsheet.url
