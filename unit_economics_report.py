import gspread
import logging
from collections import defaultdict
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


# --- Функции для создания структуры ---

def _define_headers():
    """Определяет ОДНОУРОВНЕВЫЕ заголовки для отчета."""
    headers = [
        "Артикул (nmId)", "Наименование",
        "Маржинальная прибыль", "", # <-- Пустая строка для '%'
        "Заказы руб", "Выкупы руб",
        "Себестоимость продаж", "", # <-- Пустая строка для '%'
        "Потери по браку", "",      # <-- Пустая строка для '%'
        "Возвраты по браку (руб)",
        "Заказы (шт)", "Выкупы (шт)", "Возвраты по браку (шт)",
        "% выкупа",
        "Хранение", "",             # <-- Пустая строка для '%'
        "Базовая комиссия", "",     # <-- Пустая строка для '%'
        "СПП", "",                  # <-- Пустая строка для '%'
        "Комиссия ИТОГ", "",        # <-- Пустая строка для '%'
        "Логистика прямая", "Логистика обратная",
        "% логистики", "Логистика на ед",
        "Реклама", "",              # <-- Пустая строка для '%'
        "% (ДРР)",
        "Приемка", "Штрафы", "Корректировки"
    ]
    return headers


def _build_requests(sheet_id: int):
    """
    Создает запросы для форматирования с ОДНОУРОВНЕВОЙ шапкой.
    """
    requests = []

    # --- 1. ЗАПРОСЫ НА ОБЪЕДИНЕНИЕ ЯЧЕЕК (только горизонтальное) ---
    horizontal_merges = [
        (1, 3, 1, 2),  # Маржинальная прибыль
        (1, 7, 1, 2),  # Себестоимость продаж
        (1, 9, 1, 2),  # Потери по браку
        (1, 16, 1, 2),  # Хранение
        (1, 18, 1, 2),  # Базовая комиссия
        (1, 20, 1, 2),  # СПП
        (1, 22, 1, 2),  # Комиссия ИТОГ
        (1, 28, 1, 2),  # Реклама
    ]

    for row, col, row_span, col_span in horizontal_merges:
        requests.append({"mergeCells": {"range": {
            "sheetId": sheet_id,
            "startRowIndex": row - 1, "endRowIndex": row - 1 + row_span,
            "startColumnIndex": col - 1, "endColumnIndex": col - 1 + col_span
        }, "mergeType": "MERGE_ALL"}})

    # --- 2. ЗАПРОСЫ НА ФОРМАТИРОВАНИЕ (только для одной строки) ---
    requests.append({"repeatCell": {  # Формат 1-й строки
        "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1},
        "cell": {"userEnteredFormat": {"backgroundColor": {"red": 58 / 255, "green": 111 / 255, "blue": 149 / 255},
                                       "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE",
                                       "textFormat": {"foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
                                                      "fontSize": 10, "bold": True, "fontFamily": "Verdana"}}},
        "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,verticalAlignment,textFormat)"}})

    requests.append({"repeatCell": {  # Формат данных (теперь со 2-й строки)
        "range": {"sheetId": sheet_id, "startRowIndex": 1},
        "cell": {"userEnteredFormat": {"textFormat": {"fontFamily": "Verdana", "fontSize": 11}}},
        "fields": "userEnteredFormat(textFormat)"}})

    # --- 3. ЗАПРОСЫ НА УСТАНОВКУ ШИРИНЫ СТОЛБЦОВ (без изменений) ---
    column_widths = [
        120, 250, 110, 60, 110, 110, 110, 60, 110, 60, 130, 90, 90, 130, 90,
        100, 60, 110, 60, 110, 60, 110, 60, 130, 130, 100, 130, 100, 60,
        80, 100, 100, 110
    ]
    for i, width in enumerate(column_widths):
        requests.append({"updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": i, "endIndex": i + 1},
            "properties": {"pixelSize": width}, "fields": "pixelSize"}})

    return requests


async def create_unit_economics_sheet(spreadsheet: gspread.Spreadsheet):
    """
    Создает и форматирует лист "Юнит экономика" с одноуровневой шапкой.
    """
    SHEET_NAME = "Юнит экономика"
    try:
        logger.info(f"Создание листа '{SHEET_NAME}' в таблице '{spreadsheet.title}'")

        worksheet = spreadsheet.add_worksheet(title=SHEET_NAME, rows=1000, cols=35)

        # --- Записываем только один ряд заголовков ---
        headers = _define_headers()
        worksheet.update('A1', [headers])

        sheet_id = worksheet.id
        requests = _build_requests(sheet_id)
        spreadsheet.batch_update({"requests": requests})

        # --- Закрепляем 1 строку и 2 столбца ---
        worksheet.freeze(rows=1, cols=2)

        logger.info(f"Лист '{SHEET_NAME}' успешно создан и отформатирован.")

    except Exception as e:
        logger.error(f"Ошибка при создании листа '{SHEET_NAME}': {e}", exc_info=True)

# --- ФУНКЦИИ ДЛЯ НАПОЛНЕНИЯ ДАННЫМИ ---

def _apply_data_formatting(worksheet: gspread.Worksheet, last_row: int):
    """
    Применяет форматирование чисел к строкам с данными (одноуровневая шапка).
    """
    if last_row <= 1:  # Данные начинаются со 2-й строки
        return

    # --- Формат "Рубли" (CURRENCY) ---
    # Диапазоны скорректированы с учетом сдвига колонок
    currency_ranges = [
        f"C2:C{last_row}",  # Маржинальная прибыль (руб)
        f"E2:G{last_row}",  # Заказы руб, Выкупы руб, Себестоимость продаж (руб)
        f"I2:I{last_row}",  # Потери по браку (руб)
        f"K2:K{last_row}",  # Возвраты по браку (руб)
        f"P2:P{last_row}",  # Хранение (руб)
        f"R2:R{last_row}",  # Базовая комиссия (руб)
        f"T2:T{last_row}",  # СПП (руб)
        f"V2:V{last_row}",  # Комиссия ИТОГ (руб)
        f"X2:Y{last_row}",  # Логистика прямая, Логистика обратная
        f"AA2:AA{last_row}",  # Логистика на ед
        f"AB2:AB{last_row}",  # Реклама (руб)
        f"AE2:AG{last_row}",  # Приемка, Штрафы, Корректировки
    ]
    for r in currency_ranges:
        worksheet.format(r, {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00\" ₽\""}})

    # --- Формат "Штуки" (NUMBER, целое) ---
    worksheet.format(f"L2:N{last_row}", {"numberFormat": {"type": "NUMBER", "pattern": "0"}})

    # --- Формат "Проценты" (PERCENT) ---
    percent_ranges = [
        f"D2:D{last_row}",  # Маржинальная прибыль (%)
        f"H2:H{last_row}",  # Себестоимость (%)
        f"J2:J{last_row}",  # Потери по браку (%)
        f"O2:O{last_row}",  # % выкупа
        f"Q2:Q{last_row}",  # Хранение (%)
        f"S2:S{last_row}",  # Базовая комиссия (%)
        f"U2:U{last_row}",  # СПП (%)
        f"W2:W{last_row}",  # Комиссия ИТОГ (%)
        f"Z2:Z{last_row}",  # % логистики
        f"AC2:AC{last_row}",  # Реклама (%)
        f"AD2:AD{last_row}",  # % (ДРР)
    ]
    for r in percent_ranges:
        worksheet.format(r, {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}})


async def fill_unit_economics_sheet(spreadsheet: gspread.Spreadsheet, daily_report_data: list, orders_data: list,
                                    ad_costs: dict, storage_costs: dict):
    """
    Агрегирует данные и заполняет лист "Юнит экономика" (одноуровневая шапка).
    """
    SHEET_NAME = "Юнит экономика"
    try:
        logger.info(f"Начало заполнения листа '{SHEET_NAME}' (агрегация по артикулам)...")
        worksheet = spreadsheet.worksheet(SHEET_NAME)

        # 1. Агрегация данных.
        products = defaultdict(lambda: defaultdict(float))
        product_names = {}
        for order in orders_data:
            key = order.get("nmId")
            if not key: continue
            products[key]["orders_rub"] += order.get("totalPrice", 0) * (1 - order.get("discountPercent", 0) / 100)
            products[key]["orders_pcs"] += 1
            if key not in product_names:
                product_names[key] = order.get("supplierArticle", "Не указано")
        for row in daily_report_data:
            key = row.get("nm_id")
            if not key: continue
            if key not in product_names: product_names[key] = row.get("subject_name", "Не указано")
            doc_type = (row.get("doc_type_name") or "").lower()
            if "продажа" in doc_type:
                products[key]["sales_rub"] += row.get("retail_amount", 0)
                products[key]["sales_pcs"] += row.get("quantity", 0)
            elif "возврат" in doc_type:
                products[key]["returns_rub"] += row.get("retail_amount", 0)
                products[key]["returns_pcs"] += row.get("quantity", 0)
            products[key]["logistics_forward_rub"] += row.get("delivery_rub", 0) - row.get("rebill_logistic_cost", 0)
            products[key]["logistics_reverse_rub"] += row.get("rebill_logistic_cost", 0)
            products[key]["acceptance_rub"] += row.get("acceptance", 0)
            products[key]["storage_rub"] += row.get("storage_fee", 0)
            products[key]["penalty_rub"] += row.get("penalty", 0)
            products[key]["to_pay_rub"] += row.get("ppvz_for_pay", 0)
            products[key]["total_retail_turnover_rub"] += row.get("retail_amount", 0)
            spp_amount = row.get("retail_amount", 0) * (row.get("ppvz_spp_prc", 0) / 100)
            products[key]["spp_rub"] += spp_amount
            adjustments = (
                    row.get("additional_payment", 0) +
                    row.get("cashback_amount", 0) +
                    row.get("cashback_discount", 0) +
                    row.get("cashback_commission_change", 0)
            )
            products[key]["adjustments_rub"] += adjustments

        # 2. Агрегируем рекламу и хранение.
        ad_costs_by_nm = defaultdict(float)
        for (date_str, nm_id), cost in ad_costs.items(): ad_costs_by_nm[nm_id] += cost
        storage_costs_by_nm = defaultdict(float)
        for (date_str, nm_id), cost in storage_costs.items(): storage_costs_by_nm[nm_id] += cost

        # 3. Формирование строк для таблицы
        rows_to_insert = []
        for key in sorted(products.keys()):
            p = products[key]
            advertising_cost = ad_costs_by_nm.get(key, 0.0)
            storage_cost = storage_costs_by_nm.get(key, 0.0)
            drr_percent = (advertising_cost / p["sales_rub"]) if p["sales_rub"] > 0 else 0
            buyout_percent = (p["sales_pcs"] / p["orders_pcs"]) if p["orders_pcs"] > 0 else 0
            commission_total = p["sales_rub"] - p["to_pay_rub"]
            spp = p["spp_rub"]
            commission_base = commission_total + spp

            row_data = [
                key,  # Артикул (nmId)
                product_names.get(key, ""),
                0, 0,
                p["orders_rub"], p["sales_rub"],
                0, 0, 0, 0,
                p["returns_rub"],
                p["orders_pcs"], p["sales_pcs"], p["returns_pcs"],
                buyout_percent,
                storage_cost, 0,
                commission_base, 0,  # Базовая комиссия руб, %
                spp, 0,  # СПП руб, %
                commission_total, 0,  # Комиссия ИТОГ руб, %
                p["logistics_forward_rub"], p["logistics_reverse_rub"],
                0, 0,
                advertising_cost, 0,
                drr_percent,
                p["acceptance_rub"], p["penalty_rub"], p["adjustments_rub"]
            ]
            rows_to_insert.append(row_data)

        # 4. Запись данных в таблицу
        if rows_to_insert:
            worksheet.update('A2', rows_to_insert, value_input_option='USER_ENTERED')
            # Корректируем диапазоны форматирования данных
            _apply_data_formatting(worksheet, 1 + len(rows_to_insert))

            logger.info(f"Лист '{SHEET_NAME}' успешно заполнен. Добавлено строк: {len(rows_to_insert)}")
        else:
            logger.info(f"Данные для заполнения листа '{SHEET_NAME}' отсутствуют.")

    except Exception as e:
        logger.error(f"Ошибка при заполнении листа '{SHEET_NAME}': {e}", exc_info=True)


