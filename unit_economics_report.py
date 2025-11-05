# --- START OF FILE unit_economics_report.py ---
import gspread
import logging
from collections import defaultdict
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


# --- Функции для создания структуры (остаются без изменений) ---

def _define_headers():
    """Определяет двухуровневые заголовки для отчета."""
    main_headers = [
        "Дата", "Артикул (nmId)", "Наименование",
        "Маржинальная прибыль", None, "Заказы руб", "Выкупы руб",
        "Себестоимость продаж", None, "Потери по браку", None, "Возвраты по браку (руб)",
        "Заказы (шт)", "Выкупы (шт)", "Возвраты по браку (шт)", "% выкупа",
        "Хранение", None, "Комиссия", None, "Логистика прямая", "Логистика обратная",
        "% логистики", "Логистика на ед", "Реклама", None, "% (ДРР)",
        "Приемка", "Штрафы", "Корректировки"
    ]
    sub_headers = [
        "", "", "", "руб", "%", "руб", "руб", "руб", "%", "руб", "%", "руб",
        "шт", "шт", "шт", "%", "руб", "%", "руб", "%", "руб", "руб", "%", "руб", "руб", "%", "%",
        "руб", "руб", "руб"
    ]
    return main_headers, sub_headers


def _build_requests(sheet_id: int):
    """
    Создает полный список запросов для форматирования, объединения ячеек и установки ширины столбцов.
    """
    requests = []
    horizontal_merges = [(1, 4, 1, 2), (1, 8, 1, 2), (1, 10, 1, 2), (1, 17, 1, 2), (1, 19, 1, 2), (1, 25, 1, 2)]
    vertical_merges = [(1, 1, 2, 1), (1, 2, 2, 1), (1, 3, 2, 1), (1, 6, 2, 1), (1, 7, 2, 1), (1, 12, 2, 1),
                       (1, 13, 2, 1), (1, 14, 2, 1), (1, 15, 2, 1), (1, 16, 2, 1), (1, 21, 2, 1), (1, 22, 2, 1),
                       (1, 23, 2, 1), (1, 24, 2, 1), (1, 27, 2, 1), (1, 28, 2, 1), (1, 29, 2, 1), (1, 30, 2, 1)]
    all_merges = horizontal_merges + vertical_merges
    for row, col, row_span, col_span in all_merges:
        requests.append({"mergeCells": {
            "range": {"sheetId": sheet_id, "startRowIndex": row - 1, "endRowIndex": row - 1 + row_span,
                      "startColumnIndex": col - 1, "endColumnIndex": col - 1 + col_span}, "mergeType": "MERGE_ALL"}})
    requests.append({"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1}, "cell": {
        "userEnteredFormat": {"backgroundColor": {"red": 58 / 255, "green": 111 / 255, "blue": 149 / 255},
                              "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE",
                              "textFormat": {"foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}, "fontSize": 10,
                                             "bold": True, "fontFamily": "Verdana"}}},
                                    "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,verticalAlignment,textFormat)"}})
    requests.append({"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 2}, "cell": {
        "userEnteredFormat": {"horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE",
                              "textFormat": {"foregroundColor": {"red": 0.0, "green": 0.0, "blue": 0.0}, "fontSize": 9,
                                             "bold": False, "fontFamily": "Verdana"}}},
                                    "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment,textFormat)"}})
    requests.append({"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 2}, "cell": {
        "userEnteredFormat": {"textFormat": {"fontFamily": "Verdana", "fontSize": 11}}},
                                    "fields": "userEnteredFormat(textFormat)"}})
    column_widths = [100, 120, 250, 110, 60, 110, 110, 110, 60, 110, 60, 130, 90, 90, 130, 90, 100, 60, 100, 60, 130,
                     130, 100, 130, 100, 60, 80, 100, 100, 110]
    for i, width in enumerate(column_widths):
        requests.append({"updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": i, "endIndex": i + 1},
            "properties": {"pixelSize": width}, "fields": "pixelSize"}})
    return requests


async def create_unit_economics_sheet(spreadsheet: gspread.Spreadsheet):
    """
    Создает и форматирует лист "Юнит экономика" с помощью одного пакетного запроса.
    """
    SHEET_NAME = "Юнит экономика"
    try:
        logger.info(f"Создание листа '{SHEET_NAME}' в таблице '{spreadsheet.title}'")
        worksheet = spreadsheet.add_worksheet(title=SHEET_NAME, rows=1000, cols=35)
        main_headers, sub_headers = _define_headers()
        worksheet.update('A1', [main_headers, sub_headers])
        worksheet.freeze(rows=2, cols=3)
        sheet_id = worksheet.id
        requests = _build_requests(sheet_id)
        spreadsheet.batch_update({"requests": requests})
        logger.info(f"Лист '{SHEET_NAME}' успешно создан и отформатирован.")
    except Exception as e:
        logger.error(f"Ошибка при создании листа '{SHEET_NAME}': {e}", exc_info=True)


# --- НОВЫЕ ФУНКЦИИ ДЛЯ НАПОЛНЕНИЯ ДАННЫМИ ---

def _apply_data_formatting(worksheet: gspread.Worksheet, last_row: int):
    """Применяет форматирование чисел к строкам с данными."""
    if last_row <= 2:
        return

    # Формат "Рубли"
    currency_ranges = [f"D3:K{last_row}", f"L3:L{last_row}", f"Q3:R{last_row}", f"S3:T{last_row}", f"U3:V{last_row}",
                       f"X3:Z{last_row}", f"AB3:AD{last_row}"]
    for r in currency_ranges:
        worksheet.format(r, {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00\" ₽\""}})

    # Формат "Штуки" (целое число)
    worksheet.format(f"M3:O{last_row}", {"numberFormat": {"type": "NUMBER", "pattern": "0"}})

    # Формат "Проценты"
    percent_ranges = [f"E3:E{last_row}", f"I3:I{last_row}", f"K3:K{last_row}", f"P3:P{last_row}", f"R3:R{last_row}",
                      f"T3:T{last_row}", f"W3:W{last_row}", f"AA3:AA{last_row}"]
    for r in percent_ranges:
        worksheet.format(r, {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}})


async def fill_unit_economics_sheet(spreadsheet: gspread.Spreadsheet, daily_report_data: list, orders_data: list, ad_costs: dict):
    """
    Агрегирует данные и заполняет лист "Юнит экономика".
    """
    SHEET_NAME = "Юнит экономика"
    try:
        logger.info(f"Начало заполнения листа '{SHEET_NAME}'...")
        worksheet = spreadsheet.worksheet(SHEET_NAME)

        # 1. Агрегация данных по ключу (дата, артикул)
        products = defaultdict(lambda: defaultdict(float))
        product_names = {}

        for order in orders_data:
            key = (order.get("date", "")[:10], order.get("nmId"))
            if not all(key): continue

            products[key]["orders_rub"] += order.get("totalPrice", 0) * (1 - order.get("discountPercent", 0) / 100)
            products[key]["orders_pcs"] += 1
            if key not in product_names:
                product_names[key] = order.get("subject", "Не указано")

        for row in daily_report_data:
            key = (row.get("rr_dt", "")[:10], row.get("nm_id"))
            if not all(key): continue

            if key not in product_names:
                product_names[key] = row.get("subject_name", "Не указано")

            # Выкупы и Возвраты
            doc_type = (row.get("doc_type_name") or "").lower()
            if "продажа" in doc_type:
                products[key]["sales_rub"] += row.get("retail_amount", 0)
                products[key]["sales_pcs"] += row.get("quantity", 0)
            # ВАЖНО: Пока реализуем обычные возвраты. Для возвратов по браку нужен отдельный метод.
            elif "возврат" in doc_type:
                products[key]["returns_rub"] += row.get("retail_amount", 0)
                products[key]["returns_pcs"] += row.get("quantity", 0)

            # Финансовые метрики
            products[key]["commission_rub"] += row.get("ppvz_vw", 0)
            products[key]["logistics_forward_rub"] += row.get("delivery_rub", 0) - row.get("rebill_logistic_cost", 0)
            products[key]["logistics_reverse_rub"] += row.get("rebill_logistic_cost", 0)
            products[key]["acceptance_rub"] += row.get("acceptance", 0)
            products[key]["storage_rub"] += row.get("storage_fee", 0)
            products[key]["penalty_rub"] += row.get("penalty", 0)
            products[key]["adjustments_rub"] += row.get("additional_payment", 0)

        # 2. Формирование строк для таблицы
        rows_to_insert = []
        # Сортируем по дате, затем по артикулу
        for key in sorted(products.keys()):
            date_str, nm_id = key
            p = products[key]
            # Получаем расходы на рекламу из агрегированного словаря
            advertising_cost = ad_costs.get(key, 0.0)

            # Рассчитываем ДРР (% от продаж)
            drr_percent = (advertising_cost / p["sales_rub"]) if p["sales_rub"] > 0 else 0
            buyout_percent = (p["sales_pcs"] / p["orders_pcs"]) if p["orders_pcs"] > 0 else 0

            row_data = [
                date_str, nm_id, product_names.get(key, ""),
                0, 0,  # Маржинальная прибыль (пока не считаем)
                p["orders_rub"], p["sales_rub"],
                0, 0,  # Себестоимость (пока нет)
                0, 0,  # Потери по браку (пока нет)
                p["returns_rub"],
                p["orders_pcs"], p["sales_pcs"], p["returns_pcs"],
                buyout_percent,
                p["storage_rub"], 0,  # Хранение %
                p["commission_rub"], 0,  # Комиссия %
                p["logistics_forward_rub"], p["logistics_reverse_rub"],
                0, 0,  # % логистики, Логистика на ед
                advertising_cost, 0,  # Реклама руб, Реклама %
                drr_percent,  # % ДРР
                p["acceptance_rub"], p["penalty_rub"], p["adjustments_rub"]
            ]
            rows_to_insert.append(row_data)

        # 3. Запись данных в таблицу
        if rows_to_insert:
            worksheet.update('A3', rows_to_insert, value_input_option='USER_ENTERED')
            _apply_data_formatting(worksheet, 2 + len(rows_to_insert))
            logger.info(f"Лист '{SHEET_NAME}' успешно заполнен. Добавлено строк: {len(rows_to_insert)}")
        else:
            logger.info(f"Данные для заполнения листа '{SHEET_NAME}' отсутствуют.")

    except Exception as e:
        logger.error(f"Ошибка при заполнении листа '{SHEET_NAME}': {e}", exc_info=True)