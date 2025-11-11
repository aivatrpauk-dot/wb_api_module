import gspread
import logging
from collections import defaultdict
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


# --- Функции для создания структуры ---

def _define_headers():
    """Определяет двухуровневые заголовки для отчета (без колонки 'Дата')."""
    main_headers = [
        "Артикул (nmId)", "Наименование",
        "Маржинальная прибыль", None,
        "Заказы руб", "Выкупы руб",
        "Себестоимость продаж", None,
        "Потери по браку", None,
        "Возвраты по браку (руб)",
        "Заказы (шт)", "Выкупы (шт)", "Возвраты по браку (шт)",
        "% выкупа",
        "Хранение", None,
        "Базовая комиссия", None, "СПП", None, "Комиссия ИТОГ", None,
        "Логистика прямая", "Логистика обратная",
        "% логистики", "Логистика на ед",
        "Реклама", None,
        "% (ДРР)",
        "Приемка", "Штрафы", "Корректировки"
    ]
    sub_headers = [
        "", "",
        "руб", "%", "руб", "руб", "руб", "%", "руб", "%", "руб",
        "шт", "шт", "шт", "%", "руб", "%", "руб", "%", "руб", "%", "руб", "%", "руб", "руб", "%", "руб", "руб", "%", "%",
        "руб", "руб", "руб"
    ]
    return main_headers, sub_headers


def _build_requests(sheet_id: int):
    """
    Создает запросы для форматирования с учетом новых колонок для комиссии.
    """
    requests = []

    # --- 1. ЗАПРОСЫ НА ОБЪЕДИНЕНИЕ ЯЧЕЕК ---
    # Формат кортежа: (start_row, start_col, row_span, col_span)

    # Горизонтальное объединение (в первой строке)
    horizontal_merges = [
        (1, 3, 1, 2),  # Маржинальная прибыль (C1:D1)
        (1, 7, 1, 2),  # Себестоимость продаж (G1:H1)
        (1, 9, 1, 2),  # Потери по браку (I1:J1)
        (1, 16, 1, 2),  # Хранение (P1:Q1)
        (1, 18, 1, 2),  # Базовая комиссия (R1:S1)
        (1, 20, 1, 2),  # СПП (T1:U1)
        (1, 22, 1, 2),  # Комиссия ИТОГ (V1:W1)
        (1, 28, 1, 2),  # Реклама (AB1:AC1)
    ]

    # Вертикальное объединение (строки 1 и 2)
    vertical_merges = [
        (1, 1, 2, 1), (1, 2, 2, 1),  # Артикул, Наименование
        (1, 5, 2, 1), (1, 6, 2, 1),  # Заказы руб, Выкупы руб
        (1, 11, 2, 1),  # Возвраты по браку (руб)
        (1, 12, 2, 1), (1, 13, 2, 1), (1, 14, 2, 1),  # Заказы (шт), ...
        (1, 15, 2, 1),  # % выкупа
        (1, 24, 2, 1), (1, 25, 2, 1),  # Логистика
        (1, 26, 2, 1), (1, 27, 2, 1),  # % логистики
        (1, 30, 2, 1),  # % (ДРР)
        (1, 31, 2, 1), (1, 32, 2, 1), (1, 33, 2, 1)  # Приемка, ...
    ]

    all_merges = horizontal_merges + vertical_merges
    for row, col, row_span, col_span in all_merges:
        requests.append({"mergeCells": {"range": {
            "sheetId": sheet_id,
            "startRowIndex": row - 1, "endRowIndex": row - 1 + row_span,
            "startColumnIndex": col - 1, "endColumnIndex": col - 1 + col_span
        }, "mergeType": "MERGE_ALL"}})

    # --- 2. ЗАПРОСЫ НА ФОРМАТИРОВАНИЕ ---
    requests.append({"repeatCell": {  # Формат 1-й строки
        "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1},
        "cell": {"userEnteredFormat": {"backgroundColor": {"red": 58 / 255, "green": 111 / 255, "blue": 149 / 255},
                                       "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE",
                                       "textFormat": {"foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
                                                      "fontSize": 10, "bold": True, "fontFamily": "Verdana"}}},
        "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,verticalAlignment,textFormat)"}})
    requests.append({"repeatCell": {  # Формат 2-й строки
        "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 2},
        "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE",
                                       "textFormat": {"foregroundColor": {"red": 0.0, "green": 0.0, "blue": 0.0},
                                                      "fontSize": 9, "bold": False, "fontFamily": "Verdana"}}},
        "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment,textFormat)"}})
    requests.append({"repeatCell": {  # Формат данных
        "range": {"sheetId": sheet_id, "startRowIndex": 2},
        "cell": {"userEnteredFormat": {"textFormat": {"fontFamily": "Verdana", "fontSize": 11}}},
        "fields": "userEnteredFormat(textFormat)"}})

    # --- 3. ЗАПРОСЫ НА УСТАНОВКУ ШИРИНЫ СТОЛБЦОВ ---
    column_widths = [
        # A,   B,      C,   D,    E,     F,       G,   H,    I,    J,     K,
        120, 250, 110, 60, 110, 110, 110, 60, 110, 60, 130,
        # L,    M,    N,    O,       P,    Q,
        90, 90, 130, 90, 100, 60,
        # --- НОВЫЕ КОЛОНКИ ---
        # R (Баз.Ком), S(%), T(СПП), U(%), V(Ком.Итог), W(%)
        110, 60, 110, 60, 110, 60,
        # --- СДВИНУТЫЕ КОЛОНКИ ---
        # X(Лог.Прям), Y(Лог.Обр), Z(%), AA(на ед), AB(Рек), AC(%), AD(ДРР), AE(Прием), AF(Штраф), AG(Корр)
        130, 130, 100, 130, 100, 60, 80, 100, 100, 110
    ]
    for i, width in enumerate(column_widths):
        requests.append({"updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": i, "endIndex": i + 1},
            "properties": {"pixelSize": width}, "fields": "pixelSize"}})

    return requests


async def create_unit_economics_sheet(spreadsheet: gspread.Spreadsheet):
    """
    Создает и форматирует лист "Юнит экономика" с правильным порядком операций.
    """
    SHEET_NAME = "Юнит экономика"
    try:
        logger.info(f"Создание листа '{SHEET_NAME}' в таблице '{spreadsheet.title}'")

        # 1. Создаем лист и записываем заголовки
        worksheet = spreadsheet.add_worksheet(title=SHEET_NAME, rows=1000, cols=35)
        main_headers, sub_headers = _define_headers()
        worksheet.update('A1', [main_headers, sub_headers])

        # 2. Получаем ID листа и собираем все запросы на форматирование/объединение
        sheet_id = worksheet.id
        requests = _build_requests(sheet_id)

        # 3. ВЫПОЛНЯЕМ BATCH_UPDATE (объединение, цвет, ширина)
        spreadsheet.batch_update({"requests": requests})

        # 4. ВЫПОЛНЯЕМ ЗАМОРОЗКУ (ПОСЛЕ всех структурных изменений)
        # Закрепляем 2 строки и 2 столбца (Артикул, Наименование)
        worksheet.freeze(rows=2, cols=2)

        logger.info(f"Лист '{SHEET_NAME}' успешно создан и отформатирован.")

    except Exception as e:
        logger.error(f"Ошибка при создании листа '{SHEET_NAME}': {e}", exc_info=True)


# --- ФУНКЦИИ ДЛЯ НАПОЛНЕНИЯ ДАННЫМИ ---

def _apply_data_formatting(worksheet: gspread.Worksheet, last_row: int):
    """Применяет форматирование чисел к строкам с данными (Скорректированная версия)."""
    if last_row <= 2:
        return

    # Формат "Рубли" (CURRENCY)
    currency_ranges = [
        f"C3:D{last_row}",  # Марж. прибыль
        f"E3:H{last_row}",  # Заказы, Выкупы, Себест., Потери
        f"I3:J{last_row}",
        f"K3:K{last_row}",  # Возвраты по браку
        f"P3:P{last_row}",  # Хранение руб
        f"R3:R{last_row}",  # Баз. Ком. руб
        f"T3:T{last_row}",  # СПП руб
        f"V3:V{last_row}",  # Ком. ИТОГ руб
        f"X3:Y{last_row}",  # Логистика
        f"AA3:AA{last_row}",  # Логистика на ед
        f"AB3:AB{last_row}",  # Реклама руб
        f"AD3:AG{last_row}",  # Приемка, Штрафы, Корр.
    ]
    for r in currency_ranges:
        worksheet.format(r, {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00\" ₽\""}})

    # Формат "Штуки" (NUMBER, 0)
    worksheet.format(f"L3:N{last_row}", {"numberFormat": {"type": "NUMBER", "pattern": "0"}})

    # Формат "Проценты" (PERCENT)
    percent_ranges = [
        f"D3:D{last_row}",  # Марж. прибыль %
        f"H3:H{last_row}",  # Себест. %
        f"J3:J{last_row}",  # Потери %
        f"O3:O{last_row}",  # % выкупа
        f"Q3:Q{last_row}",  # Хранение %
        f"S3:S{last_row}",  # Баз. Ком. %
        f"U3:U{last_row}",  # СПП %
        f"W3:W{last_row}",  # Ком. ИТОГ %
        f"Z3:Z{last_row}",  # % логистики
        f"AC3:AC{last_row}",  # Реклама %
        f"AD3:AD{last_row}",  # % (ДРР)
    ]
    for r in percent_ranges:
        worksheet.format(r, {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}})


async def fill_unit_economics_sheet(spreadsheet: gspread.Spreadsheet, daily_report_data: list, orders_data: list,
                                    ad_costs: dict, storage_costs: dict):
    """
    Агрегирует данные по АРТИКУЛУ за весь период и заполняет лист "Юнит экономика".
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
            if key not in product_names: product_names[key] = order.get("subject", "Не указано")
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
            worksheet.update('A3', rows_to_insert, value_input_option='USER_ENTERED')
            # Корректируем диапазоны форматирования данных
            _apply_data_formatting(worksheet, 2 + len(rows_to_insert))

            logger.info(f"Лист '{SHEET_NAME}' успешно заполнен. Добавлено строк: {len(rows_to_insert)}")
        else:
            logger.info(f"Данные для заполнения листа '{SHEET_NAME}' отсутствуют.")

    except Exception as e:
        logger.error(f"Ошибка при заполнении листа '{SHEET_NAME}': {e}", exc_info=True)


