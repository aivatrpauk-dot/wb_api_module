# --- START OF FILE unit_economics_report.py ---
import gspread
import logging

logger = logging.getLogger(__name__)


def _define_headers():
    """Определяет двухуровневые заголовки для отчета."""
    main_headers = [
        "Дата", "Артикул (nmId)", "Наименование",
        "Маржинальная прибыль", None,
        "Заказы руб", "Выкупы руб",
        "Себестоимость продаж", None,
        "Потери по браку", None,
        "Возвраты по браку (руб)",
        "Заказы (шт)", "Выкупы (шт)", "Возвраты по браку (шт)",
        "% выкупа",
        "Хранение", None,
        "Комиссия", None,
        "Логистика прямая", "Логистика обратная",
        "% логистики", "Логистика на ед",
        "Реклама", None,
        "% (ДРР)",
        "Приемка", "Штрафы", "Корректировки"
    ]
    sub_headers = [
        "", "", "",
        "руб", "%", "руб", "руб", "руб", "%", "руб", "%", "руб",
        "шт", "шт", "шт", "%", "руб", "%", "руб", "%", "руб", "руб", "%", "руб", "руб", "%", "%",
        "руб", "руб", "руб"
    ]
    return main_headers, sub_headers


def _build_requests(sheet_id: int):
    """
    Создает полный список запросов для форматирования, объединения ячеек и установки ширины столбцов.
    """
    requests = []

    # --- 1. ЗАПРОСЫ НА ОБЪЕДИНЕНИЕ ЯЧЕЕК ---
    # Формат кортежа: (start_row, start_col, row_span, col_span)
    horizontal_merges = [
        (1, 4, 1, 2), (1, 8, 1, 2), (1, 10, 1, 2), (1, 17, 1, 2),
        (1, 19, 1, 2), (1, 25, 1, 2)
    ]
    vertical_merges = [
        (1, 1, 2, 1), (1, 2, 2, 1), (1, 3, 2, 1), (1, 6, 2, 1), (1, 7, 2, 1),
        (1, 12, 2, 1), (1, 13, 2, 1), (1, 14, 2, 1), (1, 15, 2, 1), (1, 16, 2, 1),
        (1, 21, 2, 1), (1, 22, 2, 1), (1, 23, 2, 1), (1, 24, 2, 1), (1, 27, 2, 1),
        (1, 28, 2, 1), (1, 29, 2, 1), (1, 30, 2, 1)
    ]
    all_merges = horizontal_merges + vertical_merges
    for row, col, row_span, col_span in all_merges:
        requests.append({
            "mergeCells": {"range": {
                "sheetId": sheet_id,
                "startRowIndex": row - 1, "endRowIndex": row - 1 + row_span,
                "startColumnIndex": col - 1, "endColumnIndex": col - 1 + col_span
            }, "mergeType": "MERGE_ALL"}
        })

    # --- 2. ЗАПРОСЫ НА ФОРМАТИРОВАНИЕ ---
    requests.append({"repeatCell": {
        "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1},
        "cell": {"userEnteredFormat": {
            "backgroundColor": {"red": 58 / 255, "green": 111 / 255, "blue": 149 / 255},
            "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE",
            "textFormat": {"foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
                           "fontSize": 10, "bold": True, "fontFamily": "Verdana"}}},
        "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,verticalAlignment,textFormat)"}})
    requests.append({"repeatCell": {
        "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 2},
        "cell": {"userEnteredFormat": {
            "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE",
            "textFormat": {"foregroundColor": {"red": 0.0, "green": 0.0, "blue": 0.0},
                           "fontSize": 9, "bold": False, "fontFamily": "Verdana"}}},
        "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment,textFormat)"}})
    requests.append({"repeatCell": {
        "range": {"sheetId": sheet_id, "startRowIndex": 2},
        "cell": {"userEnteredFormat": {
            "textFormat": {"fontFamily": "Verdana", "fontSize": 11}}},
        "fields": "userEnteredFormat(textFormat)"}})

    # --- 3. ЗАПРОСЫ НА УСТАНОВКУ ШИРИНЫ СТОЛБЦОВ ---
    # <-- НОВЫЙ БЛОК -->
    column_widths = [
        100, 120, 250, 110, 60, 110, 110, 110, 60, 110, 60, 130,
        90, 90, 130, 90, 100, 60, 100, 60, 130, 130, 100, 130,
        100, 60, 80, 100, 100, 110
    ]
    for i, width in enumerate(column_widths):
        requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": i,
                    "endIndex": i + 1
                },
                "properties": {
                    "pixelSize": width
                },
                "fields": "pixelSize"
            }
        })

    return requests


# --- Главная публичная функция модуля ---

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

# --- END OF FILE unit_economics_report.py ---