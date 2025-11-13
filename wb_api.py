import asyncio
from typing import List
from datetime import datetime, timedelta
import aiohttp
import logging
import pytz

from typing import List, Dict, Any

logger = logging.getLogger(__name__)
MAX_RETRIES = 3
RETRY_DELAY = 60  # секунд — фиксированная пауза при 429 ошибке

ACCEPTANCE_BASE_URL = "https://seller-analytics-api.wildberries.ru/api/v1/acceptance_report"
ACCEPTANCE_STATUS_CHECK_INTERVAL = 5  # секунд между проверками статуса
ACCEPTANCE_MAX_WAIT_TIME = 300  # макс. время ожидания отчёта (5 минут)

PAID_STORAGE_BASE_URL = "https://seller-analytics-api.wildberries.ru/api/v1/paid_storage"
PAID_STORAGE_STATUS_CHECK_INTERVAL = 5  # сек
PAID_STORAGE_MAX_WAIT_TIME = 300  # 5 минут


# ========================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ========================================

def _is_within_date_range(record: dict, start_dt_moscow: datetime, end_dt_moscow: datetime) -> bool:
    """
    Проверяет, находится ли 'date' (дата создания заказа) в заданном московском диапазоне.
    """
    order_date_str = record.get("date")
    if not order_date_str:
        return False

    try:
        # Даты от API приходят как naive, но мы знаем, что это Москва
        tz_moscow = pytz.timezone('Europe/Moscow')
        order_dt_naive = datetime.fromisoformat(order_date_str)
        order_dt_moscow = tz_moscow.localize(order_dt_naive)

        # Сравниваем aware datetime с aware datetime
        return start_dt_moscow <= order_dt_moscow <= end_dt_moscow

    except (ValueError, TypeError):
        logger.warning(f"Некорректный формат 'date' в заказе: {order_date_str}")
        return False


async def _fetch_with_simple_retry(
        session: aiohttp.ClientSession,
        url: str,
        headers: dict,
        params: dict,
        method_name: str,
) -> tuple[int, list | dict | str | None]:  # <-- Обновляем типы
    """Выполняет запрос с простым повтором при 429 ошибке и улучшенным логированием."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(url, headers=headers, params=params, timeout=120) as resp:  # Увеличиваем таймаут
                if resp.status == 200:
                    return 200, await resp.json()
                elif resp.status == 429:
                    logger.warning(
                        f"{method_name}: 429 Too Many Requests (попытка {attempt}/{MAX_RETRIES})")
                    if attempt < MAX_RETRIES:
                        await asyncio.sleep(RETRY_DELAY)
                        continue
                    else:
                        return 429, await resp.text()
                else:
                    # Логируем другие ошибки API
                    error_text = await resp.text()
                    logger.error(
                        f"{method_name}: API Error (попытка {attempt}/{MAX_RETRIES}) - Status: {resp.status}, Body: {error_text[:500]}")
                    # Для 4xx ошибок (кроме 429) нет смысла повторять
                    if 400 <= resp.status < 500:
                        return resp.status, error_text
                    # Для 5xx ошибок повторяем
                    if attempt < MAX_RETRIES:
                        await asyncio.sleep(RETRY_DELAY / 2)
                        continue
                    else:
                        return resp.status, error_text

        except Exception as e:
            # ---  ЛОГИРОВАНИЕ ИСКЛЮЧЕНИЙ ---
            logger.error(
                f"{method_name}: Exception (попытка {attempt}/{MAX_RETRIES}): {type(e).__name__} - {e}",
                exc_info=True  # Добавляем полный трейсбек в лог
            )
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY / 2)
                continue
            else:
                # Если все попытки провалены, возвращаем None, чтобы вызывающая функция могла это обработать
                return None, None

    return None, None  # Если цикл завершился (не должно происходить)

# ========================================
# ЕЖЕДНЕВНЫЕ ОТЧЁТЫ
# ========================================

async def get_wb_orders(
        api_key: str,
        start_date: datetime,
        end_date: datetime
) -> List[dict] | None:
    """
    Получает список заказов через /api/v1/supplier/orders
    Args:
        api_key (str): API-ключ продавца.
        date_from (str): Дата начала периода в формате "YYYY-MM-DD".
        date_to (str): Дата окончания периода в формате "YYYY-MM-DD".

    Returns:
        list[dict]: Список заказов. Основные поля:

        📅 **Даты и статусы**
            - `date` — дата и время заказа (МСК, UTC+3)
            - `lastChangeDate` — дата и время последнего обновления (МСК, UTC+3)
            - `isCancel` — признак отмены заказа
            - `cancelDate` — дата отмены (если применимо)

        📍 **География и склад**
            - `warehouseName` — название склада отгрузки
            - `warehouseType` — тип склада ("Склад WB"/"Склад продавца")
            - `countryName` — страна доставки
            - `oblastOkrugName` — федеральный округ
            - `regionName` — регион доставки

        🏷 **Товар и артикулы**
            - `nmId` — артикул Wildberries
            - `supplierArticle` — артикул продавца
            - `barcode` — штрихкод товара
            - `brand` — бренд
            - `category` — категория товара
            - `subject` — предметная группа
            - `techSize` — размер товара

        💰 **Цены и скидки**
            - `totalPrice` — исходная цена (без скидок)
            - `discountPercent` — процент скидки продавца
            - `priceWithDisc` — цена с учётом скидки продавца
            - `spp` — размер скидки Wildberries
            - `finishedPrice` — итоговая цена (со всеми скидками кроме WB Кошелька)

        📦 **Логистика и идентификаторы**
            - `incomeID` — номер поставки
            - `sticker` — идентификатор стикера
            - `gNumber` — идентификатор корзины заказа
            - `srid` — уникальный идентификатор заказа
            - `isSupply` — признак договора поставки
            - `isRealization` — признак договора реализации
    """
    url = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"
    headers = {"Authorization": api_key}
    all_orders_raw = []

    # Готовим границы периода в московском времени для финальной фильтрации
    tz_moscow = pytz.timezone('Europe/Moscow')
    start_dt_moscow = (start_date if start_date.tzinfo else tz_moscow.localize(start_date)).replace(hour=0, minute=0,
                                                                                                    second=0)
    end_dt_moscow = (end_date if end_date.tzinfo else tz_moscow.localize(end_date)).replace(hour=23, minute=59,
                                                                                            second=59)

    # Для API WB dateFrom должен быть в формате ISO
    current_date_from = start_dt_moscow.isoformat()

    async with aiohttp.ClientSession() as session:
        while True:
            # Используем flag=0 для быстрой пагинации
            params = {"dateFrom": current_date_from, "flag": 0}

            status, data_or_text = await _fetch_with_simple_retry(
                session, url, headers, params, "Orders API (flag=0)"
            )

            if status == 200 and isinstance(data_or_text, list):
                data = data_or_text
                if not data:
                    break  # Данные закончились

                all_orders_raw.extend(data)

                last_change_date = data[-1].get("lastChangeDate")
                if not last_change_date:
                    logger.warning("Отсутствует lastChangeDate, прерывание пагинации.")
                    break

                current_date_from = last_change_date


            else:
                logger.error(f"Orders API ошибка: {status} — {data_or_text}")
                return None

    # Финальная часть: фильтруем все полученные "сырые" данные
    # по полю 'date' (дата создания заказа).
    logger.info(f"Получено {len(all_orders_raw)} сырых записей по заказам. Фильтрую по дате создания...")
    filtered_orders = [r for r in all_orders_raw if _is_within_date_range(r, start_dt_moscow, end_dt_moscow)]
    logger.info(f"Осталось {len(filtered_orders)} заказов после фильтрации.")

    return filtered_orders


### НЕ ИСПОЛЬЗОВАЛАСЬ ###

async def get_wb_sales(api_key: str, date_from: str, date_to: str) -> List[dict]:
    """
    Получает список продаж и возвратов через /api/v1/supplier/sales
    Args:
        api_key (str): API-ключ продавца.
        date_from (str): Дата начала периода в формате "YYYY-MM-DD".
        date_to (str): Дата окончания периода в формате "YYYY-MM-DD".

    Returns:
        list[dict]: Список продаж и возвратов. Основные поля:

        📅 **Даты и идентификаторы**
            - `date` — дата и время продажи (МСК, UTC+3)
            - `lastChangeDate` — дата и время последнего обновления (МСК, UTC+3)
            - `saleID` — уникальный ID операции (S********** — продажа, R********** — возврат)
            - `srid` — уникальный ID заказа
            - `gNumber` — ID корзины покупателя

        📍 **География и склад**
            - `warehouseName` — название склада отгрузки
            - `warehouseType` — тип склада ("Склад WB"/"Склад продавца")
            - `countryName`, `oblastOkrugName`, `regionName` — география доставки

        🏷 **Товар и артикулы**
            - `nmId` — артикул Wildberries
            - `supplierArticle` — артикул продавца
            - `barcode` — штрихкод товара
            - `brand`, `category`, `subject` — характеристики товара
            - `techSize` — размер товара
            - `incomeID` — номер поставки

        💰 **Цены и финансы**
            - `totalPrice` — исходная цена (без скидок)
            - `discountPercent` — процент скидки продавца
            - `priceWithDisc` — цена с учётом скидки продавца
            - `spp` — размер скидки Wildberries
            - `finishedPrice` — фактическая цена с покупателя (со всеми скидками)
            - `forPay` — сумма к перечислению продавцу
            - `paymentSaleAmount` — скидка за оплату WB Кошельком

        📦 **Дополнительная информация**
            - `isSupply`, `isRealization` — признаки договоров
            - `sticker` — идентификатор стикера
    """

    url = "https://statistics-api.wildberries.ru/api/v1/supplier/sales"
    headers = {"Authorization": api_key}
    all_sales = []

    start_dt = datetime.fromisoformat(f"{date_from}T00:00:00")
    end_dt = datetime.fromisoformat(f"{date_to}T23:59:59")
    current_date_from = f"{date_from}T00:00:00"

    async with aiohttp.ClientSession() as session:
        while True:
            params = {"dateFrom": current_date_from, "flag": 0}

            status, data_or_text = await _fetch_with_simple_retry(
                session, url, headers, params, "Sales API"
            )

            if status == 200:
                data = data_or_text
                if not data:
                    break
                all_sales.extend(data)

                last_change_date = data[-1].get("lastChangeDate")
                if not last_change_date:
                    logger.warning(
                        "Отсутствует lastChangeDate в последней записи. Прерывание.")
                    break
                current_date_from = last_change_date

                try:
                    last_dt = datetime.fromisoformat(
                        last_change_date.replace("Z", "+00:00"))
                    if last_dt > end_dt:
                        break
                except ValueError:
                    pass

            else:
                logger.error(f"Sales API ошибка: {status} — {data_or_text}")
                break

    return [r for r in all_sales if _is_within_date_range(r, start_dt, end_dt)]


### НЕ ИСПОЛЬЗОВАЛАСЬ ###

async def get_wb_acceptance_report(
    api_key: str,
    date_from: str,
    date_to: str,
) -> List[Dict[str, Any]]:
    """
    Получает отчёт о платной приёмке через API (создание задачи → ожидание → загрузка)
    Args:
        api_key (str): API-ключ продавца.
        date_from (str): Дата начала периода в формате "YYYY-MM-DD".
        date_to (str): Дата окончания периода в формате "YYYY-MM-DD".

    Returns:
        list[dict]: Записи о платной приёмке товаров. Основные поля:

        📦 **Приёмка и поставка**
            - `shkCreateDate` — дата приёмки товара
            - `giCreateDate` — дата создания поставки
            - `incomeId` — номер поставки
            - `count` — количество принятых товаров, шт.

        🏷 **Идентификация товара**
            - `nmID` — артикул Wildberries
            - `subjectName` — предметная группа

        💰 **Стоимость**
            - `total` — суммарная стоимость приёмки (рубли с копейками)
    """

    headers = {"Authorization": api_key}
    start_dt = datetime.fromisoformat(f"{date_from}T00:00:00")
    end_dt = datetime.fromisoformat(f"{date_to}T23:59:59")

    async with aiohttp.ClientSession() as session:
        # 1. Создать задачу на формирование отчёта
        payload = {
            "dateFrom": date_from,
            "dateTo": date_to
        }
        status, data = await _fetch_with_simple_retry(
            session,
            ACCEPTANCE_BASE_URL,
            headers,
            payload,
            "Acceptance Report Create"
        )
        logger.info(f"Успешный ответ: {data}")
        if status != 200:
            logger.error(
                f"Не удалось создать задачу на отчёт приёмки: {status} — {data}")
            return []

        task_id = data.get("data", {}).get("taskId")
        if not task_id:
            logger.error("Ответ на создание задачи не содержит taskId")
            return []

        logger.info(f"Создана задача на отчёт приёмки: {task_id}")

        # 2. Ожидать завершения задачи
        wait_time = 0
        while wait_time < ACCEPTANCE_MAX_WAIT_TIME:
            status_url = f"{ACCEPTANCE_BASE_URL}/tasks/{task_id}/status"
            try:
                async with session.get(status_url, headers=headers, timeout=10) as resp:
                    if resp.status == 200:
                        status_data = await resp.json()
                        task_status = status_data.get("data").get("status")
                        if task_status == "done":
                            logger.info("Отчёт о приёмке готов.")
                            break
                        elif task_status == "error":
                            logger.error(
                                f"Ошибка при генерации отчёта: {status_data}")
                            return []
                        # else: "in_progress" или другой — ждём
                    else:
                        logger.warning(
                            f"Неожиданный статус при проверке задачи: {resp.status}")
            except Exception as e:
                logger.error(f"Ошибка при проверке статуса задачи: {e}")

            await asyncio.sleep(ACCEPTANCE_STATUS_CHECK_INTERVAL)
            wait_time += ACCEPTANCE_STATUS_CHECK_INTERVAL
        else:
            logger.error(
                "Превышено время ожидания готовности отчёта о приёмке")
            return []

        # 3. Скачать отчёт
        download_url = f"{ACCEPTANCE_BASE_URL}/tasks/{task_id}/download"
        try:
            async with session.get(download_url, headers=headers, timeout=30) as resp:
                if resp.status == 200:
                    report_data = await resp.json()
                    logger.info(
                        f"Получено {len(report_data)} записей из отчёта приёмки.")
                    logger.info(f"{report_data}")
                    filtered = []
                    for record in report_data:
                        record_date_str = record.get(
                            "shkCreateDate")  # ← ИСПРАВЛЕНО
                        if not record_date_str:
                            continue
                        try:
                            record_date = datetime.fromisoformat(
                                record_date_str)
                            if start_dt.date() <= record_date.date() <= end_dt.date():
                                filtered.append(record)
                        except ValueError:
                            logger.warning(
                                f"Некорректная дата shkCreateDate: {record_date_str}")
                    return filtered
                else:
                    logger.error(f"Ошибка при скачивании отчёта: {resp.status} — {await resp.text()}")
                    return []
        except Exception as e:
            logger.error(f"Исключение при скачивании отчёта: {e}")
            return []


### Платное хранение - теперь используем ###

async def get_wb_paid_storage_report(
    api_key: str,
    start_date: datetime,
    end_date: datetime
) -> List[Dict[str, Any]] | None:
    """
    Получает отчёт о платном хранении с пагинацией по дате (чанками по 8 дней).
    Args:
        api_key (str): API-ключ продавца.
        date_from (str): Дата начала периода в формате "YYYY-MM-DD".
        date_to (str): Дата окончания периода в формате "YYYY-MM-DD".

    Returns:
        list[dict]: Записи о платном хранении товаров. Основные поля:

        📅 **Даты и расчёты**
            - `date` — дата расчёта/перерасчёта
            - `originalDate` — дата первоначального расчёта (при перерасчёте)
            - `calcType` — способ расчёта
            - `tariffFixDate` — дата фиксации тарифа
            - `tariffLowerDate` — дата понижения тарифа

        📍 **Склады и коэффициенты**
            - `warehouse` — название склада
            - `officeId` — ID склада
            - `warehouseCoef` — коэффициент склада
            - `logWarehouseCoef` — коэффициент логистики и хранения

        🏷 **Товар и идентификаторы**
            - `nmId` — артикул Wildberries
            - `vendorCode` — артикул продавца
            - `chrtId` — ID размера
            - `barcode` — штрихкод
            - `size` — размер товара
            - `brand`, `subject` — бренд и предмет
            - `giId` — ID поставки

        📊 **Объёмы и количество**
            - `volume` — объём товара
            - `barcodesCount` — количество единиц товара
            - `palletCount` — количество паллет
            - `palletPlaceCode` — код паллетоместа

        💰 **Стоимость и скидки**
            - `warehousePrice` — сумма хранения
            - `loyaltyDiscount` — скидка программы лояльности (рубли)
    """

    logger.info("--- [START] Fetching paid storage report with date pagination ---")
    all_report_data = []

    current_start = start_date
    while current_start <= end_date:
        # Определяем конец чанка - 7 дней вперед (8 дней включительно)
        chunk_end = min(end_date, current_start + timedelta(days=7))
        date_from_str = current_start.strftime("%Y-%m-%d")
        date_to_str = chunk_end.strftime("%Y-%m-%d")

        logger.info(f"Fetching paid storage for period {date_from_str} to {date_to_str}...")

        # Выполняем один цикл получения отчета для чанка
        report_chunk = await _get_single_paid_storage_chunk(api_key, date_from_str, date_to_str)

        if report_chunk is None:
            logger.error(f"Failed to fetch paid storage chunk for {date_from_str}-{date_to_str}. Aborting.")
            return None  # Критическая ошибка в одном из чанков - прерываем все

        all_report_data.extend(report_chunk)

        # Переходим к следующему чанку и делаем паузу
        current_start = chunk_end + timedelta(days=1)
        if current_start <= end_date:
            logger.info("Waiting 61 seconds before next paid storage request due to API limits...")
            await asyncio.sleep(61)

    logger.info(f"--- [SUCCESS] Paid storage report fully downloaded. Total records: {len(all_report_data)} ---")
    return all_report_data


async def _get_single_paid_storage_chunk(api_key: str, date_from: str, date_to: str) -> List[Dict[str, Any]] | None:
    """Внутренняя функция для получения одного чанка отчета по хранению."""
    # Код из старой get_wb_paid_storage_report, адаптированный
    headers = {"Authorization": api_key}
    base_url = "https://seller-analytics-api.wildberries.ru/api/v1/paid_storage"
    async with aiohttp.ClientSession() as session:
        params = {"dateFrom": date_from, "dateTo": date_to}
        status, data = await _fetch_with_simple_retry(session, base_url, headers, params, "Paid Storage Create")
        if status != 200 or not isinstance(data, dict):
            logger.error(f"Failed to create task for {date_from}-{date_to}: {status} - {data}")
            return None
        task_id = data.get("data", {}).get("taskId")
        if not task_id: return None

        status_url = f"{base_url}/tasks/{task_id}/status"
        max_wait_time, check_interval, wait_time = 300, 5, 0
        while wait_time < max_wait_time:
            await asyncio.sleep(check_interval)
            wait_time += check_interval
            try:
                async with session.get(status_url, headers=headers, timeout=10) as resp:
                    if resp.status == 200:
                        status_data = await resp.json()
                        task_status = status_data.get("data", {}).get("status")
                        if task_status == "done":
                            download_url = f"{base_url}/tasks/{task_id}/download"
                            async with session.get(download_url, headers=headers, timeout=60) as dl_resp:
                                if dl_resp.status == 200:
                                    return await dl_resp.json()
                                else:
                                    return None
                        elif task_status in ["error", "canceled", "purged"]:
                            return None
            except Exception:
                pass
        return None  # Timeout


# ========================================
# ЕЖЕНЕДЕЛЬНЫЕ ОТЧЁТЫ
# ========================================

async def get_wb_weekly_report(api_key: str, date_from: str, date_to: str, period: str = "weekly") -> list:
    """
    Получает детализированный отчёт через /api/v5/supplier/reportDetailByPeriod.
    Поддерживает пагинацию через rrdid и выбор периода (weekly/daily).
    Args:
        api_key (str): API-ключ продавца.
        date_from (str): Дата начала периода в формате "YYYY-MM-DD".
        date_to (str): Дата окончания периода в формате "YYYY-MM-DD".

    Returns:
        list[dict]: Детализированные строки отчёта. Основные поля:

        📦 **Товар и операция**
            - `rr_dt` — дата операции
            - `doc_type_name` — тип документа (продажа, возврат и т.д.)
            - `nm_id`, `brand_name`, `subject_name`, `sa_name`, `barcode` — идентификация товара
            - `quantity` — количество
            - `retail_price`, `retail_price_withdisc_rub`, `retail_amount` — цены и суммы
            - `sale_percent` — скидка, %

        🚚 **Логистика и комиссии**
            - `delivery_rub`, `rebill_logistic_cost`, `storage_fee`, `acceptance`, `deduction` — логистические расходы
            - `penalty` — штрафы
            - `bonus_type_name` — вид корректировки
            - `srv_dbs` — платная доставка (bool)

        💳 **Финансы и выплаты**
            - `ppvz_for_pay` — к перечислению продавцу
            - `ppvz_sales_commission`, `ppvz_reward`, `ppvz_vw`, `ppvz_vw_nds` — комиссии и вознаграждения WB
            - `commission_percent`, `ppvz_kvw_prc`, `ppvz_spp_prc` — процент КВВ и СПП
            - `acquiring_fee`, `acquiring_percent`, `payment_processing`, `acquiring_bank` — эквайринг

        💰 **Скидки и корректировки**
            - `additional_payment` — корректировка вознаграждения
            - `deduction` — удержания (в т.ч. реклама)
            - `cashback_amount`, `cashback_discount`, `cashback_commission_change` — лояльность/кэшбэк
            - `installment_cofinancing_amount`, `supplier_promo`, `product_discount_for_report` — промо-скидки
            - `rebill_logistic_org` — организатор перевозки

        🏷 **Прочее**
            - `office_name`, `ppvz_office_name` — склады и офисы
            - `supplier_oper_name` — обоснование для оплаты
            - `srid`, `order_uid` — идентификаторы заказов
            - `is_legal_entity` — признак B2B-продажи
    """


async def get_wb_weekly_report(
        api_key: str,
        start_date: datetime,  # <-- Меняем тип на datetime
        end_date: datetime,  # <-- Меняем тип на datetime
        period: str = "weekly"
) -> list | None:
    """
    Получает детализированный отчёт с пагинацией по дате (чанками по 30 дней)
    для надежной работы с большими периодами.
    """
    logger.info(f"--- [START] Fetching '{period}' report with date pagination ---")
    all_report_data = []

    current_start = start_date
    while current_start <= end_date:
        chunk_end = min(end_date, current_start + timedelta(days=6))
        date_from_str = current_start.strftime("%Y-%m-%d")
        date_to_str = chunk_end.strftime("%Y-%m-%d")

        logger.info(f"Fetching '{period}' report for period {date_from_str} to {date_to_str}...")

        # Выполняем один цикл получения отчета для чанка
        report_chunk = await _get_single_report_detail_chunk(api_key, date_from_str, date_to_str, period)

        if report_chunk is None:
            logger.error(f"Failed to fetch '{period}' report chunk for {date_from_str}-{date_to_str}. Aborting.")
            return None

        all_report_data.extend(report_chunk)

        current_start = chunk_end + timedelta(days=1)
        # Для этого API пауза между чанками не нужна, т.к. внутренняя пагинация уже делает паузы

    logger.info(f"--- [SUCCESS] '{period}' report fully downloaded. Total records: {len(all_report_data)} ---")
    return all_report_data


async def _get_single_report_detail_chunk(api_key: str, date_from: str, date_to: str, period: str) -> list | None:
    """Внутренняя функция для получения одного чанка отчета детализации с пагинацией по rrdid."""
    url = "https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod"
    headers = {"Authorization": api_key}
    all_data, rrdid = [], 0
    async with aiohttp.ClientSession() as session:
        while True:
            params = {"dateFrom": date_from, "dateTo": date_to, "limit": 100000, "rrdid": rrdid, "period": period}

            # --- УЛУЧШЕННАЯ ОБРАБОТКА РЕЗУЛЬТАТА ---
            status, data_or_text = await _fetch_with_simple_retry(session, url, headers, params,
                                                                  f"Report Detail '{period}'")

            # Явно проверяем на None, что означает полный провал после всех ретраев
            if status is None:
                logger.error(f"Не удалось получить данные для отчета '{period}' после всех попыток.")
                return None  # Критическая ошибка, прерываем получение чанка

            if status == 200 and isinstance(data_or_text, list):
                data = data_or_text
                if not data: break
                all_data.extend(data)
                if not (rrd_id := data[-1].get("rrd_id")): break
                rrdid = rrd_id
                await asyncio.sleep(1)
            else:
                logger.error(f"Не удалось получить страницу rrdid для отчета '{period}': статус {status}")
                return None
    return all_data

# ========================================
# ОСТАЛЬНЫЕ ФУНКЦИИ
# ========================================

async def get_supplier_name(api_key: str) -> str:
    """
    Получает название магазина из Wildberries API через /api/v1/seller-info.
    Использует tradeMark, если доступен, иначе name.
    """
    url = "https://common-api.wildberries.ru/api/v1/seller-info"
    headers = {"Authorization": api_key}

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, headers=headers, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    seller_info = data.get("data", {})
                    logger.info(f"Полученные данные продавца: {data}")
                    trade_mark = seller_info.get("tradeMark")
                    legal_name = data.get("name", "")

                    return legal_name.strip()
                else:
                    logger.warning(
                        f"Не удалось получить seller-info: статус {resp.status}")
                    return "Магазин"
        except Exception as e:
            logger.error(f"Ошибка при получении названия магазина: {e}")
            return "Магазин"
