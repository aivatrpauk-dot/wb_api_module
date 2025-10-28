import asyncio
from typing import List
from datetime import datetime
import aiohttp
import logging

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

def _is_within_date_range(record: dict, start_dt: datetime, end_dt: datetime) -> bool:
    """Проверяет, находится ли lastChangeDate записи в заданном диапазоне."""
    last_change_str = record.get("date")
    # last_change_str = record.get("lastChangeDate")
    if not last_change_str:
        return False
    try:
        last_change_dt = datetime.fromisoformat(
            last_change_str.replace("Z", "+00:00"))
        # and record.get("isCancel", False) == False
        return start_dt <= last_change_dt <= end_dt
    except ValueError:
        logger.warning(f"Некорректный формат даты в записи: {last_change_str}")
        return False


async def _fetch_with_simple_retry(
    session: aiohttp.ClientSession,
    url: str,
    headers: dict,
    params: dict,
    method_name: str,
) -> tuple[int, list | str]:
    """Выполняет запрос с простым повтором при 429 ошибке."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(url, headers=headers, params=params, timeout=20) as resp:
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
                    return resp.status, await resp.text()
        except Exception as e:
            logger.error(f"{method_name}: исключение (попытка {attempt}): {e}")
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY)
                continue
            else:
                raise
    raise RuntimeError("Недостижимо")


async def get_wb_orders(api_key: str, date_from: str, date_to: str) -> List[dict]:
    url = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"
    headers = {"Authorization": api_key}
    all_orders = []

    start_dt = datetime.fromisoformat(f"{date_from}T00:00:00")
    end_dt = datetime.fromisoformat(f"{date_to}T23:59:59")
    current_date_from = f"{date_from}T00:00:00"

    async with aiohttp.ClientSession() as session:
        while True:
            params = {"dateFrom": current_date_from, "flag": 0}

            status, data_or_text = await _fetch_with_simple_retry(
                session, url, headers, params, "Orders API"
            )

            if status == 200:
                data = data_or_text
                if not data:
                    break
                all_orders.extend(data)

                last_change_date = data[-1].get("lastChangeDate")
                if not last_change_date:
                    logger.warning(
                        "Отсутствует lastChangeDate в последней записи. Прерывание.")
                    break
                current_date_from = last_change_date

                # Проверка выхода за верхнюю границу
                try:
                    last_dt = datetime.fromisoformat(
                        last_change_date.replace("Z", "+00:00"))
                    if last_dt > end_dt:
                        break
                except ValueError:
                    pass  # игнорируем, если не распарсилось

            else:
                logger.error(f"Orders API ошибка: {status} — {data_or_text}")
                break

    return [r for r in all_orders if _is_within_date_range(r, start_dt, end_dt)]


async def get_wb_sales(api_key: str, date_from: str, date_to: str) -> List[dict]:
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


async def _fetch_with_simple_retry_get(
    session: aiohttp.ClientSession,
    url: str,
    headers: dict,
    params: dict,
    method_name: str,
) -> tuple[int, Any]:
    """GET-запрос с повторами при 429."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(url, headers=headers, params=params, timeout=20) as resp:
                if resp.status == 200:
                    return 200, await resp.json()
                elif resp.status == 429:
                    logger.warning(
                        f"{method_name}: 429 Too Many Requests (попытка {attempt}/{MAX_RETRIES})"
                    )
                    if attempt < MAX_RETRIES:
                        await asyncio.sleep(RETRY_DELAY)
                        continue
                    else:
                        return 429, await resp.text()
                else:
                    return resp.status, await resp.text()
        except Exception as e:
            logger.error(f"{method_name}: исключение (попытка {attempt}): {e}")
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY)
                continue
            else:
                raise
    raise RuntimeError("Недостижимо")


async def get_wb_acceptance_report(
    api_key: str,
    date_from: str,
    date_to: str,
) -> List[Dict[str, Any]]:
    """
    Получает отчёт о платной приёмке за указанный период.

    Args:
        api_key: API-ключ продавца.
        date_from: Дата начала в формате "YYYY-MM-DD".
        date_to: Дата окончания в формате "YYYY-MM-DD".

    Returns:
        Список записей из отчёта о платной приёмке.
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
        status, data = await _fetch_with_simple_retry_get(
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


async def get_wb_paid_storage_report(
    api_key: str,
    date_from: str,
    date_to: str,
) -> List[Dict[str, Any]]:
    """
    Получает отчёт о платном хранении за указанный период.

    Args:
        api_key: API-ключ продавца.
        date_from: Дата начала в формате "YYYY-MM-DD".
        date_to: Дата окончания в формате "YYYY-MM-DD".

    Returns:
        Список записей из отчёта о платном хранении.
    """
    headers = {"Authorization": api_key}
    start_dt = datetime.fromisoformat(f"{date_from}T00:00:00")
    end_dt = datetime.fromisoformat(f"{date_to}T23:59:59")

    async with aiohttp.ClientSession() as session:
        # 1. Создать задачу на формирование отчёта (GET с параметрами)
        params = {
            "dateFrom": date_from,
            "dateTo": date_to
        }
        status, data = await _fetch_with_simple_retry_get(
            session,
            PAID_STORAGE_BASE_URL,
            headers,
            params,
            "Paid Storage Report Create"
        )

        if status != 200:
            logger.error(
                f"Не удалось создать задачу на отчёт платного хранения: {status} — {data}")
            return []

        task_id = data.get("data", {}).get("taskId")
        if not task_id:
            logger.error("Ответ на создание задачи не содержит taskId")
            return []

        logger.info(f"Создана задача на отчёт платного хранения: {task_id}")

        # 2. Ожидание завершения задачи
        wait_time = 0
        while wait_time < PAID_STORAGE_MAX_WAIT_TIME:
            status_url = f"{PAID_STORAGE_BASE_URL}/tasks/{task_id}/status"
            try:
                async with session.get(status_url, headers=headers, timeout=10) as resp:
                    if resp.status == 200:
                        status_data = await resp.json()
                        task_status = status_data.get("data", {}).get("status")
                        if task_status == "done":
                            logger.info("Отчёт о платном хранении готов.")
                            break
                        elif task_status == "error":
                            logger.error(
                                f"Ошибка при генерации отчёта платного хранения: {status_data}")
                            return []
                    else:
                        logger.warning(
                            f"Неожиданный статус при проверке задачи: {resp.status}")
            except Exception as e:
                logger.error(
                    f"Ошибка при проверке статуса задачи платного хранения: {e}")

            await asyncio.sleep(PAID_STORAGE_STATUS_CHECK_INTERVAL)
            wait_time += PAID_STORAGE_STATUS_CHECK_INTERVAL
        else:
            logger.error(
                "Превышено время ожидания готовности отчёта о платном хранении")
            return []

        # 3. Скачать отчёт
        download_url = f"{PAID_STORAGE_BASE_URL}/tasks/{task_id}/download"
        try:
            async with session.get(download_url, headers=headers, timeout=30) as resp:
                if resp.status == 200:
                    report_data = await resp.json()
                    # Фильтрация по полю "date" (формат: "YYYY-MM-DD")
                    filtered = []
                    for record in report_data:
                        record_date_str = record.get("date")
                        if not record_date_str:
                            continue
                        try:
                            record_date = datetime.fromisoformat(
                                record_date_str)
                            if start_dt.date() <= record_date.date() <= end_dt.date():
                                filtered.append(record)
                        except ValueError:
                            logger.warning(
                                f"Некорректная дата в записи платного хранения: {record_date_str}")
                    return filtered
                else:
                    logger.error(f"Ошибка при скачивании отчёта платного хранения: {resp.status} — {await resp.text()}")
                    return []
        except Exception as e:
            logger.error(
                f"Исключение при скачивании отчёта платного хранения: {e}")
            return []


async def get_wb_weekly_report(api_key: str, date_from: str, date_to: str) -> list:
    """
    Получает еженедельный отчёт через /api/v5/supplier/reportDetailByPeriod
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
    
    url = "https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod"
    headers = {
        "Authorization": api_key,
        "Content-Type": "application/json"
    }
    params = {
        "dateFrom": date_from,
        "dateTo": date_to
    }

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, headers=headers, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    logger.info(
                        f"Получено {len(data)} записей из weekly-отчёта")
                    return data
                else:
                    logger.error(f"Ошибка API: {resp.status} — {await resp.text()}")
                    return []
        except Exception as e:
            logger.error(f"Ошибка при запросе weekly-отчёта: {e}")
            return []
