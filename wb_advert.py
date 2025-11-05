# --- START OF FILE wb_advert.py ---
import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Set, Tuple
from collections import defaultdict

import aiohttp

# Инициализация логгера для этого модуля
logger = logging.getLogger(__name__)

# --- Константы ---
ADVERT_BASE_URL = "https://advert-api.wildberries.ru"
MAX_RETRIES = 3
# Пауза между запросами к fullstats для соблюдения лимита ~3 запроса в минуту
FULLSTATS_REQUEST_DELAY = 21  # seconds
# Пауза при ошибке 429
RETRY_DELAY = 61  # seconds


# --- Вспомогательные функции ---

async def _make_advert_request(
        session: aiohttp.ClientSession,
        method: str,
        url: str,
        **kwargs
) -> aiohttp.ClientResponse | None:
    """
    Выполняет один запрос к Advert API с логикой повторных попыток (retry).
    Аналог _make_request_with_retry из референсного проекта.
    """
    for attempt in range(MAX_RETRIES):
        try:
            response = await session.request(method, url, **kwargs)

            # Успешные статусы
            if response.status in [200, 204]:
                return response

            # Ошибки, требующие повтора (лимиты, серверные сбои)
            if response.status == 429 or response.status >= 500:
                wait_time = RETRY_DELAY * (2 ** attempt)
                logger.warning(
                    f"Advert API Error ({response.status}) for {url}. "
                    f"Attempt {attempt + 1}/{MAX_RETRIES}. Retrying in {wait_time} sec..."
                )
                await asyncio.sleep(wait_time)
                continue  # Переходим к следующей попытке

            # Другие клиентские ошибки (400, 401, 403) - нет смысла повторять
            else:
                logger.error(f"Client Advert API Error for {url}: {response.status} - {await response.text()}")
                return None  # Возвращаем None, т.к. ошибка критическая

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            wait_time = 10 * (2 ** attempt)
            logger.error(
                f"Network Error for {url}: {e}. Attempt {attempt + 1}/{MAX_RETRIES}. Retrying in {wait_time} sec...")
            await asyncio.sleep(wait_time)

    logger.error(f"Failed to execute request for {url} after {MAX_RETRIES} retries.")
    return None


# --- Основные функции модуля ---

async def _get_active_campaign_ids(session: aiohttp.ClientSession, api_key: str) -> List[int]:
    """
    Шаг 2: Получает ID всех актуальных рекламных кампаний.
    Использует GET /adv/v1/promotion/count.
    """
    url = f"{ADVERT_BASE_URL}/adv/v1/promotion/count"
    headers = {"Authorization": api_key}

    response = await _make_advert_request(session, "GET", url, headers=headers, timeout=30)

    if not response or response.status != 200:
        return []

    campaign_groups = (await response.json()).get("adverts", [])
    if not campaign_groups:
        logger.info("No advertising campaigns found for this store.")
        return []

    relevant_ids = set()
    valid_statuses = {7, 9, 11}  # Готова к запуску, идет, приостановлена

    for camp_group in campaign_groups:
        if camp_group.get('status') in valid_statuses:
            list_to_iterate = camp_group.get('advert_list', [])
            for advert in list_to_iterate:
                if advert_id := advert.get('advertId'):
                    relevant_ids.add(advert_id)

    logger.info(f"Found {len(relevant_ids)} relevant campaigns.")
    return list(relevant_ids)


async def _get_fullstats_data(
        session: aiohttp.ClientSession,
        api_key: str,
        campaign_ids: List[int],
        start_date: datetime,
        end_date: datetime
) -> List[Dict]:
    """
    Шаг 3: Основной цикл сбора статистики.
    Итерируется по чанкам дат и ID кампаний, вызывая GET /adv/v3/fullstats.
    """
    url = f"{ADVERT_BASE_URL}/adv/v3/fullstats"
    headers = {"Authorization": api_key}
    all_stats_raw = []

    # Итерация по 30-дневным чанкам
    current_start_date = start_date
    while current_start_date <= end_date:
        current_end_date = min(end_date, current_start_date + timedelta(days=29))

        logger.info(f"Fetching fullstats for period {current_start_date.date()} to {current_end_date.date()}")

        # Пакетная обработка ID кампаний по 100 шт
        id_chunk_size = 100
        for i in range(0, len(campaign_ids), id_chunk_size):
            id_chunk = campaign_ids[i:i + id_chunk_size]

            params = {
                "ids": ",".join(map(str, id_chunk)),
                "beginDate": current_start_date.strftime("%Y-%m-%d"),
                "endDate": current_end_date.strftime("%Y-%m-%d")
            }

            response = await _make_advert_request(session, "GET", url, headers=headers, params=params, timeout=120)

            if response and response.status == 200:
                stats_data = await response.json()
                if stats_data:
                    all_stats_raw.extend(stats_data)

            # Пауза для соблюдения лимитов, если это не последний чанк
            is_last_chunk = (i + id_chunk_size >= len(campaign_ids)) and (current_end_date >= end_date)
            if not is_last_chunk:
                logger.info(f"Waiting for {FULLSTATS_REQUEST_DELAY} seconds before next fullstats request...")
                await asyncio.sleep(FULLSTATS_REQUEST_DELAY)

        current_start_date += timedelta(days=30)

    return all_stats_raw


# --- Главная публичная функция ---

async def get_aggregated_ad_costs(
        api_key: str,
        start_date: datetime,
        end_date: datetime
) -> Dict[Tuple[str, int], float]:
    """
    Оркестрирует полный процесс сбора и агрегации расходов на рекламу.
    Возвращает словарь {(date_str, nmId): total_cost}.
    """
    logger.info("--- [START] Fetching advertising costs ---")
    ad_costs_agg = defaultdict(float)

    async with aiohttp.ClientSession() as session:
        # Шаг 2: Получаем список ID активных кампаний
        campaign_ids = await _get_active_campaign_ids(session, api_key)
        if not campaign_ids:
            logger.info("--- [FINISH] No active campaigns to process. Returning empty ad costs. ---")
            return {}

        # Шаг 3: Получаем сырые данные статистики
        fullstats_data = await _get_fullstats_data(session, api_key, campaign_ids, start_date, end_date)
        if not fullstats_data:
            logger.info("--- [FINISH] No fullstats data received. Returning empty ad costs. ---")
            return {}

    # Шаг 4: Обработка и агрегация ответа
    logger.info(f"Processing {len(fullstats_data)} raw stats entries...")
    for campaign_stat in fullstats_data:
        # Итерируемся по дням внутри ответа кампании
        for day_stat in campaign_stat.get("days", []):
            date_str = day_stat.get("date")
            if not date_str: continue

            # Итерируемся по артикулам (nms) внутри дня
            for nm_stat in day_stat.get("nms", []):
                nm_id = nm_stat.get("nmId")
                cost = nm_stat.get("sum", 0)
                if not nm_id: continue

                # Ключ - кортеж (дата в формате YYYY-MM-DD, ID артикула)
                key = (date_str, nm_id)
                ad_costs_agg[key] += cost

    logger.info(f"--- [SUCCESS] Advertising costs aggregated for {len(ad_costs_agg)} (date, nmId) pairs. ---")
    return dict(ad_costs_agg)

# --- END OF FILE wb_advert.py ---