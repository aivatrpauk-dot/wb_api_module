import asyncio
import logging
import json
from datetime import datetime, timedelta
from typing import List, Dict, Set, Tuple
from collections import defaultdict

import aiohttp

logger = logging.getLogger(__name__)

# --- Константы ---
ADVERT_BASE_URL = "https://advert-api.wildberries.ru"
MAX_RETRIES = 3
REQUEST_DELAY = 1  # Пауза между "быстрыми" запросами
FULLSTATS_REQUEST_DELAY = 21
RETRY_DELAY = 61
DEBUG_LOG_JSON = True  # Включить/выключить сохранение JSON-ответов


# --- Отладочная функция ---
def _save_debug_json(filename: str, data: dict | list):
    """Сохраняет JSON-ответ в файл для отладки."""
    if not DEBUG_LOG_JSON: return
    try:
        with open(f"debug_logs/{filename}", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Failed to save debug JSON {filename}: {e}")


# --- Вспомогательная функция (без изменений) ---
async def _make_advert_request(session: aiohttp.ClientSession, method: str, url: str,
                               **kwargs) -> aiohttp.ClientResponse | None:
    for attempt in range(MAX_RETRIES):
        try:
            response = await session.request(method, url, **kwargs)
            if response.status in [200, 204]: return response
            if response.status == 429 or response.status >= 500:
                wait_time = RETRY_DELAY * (2 ** attempt)
                logger.warning(
                    f"Advert API Error ({response.status}) for {url}. Attempt {attempt + 1}/{MAX_RETRIES}. Retrying in {wait_time} sec...")
                await asyncio.sleep(wait_time)
                continue
            else:
                logger.error(f"Client Advert API Error for {url}: {response.status} - {await response.text()}")
                return None
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            wait_time = 10 * (2 ** attempt)
            logger.error(
                f"Network Error for {url}: {e}. Attempt {attempt + 1}/{MAX_RETRIES}. Retrying in {wait_time} sec...")
            await asyncio.sleep(wait_time)
    logger.error(f"Failed to execute request for {url} after {MAX_RETRIES} retries.")
    return None


# --- ИСПРАВЛЕННЫЕ Основные функции ---

async def _get_relevant_campaign_ids(
        session: aiohttp.ClientSession,
        api_key: str,
        target_nm_ids: Set[int],
        start_date: datetime  # <-- Добавляем start_date для фильтрации по endTime
) -> List[int]:
    """
    Реализует ПРАВИЛЬНУЮ многоступенчатую логику фильтрации кампаний
    с корректным парсингом nmId и фильтрацией по дате завершения.
    """
    # ... (код для Шага 2.1: получение всех активных ID остается тем же)
    headers = {"Authorization": api_key}
    url_count = f"{ADVERT_BASE_URL}/adv/v1/promotion/count"
    response_count = await _make_advert_request(session, "GET", url_count, headers=headers, timeout=30)
    if not response_count or response_count.status != 200: return []
    campaign_groups = await response_count.json()
    _save_debug_json("1_promotion_count.json", campaign_groups)
    all_active_ids = {advert.get('advertId') for camp_group in campaign_groups.get("adverts", []) if
                      camp_group.get('status') in {7, 9, 11} for advert in camp_group.get('advert_list', []) if
                      advert.get('advertId')}
    if not all_active_ids:
        logger.info("No active campaigns found in promotion/count response.")
        return []
    logger.info(f"Found {len(all_active_ids)} total active campaigns. Fetching details...")

    # Шаг 2.2: Получаем детали кампаний
    url_details = f"{ADVERT_BASE_URL}/adv/v1/promotion/adverts"
    all_active_ids_list = list(all_active_ids)
    campaign_details = []
    chunk_size = 50
    for i in range(0, len(all_active_ids_list), chunk_size):
        id_chunk = all_active_ids_list[i:i + chunk_size]
        response_details = await _make_advert_request(session, "POST", url_details, headers=headers, json=id_chunk,
                                                      timeout=60)
        if response_details and response_details.status == 200:
            campaign_details.extend(await response_details.json())
        await asyncio.sleep(REQUEST_DELAY)
    _save_debug_json("2_promotion_adverts_details.json", campaign_details)

    # Шаг 2.3: Фильтруем кампании (ИСПРАВЛЕННАЯ ЛОГИКА)
    final_relevant_ids = set()
    start_date_naive = start_date.date()  # Для сравнения

    for campaign in campaign_details:
        # Фильтр по дате завершения
        end_time_str = campaign.get("endTime")
        if end_time_str:
            try:
                end_date_naive = datetime.fromisoformat(end_time_str).date()
                if end_date_naive < start_date_naive:
                    continue  # Пропускаем кампанию, она закончилась до нашего периода
            except (ValueError, TypeError):
                pass  # Если дата некорректна, не можем отфильтровать, оставляем

        # Корректный парсинг nmId для РАЗНЫХ типов кампаний
        advert_nm_ids = set()
        if params := campaign.get("unitedParams"):  # Тип 9 (Поиск + Каталог)
            for param in params:
                advert_nm_ids.update(param.get("nms", []))
        elif params := campaign.get("autoParams"):  # Тип 8 (Авто)
            advert_nm_ids.update(params.get("nms", []))

        # Фильтр по пересечению nmId
        if not advert_nm_ids.isdisjoint(target_nm_ids):
            if advert_id := campaign.get('advertId'):
                final_relevant_ids.add(advert_id)

    logger.info(f"Found {len(final_relevant_ids)} campaigns to fetch stats for (filtered by nmId and endTime).")
    return list(final_relevant_ids)


async def _get_fullstats_data(session: aiohttp.ClientSession, api_key: str, campaign_ids: List[int],
                              start_date: datetime, end_date: datetime) -> List[Dict]:
    """
    Основной цикл сбора статистики с корректной обработкой ошибки 400.
    """
    url = f"{ADVERT_BASE_URL}/adv/v3/fullstats"
    headers = {"Authorization": api_key}
    all_stats_raw = []
    current_start_date = start_date
    while current_start_date <= end_date:
        current_end_date = min(end_date, current_start_date + timedelta(days=29))
        logger.info(
            f"Fetching fullstats for period {current_start_date.date()} to {current_end_date.date()} for {len(campaign_ids)} campaigns.")
        id_chunk_size = 100
        for i in range(0, len(campaign_ids), id_chunk_size):
            id_chunk = campaign_ids[i:i + id_chunk_size]
            params = {"ids": ",".join(map(str, id_chunk)), "beginDate": current_start_date.strftime("%Y-%m-%d"),
                      "endDate": current_end_date.strftime("%Y-%m-%d")}

            response = await _make_advert_request(session, "GET", url, headers=headers, params=params, timeout=120)

            # --- Обрабатываем специфичную ошибку 400 ---
            if response and response.status == 200:
                stats_data = await response.json()
                _save_debug_json(f"3_fullstats_chunk_{i}.json", stats_data)
                if stats_data: all_stats_raw.extend(stats_data)
            elif response and response.status == 400:
                error_text = await response.text()
                if "there are no statistics for this advertising period" in error_text:
                    logger.info(f"Received 400 'no statistics' for chunk {i}, considering it an empty response.")
                    _save_debug_json(f"3_fullstats_chunk_{i}_400_ignored.json", {"error": error_text})
                else:
                    # Если это другая ошибка 400, логируем ее как обычно
                    logger.error(f"Client Advert API Error 400 for {url}: {error_text}")

            is_last_chunk = (i + id_chunk_size >= len(campaign_ids)) and (current_end_date >= end_date)
            if not is_last_chunk:
                logger.info(f"Waiting for {FULLSTATS_REQUEST_DELAY} seconds before next fullstats request...")
                await asyncio.sleep(FULLSTATS_REQUEST_DELAY)
        current_start_date += timedelta(days=30)
    return all_stats_raw

# --- Главная публичная функция  ---
async def get_aggregated_ad_costs(api_key: str, start_date: datetime, end_date: datetime, target_nm_ids: Set[int]) -> \
Dict[Tuple[str, int], float]:
    """
    Оркестрирует процесс сбора расходов на рекламу с ИСПРАВЛЕННОЙ логикой парсинга.
    """
    logger.info("--- [START] Fetching advertising costs (Corrected Logic v3) ---")

    if not target_nm_ids:
        logger.info("--- [FINISH] No target nmIds provided. Skipping ad costs fetch. ---")
        return {}

    ad_costs_agg = defaultdict(float)
    async with aiohttp.ClientSession() as session:
        campaign_ids = await _get_relevant_campaign_ids(session, api_key, target_nm_ids, start_date)
        if not campaign_ids:
            logger.info("--- [FINISH] No relevant campaigns found for the given nmIds and period. ---")
            return {}

        fullstats_data = await _get_fullstats_data(session, api_key, campaign_ids, start_date, end_date)
        if not fullstats_data:
            logger.info("--- [FINISH] No fullstats data received. ---")
            return {}

    # ---  ЛОГИКА ПАРСИНГА ---
    logger.info(f"Processing {len(fullstats_data)} raw stats entries...")
    for campaign_stat in fullstats_data:
        # Итерируемся по дням
        for day_stat in campaign_stat.get("days", []):
            # Получаем дату и обрезаем время "T00:00:00Z"
            date_str_raw = day_stat.get("date")
            if not date_str_raw: continue
            date_str = date_str_raw.split("T")[0]

            # Итерируемся по типам приложений (apps) внутри дня
            for app_stat in day_stat.get("apps", []):
                # Итерируемся по артикулам (nms) внутри приложения
                for nm_stat in app_stat.get("nms", []):
                    nm_id = nm_stat.get("nmId")
                    cost = nm_stat.get("sum", 0)

                    # Проверяем, что nm_id есть и он нам нужен
                    if not nm_id or nm_id not in target_nm_ids: continue

                    key = (date_str, nm_id)
                    ad_costs_agg[key] += cost

    logger.info(f"--- [SUCCESS] Advertising costs aggregated for {len(ad_costs_agg)} (date, nmId) pairs. ---")
    return dict(ad_costs_agg)
