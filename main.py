import logging
import time
import json
import os
import re
from typing import Optional, List, Tuple
import random 

# Импорты для Cloudscraper
import cloudscraper
from bs4 import BeautifulSoup

# Импорты для Selenium
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

# --- Импорт для undetected_chromedriver ---
import undetected_chromedriver as uc

# --- КОНФИГУРАЦИЯ ЛОГИРОВАНИЯ ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
# Для отладки raw HTML установите уровень DEBUG:
# logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s")

# --- КОНФИГУРАЦИЯ ПАРСЕРА ---
MAX_RETRIES = 3
BASE_DELAY = 1.0
SCRAPER_TIMEOUT = 30 # Увеличиваем общий таймаут для запросов

# --- ИНИЦИАЛИЗАЦИЯ CLOUDSCRAPER ---
scraper = cloudscraper.create_scraper(
    browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False},
    delay=10
)

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ПАРСИНГА ---

def _extract_content_and_images(soup: BeautifulSoup, article_url: str) -> Tuple[Optional[str], List[str]]:
    """
    Вспомогательная функция для извлечения текста и изображений из объекта BeautifulSoup.
    Используется обоими методами (Cloudscraper и Selenium).
    """
    full_text_parts = []
    image_urls = []

    # Ищем основной контент-блок
    content_div = soup.select_one('div#fck_detail, article.fck_detail, div.detail-content, div.main_content_detail')

    # Дополнительные селекторы для фото-историй или других типов статей, если основной не найден
    if not content_div:
        content_div = soup.select_one('div.box_category_show, div.wrapper_detail_photo_story')

    if content_div:
        logging.debug(f"Found main/photo content div for {article_url}")
        # Извлечение всех параграфов и элементов, содержащих текст
        # Используем find_all(['p', 'h2', 'h3', 'li', 'div']) для более широкого охвата текстовых элементов
        for p_tag in content_div.find_all(['p', 'h2', 'h3', 'li', 'div'], recursive=True):
            # Пропускаем параграфы, содержащие только жирный текст (обычно это подписи к фото)
            if p_tag.name == 'p' and p_tag.find('strong') and p_tag.get_text(strip=True) == p_tag.find('strong').get_text(strip=True):
                continue
            # Избегаем дублирования, если родительский div уже является основным контентом
            if p_tag.name == 'div' and ('detail-content' in p_tag.get('class', []) or 'fck_detail' in p_tag.get('class', [])):
                continue
            
            text = p_tag.get_text(separator=' ', strip=True)
            # Фильтруем подписи к фото/видео, которые могут быть ошибочно захвачены
            if text and not text.lower().startswith("photo:") and not text.lower().startswith("video:"):
                full_text_parts.append(text)

        # --- Извлечение изображений ---
        image_tags = content_div.find_all('img') if content_div else []
        # Дополнительные селекторы для изображений, если не нашлись в content_div
        if not image_tags: 
             image_tags = soup.select('div.item_photo_detail img, .vne_lazy_image, .thumb_art img, picture img')
        
        for img_tag in image_tags:
            src = img_tag.get('data-src') or img_tag.get('src')
            if src and src.startswith(('http', '//')):
                if src.startswith('//'):
                    src = 'https:' + src
                # Удаляем параметры размера и хеш после '?', чтобы получить базовый URL
                if "?" in src:
                    src = src.split('?')[0]
                image_urls.append(src)
        
        # Удаляем дубликаты URL изображений
        image_urls = list(dict.fromkeys(image_urls))
        full_text = "\n\n".join(full_text_parts)
        
    else:
        logging.warning(f"Could not find any suitable content div in parsed HTML for {article_url}")
        full_text = ""

    return full_text, image_urls

# --- МЕТОДЫ ПАРСИНГА ---

def parse_with_cloudscraper(article_url: str) -> Tuple[Optional[str], List[str]]:
    """Попытка извлечь статью с помощью Cloudscraper."""
    logging.info(f"Attempting to fetch with Cloudscraper: {article_url}")
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = scraper.get(article_url, timeout=SCRAPER_TIMEOUT)
            r.raise_for_status()
            
            # Для отладки: если уровень логирования DEBUG, выводим часть HTML
            if logging.root.level <= logging.DEBUG:
                logging.debug(f"Cloudscraper Raw HTML for {article_url} (first 5000 chars):\n{r.text[:5000]}...")
            
            soup = BeautifulSoup(r.text, 'html.parser')
            full_text, image_urls = _extract_content_and_images(soup, article_url)

            if full_text.strip() or image_urls:
                logging.info(f"Cloudscraper SUCCESS for {article_url}")
                return full_text, image_urls
            else:
                logging.warning(f"Cloudscraper extracted empty content for {article_url}")
                return None, [] 

        except Exception as e:
            delay = BASE_DELAY * 2 ** (attempt - 1)
            logging.warning(
                "Cloudscraper failed fetching %s (try %s/%s): %s; retrying in %.1fs",
                article_url, attempt, MAX_RETRIES, e, delay
            )
            time.sleep(delay)
    logging.error(f"Cloudscraper failed after {MAX_RETRIES} attempts for {article_url}")
    return None, []

def parse_with_selenium(article_url: str) -> Tuple[Optional[str], List[str]]:
    """Попытка извлечь статью с помощью Selenium с undetected_chromedriver."""
    logging.info(f"Attempting to fetch with Selenium (undetected_chromedriver): {article_url}")
    driver = None
    try:
        local_chrome_options_uc = Options()
        local_chrome_options_uc.add_argument("--no-sandbox")
        local_chrome_options_uc.add_argument("--disable-dev-shm-usage")
        local_chrome_options_uc.add_argument("--disable-gpu")
        local_chrome_options_uc.add_argument("--window-size=1920,1080")
        local_chrome_options_uc.add_argument("--incognito")
        
        # Добавляем User-Agent, соответствующий Chrome 138 (можете обновить, если версия Chrome изменится)
        local_chrome_options_uc.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36")
        
        # Остальные опции
        local_chrome_options_uc.add_argument("--disable-extensions")
        local_chrome_options_uc.add_argument("--hide-scrollbars")
        local_chrome_options_uc.add_argument("--mute-audio")
        local_chrome_options_uc.add_argument("--no-default-browser-check")
        local_chrome_options_uc.add_argument("--no-first-run")
        local_chrome_options_uc.add_argument("--disable-infobars")
        # --- КОНЕЦ ИНИЦИАЛИЗАЦИИ CHROMEOPTIONS ---

        # headless=True: запускает браузер в безголовом режиме (без GUI)
        # use_subprocess=True: может помочь в CI окружениях
        # options=local_chrome_options_uc: передаем наш свежий объект опций
        # driver_executable_path: Указываем путь к ChromeDriver, который мы скачали вручную.
        driver = uc.Chrome(
            headless=True,
            use_subprocess=True,
            options=local_chrome_options_uc,
            driver_executable_path="/usr/local/bin/chromedriver" 
        ) 

        # Установка таймаутов для Selenium
        driver.set_page_load_timeout(90) # УВЕЛИЧЕНО до 90 секунд для загрузки страницы
        driver.set_script_timeout(30)    # Максимальное время для выполнения асинхронных скриптов

        driver.get(article_url)

        # Ждем, пока основной контентный div станет видимым/присутствующим на странице
        WebDriverWait(driver, 90).until( # УВЕЛИЧЕНО до 90 секунд для ожидания элемента
            EC.presence_of_element_located((By.ID, "fck_detail"))
        )

        page_source = driver.page_source
        
        # Для отладки: если уровень логирования DEBUG, выводим часть HTML
        if logging.root.level <= logging.DEBUG:
            logging.debug(f"Selenium Raw HTML for {article_url} (first 5000 chars):\n{page_source[:5000]}...")

        soup = BeautifulSoup(page_source, 'html.parser')
        full_text, image_urls = _extract_content_and_images(soup, article_url)

        if full_text.strip() or image_urls:
            logging.info(f"Selenium SUCCESS for {article_url}")
            return full_text, image_urls
        else:
            logging.warning(f"Selenium extracted empty content for {article_url}")
            return None, []

    except TimeoutException:
        logging.error(f"Selenium Timeout: element #fck_detail not found or page load timed out for {article_url}. This might indicate strong bot detection.")
        return None, []
    except WebDriverException as e:
        logging.error(f"Selenium WebDriver Error for {article_url}: {e}", exc_info=True)
        return None, []
    except Exception as e:
        logging.error(f"Selenium failed for {article_url}: {e}", exc_info=True)
        return None, []
    finally:
        if driver:
            driver.quit() # Важно закрывать браузер после использования

# --- ОСНОВНАЯ ФУНКЦИЯ ДЛЯ ЗАПУСКА ПАРСИНГА ---

def get_article_content(article_url: str) -> Tuple[Optional[str], List[str]]:
    """
    Пытается получить контент статьи, сначала используя Cloudscraper,
    затем, если не удалось, переключается на Selenium.
    """
    
    # Попытка с Cloudscraper
    text, images = parse_with_cloudscraper(article_url)
    if text or images:
        return text, images
    
    # Если Cloudscraper не удался, попытка с Selenium
    logging.info("Cloudscraper failed, attempting with Selenium...")
    text, images = parse_with_selenium(article_url)
    if text or images:
        return text, images
    
    logging.error(f"Failed to get content for {article_url} with all methods.")
    return None, []

# --- ГЛАВНАЯ ЛОГИКА (Если запускается как основной скрипт) ---
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Parse articles from an RSS feed.")
    parser.add_argument("--rss-url", type=str, required=True, help="URL of the RSS feed to parse.")
    parser.add_argument("-l", "--lang", type=str, default="en", help="Language code (e.g., 'ru', 'en').")
    parser.add_argument("-n", "--num-articles", type=int, default=5, help="Number of articles to process from the RSS feed.")
    parser.add_argument("--limit", type=int, default=30, help="Maximum number of articles to process per run.")
    parser.add_argument("--posted-state-file", type=str, default="articles/posted.json", help="Path to the JSON file storing IDs of already posted articles.")

    args = parser.parse_args()

    RSS_URL = args.rss_url
    TARGET_LANG = args.lang
    NUM_ARTICLES_FROM_RSS = args.num_articles
    BATCH_LIMIT = args.limit
    POSTED_STATE_FILE = args.posted_state_file

    def get_posted_article_ids(file_path: str) -> set:
        if not os.path.exists(file_path):
            return set()
        with open(file_path, 'r', encoding='utf-8') as f:
            try:
                return set(json.load(f))
            except json.JSONDecodeError:
                logging.warning(f"Error decoding {file_path}. Starting with empty posted set.")
                return set()

    def add_posted_article_id(file_path: str, article_id: str):
        posted_ids = get_posted_article_ids(file_path)
        posted_ids.add(article_id)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(list(posted_ids), f, indent=4)

    posted_ids = get_posted_article_ids(POSTED_STATE_FILE)
    new_articles_found_status = False
    processed_count = 0

    logging.info(f"Starting RSS feed processing for: {RSS_URL}")
    logging.info(f"Already posted articles count: {len(posted_ids)}")
    
    # --- Проверка внешнего IP в логах ---
    import requests
    logging.info("Checking external IP...")
    try:
        ip_response = requests.get('http://ip-api.com/json', timeout=10)
        ip_response.raise_for_status()
        ip_data = ip_response.json()
        logging.info(f"Current external IP: {ip_data.get('query')}, Country: {ip_data.get('country')}")
    except Exception as e:
        logging.warning(f"Could not check external IP: {e}")
    # --- КОНЕЦ ПРОВЕРКИ IP ---


    try:
        # Используем Cloudscraper для получения RSS-ленты, т.к. там нет JS
        rss_response = scraper.get(RSS_URL, timeout=SCRAPER_TIMEOUT)
        rss_response.raise_for_status()
        rss_soup = BeautifulSoup(rss_response.text, 'xml') # RSS - это XML

        items = rss_soup.find_all('item')
        logging.info(f"Found {len(items)} items in RSS feed.")

        for item in items:
            if processed_count >= BATCH_LIMIT:
                logging.info(f"Batch limit of {BATCH_LIMIT} reached. Stopping processing new articles from RSS.")
                break

            link = item.find('link').text
            title = item.find('title').text
            pub_date_str = item.find('pubDate').text
            
            # Извлекаем ID из URL
            match = re.search(r'-(\d+)\.html', link)
            article_id = match.group(1) if match else link # Используем ссылку как ID, если не можем извлечь числовой ID

            if article_id in posted_ids:
                logging.info(f"Article '{title}' (ID: {article_id}) already posted. Skipping.")
                continue

            logging.info(f"New article found: '{title}' (ID: {article_id})")
            
            # --- Основной вызов функции парсинга ---
            article_text, article_images = get_article_content(link)

            if article_text and article_text.strip():
                # Сохранение статьи в JSON
                output_dir = "articles"
                os.makedirs(output_dir, exist_ok=True)
                
                # Добавляем штамп времени для уникальности файла
                timestamp = int(time.time())
                output_filename = os.path.join(output_dir, f"{article_id}_{timestamp}.json")
                
                article_data = {
                    "id": article_id,
                    "title": title,
                    "url": link,
                    "pubDate": pub_date_str,
                    "text": article_text,
                    "images": article_images
                }
                
                with open(output_filename, 'w', encoding='utf-8') as f:
                    json.dump(article_data, f, ensure_ascii=False, indent=4)
                
                logging.info(f"Article '{title}' saved to {output_filename}")
                new_articles_found_status = True
                add_posted_article_id(POSTED_STATE_FILE, article_id)
                processed_count += 1
                # Добавляем случайную задержку между обработкой статей для имитации человеческого поведения
                time.sleep(BASE_DELAY + random.uniform(2.0, 5.0)) 

            else:
                logging.warning(f"Could not extract content for article '{title}' (ID: {article_id}). Skipping.")

    except Exception as e:
        logging.error(f"Error during RSS feed processing: {e}", exc_info=True)

    # Вывод статуса для GitHub Actions
    print(f"NEW_ARTICLES_STATUS:{'true' if new_articles_found_status else 'false'}")
    logging.info("→ PARSER RUN COMPLETE")
