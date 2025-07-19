import logging
import time
from typing import Optional, List, Tuple
import cloudscraper
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

# --- КОНФИГУРАЦИЯ ЛОГИРОВАНИЯ ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
# Для отладки raw HTML установите уровень DEBUG:
# logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s")

# --- КОНФИГУРАЦИЯ ПАРСЕРА ---
MAX_RETRIES = 3
BASE_DELAY = 1.0
SCRAPER_TIMEOUT = 15

# --- КОНФИГУРАЦИЯ SELENIUM ---
# Укажите путь к вашему chromedriver.exe или geckodriver.exe
# Если драйвер находится в PATH, можете установить None
CHROME_DRIVER_PATH = None  # Например: 'C:/path/to/chromedriver.exe' или '/usr/local/bin/chromedriver'

chrome_options = Options()
chrome_options.add_argument("--headless")  # Запуск в безголовом режиме (без графического интерфейса)
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("--incognito")
# Отключить логи Selenium, чтобы они не загромождали ваш лог
chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])

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

    content_div = soup.select_one('div#fck_detail, article.fck_detail, div.detail-content, div.main_content_detail')

    if not content_div:
        content_div = soup.select_one('div.box_category_show, div.wrapper_detail_photo_story')

    if content_div:
        logging.debug(f"Found main/photo content div for {article_url}")
        # Извлечение всех параграфов и элементов, содержащих текст
        for p_tag in content_div.find_all(['p', 'h2', 'h3', 'li', 'div'], recursive=True):
            if p_tag.name == 'p' and p_tag.find('strong') and p_tag.get_text(strip=True) == p_tag.find('strong').get_text(strip=True):
                # Пропускаем параграфы, содержащие только жирный текст (обычно это подписи к фото)
                continue
            if p_tag.name == 'div' and ('detail-content' in p_tag.get('class', []) or 'fck_detail' in p_tag.get('class', [])):
                continue
            
            text = p_tag.get_text(separator=' ', strip=True)
            if text and not text.lower().startswith("photo:") and not text.lower().startswith("video:"):
                full_text_parts.append(text)

        # Извлечение изображений
        image_tags = content_div.find_all('img') if content_div else []
        if not image_tags:
             image_tags = soup.select('div.item_photo_detail img, .vne_lazy_image, .thumb_art img')
        
        for img_tag in image_tags:
            src = img_tag.get('data-src') or img_tag.get('src')
            if src and src.startswith(('http', '//')):
                if src.startswith('//'):
                    src = 'https:' + src
                if "?" in src: # Удаляем параметры после '?' для получения чистого URL
                    src = src.split('?')[0]
                image_urls.append(src)
        
        image_urls = list(dict.fromkeys(image_urls)) # Удаляем дубликаты
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
            # if logging.root.level <= logging.DEBUG:
            #     logging.debug(f"Cloudscraper Raw HTML for {article_url} (first 5000 chars):\n{r.text[:5000]}...")
            
            soup = BeautifulSoup(r.text, 'html.parser')
            full_text, image_urls = _extract_content_and_images(soup, article_url)

            if full_text.strip() or image_urls:
                logging.info(f"Cloudscraper SUCCESS for {article_url}")
                return full_text, image_urls
            else:
                logging.warning(f"Cloudscraper extracted empty content for {article_url}")
                return None, [] # Возвращаем None, чтобы сигнализировать о неудаче

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
    """Попытка извлечь статью с помощью Selenium."""
    logging.info(f"Attempting to fetch with Selenium: {article_url}")
    driver = None
    try:
        if CHROME_DRIVER_PATH:
            service = Service(executable_path=CHROME_DRIVER_PATH)
            driver = webdriver.Chrome(service=service, options=chrome_options)
        else:
            driver = webdriver.Chrome(options=chrome_options)

        driver.get(article_url)

        # Ждем, пока основной контентный div появится на странице
        WebDriverWait(driver, SCRAPER_TIMEOUT).until(
            EC.presence_of_element_located((By.ID, "fck_detail"))
        )

        page_source = driver.page_source
        
        # Для отладки: если уровень логирования DEBUG, выводим часть HTML
        # if logging.root.level <= logging.DEBUG:
        #     logging.debug(f"Selenium Raw HTML for {article_url} (first 5000 chars):\n{page_source[:5000]}...")

        soup = BeautifulSoup(page_source, 'html.parser')
        full_text, image_urls = _extract_content_and_images(soup, article_url)

        if full_text.strip() or image_urls:
            logging.info(f"Selenium SUCCESS for {article_url}")
            return full_text, image_urls
        else:
            logging.warning(f"Selenium extracted empty content for {article_url}")
            return None, []

    except TimeoutException:
        logging.error(f"Selenium Timeout: element #fck_detail not found for {article_url}")
        return None, []
    except WebDriverException as e:
        logging.error(f"Selenium WebDriver Error for {article_url}: {e}")
        return None, []
    except Exception as e:
        logging.error(f"Selenium failed for {article_url}: {e}", exc_info=True)
        return None, []
    finally:
        if driver:
            driver.quit()

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

# --- ПРИМЕР ИСПОЛЬЗОВАНИЯ ---
if __name__ == "__main__":
    test_urls = [
        "https://e.vnexpress.net/news/news/malaysian-police-arrest-20-men-in-late-night-gay-party-raid-4915665.html",
        "https://e.vnexpress.net/news/news/traffic/hanoi-car-driver-causing-deadly-crash-says-he-was-drowsy-after-drinking-4915602.html",
        "https://e.vnexpress.net/news/news/cambodia-to-mandate-military-service-from-2026-4914835.html",
        "https://e.vnexpress.net/news/news/buffalo-breaks-woman-s-ribs-in-shocking-street-attack-in-vietnam-4914770.html",
        # Добавьте сюда другие URL-ы для тестирования
    ]

    for url in test_urls:
        print(f"\n--- Processing URL: {url} ---")
        article_text, article_images = get_article_content(url)

        if article_text:
            logging.info(f"Successfully extracted text from {url}. Length: {len(article_text)} chars.")
            # print("--- Extracted Text (first 500 chars) ---")
            # print(article_text[:500])
            # print("...")
        else:
            logging.error(f"Failed to extract text from {url}.")

        if article_images:
            logging.info(f"Successfully extracted {len(article_images)} images from {url}.")
            # print("--- Extracted Images ---")
            # for img in article_images:
            #     print(img)
        else:
            logging.error(f"Failed to extract images from {url}.")
        print("--------------------------------------")
