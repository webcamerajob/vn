#!/usr/bin/env python3
import argparse
import logging
import json
import hashlib
import time
import re
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Set, Tuple

# Убедитесь, что эти импорты у вас есть
from bs4 import BeautifulSoup
import cloudscraper # Для HTTP запросов и обхода Cloudflare
import translators as ts # Для translate_text
import fcntl # Для блокировки файлов в load_catalog и save_catalog
import feedparser # !!! НОВЫЙ ИМПОРТ !!!

# Импорт для обработки исключений requests
from requests.exceptions import RequestException, Timeout as ReqTimeout # ИСПРАВЛЕНО

# Настройка переменной окружения (должна быть в начале, один раз)
os.environ["translators_default_region"] = "EN"

# Настройки логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Предполагаемые константы
OUTPUT_DIR = Path("articles")
CATALOG_PATH = OUTPUT_DIR / "catalog.json"
MAX_RETRIES = 3
BASE_DELAY = 1.0 # Базовая задержка для ретраев

# cloudscraper для обхода Cloudflare
SCRAPER = cloudscraper.create_scraper()
SCRAPER_TIMEOUT = (10.0, 60.0) # (connect_timeout, read_timeout) в секундах

# --- Вспомогательные функции (реальные реализации из нашего обсуждения) ---
def load_posted_ids(state_file_path: Path) -> Set[str]:
    """
    Загружает множество ID из файла состояния (например, posted.json).
    Используется блокировка файла для безопасного чтения.
    """
    try:
        if state_file_path.exists():
            with open(state_file_path, 'r', encoding='utf-8') as f:
                fcntl.flock(f, fcntl.LOCK_SH) # Блокировка для чтения
                # Преобразуем все в строки, так как GUIDы могут быть нечисловыми
                return {str(item) for item in json.load(f)}
        return set()
    except (FileNotFoundError, json.JSONDecodeError, IOError) as e:
        logging.warning(f"Could not load posted IDs from {state_file_path}: {e}. Assuming empty set.")
        return set()

def extract_img_url(img_tag: Any) -> Optional[str]:
    """Извлекает URL изображения из тега <img>."""
    for attr in ("data-src", "data-lazy-src", "data-srcset", "srcset", "src"):
        val = img_tag.get(attr)
        if not val:
            continue
        # Убираем параметры запроса из URL изображения
        parts = val.split()
        if parts:
            # Для VnExpress, если есть .webp или .jpg, берем полный URL
            # Некоторые изображения могут быть относительными
            full_url = parts[0]
            if not full_url.startswith(('http://', 'https://')):
                # Если URL относительный, возможно, нужно добавить базовый URL.
                # Для VnExpress обычно полные URL, но лучше перестраховаться.
                # Однако, в данном случае, e.vnexpress.net дает полные пути.
                logging.debug(f"Relative image URL found: {full_url}, skipping for now as it's not expected for VnExpress img src.")
                continue # Пропускаем относительные URL пока что
            return full_url.split('?', 1)[0] # Убираем query параметры
    return None

# --- Эти функции больше не нужны для RSS парсинга ---
# def fetch_category_id(base_url: str, slug: str) -> int:
#     # ... (логика WP API) ...
#     pass

# def fetch_posts(base_url: str, cat_id: int, per_page: int = 10) -> List[Dict[str, Any]]:
#     # ... (логика WP API) ...
#     pass
# --- Конец ненужных функций ---

def save_image(src_url: str, folder: Path) -> Optional[str]:
    """Сохраняет изображение по URL в указанную папку."""
    logging.info(f"Saving image from {src_url} to {folder}...")
    folder.mkdir(parents=True, exist_ok=True)
    
    # Для VnExpress URL изображений могут быть очень длинными, 
    # иногда без расширения в конце (например, webp).
    # Используем хэш от URL для имени файла, чтобы избежать проблем с длиной и дубликатами.
    filename_hash = hashlib.md5(src_url.encode('utf-8')).hexdigest()
    # Попробуем определить расширение из URL, если есть.
    # VnExpress часто использует что-то вроде .jpg?w=120&h=80
    ext_match = re.search(r'\.(jpg|jpeg|png|gif|webp)(\?|$)', src_url, re.IGNORECASE)
    extension = f".{ext_match.group(1).lower()}" if ext_match else ".bin" # .bin как запасной вариант

    dest = folder / f"{filename_hash}{extension}"

    if dest.exists() and dest.stat().st_size > 0: # Проверяем, что файл уже есть и не пустой
        logging.info(f"Image already exists: {dest}. Skipping download.")
        return str(dest)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = SCRAPER.get(src_url, timeout=SCRAPER_TIMEOUT)
            r.raise_for_status()
            if r.content: # Проверяем, что контент не пустой
                dest.write_bytes(r.content)
                logging.info(f"Successfully saved image to {dest}")
                return str(dest)
            else:
                logging.warning(f"Downloaded content for {src_url} is empty.")
                continue # Повторить попытку, возможно, временная проблема
        except (ReqTimeout, RequestException) as e:
            delay = BASE_DELAY * 2 ** (attempt - 1)
            logging.warning(
                "Timeout saving image %s (try %s/%s): %s; retry in %.1fs",
                src_url, attempt, MAX_RETRIES, e, delay
            )
            time.sleep(delay)
        except Exception as e: # Общая ошибка при сохранении
            logging.error(f"Error saving image {src_url}: {e}")
            break # Не повторять, если это не проблема сети

    logging.error("Failed saving image %s after %s attempts", src_url, MAX_RETRIES)
    return None

def load_catalog() -> List[Dict[str, Any]]:
    """Загружает каталог статей из catalog.json с блокировкой файла."""
    if not CATALOG_PATH.exists():
        return []
    try:
        with open(CATALOG_PATH, "r", encoding="utf-8") as f:
            fcntl.flock(f, fcntl.LOCK_SH)  # Блокировка для чтения
            data = json.load(f)
            # Валидация данных: фильтруем некорректные записи
            return [item for item in data if isinstance(item, dict) and "id" in item]
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        logging.error("Catalog JSON decode error: %s", e)
        return []
    except IOError as e:
        logging.error("Catalog read error: %s", e)
        return []

def save_catalog(catalog: List[Dict[str, Any]]) -> None:
    """
    Сохраняет каталог статей в catalog.json с блокировкой файла.
    Сохраняет только минимальный набор полей для защиты от дублей:
    id, hash, translated_to
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    # Фильтруем каждую запись
    minimal = []
    for item in catalog:
        if isinstance(item, dict) and "id" in item:
            minimal.append({
                "id": item["id"],
                "hash": item.get("hash", ""),
                "translated_to": item.get("translated_to", "")
            })
        else:
            logging.warning(f"Skipping malformed catalog entry: {item}")

    try:
        with open(CATALOG_PATH, "w", encoding="utf-8") as f:
            fcntl.flock(f, fcntl.LOCK_EX) # Блокировка для записи
            json.dump(minimal, f, ensure_ascii=False, indent=2)
    except IOError as e:
        logging.error("Failed to save catalog: %s", e)

def translate_text(text: str, to_lang: str = "ru", provider: str = "yandex") -> str:
    """
    Перевод текста через translators с защитой от ошибок.
    Возвращает оригинал, если перевод недоступен.
    """
    logging.info(f"Translating text (provider: {provider}) to {to_lang}...")
    if not text or not isinstance(text, str):
        return ""
    try:
        translated = ts.translate_text(text, translator=provider, from_language="en", to_language=to_lang)
        if isinstance(translated, str):
            return translated
        logging.warning("Translator returned non-str for text: %s", text[:50])
    except Exception as e:
        logging.warning("Translation error [%s → %s]: %s", provider, to_lang, e)
    return text

# Регулярные выражения для очистки текста
# Добавил очистку характерных для VnExpress элементов
bad_re = re.compile(r"[\u200b-\u200f\uFEFF\u200E\u00A0]|(document\.getElementById\('track_click_pr_[^;]+;\)|var _vne_html_news_tag = '')") # Убираем невидимые символы и JS-вставки

# --- НОВАЯ ФУНКЦИЯ: Парсинг полной статьи с VnExpress ---
def parse_full_article(article_url: str) -> Tuple[Optional[str], List[str]]:
    """
    Извлекает полный текст и URL изображений из страницы статьи VnExpress.
    """
    logging.info(f"Fetching full article from {article_url}...")
    full_text = []
    image_urls = []

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = SCRAPER.get(article_url, timeout=SCRAPER_TIMEOUT)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, 'html.parser')

            # --- Извлечение текста ---
            # Основной контент статьи в VnExpress часто находится в <article class="fck_detail">
            # или внутри div с классом, например, "detail-content" или "main_content"
            content_div = soup.find('article', class_='fck_detail')
            if not content_div:
                content_div = soup.find('div', class_='detail-content') # Другой возможный класс
            if not content_div:
                content_div = soup.find('div', class_='main_content_detail') # Ещё один возможный класс
            
            if content_div:
                # Удаляем ненужные элементы (например, рекламные блоки, скрипты, подписи)
                for unwanted_tag in content_div.find_all(['script', 'style', 'figure', 'figcaption', 'span', 'p', 'div']):
                    # Удаляем только те, которые точно не являются частью текста
                    # Например, рекламные блоки с id или классами
                    if unwanted_tag.name == 'p' and unwanted_tag.get_text(strip=True).lower() in ["read more", "related news", "video", "see more"]:
                        unwanted_tag.decompose()
                    elif unwanted_tag.name == 'div' and ('vne_s_tag' in unwanted_tag.get('class', []) or 'share_box' in unwanted_tag.get('class', [])):
                         unwanted_tag.decompose()
                    elif unwanted_tag.name == 'figure': # Удаляем figure, чтобы обработать img отдельно
                        unwanted_tag.decompose()
                    elif unwanted_tag.name == 'figcaption':
                        unwanted_tag.decompose() # Удаляем подписи к картинкам
                    elif unwanted_tag.name == 'a' and unwanted_tag.get_text(strip=True).lower() == "read more": # Удаляем "Read more" ссылки
                         unwanted_tag.decompose()
                
                # Собираем параграфы. VnExpress часто использует <p> внутри fck_detail
                paras = [p.get_text(strip=True) for p in content_div.find_all('p') if p.get_text(strip=True)]
                full_text = "\n\n".join(paras)
                full_text = bad_re.sub("", full_text)
                full_text = re.sub(r"[ \t]+", " ", full_text)
                full_text = re.sub(r"\n{3,}", "\n\n", full_text)
            else:
                logging.warning(f"Could not find main content div for {article_url}")
                full_text = ""

            # --- Извлечение изображений ---
            # Ищем изображения в основном контенте или в специальных блоках галереи
            # VnExpress часто использует <img> внутри <figure> или напрямую
            img_tags = content_div.find_all('img') if content_div else []
            # Дополнительно ищем изображения в div с классом "container" или "item_slide_show"
            # или в любом другом блоке, где могут быть основные изображения статьи
            # Это может потребовать ручного анализа для каждого типа страницы (статья, галерея)
            
            # Попробуем найти общие места для изображений, если не в fck_detail
            if not img_tags:
                img_tags = soup.select('.img_general img, .item_slide_show img, .photo img')
                
            for img_tag in img_tags:
                img_url = extract_img_url(img_tag)
                if img_url and img_url.startswith('https://i-vnexpress.vnecdn.net'): # Фильтруем по домену изображений VnExpress
                    image_urls.append(img_url)
            
            # Уникализируем список изображений
            image_urls = list(dict.fromkeys(image_urls)) # Сохраняем порядок
            
            return full_text, image_urls

        except (ReqTimeout, RequestException) as e:
            delay = BASE_DELAY * 2 ** (attempt - 1)
            logging.warning(
                "Timeout fetching full article %s (try %s/%s): %s; retry in %.1fs",
                article_url, attempt, MAX_RETRIES, e, delay
            )
            time.sleep(delay)
        except Exception as e:
            logging.error(f"Error parsing full article {article_url}: {e}")
            break # Не повторять, если это не проблема сети
    
    return None, [] # Возвращаем None, если не удалось получить контент

# Функция parse_and_save (значительные изменения для работы с RSS-данными и полным парсингом)
def parse_and_save(article_info: Dict[str, Any], translate_to: str) -> Optional[Dict[str, Any]]:
    """Парсит и сохраняет статью, включая перевод и загрузку изображений."""
    # Вместо post["id"] и post["slug"] используем article_info["guid"] и "title"
    # Для poster.py нам нужен "id", поэтому guid становится "id"
    aid = article_info["guid"] 
    # Slug в случае RSS может быть неактуален или генерироваться из заголовка
    # Для совместимости с именем папки, сделаем slug из заголовка, убрав спецсимволы
    # Исправлено: получаем строку из словаря 'title'
    clean_title = re.sub(r'[^\w\s-]', '', article_info["title"]["rendered"]).strip().lower()
    slug = re.sub(r'[-\s]+', '-', clean_title) # Заменяем пробелы и дефисы одним дефисом

    art_dir = OUTPUT_DIR / f"{aid}_{slug}" # Имя папки теперь будет aid_slug
    art_dir.mkdir(parents=True, exist_ok=True)

    # Проверяем существующую статью (по guid)
    meta_path = art_dir / "meta.json"
    
    # !!! НОВОЕ: Парсим полный текст и изображения прямо здесь !!!
    full_article_text, full_article_image_urls = parse_full_article(article_info["link"])
    
    if not full_article_text:
        logging.warning(f"Failed to get full text for article GUID={aid}. Skipping.")
        return None

    current_content_hash = hashlib.sha256(full_article_text.encode()).hexdigest()

    if meta_path.exists():
        try:
            existing_meta = json.loads(meta_path.read_text(encoding="utf-8"))
            # Проверяем, изменился ли контент (по хэшу) или язык перевода
            if existing_meta.get("hash") == current_content_hash and \
               existing_meta.get("translated_to", "") == translate_to and \
               existing_meta.get("id") == aid: # Также проверяем, что ID совпадает
                logging.info(f"Skipping unchanged article GUID={aid} (content and translation match local cache).")
                return existing_meta # Возвращаем существующие метаданные
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logging.warning(f"Failed to read existing meta for GUID={aid}: {e}. Reparsing.")

    orig_title = article_info["title"]
    title = orig_title

    if translate_to:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                title = translate_text(orig_title, to_lang=translate_to, provider="yandex")
                break
            except Exception as e:
                delay = BASE_DELAY * 2 ** (attempt - 1)
                logging.warning(
                    "Translate title attempt %s failed: %s; retry in %.1fs",
                    attempt, e, delay # Исправил, было attempt, MAX_RETRIES, e, delay
                )
                time.sleep(delay)
        else: # Этот else относится к for-циклу, если все попытки неудачны
            logging.warning(f"Failed to translate title for GUID={aid} after {MAX_RETRIES} attempts. Using original title.")

    # Используем полный текст, полученный из parse_full_article
    raw_text = full_article_text

    img_dir = art_dir / "images"
    images: List[str] = []
    
    # Загружаем изображения, найденные в parse_full_article
    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = {ex.submit(save_image, url, img_dir): url for url in full_article_image_urls}
        for fut in as_completed(futures):
            if path := fut.result():
                images.append(path)

    if not images:
        logging.warning("No images found or saved for GUID=%s; skipping article parsing and saving.", aid)
        return None

    # Формируем метаданные в формате, ожидаемом постером
    meta = {
        "id": aid, # GUID теперь является "id"
        "slug": slug, # Сгенерированный slug
        "date": article_info.get("published_date"), # Дата из RSS
        "link": article_info["link"], # Оригинальная ссылка на статью
        "title": title, # Переведенный или оригинальный заголовок
        "text_file": str(art_dir / "content.txt"), # Путь к оригинальному тексту
        "images": images,
        "posted": False, # Изначально не опубликовано
        "hash": current_content_hash, # Хэш полного контента
        "description": article_info.get("description", "") # Краткое описание из RSS
    }
    
    # Сохраняем оригинальный текст статьи
    (art_dir / "content.txt").write_text(raw_text, encoding="utf-8")

    # Обработка перевода
    if translate_to:
        h = meta["hash"] # Хэш оригинального текста
        old = {}
        if meta_path.exists():
            try:
                old = json.loads(meta_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                pass

        if old.get("hash") != h or old.get("translated_to") != translate_to:
            # Переводим текст статьи по параграфам (если это необходимо)
            # Разделяем full_article_text на параграфы для перевода
            
            # Разделение по двойному переводу строки, затем фильтрация пустых строк
            clean_paras_for_translation = [p.strip() for p in raw_text.split('\n\n') if p.strip()]

            if not clean_paras_for_translation:
                logging.warning(f"No clean paragraphs to translate for GUID={aid}.")
                meta.update({"translated_to": ""}) # Сбрасываем флаг перевода, если нет текста
            else:
                translated_paras: List[str] = []
                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        translated_paras = [translate_text(p, to_lang=translate_to, provider="yandex") for p in clean_paras_for_translation]
                        break
                    except Exception as e:
                        delay = BASE_DELAY * 2 ** (attempt - 1)
                        logging.warning("Translate text attempt %s failed: %s; retry in %.1fs", attempt, e, delay)
                        time.sleep(delay)
                else:
                    logging.warning(f"Text translation failed after max retries for GUID={aid}. Using original text.")
                    translated_paras = clean_paras_for_translation # Если перевод не удался, используем оригинал

                txt_t = art_dir / f"content.{translate_to}.txt"
                trans_txt = "\n\n".join(translated_paras)
                header_t = f"{title}\n\n\n" # Переведенный заголовок
                txt_t.write_text(header_t + trans_txt, encoding="utf-8")

                meta.update({
                    "translated_to": translate_to,
                    "translated_paras": translated_paras, # Список переведенных параграфов
                    "translated_file": str(txt_t),
                    "text_file": str(txt_t) # poster.py будет использовать это как основной файл
                })
        else:
            logging.info(f"Using cached translation {translate_to} for GUID={aid}")
            # Если перевод не менялся, убедимся, что text_file указывает на переведенный файл
            meta["text_file"] = str(art_dir / f"content.{translate_to}.txt")
            meta["translated_to"] = translate_to # Убедимся, что флаг установлен
    else: # Если перевод не запрошен
        meta["text_file"] = str(art_dir / "content.txt") # Используем оригинальный файл

    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    return meta


# --- НОВАЯ ФУНКЦИЯ: Парсинг RSS-ленты ---
def parse_rss_feed(rss_url: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Парсит RSS-ленту и возвращает список словарей с информацией о статьях.
    Преобразует данные в формат, близкий к WP API для совместимости с parse_and_save.
    """
    logging.info(f"Fetching RSS feed from {rss_url}...")
    try:
        # Используем cloudscraper для запроса RSS-ленты
        response = SCRAPER.get(rss_url, timeout=SCRAPER_TIMEOUT)
        response.raise_for_status()
        feed = feedparser.parse(response.text) # feedparser может парсить прямо из строки
    except (ReqTimeout, RequestException) as e:
        logging.error(f"Failed to fetch RSS feed from {rss_url}: {e}")
        return []
    except Exception as e:
        logging.error(f"Failed to parse RSS feed from {rss_url}: {e}")
        return []

    if feed.bozo:
        logging.warning(f"RSS feed has parsing errors: {feed.bozo_exception}")

    articles_from_rss = []
    parsed_count = 0

    for entry in feed.entries:
        if limit and parsed_count >= limit:
            break

        # GUID - это уникальный идентификатор, который будет использоваться как "id"
        # Некоторые GUIDs могут быть пустыми, генерируем SHA256 от ссылки в таком случае
        guid = entry.get('guid')
        if not guid:
            guid = hashlib.sha256(entry.get('link', '').encode('utf-8')).hexdigest()
            logging.warning(f"Missing GUID for entry, generating from link: {entry.get('link')}")

        # Простая имитация структуры WP API post для совместимости с parse_and_save
        # Некоторые поля будут отсутствовать, но parse_and_save сможет их обработать
        article_info = {
            "id": guid, # GUID теперь - это "id" для постера
            "slug": re.sub(r'[-\s]+', '-', re.sub(r'[^\w\s-]', '', entry.get('title', 'no-title')).strip().lower()), # Генерируем slug
            "link": entry.get('link', ''),
            "title": {"rendered": entry.get('title', '')}, # Mock-объект для title
            "content": {"rendered": entry.get('description', '')}, # Mock-объект для content (краткое описание)
            "date": entry.get('published_parsed'), # feedparser уже разбирает дату
            # Добавим сюда оригинальные поля, которые могут быть полезны в `article_info`
            "guid": guid,
            "description": entry.get('description', ''),
            "published_date": entry.get('published_parsed'), # Сохраняем разобранную дату для meta.json
        }
        articles_from_rss.append(article_info)
        parsed_count += 1
        logging.info(f"Found RSS entry: {article_info['title']['rendered']} (GUID: {guid})")

    return articles_from_rss


# --- Основная функция main() ---
def main():
    parser = argparse.ArgumentParser(description="Parser with translation")
    parser.add_argument("--base-url", type=str,
                        default="https://e.vnexpress.net/",
                        help="Base URL of the site (used for relative paths if any).")
    # Удаляем --slug, так как он не используется для RSS
    parser.add_argument("--rss-url", type=str,
                        default="https://e.vnexpress.net/rss/news.rss", # Прямой URL RSS ленты
                        help="RSS feed URL to parse.")
    parser.add_argument("-n", "--limit", type=int, default=None,
                        help="Max posts to parse")
    parser.add_argument("-l", "--lang", type=str, default="",
                        help="Translate to language code")
    parser.add_argument(
        "--posted-state-file",
        type=str,
        default="articles/posted.json",
        help="Путь к файлу состояния с ID уже опубликованных статей (только для чтения)"
    )
    args = parser.parse_args()

    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        
        # Загрузка уже опубликованных ID из posted.json
        posted_ids_from_repo = load_posted_ids(Path(args.posted_state_file))
        logging.info(f"Loaded {len(posted_ids_from_repo)} posted IDs from {args.posted_state_file}.")

        # Загрузка статей из RSS-ленты
        # Передаем limit в parse_rss_feed, чтобы ограничить количество записей из RSS
        rss_entries = parse_rss_feed(args.rss_url, limit=args.limit) 

        catalog = load_catalog()
        # Для каталога также используем guid в качестве id
        existing_ids_in_catalog = {article["id"] for article in catalog}
        
        new_articles_processed_in_run = 0
        updated_guids_to_post = set() # Список GUIDов, которые мы хотим отметить как опубликованные

        for entry in rss_entries:
            # Используем guid как id
            article_guid = str(entry["id"]) # ID теперь это GUID
            
            if article_guid in posted_ids_from_repo:
                logging.info(f"Skipping article GUID={article_guid} as it's already in {args.posted_state_file}.")
                continue

            # `parse_and_save` теперь принимает `article_info` и `translate_to`
            # и сама парсит полный контент со страницы статьи
            if meta := parse_and_save(entry, args.lang):
                if article_guid in existing_ids_in_catalog:
                    catalog = [item for item in catalog if item["id"] != article_guid]
                    logging.info(f"Updated article GUID={article_guid} in local catalog (content changed or re-translated).")
                else:
                    new_articles_processed_in_run += 1
                    logging.info(f"Processed new article GUID={article_guid} and added to local catalog.")
                
                catalog.append(meta)
                existing_ids_in_catalog.add(article_guid) # Обновляем существующие ID в каталоге
                updated_guids_to_post.add(article_guid) # Добавляем для обновления posted.json

        # Сохранение каталога и обновление posted.json
        if new_articles_processed_in_run > 0:
            save_catalog(catalog) # Сохраняем обновленный каталог
            
            # Обновляем posted.json, добавляя новые обработанные GUIDы
            final_posted_ids = posted_ids_from_repo.union(updated_guids_to_post)
            try:
                with open(Path(args.posted_state_file), 'w', encoding='utf-8') as f:
                    fcntl.flock(f, fcntl.LOCK_EX) # Блокировка для записи
                    json.dump(list(final_posted_ids), f, indent=4)
                logging.info(f"Updated {args.posted_state_file} with {len(updated_guids_to_post)} new GUIDs.")
            except IOError as e:
                logging.error(f"Failed to save updated posted IDs to {args.posted_state_file}: {e}")

            logging.info(f"Added {new_articles_processed_in_run} truly new articles. Total parsed articles in catalog: {len(catalog)}")
            print("NEW_ARTICLES_STATUS:true")
        else:
            logging.info("No new articles found or processed that are not already in posted.json or local catalog.")
            print("NEW_ARTICLES_STATUS:false")

    except Exception as e:
        logging.exception("Fatal error in main:")
        exit(1)

# Убедитесь, что этот блок БЕЗ ОТСТУПА!
if __name__ == "__main__":
    main()
