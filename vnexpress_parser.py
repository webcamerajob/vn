import requests
from bs4 import BeautifulSoup
import time
import json
import logging
import sys
import yaml
import random # Добавлено для возможной ротации User-Agent

# --- Настройка логирования ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("vnexpress_parser.log", encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

# --- Заголовки для запросов ---
# Расширенный набор заголовков для лучшей имитации браузера
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "en-US,en;q=0.9,vi;q=0.8", # Важно для вьетнамского сайта
    "Accept-Encoding": "gzip, deflate, br", # Принимаем сжатие
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    # "Referer": "https://vnexpress.net/", # Можно добавить, если сайт проверяет реферер
    # "DNT": "1" # Do Not Track request header
}

# Если понадобится ротация User-Agent, можно раскомментировать и использовать в fetch_page_content
# USER_AGENT_LIST = [
#     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
#     "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
#     "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0",
#     # Добавьте больше реальных User-Agent строк сюда
# ]


def load_config(config_path='config.yml'):
    """Загружает конфигурацию из файла YAML."""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        logging.info("Configuration loaded successfully.")
        return config
    except FileNotFoundError:
        logging.error(f"Config file '{config_path}' not found. Please create it.")
        sys.exit(1)
    except yaml.YAMLError as e:
        logging.error(f"Error parsing config file '{config_path}': {e}")
        sys.exit(1)

def fetch_page_content(url, config):
    """
    Выполняет HTTP-запрос к указанному URL и возвращает HTML-содержимое.
    Использует таймаут из конфигурации и текущие заголовки.
    """
    # Если вы решили использовать ротацию User-Agent, раскомментируйте следующие строки:
    # current_headers = HEADERS.copy()
    # current_headers['User-Agent'] = random.choice(USER_AGENT_LIST)

    try:
        logging.info(f"Fetching URL: {url}")
        response = requests.get(url, headers=HEADERS, timeout=config['request_timeout_seconds']) # Используем HEADERS
        response.raise_for_status() # Вызывает исключение для ошибок HTTP (4xx или 5xx)
        return response.text
    except requests.exceptions.Timeout:
        logging.error(f"Timeout occurred while fetching {url}")
        return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching {url}: {e}")
        # Дополнительная проверка на 406 ошибку
        if response is not None and response.status_code == 406:
            logging.error("Received 406 Not Acceptable. Website is likely blocking the request based on headers or rate.")
        return None

def get_article_links_from_page(html_content, base_url, selector):
    """
    Извлекает ссылки на статьи с одной страницы HTML-контента, используя предоставленный селектор.
    """
    if not html_content:
        return []

    soup = BeautifulSoup(html_content, 'html.parser')
    article_links = []
    
    # Используем селектор из конфига
    news_items = soup.select(selector) 
    
    for a_tag in news_items:
        link = a_tag.get('href')
        if link:
            # Преобразуем относительные ссылки в абсолютные
            if link.startswith('/'):
                link = base_url + link
            
            # Проверяем, что ссылка действительно ведет на статью VnExpress и не является якорем/javascript
            if "vnexpress.net" in link and not link.startswith(("#", "javascript:")): 
                article_links.append(link)
    
    return list(set(article_links)) # Возвращаем уникальные ссылки

def parse_article(article_url, config):
    """
    Парсит отдельную статью и извлекает данные, используя селекторы из конфигурации.
    """
    html_content = fetch_page_content(article_url, config)
    if not html_content:
        return None # Возвращаем None, если контент не был получен

    soup = BeautifulSoup(html_content, 'html.parser')

    article_data = {'url': article_url}
    
    # --- СЕЛЕКТОРЫ ДЛЯ СТАТЬИ (ПОТРЕБУЮТ ПРОВЕРКИ И КОРРЕКТИРОВКИ!) ---
    # VnExpress часто использует классы, поэтому используем find('tag', class_='class_name')

    # Заголовок
    title_tag = soup.find('h1', class_=config['selectors']['article_title_class'])
    if title_tag:
        article_data['title'] = title_tag.get_text(strip=True)
    else:
        logging.warning(f"Could not find title for {article_url}")
        article_data['title'] = "N/A"

    # Дата публикации
    date_tag = soup.find('span', class_=config['selectors']['article_date_class'])
    if date_tag:
        article_data['date'] = date_tag.get_text(strip=True)
    else:
        logging.warning(f"Could not find date for {article_url}")
        article_data['date'] = "N/A"

    # Автор (если есть и можно найти)
    author_tag = soup.find('p', class_=config['selectors']['article_author_class'])
    if author_tag:
        article_data['author'] = author_tag.get_text(strip=True)
    else:
        article_data['author'] = "N/A"

    # Основной текст статьи
    content_div = soup.find('article', class_=config['selectors']['article_content_class'])
    if content_div:
        paragraphs = content_div.find_all('p')
        # Исключаем пустые параграфы и объединяем
        article_text = "\n".join([p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)])
        article_data['content'] = article_text
    else:
        logging.warning(f"Could not find main content for {article_url}")
        article_data['content'] = "N/A"
    
    return article_data

def get_next_page_url(soup, base_url, selector):
    """
    Ищет ссылку на следующую страницу пагинации, используя селектор из конфигурации.
    """
    next_button = soup.select_one(selector)
    
    if next_button and next_button.get('href'):
        next_page_relative_url = next_button.get('href')
        if next_page_relative_url.startswith('/'):
            return base_url + next_page_relative_url
        return next_page_relative_url
    return None

def main():
    config = load_config()
    
    all_articles_data = []
    current_page_url = config['base_url'] + config['category_path']
    page_count = 0

    while current_page_url and page_count < config['max_pages_to_scrape']:
        page_count += 1
        logging.info(f"Scraping page {page_count}: {current_page_url}")
        
        html_content = fetch_page_content(current_page_url, config)
        if not html_content:
            logging.error(f"Failed to fetch content for page {current_page_url}. Stopping.")
            break

        article_links_on_page = get_article_links_from_page(
            html_content, 
            config['base_url'], 
            config['selectors']['article_link_selector']
        )
        logging.info(f"Found {len(article_links_on_page)} article links on page {page_count}.")
        
        if not article_links_on_page:
            logging.info(f"No more article links found on page {page_count}. Stopping.")
            break

        for i, link in enumerate(article_links_on_page):
            logging.info(f"Parsing article {i+1}/{len(article_links_on_page)} from page {page_count}: {link}")
            article = parse_article(link, config)
            if article:
                all_articles_data.append(article)
            
            # Задержка для этичного скрейпинга
            time.sleep(config['request_delay_seconds']) 

        # Находим URL следующей страницы для пагинации
        current_page_url = get_next_page_url(
            BeautifulSoup(html_content, 'html.parser'), 
            config['base_url'], 
            config['selectors']['pagination_next_selector']
        )
        if current_page_url:
            logging.info(f"Moving to next page: {current_page_url}")
            time.sleep(config['request_delay_seconds']) # Дополнительная задержка перед переходом на следующую страницу
        else:
            logging.info("No next page found. Stopping pagination.")

    try:
        with open(config['output_filename'], 'w', encoding='utf-8') as f:
            json.dump(all_articles_data, f, ensure_ascii=False, indent=4)
        logging.info(f"Parsing completed. Saved {len(all_articles_data)} articles to '{config['output_filename']}'")
    except IOError as e:
        logging.error(f"Error saving data to file {config['output_filename']}: {e}")

if __name__ == "__main__":
    logging.info("Starting VnExpress article parser...")
    main()
    logging.info("VnExpress article parser finished.")
