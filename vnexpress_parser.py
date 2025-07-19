import requests
from bs4 import BeautifulSoup
import time
import json
import logging
import sys
import yaml # Добавлено для чтения config.yml

# --- Настройка логирования ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("vnexpress_parser.log", encoding='utf-8'), # Указание кодировки
        logging.StreamHandler(sys.stdout)
    ]
)

# --- Заголовки для запросов ---
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive"
}

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
    Использует таймаут из конфигурации.
    """
    try:
        logging.info(f"Fetching URL: {url}")
        response = requests.get(url, headers=HEADERS, timeout=config['request_timeout_seconds'])
        response.raise_for_status()
        return response.text
    except requests.exceptions.Timeout:
        logging.error(f"Timeout occurred while fetching {url}")
        return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching {url}: {e}")
        return None

def get_article_links_from_page(html_content, base_url, selector):
    """
    Извлекает ссылки на статьи с одной страницы HTML-контента, используя предоставленный селектор.
    """
    if not html_content:
        return []

    soup = BeautifulSoup(html_content, 'html.parser')
    article_links = []
    
    news_items = soup.select(selector) # Используем селектор из конфига
    
    for a_tag in news_items:
        link = a_tag.get('href')
        if link:
            if link.startswith('/'):
                link = base_url + link
            
            if "vnexpress.net" in link and not link.startswith(("#", "javascript:")): 
                article_links.append(link)
    
    return list(set(article_links))

def parse_article(article_url, config):
    """
    Парсит отдельную статью и извлекает данные, используя селекторы из конфигурации.
    """
    html_content = fetch_page_content(article_url, config)
    if not html_content:
        return None

    soup = BeautifulSoup(html_content, 'html.parser')

    article_data = {'url': article_url}
    
    # Извлечение данных с использованием селекторов из конфига
    title_tag = soup.find('h1', class_=config['selectors']['article_title_class']) # Используем class_
    if title_tag:
        article_data['title'] = title_tag.get_text(strip=True)
    else:
        logging.warning(f"Could not find title for {article_url}")
        article_data['title'] = "N/A"

    date_tag = soup.find('span', class_=config['selectors']['article_date_class']) # Используем class_
    if date_tag:
        article_data['date'] = date_tag.get_text(strip=True)
    else:
        logging.warning(f"Could not find date for {article_url}")
        article_data['date'] = "N/A"

    author_tag = soup.find('p', class_=config['selectors']['article_author_class']) # Используем class_
    if author_tag:
        article_data['author'] = author_tag.get_text(strip=True)
    else:
        article_data['author'] = "N/A"

    content_div = soup.find('article', class_=config['selectors']['article_content_class']) # Используем class_
    if content_div:
        paragraphs = content_div.find_all('p')
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
            
            time.sleep(config['request_delay_seconds'])

        # Находим URL следующей страницы для пагинации
        current_page_url = get_next_page_url(
            BeautifulSoup(html_content, 'html.parser'), 
            config['base_url'], 
            config['selectors']['pagination_next_selector']
        )
        if current_page_url:
            logging.info(f"Moving to next page: {current_page_url}")
            time.sleep(config['request_delay_seconds'])
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
