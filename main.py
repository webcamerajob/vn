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

from bs4 import BeautifulSoup
import cloudscraper
import translators as ts
import fcntl
import feedparser

from requests.exceptions import RequestException, Timeout as ReqTimeout

os.environ["translators_default_region"] = "EN"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

OUTPUT_DIR = Path("articles")
CATALOG_PATH = OUTPUT_DIR / "catalog.json"
MAX_RETRIES = 3
BASE_DELAY = 1.0

SCRAPER = cloudscraper.create_scraper()
SCRAPER_TIMEOUT = (10.0, 60.0)

def load_posted_ids(state_file_path: Path) -> Set[str]:
    try:
        if state_file_path.exists():
            with open(state_file_path, 'r', encoding='utf-8') as f:
                fcntl.flock(f, fcntl.LOCK_SH)
                return {str(item) for item in json.load(f)}
        return set()
    except (FileNotFoundError, json.JSONDecodeError, IOError) as e:
        logging.warning(f"Could not load posted IDs from {state_file_path}: {e}. Assuming empty set.")
        return set()

def extract_img_url(img_tag: Any) -> Optional[str]:
    for attr in ("data-src", "data-lazy-src", "data-srcset", "srcset", "src"):
        val = img_tag.get(attr)
        if not val:
            continue
        parts = val.split()
        if parts:
            full_url = parts[0]
            if not full_url.startswith(('http://', 'https://')):
                logging.debug(f"Relative image URL found: {full_url}, skipping.")
                continue
            return full_url.split('?', 1)[0]
    return None

def save_image(src_url: str, folder: Path) -> Optional[str]:
    logging.info(f"Saving image from {src_url} to {folder}...")
    folder.mkdir(parents=True, exist_ok=True)
    
    filename_hash = hashlib.md5(src_url.encode('utf-8')).hexdigest()
    ext_match = re.search(r'\.(jpg|jpeg|png|gif|webp)(\?|$)', src_url, re.IGNORECASE)
    extension = f".{ext_match.group(1).lower()}" if ext_match else ".bin"

    dest = folder / f"{filename_hash}{extension}"

    if dest.exists() and dest.stat().st_size > 0:
        logging.info(f"Image already exists: {dest}. Skipping download.")
        return str(dest)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = SCRAPER.get(src_url, timeout=SCRAPER_TIMEOUT)
            r.raise_for_status()
            if r.content:
                dest.write_bytes(r.content)
                logging.info(f"Successfully saved image to {dest}")
                return str(dest)
            else:
                logging.warning(f"Downloaded content for {src_url} is empty.")
                continue
        except (ReqTimeout, RequestException) as e:
            delay = BASE_DELAY * 2 ** (attempt - 1)
            logging.warning(
                "Timeout saving image %s (try %s/%s): %s; retry in %.1fs",
                src_url, attempt, MAX_RETRIES, e, delay
            )
            time.sleep(delay)
        except Exception as e:
            logging.error(f"Error saving image {src_url}: {e}")
            break
    
    logging.error("Failed saving image %s after %s attempts", src_url, MAX_RETRIES)
    return None

def load_catalog() -> List[Dict[str, Any]]:
    if not CATALOG_PATH.exists():
        return []
    try:
        with open(CATALOG_PATH, "r", encoding="utf-8") as f:
            fcntl.flock(f, fcntl.LOCK_SH)
            data = json.load(f)
            return [item for item in data if isinstance(item, dict) and "id" in item]
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        logging.error("Catalog JSON decode error: %s", e)
        return []
    except IOError as e:
        logging.error("Catalog read error: %s", e)
        return []

def save_catalog(catalog: List[Dict[str, Any]]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
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
            fcntl.flock(f, fcntl.LOCK_EX)
            json.dump(minimal, f, ensure_ascii=False, indent=2)
    except IOError as e:
        logging.error("Failed to save catalog: %s", e)

def translate_text(text: str, to_lang: str = "ru", provider: str = "yandex") -> str:
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

bad_re = re.compile(r"[\u200b-\u200f\uFEFF\u200E\u00A0]|(document\.getElementById\('track_click_pr_[^;]+;\)|var _vne_html_news_tag = '')")

# --- REVISED parse_full_article function ---
def parse_full_article(article_url: str) -> Tuple[Optional[str], List[str]]:
    """
    Extracts the full text and image URLs from a VnExpress article page,
    adapting to different article types (standard news, photo stories).
    """
    logging.info(f"Fetching full article from {article_url}...")
    full_text_parts = []
    image_urls = []

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = SCRAPER.get(article_url, timeout=SCRAPER_TIMEOUT)
            r.raise_for_status()
            
            # logging.debug(f"Fetched HTML snippet for {article_url}:\n{r.text[:1000]}") # Uncomment for deeper debug
            
            soup = BeautifulSoup(r.text, 'html.parser')

            # --- Extracting Text ---
            # Priority 1: Main article content (for standard news)
            content_div = soup.select_one('div#fck_detail, article.fck_detail, div.detail-content, div.main_content_detail')

            # Priority 2: Photo story lead text (if not found in main content)
            if not content_div:
                content_div = soup.select_one('div.box_category_show, div.wrapper_detail_photo_story') # These often contain the fck_detail inside
            
            # --- Text Extraction Logic ---
            if content_div:
                logging.info(f"Found main/photo content div with tag: {content_div.name} and attrs: {content_div.attrs.get('id')} {content_div.attrs.get('class')} for {article_url}")

                # Remove unwanted elements that are NOT part of the main narrative text
                # Adjusted for photo stories: sometimes <figcaption> should be kept if it has text.
                # Here, we keep it simple: remove scripts, styles, certain known ad/social blocks.
                for unwanted_tag_selector in [
                    'script', 'style', '.unprint', '.vne_s_tag', '.share_box',
                    'a.read_more', 'p.short_intro_detail', 'p.description',
                    'div.item_slide_show' # Remove this entire block if text isn't in it (images are handled separately)
                ]:
                    for tag_to_remove in content_div.select(unwanted_tag_selector):
                        tag_to_remove.decompose()
                
                # For regular articles, find p and h3 inside the content_div
                text_elements = content_div.find_all(['p', 'h3'])
                
                # For photo stories, text can also be in span tags with specific classes,
                # or in <div class="item_description"> within a slide show item.
                if not text_elements: # If no <p> or <h3> were found, try photo story specific text containers
                    # Try finding text within photo item descriptions
                    for desc_div in soup.select('div.item_slide_show p, div.item_slide_show span.description, div.photo_item .description'):
                        text = desc_div.get_text(strip=True)
                        if text:
                            full_text_parts.append(text)
                
                for el in text_elements:
                    text = el.get_text(strip=True)
                    if text:
                        full_text_parts.append(text)
                
                # Fallback: if specific elements failed, try to get all text from the main content div
                if not full_text_parts and content_div:
                    # Get all text, then split by lines to simulate paragraphs
                    raw_content_text = content_div.get_text(separator="\n", strip=True)
                    full_text_parts.extend([p.strip() for p in raw_content_text.split('\n') if p.strip()])


                full_text = "\n\n".join(full_text_parts)
                full_text = bad_re.sub("", full_text)
                full_text = re.sub(r"[ \t]+", " ", full_text)
                full_text = re.sub(r"\n{3,}", "\n\n", full_text)
            else:
                logging.warning(f"Could not find any suitable content div for {article_url}")
                full_text = ""

            # --- Extracting Images ---
            image_selectors = [
                'div#fck_detail img',              # Images directly in standard content div
                'div.thumb_art img',               # Featured image
                'picture img',                     # Images within picture tags
                'div.item_slide_show img',         # Images in slideshows (common for photo stories)
                'div.wrap_item_detail img',        # Another common image wrapper
                'div.img_side_detail img',         # Side images/galleries
                'div.item_content_focus img',      # Specific for certain article types
                'div.box_img_detail img',          # Another possible container
                'img.img_general'                  # General image class often outside specific divs
            ]
            
            for selector in image_selectors:
                for img_tag in soup.select(selector):
                    img_url = extract_img_url(img_tag)
                    # Filter by VnExpress image domains
                    if img_url and (img_url.startswith('https://i-vnexpress.vnecdn.net') or \
                                    img_url.startswith('https://image.vnecdn.net') or \
                                    img_url.startswith('https://vcdn-e.vnexpress.net')):
                        image_urls.append(img_url)
            
            # Deduplicate image URLs while maintaining order
            image_urls = list(dict.fromkeys(image_urls))
            
            if not full_text.strip() and not image_urls: # If no text and no images, it's a failure
                logging.warning(f"Extracted empty text AND no images for {article_url}. Returning None.")
                return None, []

            # If it's a photo story, text might be minimal (just captions) but images are crucial
            if not image_urls and "photo/" in article_url:
                logging.warning(f"No images found for photo story {article_url}. Skipping.")
                return None, []

            return full_text, image_urls

        except (ReqTimeout, RequestException) as e:
            delay = BASE_DELAY * 2 ** (attempt - 1)
            logging.warning(
                "Timeout fetching full article %s (try %s/%s): %s; retry in %.1fs",
                article_url, attempt, MAX_RETRIES, e, delay
            )
            time.sleep(delay)
        except Exception as e:
            logging.error(f"Error parsing full article {article_url}: {e}", exc_info=True) # Added exc_info for full traceback
            break
    
    return None, [] # Return None for text if all attempts fail


def parse_and_save(article_info: Dict[str, Any], translate_to: str) -> Optional[Dict[str, Any]]:
    aid = article_info["guid"] 
    clean_title = re.sub(r'[^\w\s-]', '', article_info["title"]["rendered"]).strip().lower()
    slug = re.sub(r'[-\s]+', '-', clean_title)

    art_dir = OUTPUT_DIR / f"{aid}_{slug}"
    art_dir.mkdir(parents=True, exist_ok=True)

    meta_path = art_dir / "meta.json"
    
    full_article_text, full_article_image_urls = parse_full_article(article_info["link"])
    
    # If parse_full_article returns None for text or empty image list, skip
    if not full_article_text or not full_article_image_urls: # Ensure both text AND images are present
        logging.warning(f"Skipping article GUID={aid} due to missing text or images after full parsing.")
        return None

    current_content_hash = hashlib.sha256(full_article_text.encode()).hexdigest()

    if meta_path.exists():
        try:
            existing_meta = json.loads(meta_path.read_text(encoding="utf-8"))
            if existing_meta.get("hash") == current_content_hash and \
               existing_meta.get("translated_to", "") == translate_to and \
               existing_meta.get("id") == aid:
                logging.info(f"Skipping unchanged article GUID={aid} (content and translation match local cache).")
                return existing_meta
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logging.warning(f"Failed to read existing meta for GUID={aid}: {e}. Reparsing.")

    orig_title = article_info["title"]["rendered"]
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
                    attempt, e, delay
                )
                time.sleep(delay)
        else:
            logging.warning(f"Failed to translate title for GUID={aid} after {MAX_RETRIES} attempts. Using original title.")

    raw_text = full_article_text

    img_dir = art_dir / "images"
    images: List[str] = []
    
    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = {ex.submit(save_image, url, img_dir): url for url in full_article_image_urls}
        for fut in as_completed(futures):
            if path := fut.result():
                images.append(path)

    if not images:
        logging.warning("No images found or saved for GUID=%s; skipping article parsing and saving.", aid)
        return None

    meta = {
        "id": aid,
        "slug": slug,
        "date": article_info.get("published_date"),
        "link": article_info["link"],
        "title": title,
        "text_file": str(art_dir / "content.txt"),
        "images": images,
        "posted": False,
        "hash": current_content_hash,
        "description": article_info.get("description", "")
    }
    
    (art_dir / "content.txt").write_text(raw_text, encoding="utf-8")

    if translate_to:
        h = meta["hash"]
        old = {}
        if meta_path.exists():
            try:
                old = json.loads(meta_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                pass

        if old.get("hash") != h or old.get("translated_to") != translate_to:
            clean_paras_for_translation = [p.strip() for p in raw_text.split('\n\n') if p.strip()]

            if not clean_paras_for_translation:
                logging.warning(f"No clean paragraphs to translate for GUID={aid}.")
                meta.update({"translated_to": ""})
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
                    translated_paras = clean_paras_for_translation

                txt_t = art_dir / f"content.{translate_to}.txt"
                trans_txt = "\n\n".join(translated_paras)
                header_t = f"{title}\n\n\n"
                txt_t.write_text(header_t + trans_txt, encoding="utf-8")

                meta.update({
                    "translated_to": translate_to,
                    "translated_paras": translated_paras,
                    "translated_file": str(txt_t),
                    "text_file": str(txt_t)
                })
        else:
            logging.info(f"Using cached translation {translate_to} for GUID={aid}")
            meta["text_file"] = str(art_dir / f"content.{translate_to}.txt")
            meta["translated_to"] = translate_to
    else:
        meta["text_file"] = str(art_dir / "content.txt")

    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    return meta

def parse_rss_feed(rss_url: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    logging.info(f"Fetching RSS feed from {rss_url}...")
    try:
        response = SCRAPER.get(rss_url, timeout=SCRAPER_TIMEOUT)
        response.raise_for_status()
        feed = feedparser.parse(response.text)
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

        guid = entry.get('guid')
        if not guid:
            guid = hashlib.sha256(entry.get('link', '').encode('utf-8')).hexdigest()
            logging.warning(f"Missing GUID for entry, generating from link: {entry.get('link')}")

        article_info = {
            "id": guid,
            "slug": re.sub(r'[-\s]+', '-', re.sub(r'[^\w\s-]', '', entry.get('title', 'no-title')).strip().lower()),
            "link": entry.get('link', ''),
            "title": {"rendered": entry.get('title', '')},
            "content": {"rendered": entry.get('description', '')},
            "date": entry.get('published_parsed'),
            "guid": guid,
            "description": entry.get('description', ''),
            "published_date": entry.get('published_parsed'),
        }
        articles_from_rss.append(article_info)
        parsed_count += 1
        logging.info(f"Found RSS entry: {article_info['title']['rendered']} (GUID: {guid})")

    return articles_from_rss


def main():
    parser = argparse.ArgumentParser(description="Parser with translation")
    parser.add_argument("--base-url", type=str,
                        default="https://e.vnexpress.net/",
                        help="Base URL of the site (used for relative paths if any).")
    parser.add_argument("--rss-url", type=str,
                        default="https://e.vnexpress.net/rss/news.rss",
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
        
        posted_ids_from_repo = load_posted_ids(Path(args.posted_state_file))
        logging.info(f"Loaded {len(posted_ids_from_repo)} posted IDs from {args.posted_state_file}.")

        rss_entries = parse_rss_feed(args.rss_url, limit=args.limit) 

        catalog = load_catalog()
        existing_ids_in_catalog = {article["id"] for article in catalog}
        
        new_articles_processed_in_run = 0
        updated_guids_to_post = set()

        for entry in rss_entries:
            article_guid = str(entry["id"])
            
            if article_guid in posted_ids_from_repo:
                logging.info(f"Skipping article GUID={article_guid} as it's already in {args.posted_state_file}.")
                continue

            if meta := parse_and_save(entry, args.lang):
                if article_guid in existing_ids_in_catalog:
                    catalog = [item for item in catalog if item["id"] != article_guid]
                    logging.info(f"Updated article GUID={article_guid} in local catalog (content changed or re-translated).")
                else:
                    new_articles_processed_in_run += 1
                    logging.info(f"Processed new article GUID={article_guid} and added to local catalog.")
                
                catalog.append(meta)
                existing_ids_in_catalog.add(article_guid)
                updated_guids_to_post.add(article_guid)

        if new_articles_processed_in_run > 0:
            save_catalog(catalog)
            
            final_posted_ids = posted_ids_from_repo.union(updated_guids_to_post)
            try:
                with open(Path(args.posted_state_file), 'w', encoding='utf-8') as f:
                    fcntl.flock(f, fcntl.LOCK_EX)
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

if __name__ == "__main__":
    main()
