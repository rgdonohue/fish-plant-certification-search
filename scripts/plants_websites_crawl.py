# %%
import asyncio
import io
import os
import time
from urllib.parse import urljoin, urlparse, urlunparse

import aiohttp
import nest_asyncio
import pandas as pd
import PyPDF2
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from itertools import islice
import math
import validators
from urllib3.util import parse_url
from aiohttp import ClientTimeout
from asyncio import TimeoutError

# nest_asyncio.apply() # for running async code in notebook

# %%
# Define cert->keywords mapping
CERT_KEYWORDS = {
    "ASC Cert": [
        "ASC", "A.S.C.", "Aquaculture Stewardship Council",
    ],
    "BAP Cert": [
        "BAP", "Best Aquaculture Practices", "Global Seafood Alliance"
    ],
    "FOS Cert": [
        "Friend of the Sea", "FOS", "WSO", "World Sustainability Organization"
    ],
    "FIP Cert": [
        "FIP", "Fisheries Improvement Project"
    ],
    "MarinTrust Cert": [
        "Marin Trust"
    ]
}

# %%
def extract_text_from_pdf(pdf_content: bytes) -> str:
    """
    Enhanced PDF text extraction with better error handling and memory management.
    """
    text = ""
    try:
        # Limit PDF size to prevent memory issues
        if len(pdf_content) > 10 * 1024 * 1024:  # 10MB limit
            print("[PDF ERROR] File too large")
            return ""
            
        with io.BytesIO(pdf_content) as pdf_file:
            try:
                reader = PyPDF2.PdfReader(pdf_file)
                
                # Limit number of pages processed
                max_pages = min(len(reader.pages), 50)
                
                for page_num in range(max_pages):
                    try:
                        page = reader.pages[page_num]
                        page_text = page.extract_text()
                        if page_text:
                            text += " " + page_text
                    except Exception as e:
                        print(f"[PDF ERROR] Failed to extract page {page_num}: {e}")
                        continue
                        
            except PyPDF2.PdfReadError as e:
                print(f"[PDF ERROR] Invalid PDF format: {e}")
                return ""
                
    except Exception as e:
        print(f"[PDF ERROR] {e}")
        return ""
        
    return text.lower()

# %%
def is_same_domain(base_url: str, new_url: str) -> bool:
    """Returns True if new_url is on the same domain (or subdomain) as base_url."""
    base_domain = urlparse(base_url).netloc
    check_domain = urlparse(new_url).netloc
    # A loose check: ensure the base domain is a substring of check_domain
    # e.g. base: "example.com", check: "sub.example.com" => True
    return base_domain in check_domain

# %%
def clean_and_validate_url(url: str) -> str:
    """Cleans and validates a URL, returns None if invalid."""
    if not url:
        return None
        
    # Ensure URL has protocol
    if not url.startswith(('http://', 'https://')):
        url = 'http://' + url
        
    try:
        # Basic URL validation
        if not validators.url(url):
            return None
            
        # Parse and reconstruct to normalize
        parsed = parse_url(url)
        return parsed.url
    except Exception:
        return None

# %%
def normalize_url(url: str) -> str:
    """Improved URL normalization with better error handling."""
    try:
        parsed = urlparse(url)
        
        # Convert to lowercase
        netloc = parsed.netloc.lower()
        path = parsed.path.lower()
        
        # Remove common tracking parameters
        query_params = urlparse(url).query.split('&')
        filtered_params = []
        blocked_params = {'utm_source', 'utm_medium', 'utm_campaign', 'fbclid', 'gclid'}
        
        for param in query_params:
            if '=' in param:
                name = param.split('=')[0]
                if name not in blocked_params:
                    filtered_params.append(param)
                    
        query = '&'.join(filtered_params)
        
        # Remove trailing slashes and index files
        if path.endswith(('/index.html', '/index.php', '/index.asp')):
            path = path.rsplit('/', 1)[0]
        if path.endswith('/') and len(path) > 1:
            path = path[:-1]
            
        cleaned = urlunparse((
            parsed.scheme,
            netloc,
            path,
            parsed.params,
            query,
            ''  # Remove fragments
        ))
        
        return cleaned
    except Exception:
        return url

# %%
async def crawl_for_keywords_async(
    session: aiohttp.ClientSession,
    seed_url: str,
    cert_keywords: dict,
    max_depth: int = 2,
    limit_pages: int = 50,
    politeness_delay: float = 0.5
) -> dict:
    """
    Asynchronously crawl a website starting from seed_url, searching for certification keywords.
    Uses breadth-first search (BFS) to explore links up to max_depth away from the seed URL.
    
    Args:
        session: aiohttp client session for making HTTP requests
        seed_url: Starting URL to begin crawl from
        cert_keywords: Dictionary mapping certification columns to lists of keywords
        max_depth: Maximum link depth to explore from seed_url (default: 2)
        limit_pages: Maximum total pages to fetch across all depths (default: 50)
        politeness_delay: Seconds to wait between requests to same domain (default: 0.5)
    
    Returns:
        Dictionary mapping certification columns to sets of URLs where keywords were found
    """
    # Add timeout configuration
    timeout = ClientTimeout(total=30, connect=10)
    
    # Track URLs found for each certification type
    found_urls_by_cert = {col: set() for col in cert_keywords.keys()}
    
    # Track visited URLs to avoid cycles
    visited = set()
    
    # Queue for BFS traversal - each item is (url, depth) tuple
    queue = asyncio.Queue()
    await queue.put((seed_url, 0))

    pages_crawled = 0
    
    print(f"Starting crawl from: {seed_url}")
    
    while not queue.empty():
        try:
            current_url, depth = await queue.get()
            queue.task_done()

            # Add URL validation
            current_url = clean_and_validate_url(current_url)
            if not current_url:
                continue

            # Skip if we've exceeded max depth
            if depth > max_depth:
                continue
            
            # Skip if already visited this URL
            if current_url in visited:
                continue
            visited.add(current_url)

            # Only crawl URLs on same domain as seed
            if not is_same_domain(seed_url, current_url):
                continue

            # Stop if we've hit the page limit
            if pages_crawled >= limit_pages:
                break

            # Respect crawl delay
            await asyncio.sleep(politeness_delay)

            try:
                async with session.get(current_url, timeout=10) as resp:
                    if resp.status != 200:
                        print(f"[WARNING] {current_url} returned status {resp.status}")
                        continue
                    pages_crawled += 1
                    print(f"[INFO] Crawled: {current_url} (Total pages crawled: {pages_crawled})")
                    
                    content_type = resp.headers.get('Content-Type', '').lower()
                    
                    if 'pdf' in content_type:
                        # For PDFs: download bytes, extract text, check for keywords
                        pdf_bytes = await resp.read()
                        pdf_text = extract_text_from_pdf(pdf_bytes)
                        for cert_col, kws in cert_keywords.items():
                            for kw in kws:
                                if kw.lower() in pdf_text:
                                    normalized = normalize_url(current_url) 
                                    found_urls_by_cert[cert_col].add(normalized)
                    
                    elif 'html' in content_type:
                        # For HTML: parse content, extract text, check for keywords
                        html = await resp.text(errors='ignore')
                        soup = BeautifulSoup(html, 'html.parser')
                        page_text = soup.get_text(separator=' ').lower()

                        # Check each certification's keywords against page text
                        for cert_col, kws in cert_keywords.items():
                            for kw in kws:
                                if kw.lower() in page_text:
                                    normalized = normalize_url(current_url)
                                    found_urls_by_cert[cert_col].add(normalized)

                        # If not at max depth, add all links to queue
                        if depth < max_depth:
                            for link_tag in soup.find_all("a", href=True):
                                child_url = urljoin(current_url, link_tag['href'])
                                if child_url not in visited:
                                    await queue.put((child_url, depth + 1))
                    else:
                        # Skip non-HTML/PDF content types
                        pass

            except Exception as e:
                print(f"[ERROR] {current_url} => {e}")
                continue

        except asyncio.TimeoutError:
            print(f"[TIMEOUT] {current_url}")
            continue
        except aiohttp.ClientError as e:
            print(f"[HTTP ERROR] {current_url}: {e}")
            continue
        except Exception as e:
            print(f"[UNEXPECTED ERROR] {current_url}: {e}")
            continue

    return found_urls_by_cert

# %%
async def process_df_with_sites(df_with_sites: pd.DataFrame) -> pd.DataFrame:
    """
    Process websites in parallel batches for better performance.
    """
    # Add error handling for invalid URLs
    df_with_sites['Company website'] = df_with_sites['Company website'].apply(clean_and_validate_url)
    df_with_sites = df_with_sites.dropna(subset=['Company website'])
    
    # Increase connector limit and add timeout
    timeout = ClientTimeout(total=30, connect=10)
    connector = aiohttp.TCPConnector(limit=20, force_close=True)
    
    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    ) as session:
        # Calculate optimal batch size based on DataFrame length
        batch_size = min(10, math.ceil(len(df_with_sites) / 4))
        
        # Process in batches
        for start_idx in range(0, len(df_with_sites), batch_size):
            batch = df_with_sites.iloc[start_idx:start_idx + batch_size]
            print(f"[INFO] Processing batch starting at index {start_idx} (Batch size: {len(batch)})")
            
            # Create tasks for each website in the batch
            tasks = []
            for idx, row in batch.iterrows():
                seed_url = row["Company website"]
                if not seed_url.startswith("http"):
                    seed_url = "http://" + seed_url
                
                print(f"\nQueuing: {seed_url} (index: {idx})")
                
                task = crawl_for_keywords_async(
                    session=session,
                    seed_url=seed_url,
                    cert_keywords=CERT_KEYWORDS,
                    max_depth=2,
                    limit_pages=50,
                    politeness_delay=0.5
                )
                tasks.append((idx, task))
            
            # Wait for all tasks in batch to complete
            for idx, task in tasks:
                try:
                    found_by_cert = await task
                    # Update DataFrame with results
                    for cert_col, found_urls in found_by_cert.items():
                        if found_urls:
                            existing_val = df_with_sites.at[idx, cert_col]
                            if not isinstance(existing_val, str):
                                existing_val = ""
                            old_urls = set(u.strip() for u in existing_val.split(";") if u.strip())
                            merged = old_urls.union(found_urls)
                            df_with_sites.at[idx, cert_col] = ";".join(sorted(merged))
                except Exception as e:
                    print(f"[ERROR] Failed to process index {idx}: {e}")
    
    return df_with_sites

# %%
async def fetch_with_retry(session: aiohttp.ClientSession, url: str, max_retries: int = 3) -> tuple:
    """
    Fetch a URL with retry logic for better reliability.
    Returns (content, content_type) tuple.
    """
    timeout = ClientTimeout(total=30)
    
    for attempt in range(max_retries):
        try:
            async with session.get(url, timeout=timeout) as resp:
                if resp.status == 200:
                    content_type = resp.headers.get('Content-Type', '').lower()
                    if 'pdf' in content_type:
                        content = await resp.read()
                    else:
                        content = await resp.text(errors='ignore')
                    return content, content_type
                    
                elif resp.status in {429, 503}:  # Rate limited or service unavailable
                    wait_time = min(2 ** attempt, 8)  # Exponential backoff
                    await asyncio.sleep(wait_time)
                    continue
                    
                else:
                    return None, None
                    
        except (TimeoutError, aiohttp.ClientError) as e:
            if attempt == max_retries - 1:
                print(f"[ERROR] Failed to fetch {url} after {max_retries} attempts: {e}")
                return None, None
            await asyncio.sleep(1)
            
    return None, None

# %%
async def main():
    try:
        # Load company data from Google Sheets into DataFrame
        print("Loading company data from CSV...")
        df_all = pd.read_csv("companies_info_updated.csv")
        print("Company data loaded successfully.")
        
        # Initialize and standardize certification columns as empty strings
        # This prevents dtype warnings and ensures consistent handling
        for cert_col in CERT_KEYWORDS.keys():
            if cert_col not in df_all.columns:
                df_all[cert_col] = ""
            df_all[cert_col] = df_all[cert_col].fillna("").astype(str)
        
        # Extract subset of companies that have website URLs
        # Create clean copy with stripped website URLs
        df_with_sites = df_all.dropna(subset=["Company website"]).copy()
        df_with_sites["Company website"] = df_with_sites["Company website"].str.strip()
        
        # Add progress tracking
        total_companies = len(df_with_sites)
        print(f"Processing {total_companies} companies with websites...")
        
        updated_df_with_sites = await process_df_with_sites(df_with_sites)
        
        # Add basic statistics
        certs_found = {
            cert: updated_df_with_sites[cert].str.len().gt(0).sum()
            for cert in CERT_KEYWORDS.keys()
        }
        print("\nCertifications found:")
        for cert, count in certs_found.items():
            print(f"{cert}: {count} companies")
            
    except Exception as e:
        print(f"[CRITICAL ERROR] {e}")
        raise
    finally:
        # Ensure DataFrame is saved even if there's an error
        if 'updated_df_with_sites' in locals():
            output_file = f"plants_updated_{int(time.time())}.csv"
            updated_df_with_sites.to_csv(output_file, index=False)
            print(f"\nResults saved to {output_file}")


# %%
if __name__ == "__main__":
    asyncio.run(main())


# %%
