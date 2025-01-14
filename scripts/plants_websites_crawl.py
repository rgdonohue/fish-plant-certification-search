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

# Uncomment the following line to enable nest_asyncio for running async code in Jupyter notebooks
# nest_asyncio.apply() 

# %%
# Mapping of certification types to their associated keywords
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
    Extracts text from a PDF file, with enhanced error handling and memory management.
    
    Args:
        pdf_content: The raw bytes of the PDF file.
    
    Returns:
        A string containing the extracted text in lowercase, or an empty string if extraction fails.
    """
    text = ""
    try:
        # Check PDF size to prevent excessive memory usage (limit set to 10MB)
        if len(pdf_content) > 10 * 1024 * 1024:  # 10MB limit
            print("[PDF ERROR] File too large")
            return ""
            
        with io.BytesIO(pdf_content) as pdf_file:
            try:
                reader = PyPDF2.PdfReader(pdf_file)
                
                # Process a maximum of 50 pages from the PDF
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
    """
    Checks if the new URL belongs to the same domain or subdomain as the base URL.
    
    Args:
        base_url: The base URL to compare against.
        new_url: The new URL to check.
    
    Returns:
        True if new_url is on the same domain or subdomain as base_url, otherwise False.
    """
    base_domain = urlparse(base_url).netloc
    check_domain = urlparse(new_url).netloc
    # A loose check: ensure the base domain is a substring of check_domain
    # e.g. base: "example.com", check: "sub.example.com" => True
    return base_domain in check_domain

# %%
def clean_and_validate_url(url: str) -> str:
    """
    Cleans and validates a URL, ensuring it is well-formed and returns None if invalid.
    
    Args:
        url: The URL string to be cleaned and validated.
    
    Returns:
        A cleaned and validated URL string, or None if the URL is invalid.
    """
    if not url:
        return None
        
    # Ensure the URL has a valid protocol (http or https)
    if not url.startswith(('http://', 'https://')):
        url = 'http://' + url
        
    try:
        # Perform basic URL validation
        if not validators.url(url):
            return None
            
        # Parse and reconstruct the URL to normalize it
        parsed = parse_url(url)
        return parsed.url
    except Exception:
        return None

# %%
def normalize_url(url: str) -> str:
    """
    Normalizes a URL by converting it to lowercase and removing unnecessary components.
    
    Args:
        url: The URL string to be normalized.
    
    Returns:
        A normalized URL string, or the original URL if normalization fails.
    """
    try:
        parsed = urlparse(url)
        
        # Convert the network location and path to lowercase
        netloc = parsed.netloc.lower()
        path = parsed.path.lower()
        
        # Remove common tracking parameters from the query string
        query_params = urlparse(url).query.split('&')
        filtered_params = []
        blocked_params = {'utm_source', 'utm_medium', 'utm_campaign', 'fbclid', 'gclid'}
        
        for param in query_params:
            if '=' in param:
