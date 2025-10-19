import requests
import time
import json
from typing import Set, Dict


# def get_backlinks(page_title, limit=500):
#     url = "https://en.wikipedia.org/w/api.php"
#
#     params = {
#         'action': 'query',
#         'format': 'json',
#         'list': 'backlinks',
#         'bltitle': page_title,
#         'bllimit': limit,
#         'blnamespace': 0,
#     }
#
#     headers = {
#         'User-Agent': ''
#     }
#
#
#     response = requests.get(url, params=params, headers=headers)
#
#     if response.status_code != 200:
#         raise Exception(f"API request failed with status {response.status_code}")
#
#     try:
#         data = response.json()
#     except Exception as e:
#         print(f"Failed to parse JSON. Raw response: {response.text}")
#         raise e
#
#     pages = []
#     if 'query' in data and 'backlinks' in data['query']:
#         for result in data['query']['backlinks']:
#             page_name = result['title'].replace(' ', '_')
#             pages.append(page_name)
#     else:
#         print(f"Unexpected API structure: {data}")
#
#     return pages
#
#
# try:
#     amazon_pages = get_backlinks('Amazon_(company)', limit=50)
#     print(f"\nFound {len(amazon_pages)} pages")
#
# except Exception as e:
#     print(f"Error: {e}")


USER_AGENT = 'CoreSentimentBot/1.0 (Educational Project) Python/requests'

def get_category_members(category: str, depth: int = 1) -> Set[str]:
    url = "https://en.wikipedia.org/w/api.php"
    headers = {'User-Agent': USER_AGENT}

    pages = set()
    subcategories = set()

    params = {
        'action': 'query',
        'format': 'json',
        'list': 'categorymembers',
        'cmtitle': f'Category:{category}',
        'cmlimit': 500,
        'cmtype': 'page|subcat',
    }

    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()

        if 'query' in data and 'categorymembers' in data['query']:
            for member in data['query']['categorymembers']:
                title = member['title'].replace(' ', '_')

                if member['ns'] == 14:
                    subcat_name = title.replace('Category:', '')
                    subcategories.add(subcat_name)
                else:
                    pages.add(title)

        if depth > 0:
            for subcat in subcategories:
                print(f"  → Recursing into subcategory: {subcat}")
                subpages = get_category_members(subcat, depth=depth - 1)
                pages.update(subpages)
                time.sleep(0.5)

    except Exception as e:
        print(f"Error fetching category '{category}': {e}")

    return pages



COMPANIES = {
    'Amazon': 'Amazon (company)',
    'Apple': 'Apple_Inc.',
    'Meta': 'Meta_Platforms',
    'Google': 'Google',
    'Microsoft': 'Microsoft',
}

company_pages = {}

for company, category in COMPANIES.items():
    print(f"Fetching pages for {company} (Category:{category})")

    pages = get_category_members(category, depth=1)

    filtered_pages = set()
    for page in pages:
        if 'disambiguation' in page.lower():
            continue
        filtered_pages.add(page)

    company_pages[company] = sorted(filtered_pages)
    print(f"Found {len(filtered_pages)} pages for {company}")


with open('company_pages.txt', 'w', encoding='utf-8') as f:
    for company, pages in company_pages.items():
        f.write(f"{company} ({len(pages)} pages)\n")
        f.write(f"{'-' * 20}\n")
        for page in pages:
            f.write(f"  {page}\n")

