"""Enricher for getting INN and financial data from list-org.com."""
import requests
from typing import Dict, Optional, List
import json
from pathlib import Path
import time
import re
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.logger import logger
from utils.helpers import rate_limit, clean_text, clean_inn, normalize_revenue
from config import RAW_DATA_DIR, INTERIM_DATA_DIR


class ListOrgEnricher:
    """Enricher using list-org.com API."""
    BASE_URL = "https://api.list-org.com"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
        })

    @rate_limit(delay=3)
    def search_company(self, company_name: str) -> Optional[Dict]:
        """Search company by name."""
        try:
            url = f"{self.BASE_URL}/api/search"
            params = {'query': company_name}
            response = self.session.get(url, params=params, timeout=30)
            if response.status_code != 200:
                logger.warning(f"List-Org API error {response.status_code}: {response.text}")
                return None

            data = response.json()
            if not data or 'result' not in data or not data['result']:
                return None

            first_match = data['result'][0]
            inn = first_match.get('inn', '')
            if not inn:
                return None

            return {
                'inn': clean_inn(inn),
                'name': clean_text(first_match.get('name', '')),
                'region': clean_text(first_match.get('region', '')),
                'status': clean_text(first_match.get('state', '')),
                'ogrn': clean_text(first_match.get('ogrn', '')),
                'okved_main': clean_text(first_match.get('okved', '')[:5])  # первые 5 символов
            }
        except Exception as e:
            logger.error(f"Error searching {company_name} on list-org: {e}")
            return None

    @rate_limit(delay=3)
    def get_financials(self, inn: str) -> Optional[Dict]:
        """Get revenue and employees from financial reports."""
        try:
            url = f"{self.BASE_URL}/api/company"
            params = {'inn': inn}
            response = self.session.get(url, params=params, timeout=30)
            if response.status_code != 200:
                return None

            data = response.json()
            result = {}

            # Выручка из последнего финансового отчёта
            if 'finance' in data and data['finance']:
                latest_report = data['finance'][0]  # самый свежий
                revenue_str = latest_report.get('revenue', '')
                if revenue_str:
                    result['revenue'] = normalize_revenue(revenue_str)
                    result['revenue_year'] = int(latest_report.get('year', 2023))

            # Сотрудники
            employees = data.get('employees', '')
            if employees and re.search(r'\d+', employees):
                result['employees'] = int(re.search(r'\d+', employees).group())

            # Сайт
            website = data.get('website', '')
            if website:
                result['site'] = website.strip()

            return result
        except Exception as e:
            logger.error(f"Error fetching financials for INN {inn}: {e}")
            return None

    def enrich_companies(self, companies: List[Dict]) -> List[Dict]:
        """Main enrichment method."""
        logger.info(f"Enriching {len(companies)} companies via list-org.com...")
        enriched = []

        for idx, company in enumerate(companies, 1):
            logger.info(f"Processing {idx}/{len(companies)}: {company['name']}")

            search_result = self.search_company(company['name'])
            if not search_result or not search_result['inn']:
                logger.warning(f"No match found for: {company['name']}")
                continue

            enriched_company = {**company, **search_result}

            # Дополним финансовыми данными
            financials = self.get_financials(search_result['inn'])
            if financials:
                enriched_company.update(financials)

            enriched.append(enriched_company)

            if idx % 10 == 0:
                time.sleep(5)

        logger.success(f"Enriched {len(enriched)} out of {len(companies)} companies")
        self.save_enriched_data(enriched)
        return enriched

    def save_enriched_data(self, companies: List[Dict]):
        """Save to interim folder."""
        output_file = INTERIM_DATA_DIR / "enriched_listorg.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(companies, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved enriched data to {output_file}")


if __name__ == "__main__":
    # Тест
    enricher = ListOrgEnricher()
    test_company = {"name": "Группа АДВ", "segment_tag": "BTL", "source": "test"}
    result = enricher.search_company(test_company["name"])
    print("Search result:", result)
    if result:
        fin = enricher.get_financials(result["inn"])
        print("Financials:", fin)
