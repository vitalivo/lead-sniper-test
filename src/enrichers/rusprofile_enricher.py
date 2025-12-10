"""Enricher for getting INN and financial data from Rusprofile."""
import requests
from bs4 import BeautifulSoup
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


class RusprofileEnricher:
    """Enricher for Rusprofile data."""
    
    BASE_URL = "https://www.rusprofile.ru"
    SEARCH_URL = f"{BASE_URL}/search"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        })
    
    @rate_limit(delay=3)
    def search_company(self, company_name: str) -> Optional[Dict]:
        """Search for company on Rusprofile and get basic info."""
        try:
            logger.info(f"Searching for: {company_name}")
            
            # Поиск компании
            response = self.session.get(
                self.SEARCH_URL,
                params={'query': company_name, 'type': 'ul'},
                timeout=30
            )
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            # Ищем первый результат поиска
            first_result = soup.find('div', class_='company-item')
            if not first_result:
                logger.warning(f"No results found for: {company_name}")
                return None
            
            # Извлекаем данные
            data = self._parse_search_result(first_result)
            
            if data:
                logger.success(f"Found: {data.get('name')} (ИНН: {data.get('inn')})")
                return data
            
            return None
            
        except Exception as e:
            logger.error(f"Error searching {company_name}: {e}")
            return None
    
    def _parse_search_result(self, result_elem) -> Optional[Dict]:
        """Parse company data from search result element."""
        try:
            data = {}
            
            # Название
            name_elem = result_elem.find('a', class_='company-name')
            if name_elem:
                data['name'] = clean_text(name_elem.get_text())
            
            # ИНН
            inn_elem = result_elem.find('div', class_='company-inn')
            if inn_elem:
                inn_text = clean_text(inn_elem.get_text())
                inn_match = re.search(r'\d{10,12}', inn_text)
                if inn_match:
                    data['inn'] = clean_inn(inn_match.group())
            
            # ОГРН
            ogrn_elem = result_elem.find('div', class_='company-ogrn')
            if ogrn_elem:
                ogrn_text = clean_text(ogrn_elem.get_text())
                ogrn_match = re.search(r'\d{13,15}', ogrn_text)
                if ogrn_match:
                    data['ogrn'] = ogrn_match.group()
            
            # Регион
            region_elem = result_elem.find('div', class_='company-region')
            if region_elem:
                data['region'] = clean_text(region_elem.get_text())
            
            # Статус
            status_elem = result_elem.find('div', class_='company-status')
            if status_elem:
                data['status'] = clean_text(status_elem.get_text())
            
            return data if data.get('inn') else None
            
        except Exception as e:
            logger.error(f"Error parsing search result: {e}")
            return None
    
    @rate_limit(delay=3)
    def get_company_details(self, inn: str) -> Optional[Dict]:
        """Get detailed company info by INN."""
        try:
            logger.info(f"Getting details for INN: {inn}")
            
            # Страница компании
            company_url = f"{self.BASE_URL}/{inn}"
            response = self.session.get(company_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            details = {}
            
            # Выручка (обычно в блоке финансовой отчётности)
            revenue_elem = soup.find('div', string=re.compile('Выручка'))
            if revenue_elem:
                revenue_value = revenue_elem.find_next('div', class_='value')
                if revenue_value:
                    revenue_text = clean_text(revenue_value.get_text())
                    details['revenue'] = normalize_revenue(revenue_text)
                    
                    # Год отчётности
                    year_elem = soup.find('div', class_='report-year')
                    if year_elem:
                        year_text = clean_text(year_elem.get_text())
                        year_match = re.search(r'20\d{2}', year_text)
                        if year_match:
                            details['revenue_year'] = int(year_match.group())
            
            # ОКВЭД
            okved_elem = soup.find('div', string=re.compile('ОКВЭД'))
            if okved_elem:
                okved_value = okved_elem.find_next('div', class_='value')
                if okved_value:
                    okved_text = clean_text(okved_value.get_text())
                    okved_match = re.search(r'\d{2}\.\d{2}', okved_text)
                    if okved_match:
                        details['okved_main'] = okved_match.group()
            
            # Количество сотрудников
            employees_elem = soup.find('div', string=re.compile('Среднесписочная численность'))
            if employees_elem:
                emp_value = employees_elem.find_next('div', class_='value')
                if emp_value:
                    emp_text = clean_text(emp_value.get_text())
                    emp_match = re.search(r'\d+', emp_text)
                    if emp_match:
                        details['employees'] = int(emp_match.group())
            
            # Сайт
            site_elem = soup.find('a', class_='company-website')
            if site_elem:
                details['site'] = site_elem.get('href', '').strip()
            
            # Телефон
            phone_elem = soup.find('div', class_='company-phone')
            if phone_elem:
                details['contacts'] = clean_text(phone_elem.get_text())
            
            return details
            
        except Exception as e:
            logger.error(f"Error getting details for INN {inn}: {e}")
            return None
    
    def enrich_companies(self, companies: List[Dict]) -> List[Dict]:
        """Enrich list of companies with INN and financial data."""
        logger.info(f"Enriching {len(companies)} companies with Rusprofile data...")
        
        enriched = []
        
        for idx, company in enumerate(companies, 1):
            logger.info(f"Processing {idx}/{len(companies)}: {company['name']}")
            
            # Поиск компании
            search_result = self.search_company(company['name'])
            
            if not search_result or not search_result.get('inn'):
                logger.warning(f"Could not find INN for: {company['name']}")
                continue
            
            # Объединяем данные
            enriched_company = {**company, **search_result}
            
            # Получаем детальную информацию
            details = self.get_company_details(search_result['inn'])
            if details:
                enriched_company.update(details)
            
            enriched.append(enriched_company)
            
            # Пауза для избежания блокировки
            if idx % 10 == 0:
                logger.info(f"Processed {idx} companies, taking a short break...")
                time.sleep(5)
        
        logger.success(f"Successfully enriched {len(enriched)}/{len(companies)} companies")
        
        # Сохраняем промежуточный результат
        self.save_enriched_data(enriched)
        
        return enriched
    
    def save_enriched_data(self, companies: List[Dict]):
        """Save enriched data to JSON."""
        output_file = INTERIM_DATA_DIR / "enriched_rusprofile.json"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(companies, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Saved enriched data to {output_file}")


if __name__ == "__main__":
    # Тестовый запуск
    enricher = RusprofileEnricher()
    
    # Тест на одной компании
    test_company = {"name": "Группа АДВ", "segment_tag": "BTL", "source": "test"}
    result = enricher.search_company(test_company['name'])
    
    if result:
        print("\nSearch result:")
        print(json.dumps(result, ensure_ascii=False, indent=2))
        
        if result.get('inn'):
            details = enricher.get_company_details(result['inn'])
            if details:
                print("\nDetails:")
                print(json.dumps(details, ensure_ascii=False, indent=2))
