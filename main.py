# Copyright https://github.com/VolkanSah/
import asyncio
import aiohttp
import json
import csv
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import List, Optional, Dict, Any
import logging
from urllib.parse import urljoin, urlparse
import time

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class PraxisResult:
    url: str
    timestamp: datetime
    status_code: Optional[int]
    response_time: float
    content_length: int
    title: str
    keywords_found: List[str]
    classification: str
    error: Optional[str]
    contact_info: Dict[str, str]

class PraxisMonitor:
    def __init__(self, max_concurrent=10, timeout=15):
        self.max_concurrent = max_concurrent
        self.timeout = timeout
        self.session = None
        self.semaphore = asyncio.Semaphore(max_concurrent)
        
        # Erweiterte Keyword-Listen für verschiedene Kategorien
        self.rejection_keywords = [
            "behandlung verweigert", "keine termine", "nicht versichert",
            "privatpatient bevorzugt", "kassenpatienten abgelehnt",
            "warteliste geschlossen", "aufnahmestopp"
        ]
        
        self.suspicious_keywords = [
            "sofortüberweisung erforderlich", "vorauszahlung",
            "zusatzversicherung notwendig", "igel-leistungen pflicht"
        ]
        
        self.positive_keywords = [
            "termine verfügbar", "kassenpatienten willkommen",
            "barrierefreie praxis", "notfall behandlung"
        ]

    async def __aenter__(self):
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=10)
        timeout = aiohttp.ClientTimeout(total=self.timeout)
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    def extract_contact_info(self, html_content: str) -> Dict[str, str]:
        """Extrahiert Kontaktinformationen aus HTML"""
        import re
        
        contact_info = {}
        
        # Telefonnummer-Pattern
        phone_pattern = r'(?:\+49|0)\s*\d{2,5}[\s\-/]*\d{3,8}[\s\-/]*\d{0,8}'
        phone_matches = re.findall(phone_pattern, html_content)
        if phone_matches:
            contact_info['phone'] = phone_matches[0].strip()
            
        # Email-Pattern
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        email_matches = re.findall(email_pattern, html_content)
        if email_matches:
            contact_info['email'] = email_matches[0].strip()
            
        # Adresse (vereinfacht)
        address_patterns = [
            r'\d{5}\s+[A-Za-zäöüß\s]+',  # PLZ + Ort
            r'[A-Za-zäöüß\s]+\s+\d{1,3}[a-z]?'  # Straße + Hausnummer
        ]
        
        for pattern in address_patterns:
            matches = re.findall(pattern, html_content)
            if matches:
                contact_info['address'] = matches[0].strip()
                break
                
        return contact_info

    def analyze_content(self, content: str) -> tuple[List[str], str]:
        """Analysiert den Seiteninhalt und klassifiziert die Praxis"""
        content_lower = content.lower()
        found_keywords = []
        
        # Keywords sammeln
        for keyword in self.rejection_keywords:
            if keyword in content_lower:
                found_keywords.append(f"REJECTION: {keyword}")
                
        for keyword in self.suspicious_keywords:
            if keyword in content_lower:
                found_keywords.append(f"SUSPICIOUS: {keyword}")
                
        for keyword in self.positive_keywords:
            if keyword in content_lower:
                found_keywords.append(f"POSITIVE: {keyword}")
        
        # Klassifikation basierend auf gefundenen Keywords
        if any("REJECTION:" in kw for kw in found_keywords):
            classification = "PROBLEMATISCH"
        elif any("SUSPICIOUS:" in kw for kw in found_keywords):
            classification = "VERDÄCHTIG"
        elif any("POSITIVE:" in kw for kw in found_keywords):
            classification = "POSITIV"
        else:
            classification = "NEUTRAL"
            
        return found_keywords, classification

    def extract_title(self, html_content: str) -> str:
        """Extrahiert den Seitentitel"""
        import re
        title_match = re.search(r'<title[^>]*>([^<]*)</title>', html_content, re.IGNORECASE)
        return title_match.group(1).strip() if title_match else "Kein Titel"

    async def check_single_praxis(self, url: str) -> PraxisResult:
        """Überprüft eine einzelne Praxis"""
        async with self.semaphore:
            start_time = time.time()
            
            try:
                async with self.session.get(url) as response:
                    content = await response.text()
                    response_time = time.time() - start_time
                    
                    title = self.extract_title(content)
                    keywords_found, classification = self.analyze_content(content)
                    contact_info = self.extract_contact_info(content)
                    
                    result = PraxisResult(
                        url=url,
                        timestamp=datetime.now(),
                        status_code=response.status,
                        response_time=response_time,
                        content_length=len(content),
                        title=title,
                        keywords_found=keywords_found,
                        classification=classification,
                        error=None,
                        contact_info=contact_info
                    )
                    
                    logger.info(f"✅ {url} - {classification} ({response.status})")
                    return result
                    
            except asyncio.TimeoutError:
                error_msg = "Timeout"
            except aiohttp.ClientError as e:
                error_msg = f"Client Error: {str(e)}"
            except Exception as e:
                error_msg = f"Unbekannter Fehler: {str(e)}"
            
            logger.warning(f"❌ {url} - {error_msg}")
            
            return PraxisResult(
                url=url,
                timestamp=datetime.now(),
                status_code=None,
                response_time=time.time() - start_time,
                content_length=0,
                title="",
                keywords_found=[],
                classification="FEHLER",
                error=error_msg,
                contact_info={}
            )

    async def monitor_praxen(self, urls: List[str]) -> List[PraxisResult]:
        """Überwacht mehrere Praxen parallel"""
        logger.info(f"Starte Monitoring von {len(urls)} Praxen...")
        
        tasks = [self.check_single_praxis(url) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Exception-Handling für gather
        valid_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Task für {urls[i]} fehlgeschlagen: {result}")
                # Erstelle Fehler-Result
                error_result = PraxisResult(
                    url=urls[i],
                    timestamp=datetime.now(),
                    status_code=None,
                    response_time=0,
                    content_length=0,
                    title="",
                    keywords_found=[],
                    classification="TASK_FEHLER",
                    error=str(result),
                    contact_info={}
                )
                valid_results.append(error_result)
            else:
                valid_results.append(result)
        
        return valid_results

    def save_results(self, results: List[PraxisResult], 
                    json_file: str = None, csv_file: str = None):
        """Speichert Ergebnisse in verschiedenen Formaten"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if json_file is None:
            json_file = f"praxis_monitoring_{timestamp}.json"
            
        if csv_file is None:
            csv_file = f"praxis_monitoring_{timestamp}.csv"
        
        # JSON Export
        json_data = [asdict(result) for result in results]
        for item in json_data:
            item['timestamp'] = item['timestamp'].isoformat()
            
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)
        
        # CSV Export
        if results:
            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                
                # Header
                writer.writerow([
                    'URL', 'Timestamp', 'Status_Code', 'Response_Time', 
                    'Classification', 'Title', 'Keywords_Found', 
                    'Phone', 'Email', 'Address', 'Error'
                ])
                
                # Daten
                for result in results:
                    writer.writerow([
                        result.url,
                        result.timestamp.isoformat(),
                        result.status_code,
                        f"{result.response_time:.2f}",
                        result.classification,
                        result.title,
                        '; '.join(result.keywords_found),
                        result.contact_info.get('phone', ''),
                        result.contact_info.get('email', ''),
                        result.contact_info.get('address', ''),
                        result.error or ''
                    ])
        
        logger.info(f"Ergebnisse gespeichert: {json_file}, {csv_file}")

    def generate_report(self, results: List[PraxisResult]) -> str:
        """Generiert einen zusammenfassenden Report"""
        total = len(results)
        successful = len([r for r in results if r.status_code and r.status_code < 400])
        
        classifications = {}
        for result in results:
            classifications[result.classification] = classifications.get(result.classification, 0) + 1
        
        avg_response_time = sum(r.response_time for r in results) / total if total > 0 else 0
        
        report = f"""
=== PRAXIS MONITORING REPORT ===
Zeitpunkt: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Übersicht:
- Gesamtzahl URLs: {total}
- Erfolgreich abgerufen: {successful} ({successful/total*100:.1f}%)
- Durchschnittliche Antwortzeit: {avg_response_time:.2f}s

Klassifikationen:
"""
        for classification, count in sorted(classifications.items()):
            percentage = count/total*100 if total > 0 else 0
            report += f"- {classification}: {count} ({percentage:.1f}%)\n"
        
        # Top problematische Praxen
        problematic = [r for r in results if r.classification in ['PROBLEMATISCH', 'VERDÄCHTIG']]
        if problematic:
            report += f"\nProblematische Praxen ({len(problematic)}):\n"
            for result in problematic[:10]:  # Top 10
                report += f"- {result.url}: {result.classification}\n"
                if result.keywords_found:
                    report += f"  Keywords: {', '.join(result.keywords_found[:3])}\n"
        
        return report


# Beispiel-Usage
async def main():
    # Test-URLs (echte Praxis-URLs einsetzen)
    praxis_urls = [
        "https://example-praxis1.de",
        "https://example-praxis2.de",
        # ... weitere URLs
    ]
    
    async with PraxisMonitor(max_concurrent=5, timeout=10) as monitor:
        results = await monitor.monitor_praxen(praxis_urls)
        
        # Ergebnisse speichern
        monitor.save_results(results)
        
        # Report generieren und ausgeben
        report = monitor.generate_report(results)
        print(report)
        
        # Zusätzliche Auswertungen
        problematic_praxen = [r for r in results if r.classification == 'PROBLEMATISCH']
        print(f"\n{len(problematic_praxen)} problematische Praxen gefunden.")

if __name__ == "__main__":
    asyncio.run(main())
