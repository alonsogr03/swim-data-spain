#!/usr/bin/env python3
"""
Script de Web Scraping para la Federación Madrileña de Natación (FMN)

Descarga archivos ZIP con resultados (.res) de competiciones de natación desde:
https://www.federacionmadridnatacion.es/index.php/competiciones-natacion

El script navega por todas las páginas del calendario de competiciones,
entra en cada una, y busca el enlace ".res" que apunta al ZIP con los
archivos de resultados.

Autor: Alonso González Romero
Fecha: 2026
"""

import os
import re
import time
import json
import logging
from datetime import datetime
from urllib.parse import urljoin, urlparse, unquote
from pathlib import Path
from typing import Optional, List, Dict, Set, Tuple

import requests
from bs4 import BeautifulSoup

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class FMNScraper:
    """
    Scraper para descargar archivos ZIP con resultados (.res) de la FMN.
    
    La web tiene la siguiente estructura:
    - Página principal con calendario paginado (?page=1, ?page=2, etc.)
    - Cada competición tiene una página con tabla que incluye enlaces a:
      - Normativa (PDF)
      - Series (PDF) 
      - Resultados (PDF)
      - .res (ZIP con archivos .res dentro) <- ESTO ES LO QUE QUEREMOS
    """

    BASE_URL = "https://www.federacionmadridnatacion.es"
    CALENDAR_URL = f"{BASE_URL}/index.php/competiciones-natacion"

    def __init__(
        self,
        output_base_dir: str,
        delay_between_requests: float = 1.0,
        max_pages: Optional[int] = None,
        start_year: int = 2018,
        end_year: Optional[int] = None
    ):
        """
        Inicializa el scraper.

        Args:
            output_base_dir: Directorio base donde guardar los archivos (data/raw/FMN/natacion)
            delay_between_requests: Segundos de espera entre peticiones
            max_pages: Número máximo de páginas (None = detectar automáticamente)
            start_year: Año inicial para filtrar competiciones
            end_year: Año final para filtrar competiciones (None = año actual)
        """
        self.output_base_dir = Path(output_base_dir)
        self.delay = delay_between_requests
        self.max_pages = max_pages  # None = dinámico
        self.start_year = start_year
        self.end_year = end_year or datetime.now().year

        # Sesión HTTP para mantener cookies
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
        })

        # Conjuntos para evitar duplicados
        self.processed_competitions: Set[str] = set()
        self.downloaded_files: Set[str] = set()
        
        # Archivo de registro para persistir competiciones descargadas
        self.registry_file = self.output_base_dir / ".downloaded_competitions.json"
        self._load_registry()

        # Estadísticas
        self.stats = {
            'pages_scraped': 0,
            'competitions_found': 0,
            'competitions_already_downloaded': 0,
            'competitions_with_res': 0,
            'files_downloaded': 0,
            'files_skipped': 0,
            'errors': 0
        }

    def _load_registry(self) -> None:
        """Carga el registro de competiciones ya descargadas."""
        if self.registry_file.exists():
            try:
                with open(self.registry_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.downloaded_files = set(data.get('downloaded_urls', []))
                    self.processed_competitions = set(data.get('competition_ids', []))
                logger.info(f"Registro cargado: {len(self.processed_competitions)} competiciones previas")
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"Error al cargar registro: {e}")
                self.downloaded_files = set()
                self.processed_competitions = set()

    def _save_registry(self) -> None:
        """Guarda el registro de competiciones descargadas."""
        try:
            self.output_base_dir.mkdir(parents=True, exist_ok=True)
            with open(self.registry_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'downloaded_urls': list(self.downloaded_files),
                    'competition_ids': list(self.processed_competitions),
                    'last_updated': datetime.now().isoformat()
                }, f, indent=2, ensure_ascii=False)
        except IOError as e:
            logger.error(f"Error al guardar registro: {e}")

    def _make_request(self, url: str, stream: bool = False) -> Optional[requests.Response]:
        """Realiza una petición HTTP con manejo de errores."""
        try:
            time.sleep(self.delay)
            response = self.session.get(url, timeout=30, stream=stream)
            response.raise_for_status()
            return response
        except requests.exceptions.Timeout:
            logger.error(f"Timeout: {url}")
            self.stats['errors'] += 1
        except requests.exceptions.ConnectionError:
            logger.error(f"Error de conexión: {url}")
            self.stats['errors'] += 1
        except requests.exceptions.HTTPError as e:
            logger.error(f"Error HTTP {e.response.status_code}: {url}")
            self.stats['errors'] += 1
        except requests.exceptions.RequestException as e:
            logger.error(f"Error: {e}")
            self.stats['errors'] += 1
        return None

    def _extract_year_from_text(self, text: str) -> int:
        """Extrae el año de la temporada del texto (ej: '22-23' -> 2022)."""
        # Buscar patrón de temporada XX-YY
        season_match = re.search(r'(\d{2})-(\d{2})', text)
        if season_match:
            year = int('20' + season_match.group(1))
            if self.start_year <= year <= self.end_year:
                return year
        
        # Buscar año explícito 20XX
        year_match = re.search(r'(20\d{2})', text)
        if year_match:
            return int(year_match.group(1))
        
        return datetime.now().year

    def _sanitize_filename(self, name: str) -> str:
        """Limpia un nombre para usarlo como nombre de archivo."""
        # Reemplazar caracteres no válidos
        name = re.sub(r'[<>:"/\\|?*]', '_', name)
        name = re.sub(r'\s+', '_', name)
        name = re.sub(r'_+', '_', name)
        name = name.strip('._-')
        return name[:100] if len(name) > 100 else name

    def _extract_date_from_page(self, soup: BeautifulSoup) -> Optional[str]:
        """Extrae la fecha de la competición del contenido de la página."""
        text = soup.get_text()
        
        # Buscar patrón "Sábado, 31 de Enero de 2026" o similar
        months_es = {
            'enero': '01', 'febrero': '02', 'marzo': '03', 'abril': '04',
            'mayo': '05', 'junio': '06', 'julio': '07', 'agosto': '08',
            'septiembre': '09', 'octubre': '10', 'noviembre': '11', 'diciembre': '12'
        }
        
        pattern = r'(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})'
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            day = match.group(1).zfill(2)
            month_name = match.group(2).lower()
            year = match.group(3)
            if month_name in months_es:
                return f"{year}-{months_es[month_name]}-{day}"
        
        return None

    def _get_competitions_from_page(self, page_num: int) -> List[Dict]:
        """Obtiene las competiciones de una página del calendario."""
        competitions = []
        
        if page_num == 1:
            url = self.CALENDAR_URL
        else:
            url = f"{self.CALENDAR_URL}?page={page_num}"
        
        response = self._make_request(url)
        if not response:
            return competitions

        soup = BeautifulSoup(response.text, 'html.parser')

        # Buscar enlaces a competiciones individuales
        # Formato: /index.php/competiciones-natacion/XXX-nombre-competicion
        for link in soup.find_all('a', href=True):
            href = link['href']
            
            # Solo enlaces de competiciones con ID numérico
            if '/competiciones-natacion/' not in href:
                continue
            
            match = re.search(r'/competiciones-natacion/(\d+)-(.+?)(?:/|$)', href)
            if not match:
                continue
            
            comp_id = match.group(1)
            
            # Verificar si ya fue descargada previamente (del registro)
            if comp_id in self.processed_competitions:
                self.stats['competitions_already_downloaded'] += 1
                continue
            
            # Construir URL completa
            full_url = urljoin(self.BASE_URL, href.split('?')[0])
            
            # Obtener título
            title = link.get_text(strip=True)
            if not title or len(title) < 5:
                continue
            
            # Limpiar título (a veces viene con texto adicional)
            title = re.sub(r'^NATACIÓN', '', title).strip()
            
            # Extraer año de temporada (para filtrado inicial)
            season_year = self._extract_year_from_text(title)
            
            # Filtrar por rango de años
            if not (self.start_year <= season_year <= self.end_year):
                continue
            
            # Añadir a lista temporal (no marcar como procesada aún)
            if comp_id not in [c['id'] for c in competitions]:
                competitions.append({
                    'id': comp_id,
                    'url': full_url,
                    'title': title,
                    'season_year': season_year
                })

        return competitions

    def _find_res_zip_link(self, competition_url: str) -> Optional[Tuple[str, str]]:
        """
        Busca el enlace al archivo ZIP con .res en la página de la competición.
        
        Returns:
            Tupla (url_zip, fecha) o None si no se encuentra
        """
        response = self._make_request(competition_url)
        if not response:
            return None

        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extraer fecha de la página
        date_str = self._extract_date_from_page(soup)
        
        # Buscar enlaces con texto ".res" que apunten a archivos .zip
        for link in soup.find_all('a', href=True):
            href = link['href']
            text = link.get_text(strip=True).lower()
            
            # El enlace debe tener texto ".res" y apuntar a un .zip
            if text == '.res' and '.zip' in href.lower():
                zip_url = urljoin(self.BASE_URL, href)
                return (zip_url, date_str)
        
        # Alternativa: buscar cualquier enlace .zip en la tabla de la competición
        for link in soup.find_all('a', href=True):
            href = link['href']
            if '.zip' in href.lower() and '/res' in href.lower():
                zip_url = urljoin(self.BASE_URL, href)
                return (zip_url, date_str)
        
        return None

    def _download_file(self, url: str, save_path: Path) -> bool:
        """Descarga un archivo y lo guarda en la ruta especificada."""
        if url in self.downloaded_files:
            logger.info(f"    Ya descargado anteriormente")
            self.stats['files_skipped'] += 1
            return False

        if save_path.exists():
            logger.info(f"    Archivo ya existe: {save_path.name}")
            self.stats['files_skipped'] += 1
            self.downloaded_files.add(url)
            return False

        try:
            response = self._make_request(url, stream=True)
            if not response:
                return False

            # Verificar que es un archivo ZIP
            content_type = response.headers.get('Content-Type', '')
            if 'text/html' in content_type:
                logger.warning(f"    No es un archivo ZIP válido")
                return False

            # Crear directorio
            save_path.parent.mkdir(parents=True, exist_ok=True)

            # Descargar
            with open(save_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            file_size = save_path.stat().st_size
            logger.info(f"    ✓ Descargado: {save_path.name} ({file_size / 1024:.1f} KB)")
            
            self.downloaded_files.add(url)
            self.stats['files_downloaded'] += 1
            return True

        except IOError as e:
            logger.error(f"    ✗ Error al guardar: {e}")
            self.stats['errors'] += 1
            return False

    def _process_competition(self, competition: Dict) -> None:
        """Procesa una competición: busca y descarga el ZIP con .res."""
        logger.info(f"  [{competition['id']}] {competition['title'][:60]}")
        
        # Buscar enlace al ZIP
        result = self._find_res_zip_link(competition['url'])
        
        if not result:
            logger.info(f"    - No tiene archivo .res")
            # Marcar como procesada aunque no tenga .res (para no volver a revisar)
            self.processed_competitions.add(competition['id'])
            return
        
        zip_url, date_str = result
        self.stats['competitions_with_res'] += 1
        
        # Determinar el año REAL de la competición desde la fecha
        if date_str:
            # Extraer año de la fecha (formato: YYYY-MM-DD)
            competition_year = int(date_str.split('-')[0])
            filename = f"{date_str}__{self._sanitize_filename(competition['title'])}.zip"
        else:
            # Fallback al año de temporada si no hay fecha
            competition_year = competition['season_year']
            filename = f"{competition_year}__{self._sanitize_filename(competition['title'])}.zip"
        
        # Ruta de guardado usando el año REAL
        year_dir = self.output_base_dir / str(competition_year)
        save_path = year_dir / filename
        
        # Descargar
        if self._download_file(zip_url, save_path):
            # Solo marcar como procesada si se descargó correctamente
            self.processed_competitions.add(competition['id'])
        else:
            # También marcar si ya existía
            self.processed_competitions.add(competition['id'])

    def _detect_total_pages(self) -> int:
        """Detecta el número total de páginas del calendario."""
        response = self._make_request(self.CALENDAR_URL)
        if not response:
            return 100  # Fallback conservador
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Buscar el enlace a la última página (normalmente muestra el número)
        max_page = 1
        for link in soup.find_all('a', href=True):
            href = link['href']
            text = link.get_text(strip=True)
            
            # Buscar enlaces de paginación con números
            if '?page=' in href and text.isdigit():
                page_num = int(text)
                if page_num > max_page:
                    max_page = page_num
        
        logger.info(f"Detectadas {max_page} páginas en el calendario")
        return max_page

    def run(self) -> Dict:
        """Ejecuta el proceso completo de scraping."""
        logger.info("=" * 60)
        logger.info("SCRAPER FMN - Federación Madrileña de Natación")
        logger.info("=" * 60)
        logger.info(f"Directorio de salida: {self.output_base_dir}")
        logger.info(f"Rango de años: {self.start_year} - {self.end_year}")
        logger.info(f"Competiciones ya descargadas: {len(self.processed_competitions)}")
        
        # Detectar páginas automáticamente si no se especificó
        if self.max_pages is None:
            self.max_pages = self._detect_total_pages()
        else:
            logger.info(f"Máximo de páginas (manual): {self.max_pages}")
        
        logger.info("=" * 60)

        # Crear directorio base
        self.output_base_dir.mkdir(parents=True, exist_ok=True)

        # Iterar por todas las páginas del calendario
        page = 1
        consecutive_empty = 0
        
        while page <= self.max_pages:
            logger.info(f"\n--- Página {page}/{self.max_pages} del calendario ---")
            
            competitions = self._get_competitions_from_page(page)
            self.stats['pages_scraped'] += 1
            
            if not competitions:
                # Verificar si hay competiciones pero todas ya descargadas
                if self.stats['competitions_already_downloaded'] > 0:
                    logger.info("Todas las competiciones de esta página ya fueron descargadas")
                    consecutive_empty = 0  # Resetear contador
                else:
                    consecutive_empty += 1
                    logger.info(f"No hay competiciones nuevas (vacías consecutivas: {consecutive_empty})")
                    # Si hay 3 páginas vacías consecutivas, probablemente terminamos
                    if consecutive_empty >= 3:
                        logger.info("3 páginas vacías consecutivas, finalizando...")
                        break
            else:
                consecutive_empty = 0
                logger.info(f"Encontradas {len(competitions)} competiciones nuevas")
                self.stats['competitions_found'] += len(competitions)
                
                # Procesar cada competición
                for comp in competitions:
                    self._process_competition(comp)
                
                # Guardar registro periódicamente
                self._save_registry()
            
            page += 1

        # Guardar registro final
        self._save_registry()

        # Resumen
        logger.info("\n" + "=" * 60)
        logger.info("RESUMEN")
        logger.info("=" * 60)
        logger.info(f"Páginas procesadas: {self.stats['pages_scraped']}")
        logger.info(f"Competiciones nuevas encontradas: {self.stats['competitions_found']}")
        logger.info(f"Competiciones ya descargadas (omitidas): {self.stats['competitions_already_downloaded']}")
        logger.info(f"Competiciones con .res: {self.stats['competitions_with_res']}")
        logger.info(f"Archivos descargados: {self.stats['files_downloaded']}")
        logger.info(f"Archivos omitidos: {self.stats['files_skipped']}")
        logger.info(f"Errores: {self.stats['errors']}")
        logger.info(f"Total competiciones en registro: {len(self.processed_competitions)}")
        logger.info("=" * 60)

        return self.stats


def main():
    """Función principal."""
    # Ruta base del proyecto
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent.parent

    # Directorio de salida
    output_dir = project_root / "data" / "raw" / "FMN" / "natacion"

    # Configurar y ejecutar
    # max_pages=None -> detecta automáticamente cuántas páginas hay
    # end_year=None -> usa el año actual
    scraper = FMNScraper(
        output_base_dir=str(output_dir),
        delay_between_requests=1.0,
        max_pages=None,      # Detectar automáticamente
        start_year=2018,
        end_year=None        # Año actual dinámicamente
    )

    try:
        stats = scraper.run()
        return 0 if stats['errors'] == 0 else 1
    except KeyboardInterrupt:
        logger.info("\n\nProceso interrumpido por el usuario")
        return 1
    except Exception as e:
        logger.exception(f"Error inesperado: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
