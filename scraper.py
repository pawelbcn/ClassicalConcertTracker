import re
import logging
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import trafilatura
from models import Concert, Performer, Piece, Venue
from app import db

logger = logging.getLogger(__name__)

class BaseScraper:
    """Base class for all scrapers"""
    
    def __init__(self, venue):
        self.venue = venue
        self.base_url = venue.url
    
    def scrape(self):
        """Main scraping method, to be implemented by child classes"""
        raise NotImplementedError("Subclasses must implement scrape method")
    
    def _get_html(self, url):
        """Get HTML content from a URL with error handling"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            logger.error(f"Error fetching URL {url}: {str(e)}")
            return None
    
    def _get_trafilatura_content(self, url):
        """Get processed content using trafilatura"""
        try:
            downloaded = trafilatura.fetch_url(url)
            if downloaded:
                return trafilatura.extract(downloaded)
            return None
        except Exception as e:
            logger.error(f"Error processing URL {url} with trafilatura: {str(e)}")
            return None
    
    def _save_concert(self, title, date, external_url, performers, pieces):
        """Save concert and related data to database"""
        try:
            # Check if concert already exists by external_url
            existing_concert = Concert.query.filter_by(
                external_url=external_url, 
                venue_id=self.venue.id
            ).first()
            
            if existing_concert:
                logger.info(f"Concert already exists: {title}")
                # Update existing concert details if needed
                existing_concert.title = title
                existing_concert.date = date
                existing_concert.updated_at = datetime.utcnow()
                
                # Clear existing relationships to rebuild them
                existing_concert.performers = []
                existing_concert.pieces = []
                
                concert = existing_concert
            else:
                # Create new concert
                concert = Concert(
                    title=title,
                    date=date,
                    venue_id=self.venue.id,
                    external_url=external_url
                )
                db.session.add(concert)
            
            # Add performers
            for performer_data in performers:
                # Check if performer already exists
                performer = Performer.query.filter_by(
                    name=performer_data['name'],
                    role=performer_data['role']
                ).first()
                
                if not performer:
                    performer = Performer(
                        name=performer_data['name'],
                        role=performer_data['role']
                    )
                    db.session.add(performer)
                
                concert.performers.append(performer)
            
            # Add pieces
            for piece_data in pieces:
                # Check if piece already exists
                piece = Piece.query.filter_by(
                    title=piece_data['title'],
                    composer=piece_data['composer']
                ).first()
                
                if not piece:
                    piece = Piece(
                        title=piece_data['title'],
                        composer=piece_data['composer']
                    )
                    db.session.add(piece)
                
                concert.pieces.append(piece)
            
            db.session.commit()
            logger.info(f"Saved concert: {title}")
            return True
            
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error saving concert {title}: {str(e)}")
            return False


class GenericScraper(BaseScraper):
    """Generic scraper that attempts to handle common concert site formats"""
    
    def scrape(self):
        """Scrape concerts using a generic approach"""
        html = self._get_html(self.base_url)
        if not html:
            return False
            
        # Use trafilatura to get better processed content and structure
        processed_content = self._get_trafilatura_content(self.base_url)
        
        soup = BeautifulSoup(html, 'html.parser')
        concert_count = 0
        
        # Look for common concert listing patterns - expanded search terms
        concert_elements = soup.find_all(
            ['div', 'article', 'section', 'li'], 
            class_=lambda c: c and any(term in str(c).lower() for term in [
                'concert', 'event', 'performance', 'program', 'repertoire', 
                'season', 'schedule', 'calendar', 'listing', 'music'
            ])
        )
        
        if not concert_elements:
            # Try finding elements by headings with expanded terms
            concert_elements = soup.find_all(
                ['h1', 'h2', 'h3', 'h4'], 
                string=lambda s: s and any(term in s.lower() for term in [
                    'concert', 'symphony', 'orchestra', 'philharmonic', 'recital',
                    'chamber', 'quartet', 'sonata', 'concerto'
                ])
            )
            # Get parent containers of these headings
            if concert_elements:
                concert_elements = [h.parent for h in concert_elements]
        
        # If still no elements found, try to find event listings by date patterns
        if not concert_elements:
            date_elements = soup.find_all(
                string=re.compile(r'\d{1,2}[/\.-]\d{1,2}[/\.-]\d{2,4}|\d{4}[/\.-]\d{1,2}[/\.-]\d{1,2}|\d{1,2}\s+[A-Za-z]+\s+\d{4}')
            )
            if date_elements:
                concert_elements = []
                for date_elem in date_elements:
                    # Get parent or grandparent element as it likely contains the full concert info
                    parent = date_elem.parent
                    if parent:
                        concert_elements.append(parent.parent if parent.parent else parent)
        
        # Expanded list of classical music composers 
        composers = [
            'Mozart', 'Beethoven', 'Bach', 'Tchaikovsky', 'Brahms', 'Chopin', 'Debussy', 
            'Ravel', 'Rachmaninoff', 'Stravinsky', 'Schubert', 'Handel', 'Haydn', 'Liszt', 
            'Mahler', 'Mendelssohn', 'Prokofiev', 'Puccini', 'Shostakovich', 'Sibelius', 
            'Schumann', 'Verdi', 'Wagner', 'Vivaldi', 'Dvořák', 'Grieg', 'Berlioz', 
            'Britten', 'Bartók', 'Bruckner', 'Elgar', 'Fauré', 'Gershwin', 'Glass', 
            'Holst', 'Ligeti', 'Monteverdi', 'Mussorgsky', 'Pärt', 'Purcell', 'Reich', 
            'Rimsky-Korsakov', 'Saint-Saëns', 'Satie', 'Schoenberg', 'Tallis', 'Vaughan Williams',
            'Bernstein', 'Copland', 'Barber'
        ]

        # Extended list of instruments/roles in classical concerts
        instruments = [
            'conductor', 'piano', 'violin', 'cello', 'viola', 'bass', 'flute', 
            'clarinet', 'oboe', 'bassoon', 'trumpet', 'horn', 'trombone', 'tuba', 
            'percussion', 'harp', 'organ', 'harpsichord', 'guitar', 'soprano', 
            'mezzo-soprano', 'alto', 'tenor', 'baritone', 'bass', 'choir', 'orchestra',
            'soloist', 'quartet', 'ensemble', 'pianist', 'violinist', 'cellist'
        ]
        
        # Try a more comprehensive approach if we have few or no elements
        if len(concert_elements) < 3:
            # Look for tables, which are often used for concert listings
            tables = soup.find_all('table')
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    concert_elements.append(row)
                    
            # Also try to find all anchor tags with links containing typical concert keywords
            concert_links = soup.find_all('a', href=lambda h: h and any(term in h.lower() for term in [
                'concert', 'event', 'performance', 'program', 'season', 'schedule'
            ]))
            for link in concert_links:
                parent = link.parent
                if parent and parent not in concert_elements:
                    concert_elements.append(parent)
        
        # Process the elements we found
        processed_elements = set()  # To avoid duplicates
        for element in concert_elements[:15]:  # Limit to first 15 to prevent overloading
            # Skip if we've already processed an identical or very similar element
            element_content = element.get_text().strip()
            skip = False
            for processed in processed_elements:
                if processed in element_content or element_content in processed:
                    skip = True
                    break
            if skip:
                continue
                
            processed_elements.add(element_content)
            
            try:
                # Extract concert title - look more broadly for title elements
                title = "Classical Concert"
                title_elem = element.find(['h1', 'h2', 'h3', 'h4', 'h5', 'b', 'strong', 'span', 'div'], 
                                        class_=lambda c: c and any(term in str(c).lower() for term in [
                                            'title', 'event', 'name', 'concert', 'heading'
                                        ]))
                
                if title_elem:
                    title = title_elem.text.strip()
                else:
                    # Try to find first heading or emphasized text
                    title_elem = element.find(['h1', 'h2', 'h3', 'h4', 'h5', 'b', 'strong'])
                    if title_elem:
                        title = title_elem.text.strip()
                    else:
                        # Get first significant text from element if no clear title
                        title_text = element.get_text().strip()
                        if title_text:
                            # Extract up to first 60 chars
                            title = title_text[:60].strip()
                            if len(title_text) > 60:
                                title += '...'
                
                # Look for date patterns - expanded regex for more date formats
                date_text = None
                date_patterns = [
                    r'\d{1,2}[/\.-]\d{1,2}[/\.-]\d{2,4}',  # DD/MM/YYYY, MM/DD/YYYY, etc.
                    r'\d{4}[/\.-]\d{1,2}[/\.-]\d{1,2}',  # YYYY/MM/DD, etc.
                    r'\d{1,2}\s+[A-Za-z]+\s+\d{4}',  # DD Month YYYY
                    r'[A-Za-z]+\s+\d{1,2}\s*,?\s*\d{4}',  # Month DD, YYYY
                    r'\d{4}-\d{1,2}-\d{1,2}',  # ISO date
                    r'\d{1,2}\.\d{1,2}\.\d{4}',  # European format with dots
                    r'\d{1,2}/\d{1,2}/\d{4}'  # US/UK format with slashes
                ]
                
                # Build a combined pattern
                combined_pattern = '|'.join(f'({p})' for p in date_patterns)
                date_match = re.search(combined_pattern, element_text := element.get_text())
                
                if date_match:
                    # Get the first group that matched
                    date_text = next(group for group in date_match.groups() if group is not None)
                
                if not date_text:
                    # Look for elements with date-related classes, ids, or aria labels
                    date_indicators = ['date', 'time', 'when', 'calendar', 'schedule']
                    for indicator in date_indicators:
                        date_elem = element.find(
                            ['span', 'div', 'p', 'time'], 
                            attrs=lambda a: a and any(indicator in str(v).lower() for k, v in a.items())
                        )
                        if date_elem:
                            date_text = date_elem.text.strip()
                            break
                
                # Parse date - expanded date formats
                date = datetime.now()  # Default to current date if parsing fails
                if date_text:
                    date_formats = [
                        '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%d.%m.%Y', '%Y/%m/%d',
                        '%B %d, %Y', '%d %B %Y', '%B %d %Y', '%d %b %Y', '%b %d, %Y',
                        '%d-%m-%Y', '%m-%d-%Y', '%Y.%m.%d', '%d.%b.%Y'
                    ]
                    
                    # Clean up date text
                    date_text = re.sub(r'[^\w\s\d/.,:-]', '', date_text).strip()
                    
                    # Try all formats
                    for fmt in date_formats:
                        try:
                            parsed_date = datetime.strptime(date_text, fmt)
                            date = parsed_date
                            break
                        except ValueError:
                            continue
                
                # Get link to full concert page
                external_url = self.base_url
                link_elem = element.find('a')
                if link_elem and 'href' in link_elem.attrs:
                    external_url = urljoin(self.base_url, link_elem['href'])
                
                # Extract performers with improved detection
                performers = []
                element_text = element.get_text().lower()
                
                # Check for common performer roles in the text
                for instrument in instruments:
                    pattern = rf'{instrument}\s*:?\s*([\w\s\-\']+)'
                    matches = re.finditer(pattern, element_text, re.IGNORECASE)
                    for match in matches:
                        name = match.group(1).strip()
                        # Filter out short or empty names
                        if len(name) > 2 and not name.isdigit() and not re.match(r'^[\W_]+$', name):
                            performers.append({'name': name.title(), 'role': instrument.lower()})
                
                # If no performers found, look for names near instrument/role names
                if not performers:
                    for instrument in instruments:
                        if instrument.lower() in element_text:
                            # Get text surrounding the instrument mention
                            instrument_idx = element_text.find(instrument.lower())
                            surrounding_text = element_text[max(0, instrument_idx-30):min(len(element_text), instrument_idx+30)]
                            
                            # Look for capitalized names nearby
                            names = re.findall(r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})', surrounding_text)
                            for name in names:
                                if name.lower() not in ['concert', 'symphony', 'orchestra', 'hall']:
                                    performers.append({'name': name, 'role': instrument.lower()})
                
                # If still no performers, look for any capitalized names in the element
                if not performers:
                    names = re.findall(r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})', element.get_text())
                    for name in names:
                        # Filter out common non-person terms
                        if name.lower() not in ['concert', 'symphony', 'orchestra', 'hall', 'center', 'theatre', 'music',
                                              'program', 'season', 'series', 'performance']:
                            performers.append({'name': name, 'role': 'performer'})
                
                # If still no performers, add a placeholder
                if not performers:
                    performers.append({'name': 'TBA', 'role': 'performer'})
                
                # Extract repertoire with improved detection
                pieces = []
                
                # Look for composer names in the text
                for composer in composers:
                    if composer in element.get_text():
                        # Get text surrounding the composer mention
                        composer_idx = element.get_text().find(composer)
                        surrounding_text = element.get_text()[max(0, composer_idx-10):min(len(element.get_text()), composer_idx+100)]
                        
                        # Extract title after composer name
                        # Look for patterns like "Composer: Title" or "Composer - Title" or just "Composer Title"
                        title_patterns = [
                            rf'{composer}\s*:\s*([^\n,.]+)',
                            rf'{composer}\s*-\s*([^\n,.]+)',
                            rf'{composer}[\s\'"]+(No\.\s+\d+|[A-Z][^\n,.]+)'
                        ]
                        
                        for pattern in title_patterns:
                            match = re.search(pattern, surrounding_text)
                            if match:
                                title = match.group(1).strip()
                                if len(title) > 2:  # Ensure title is meaningful
                                    pieces.append({'composer': composer, 'title': title})
                                    break
                        
                        # If no specific title found but composer is mentioned, add generic work
                        if composer not in [p['composer'] for p in pieces]:
                            pieces.append({'composer': composer, 'title': 'Work'})
                
                # Look for common classical piece keywords
                piece_keywords = [
                    'symphony', 'concerto', 'sonata', 'quartet', 'quintet', 'trio', 'etude',
                    'nocturne', 'rhapsody', 'suite', 'prelude', 'fugue', 'variations', 'ballet',
                    'opera', 'mass', 'requiem', 'cantata', 'oratorio', 'overture'
                ]
                
                for keyword in piece_keywords:
                    if keyword.lower() in element.get_text().lower():
                        # Find the piece by looking for "Keyword in X Major/Minor" or similar patterns
                        pattern = rf'({keyword}\s+(?:No\.)?\s*\d*\s*(?:in\s+[A-G](?:\s*(?:flat|sharp|major|minor)))?)'  
                        matches = re.finditer(pattern, element.get_text(), re.IGNORECASE)
                        
                        for match in matches:
                            piece_title = match.group(1).strip()
                            
                            # Try to find composer near this piece
                            surrounding = element.get_text()[max(0, match.start()-50):match.start()]
                            composer_found = False
                            
                            for composer in composers:
                                if composer in surrounding:
                                    pieces.append({'composer': composer, 'title': piece_title})
                                    composer_found = True
                                    break
                            
                            # If no composer found, add with unknown composer
                            if not composer_found and piece_title not in [p['title'] for p in pieces]:
                                pieces.append({'composer': 'Unknown', 'title': piece_title})
                
                # If no pieces found, check for any program keywords
                if not pieces:
                    program_keywords = ['program', 'repertoire', 'works', 'pieces', 'music by']
                    for keyword in program_keywords:
                        if keyword in element.get_text().lower():
                            # Get text after program keyword
                            keyword_idx = element.get_text().lower().find(keyword)
                            program_text = element.get_text()[keyword_idx:keyword_idx+200]  # Grab some text after the keyword
                            
                            # Look for composer names in this text
                            for composer in composers:
                                if composer in program_text:
                                    pieces.append({'composer': composer, 'title': 'TBA'})
                
                # If still no pieces found, add a placeholder
                if not pieces:
                    pieces.append({'composer': 'TBA', 'title': 'TBA'})
                
                # Save concert to database
                self._save_concert(title, date, external_url, performers, pieces)
                concert_count += 1
                
            except Exception as e:
                logger.error(f"Error processing concert element: {str(e)}")
                continue
        
        # Try scraping the venue website using trafilatura as a backup method
        if concert_count == 0 and processed_content:
            try:
                # Parse the processed content for concert information
                logger.info("Attempting to extract concerts using trafilatura content")
                
                # Look for date patterns in the processed content
                date_matches = re.finditer(
                    r'(\d{1,2}[/\.-]\d{1,2}[/\.-]\d{2,4}|\d{4}[/\.-]\d{1,2}[/\.-]\d{1,2}|\d{1,2}\s+[A-Za-z]+\s+\d{4}|[A-Za-z]+\s+\d{1,2}\s*,?\s*\d{4})',
                    processed_content
                )
                
                for date_match in date_matches:
                    date_text = date_match.group(0)
                    # Get surrounding text (100 chars before and 300 after the date)
                    date_pos = date_match.start()
                    surrounding_text = processed_content[max(0, date_pos-100):min(len(processed_content), date_pos+300)]
                    
                    # Try to parse this text as a concert
                    try:
                        # Parse date
                        date = datetime.now()  # Default
                        date_formats = [
                            '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%d.%m.%Y', '%Y/%m/%d',
                            '%B %d, %Y', '%d %B %Y', '%B %d %Y', '%d %b %Y', '%b %d, %Y',
                            '%d-%m-%Y', '%m-%d-%Y', '%Y.%m.%d', '%d.%b.%Y'
                        ]
                        
                        for fmt in date_formats:
                            try:
                                date = datetime.strptime(date_text, fmt)
                                break
                            except ValueError:
                                continue
                                
                        # Extract title - use first line or sentence of surrounding text
                        title_match = re.search(r'^([^\n\.]+)', surrounding_text)
                        title = title_match.group(1).strip() if title_match else "Classical Concert"
                        
                        # Detect performers
                        performers = []
                        for instrument in instruments:
                            pattern = rf'{instrument}\s*:?\s*([\w\s\-\']+)'
                            matches = re.finditer(pattern, surrounding_text, re.IGNORECASE)
                            for match in matches:
                                name = match.group(1).strip()
                                if len(name) > 2 and not name.isdigit():
                                    performers.append({'name': name.title(), 'role': instrument.lower()})
                                    
                        # If no performers found, check for capitalized names
                        if not performers:
                            names = re.findall(r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})', surrounding_text)
                            for name in names:
                                if name.lower() not in ['concert', 'symphony', 'orchestra', 'hall'] and \
                                   name not in composers:  # Avoid treating composers as performers
                                    performers.append({'name': name, 'role': 'performer'})
                                    
                        # If still no performers found
                        if not performers:
                            performers.append({'name': 'TBA', 'role': 'performer'})
                        
                        # Detect repertoire
                        pieces = []
                        for composer in composers:
                            if composer in surrounding_text:
                                # Try to find work titles
                                pattern = rf'{composer}\s*:?\s*([^\n,.;]+)'
                                match = re.search(pattern, surrounding_text)
                                if match:
                                    title = match.group(1).strip()
                                    pieces.append({'composer': composer, 'title': title})
                                else:
                                    pieces.append({'composer': composer, 'title': 'TBA'})
                                    
                        # If no pieces found
                        if not pieces:
                            pieces.append({'composer': 'TBA', 'title': 'TBA'})
                        
                        # Save concert
                        self._save_concert(title, date, self.base_url, performers, pieces)
                        concert_count += 1
                        
                    except Exception as e:
                        logger.error(f"Error processing trafilatura content section: {str(e)}")
                        continue
                        
            except Exception as e:
                logger.error(f"Error using trafilatura backup method: {str(e)}")
        
        # Update venue's last_scraped timestamp
        self.venue.last_scraped = datetime.utcnow()
        db.session.commit()
        
        return concert_count > 0


class ClassicalMusicScraper(GenericScraper):
    """Specialized scraper for classical music websites with enhanced detection"""
    # Inherits all functionality from GenericScraper but can be extended with specialized methods
    pass

class FilharmoniaNarodowaScraper(BaseScraper):
    """Specialized scraper for Filharmonia Narodowa website"""
    
    def get_concert_details(self, url):
        """Get detailed concert information from the concert's dedicated page"""
        try:
            html = self._get_html(url)
            if not html:
                return None
                
            soup = BeautifulSoup(html, 'html.parser')
            
            # Extract more detailed information about the concert
            details = {}
            
            # Get the title
            title_elem = soup.find(['h1', 'h2'], class_=lambda c: c and ('title' in str(c) or 'heading' in str(c)))
            if title_elem:
                details['title'] = title_elem.get_text().strip()
            
            # Look for date and time information
            date_elem = soup.find(['time', 'div', 'span'], class_=lambda c: c and ('date' in str(c) or 'time' in str(c) or 'when' in str(c)))
            if date_elem:
                details['date_text'] = date_elem.get_text().strip()
            
            # Look for time
            time_pattern = r'(\d{1,2})\s*[:\.](\d{2})'
            time_match = re.search(time_pattern, soup.get_text())
            if time_match:
                details['time'] = f"{time_match.group(1)}:{time_match.group(2)}"
                
            # Look for venue
            venue_keywords = ['Sala Koncertowa', 'Sala Kameralna', 'Sala']
            for keyword in venue_keywords:
                if keyword in soup.get_text():
                    details['venue'] = keyword
                    break
            
            # Look for program description
            program_elem = soup.find(['div', 'section'], class_=lambda c: c and ('program' in str(c) or 'repertoire' in str(c) or 'description' in str(c)))
            if program_elem:
                details['program_description'] = program_elem.get_text().strip()
            
            # Try to find performers with specific roles
            performers_section = soup.find(['div', 'section'], class_=lambda c: c and ('performers' in str(c) or 'artists' in str(c) or 'zespol' in str(c)))
            if performers_section:
                details['performers_text'] = performers_section.get_text().strip()
            
            # Look for repertoire/program details
            program_list = soup.find(['ul', 'ol'], class_=lambda c: c and ('program' in str(c) or 'repertoire' in str(c)))
            if program_list:
                program_items = program_list.find_all('li')
                if program_items:
                    details['program_items'] = [item.get_text().strip() for item in program_items]
            
            return details
            
        except Exception as e:
            logger.error(f"Error fetching concert details from {url}: {str(e)}")
            return None
    
    def extract_date_from_text(self, text, current_year=None):
        """Extract date from various Polish date formats"""
        if not text:
            return datetime.now()
            
        if not current_year:
            current_year = datetime.now().year
            
        # Try to extract day and month from formats like "13.05" or "13.05." (day.month)
        date_match = re.search(r'(\d{1,2})[\.\s/]+(\d{1,2})', text)
        month_names = {
            'stycznia': 1, 'lutego': 2, 'marca': 3, 'kwietnia': 4,
            'maja': 5, 'czerwca': 6, 'lipca': 7, 'sierpnia': 8,
            'września': 9, 'października': 10, 'listopada': 11, 'grudnia': 12,
            'styczeń': 1, 'luty': 2, 'marzec': 3, 'kwiecień': 4,
            'maj': 5, 'czerwiec': 6, 'lipiec': 7, 'sierpień': 8,
            'wrzesień': 9, 'październik': 10, 'listopad': 11, 'grudzień': 12
        }
        
        # If we found day.month format
        if date_match:
            day = int(date_match.group(1))
            month = int(date_match.group(2))
            year = current_year
            
            # Sanity check for valid month
            if 1 <= month <= 12 and 1 <= day <= 31:
                try:
                    return datetime(year, month, day)
                except ValueError:
                    pass  # Invalid date like Feb 30
                
        # Try Polish format: "13 maja" (day month_name)
        for month_name, month_num in month_names.items():
            pattern = r'(\d{1,2})\s+' + month_name
            match = re.search(pattern, text.lower())
            if match:
                day = int(match.group(1))
                month = month_num
                year = current_year
                
                # Check if year is specified in the text
                year_match = re.search(r'\s+(\d{4})', text)
                if year_match:
                    year = int(year_match.group(1))
                    
                try:
                    return datetime(year, month, day)
                except ValueError:
                    pass  # Invalid date
                    
        # Try formats like "maj 2025" (month_name year)
        for month_name, month_num in month_names.items():
            pattern = month_name + r'\s+(\d{4})'
            match = re.search(pattern, text.lower())
            if match:
                day = 1  # Default to first day if only month and year
                month = month_num
                year = int(match.group(1))
                
                try:
                    return datetime(year, month, day)
                except ValueError:
                    pass  # Invalid date
        
        # Default to current date if no valid date found
        return datetime.now()
    
    def extract_performers(self, text):
        """Extract performer information from text"""
        if not text:
            return [{'name': 'Orkiestra Filharmonii Narodowej', 'role': 'orchestra'}]
            
        performers = []
        # Look for specific ensemble names that may contain "w" (Polish preposition)
        ensemble_pattern = r'([A-Z][a-zżźćńółęąśŻŹĆĄŚĘŁÓŃ]+(?:\s+[A-Z][a-zżźćńółęąśŻŹĆĄŚĘŁÓŃ\-]+)+)\s+w\s+'
        ensemble_match = re.search(ensemble_pattern, text)
        if ensemble_match:
            ensemble_name = ensemble_match.group(1).strip()
            if len(ensemble_name.split()) >= 2:  # Ensure it's at least two words
                performers.append({
                    'name': ensemble_name,
                    'role': 'ensemble'
                })
        
        # Look for patterns like "X na Y" (X on Y) where X is often a performer and Y is an instrument
        instrument_pattern = r'([A-Z][a-zżźćńółęąśŻŹĆĄŚĘŁÓŃ]+(\s+[A-Z][a-zżźćńółęąśŻŹĆĄŚĘŁÓŃ\-]+)*)\s+(?:na|w)\s+(\w+)'
        for match in re.finditer(instrument_pattern, text):
            name = match.group(1).strip()
            instrument = match.group(3).strip().lower()
            if len(name.split()) >= 2:  # Ensure it's at least two words
                performers.append({
                    'name': name,
                    'role': instrument
                })
        
        # Look for duo/trio names
        ensemble_patterns = [
            r'([A-Z][a-zżźćńółęąśŻŹĆĄŚĘŁÓŃ]+(\s+[A-Z][a-zżźćńółęąśŻŹĆĄŚĘŁÓŃ\-]+)*)\s+(?:Duo|Trio|Quartet|Kwartet)'
        ]
        
        for pattern in ensemble_patterns:
            for match in re.finditer(pattern, text):
                name = match.group(0).strip()
                if len(name.split()) >= 2:  # Ensure it's at least two words
                    performers.append({
                        'name': name,
                        'role': 'ensemble'
                    })
                    
        # Look for specific performer patterns
        performer_patterns = [
            r'([A-Z][a-zżźćńółęąśŻŹĆĄŚĘŁÓŃ]+(\s+[A-Z][a-zżźćńółęąśŻŹĆĄŚĘŁÓŃ\-]+)+)\s+(?:fortepian|skrzypce|wiolonczela|altówka|flet)'
        ]
        
        for pattern in performer_patterns:
            for match in re.finditer(pattern, text):
                parts = match.group(0).strip().split()
                if len(parts) >= 2:
                    name_parts = []
                    role = ""
                    
                    for part in parts:
                        if part.lower() in ['fortepian', 'skrzypce', 'wiolonczela', 'altówka', 'flet']:
                            role = part.lower()
                        else:
                            name_parts.append(part)
                    
                    name = " ".join(name_parts)
                    if name and role:
                        performers.append({
                            'name': name,
                            'role': role
                        })
        
        # Look for 'Duo' patterns with instrument information
        duo_pattern = r'([A-Z][a-zżźćńółęąśŻŹĆĄŚĘŁÓŃ]+(?:[A-Z][a-zżźćńółęąśŻŹĆĄŚĘŁÓŃ]+)+)\s*(?:Duo)'
        duo_match = re.search(duo_pattern, text)
        if duo_match:
            duo_name = duo_match.group(0).strip()
            performers.append({
                'name': duo_name,
                'role': 'ensemble'
            })
            
        # Look for specific ensemble names that might be in the text
        ensemble_names = [
            'FudalaRot Duo', 'Sinfonia Varsovia', 'Orkiestra Filharmonii Narodowej',
            'Chór Filharmonii Narodowej', 'Warsaw Philharmonic Orchestra',
            'Warsaw Philharmonic Choir'
        ]
        
        for ensemble in ensemble_names:
            if ensemble in text:
                performers.append({
                    'name': ensemble,
                    'role': 'ensemble'
                })
                break
        
        # If we still haven't found any performers, look for capitalized names
        if not performers:
            # Look for patterns that might indicate performers (Polish names often have specific patterns)
            names = re.findall(r'([A-Z][a-zżźćńółęąśŻŹĆĄŚĘŁÓŃ]+\s+[A-Z][a-zżźćńółęąśŻŹĆĄŚĘŁÓŃ\-]+)', text)
            for name in names:
                # Filter out common words that aren't likely to be performer names
                if name not in ['Filharmonia Narodowa', 'Sala Koncertowa', 'Sala Kameralna', 'Scena Muzyki']:
                    performers.append({
                        'name': name,
                        'role': 'performer'
                    })
        
        # If still no performers found
        if not performers:
            performers.append({
                'name': 'Orkiestra Filharmonii Narodowej',  # Default: Polish Philharmonic Orchestra
                'role': 'orchestra'
            })
            
        return performers
    
    def extract_program(self, text, composers):
        """Extract program information from text"""
        if not text:
            return [{'composer': 'W programie', 'title': 'Repertuar do potwierdzenia'}]
            
        pieces = []
        text = text.replace('\n', ' ')
        
        # Look for program description with instrument information
        repertoire_patterns = [
            r'(?:w\s+repertuarze|wykonują|program[:\s]+|w\s+programie[:\s]+)\s+([^\.]*)'
        ]
        
        for pattern in repertoire_patterns:
            repertoire_match = re.search(pattern, text, re.IGNORECASE)
            if repertoire_match:
                repertoire_text = repertoire_match.group(1).strip()
                if repertoire_text and len(repertoire_text) > 5:
                    pieces.append({
                        'composer': 'W programie',
                        'title': repertoire_text
                    })
        
        # Look for composer names in the text
        if not pieces:
            for composer in composers:
                if composer in text:
                    # Try to extract the piece title that follows the composer name
                    composer_pattern = re.escape(composer) + r'[\s\:\-–—]+(.*?)(?:\n|$|\.|\,|\;|\(|\[|[A-Z])'
                    piece_match = re.search(composer_pattern, text)
                    
                    if piece_match:
                        title = piece_match.group(1).strip()
                        if title:
                            pieces.append({
                                'composer': composer,
                                'title': title
                            })
                    else:
                        # If we found a composer but no specific piece, add a generic entry
                        pieces.append({
                            'composer': composer,
                            'title': 'Utwór' # Polish for 'Work'
                        })
        
        # Look for specific music terms if we still don't have program information
        if not pieces:
            music_terms = [
                'sonata', 'koncert', 'symfonia', 'kwartet', 'trio', 'suita',
                'preludium', 'etiuda', 'nokturn', 'walc', 'mazurek', 'polonez'
            ]
            
            for term in music_terms:
                term_pattern = r'([A-Z][a-zżźćńółęąśŻŹĆĄŚĘŁÓŃ]+)\s+' + term
                for match in re.finditer(term_pattern, text, re.IGNORECASE):
                    composer = match.group(1).strip()
                    if composer in composers:
                        pieces.append({
                            'composer': composer,
                            'title': term.capitalize()
                        })
        
        # If any phrase ends with "na fortepian i wiolonczelę" or similar, add it as a piece
        instrument_patterns = [
            r'([^\.]*)\s+(?:na|dla)\s+(?:fortepian|skrzypce|wiolonczelę|altówkę|flet)'
        ]
        
        for pattern in instrument_patterns:
            for match in re.finditer(pattern, text.lower()):
                piece_desc = match.group(0).strip()
                if piece_desc and len(piece_desc) > 10 and not any(piece['title'] == piece_desc for piece in pieces):
                    pieces.append({
                        'composer': 'Program',
                        'title': piece_desc
                    })
        
        # If we still don't have any pieces and we have a generic program description
        if not pieces and 'repertuar' in text.lower():
            # Extract text after "repertuar" keyword
            repertoire_match = re.search(r'repertuar[:\s]+(.*?)(?:\.|$)', text.lower())
            if repertoire_match:
                repertoire = repertoire_match.group(1).strip()
                if repertoire:
                    pieces.append({
                        'composer': 'W programie',
                        'title': repertoire.capitalize()
                    })
        
        # If still no pieces found, add a default entry
        if not pieces:
            # Check if there's any useful description in the text
            if text and len(text) > 10:
                # Use the first sentence or up to 100 chars as description
                desc = text.split('.')[0].strip()
                if len(desc) > 100:
                    desc = desc[:97] + '...'
                    
                pieces.append({
                    'composer': 'W programie',
                    'title': desc
                })
            else:
                pieces.append({
                    'composer': 'W programie',
                    'title': 'Repertuar do potwierdzenia'
                })
                
        return pieces
    
    def scrape(self):
        """Scrape concerts from Filharmonia Narodowa website"""
        html = self._get_html(self.base_url)
        if not html:
            return False
        
        soup = BeautifulSoup(html, 'html.parser')
        concert_count = 0
        
        # List of common Polish and international composers
        composers = [
            'Mozart', 'Beethoven', 'Bach', 'Chopin', 'Szymanowski', 'Moniuszko', 'Wieniawski', 'Lutosławski',
            'Penderecki', 'Górecki', 'Kilar', 'Tchaikovsky', 'Brahms', 'Mahler', 'Schumann', 'Schubert', 
            'Debussy', 'Ravel', 'Shostakovich', 'Prokofiev', 'Stravinsky', 'Dvořák', 'Bartók', 'Rachmaninoff'
        ]
        
        # Polish months with their corresponding numbers for date parsing
        polish_months = {
            'stycznia': '01', 'lutego': '02', 'marca': '03', 'kwietnia': '04',
            'maja': '05', 'czerwca': '06', 'lipca': '07', 'sierpnia': '08',
            'września': '09', 'października': '10', 'listopada': '11', 'grudnia': '12'
        }
        
        # Current year for date parsing
        current_year = datetime.now().year
        
        # Look for calendar items which typically contain concert listings
        calendar_items = soup.find_all(['article', 'div', 'li'], class_=lambda c: c and ('item' in str(c) or 'event' in str(c) or 'koncert' in str(c)))
        
        if not calendar_items:
            # Try a more generic approach
            calendar_containers = soup.find_all(['div', 'section', 'ul'], class_=lambda c: c and ('calendar' in str(c) or 'repertuar' in str(c) or 'koncerty' in str(c)))
            
            for container in calendar_containers:
                items = container.find_all(['article', 'div', 'li'])
                calendar_items.extend(items)
        
        # Process each calendar item
        for item in calendar_items[:30]:  # Limit to 30 items to prevent overload
            try:
                # Look for a link to the detailed concert page
                concert_link = None
                link_elem = item.find('a')
                if link_elem and 'href' in link_elem.attrs:
                    concert_link = urljoin(self.base_url, link_elem['href'])
                
                # Extract initial data from the calendar item
                title = "Koncert Filharmonii Narodowej"  # Default title
                title_elem = item.find(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'strong', 'b', 'span'], 
                                       class_=lambda c: c and ('title' in str(c) or 'name' in str(c) or 'heading' in str(c)))
                
                if title_elem:
                    title = title_elem.get_text().strip()
                else:
                    # Try finding any heading without class specification
                    title_elem = item.find(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'strong', 'b'])
                    if title_elem:
                        title = title_elem.get_text().strip()
                
                # Extract date information
                date_text = None
                date_elem = item.find(['div', 'span', 'time'], class_=lambda c: c and ('date' in str(c) or 'day' in str(c)))
                if date_elem:
                    date_text = date_elem.get_text().strip()
                
                # If no date found, look for patterns in text
                if not date_text:
                    # Look for Polish date patterns like "13.05" or "13 maja"
                    date_patterns = [
                        r'(\d{1,2})[\./](\d{1,2})',  # DD.MM or DD/MM
                        r'(\d{1,2})\s+(?:stycznia|lutego|marca|kwietnia|maja|czerwca|lipca|sierpnia|września|października|listopada|grudnia)',  # DD Month
                    ]
                    
                    for pattern in date_patterns:
                        match = re.search(pattern, item.get_text())
                        if match:
                            date_text = match.group(0)
                            break
                
                # If still no date, look for other patterns
                if not date_text:
                    # Look for weekday patterns like "wtorek" (Tuesday)
                    weekday_pattern = r'(poniedziałek|wtorek|środa|czwartek|piątek|sobota|niedziela)\s+\d{1,2}'
                    weekday_match = re.search(weekday_pattern, item.get_text().lower())
                    if weekday_match:
                        date_text = weekday_match.group(0)
                
                # Extract time information
                time_text = None
                time_elem = item.find(['div', 'span', 'time'], class_=lambda c: c and 'time' in str(c))
                if time_elem:
                    time_text = time_elem.get_text().strip()
                
                if not time_text:
                    # Look for time pattern like "19:00" or "19.00"
                    time_pattern = r'(\d{1,2})[\.:\s](\d{2})'
                    time_match = re.search(time_pattern, item.get_text())
                    if time_match:
                        time_text = f"{time_match.group(1)}:{time_match.group(2)}"
                
                # Extract venue information
                venue_text = None
                venue_elem = item.find(['div', 'span'], class_=lambda c: c and ('venue' in str(c) or 'location' in str(c) or 'sala' in str(c)))
                if venue_elem:
                    venue_text = venue_elem.get_text().strip()
                
                if not venue_text:
                    # Look for common venues
                    venue_keywords = ['Sala Koncertowa', 'Sala Kameralna']
                    for keyword in venue_keywords:
                        if keyword in item.get_text():
                            venue_text = keyword
                            break
                
                # Get initial program and performer information
                program_text = item.get_text()
                
                # If we have a link to the detailed page, scrape additional information
                details = None
                if concert_link:
                    details = self.get_concert_details(concert_link)
                
                # Merge information from the concert page (if available) with the calendar item
                if details:
                    # Use the title from the details page if available
                    if 'title' in details and details['title']:
                        title = details['title']
                    
                    # Use date from details page if available
                    if 'date_text' in details and details['date_text']:
                        date_text = details['date_text']
                    
                    # Use time from details page if available
                    if 'time' in details and details['time'] and not time_text:
                        time_text = details['time']
                        
                    # Use venue from details page if available
                    if 'venue' in details and details['venue'] and not venue_text:
                        venue_text = details['venue']
                    
                    # Use program description from details page if available
                    if 'program_description' in details and details['program_description']:
                        program_text = details['program_description']
                    
                    # Add performers information if available
                    if 'performers_text' in details and details['performers_text']:
                        program_text += " " + details['performers_text']
                    
                    # Add program items if available
                    if 'program_items' in details and details['program_items']:
                        program_text += " " + " ".join(details['program_items'])
                
                # Parse the date
                concert_date = self.extract_date_from_text(date_text, current_year)
                
                # If we have time information, update the date with it
                if time_text:
                    time_match = re.search(r'(\d{1,2})[\.:]?(\d{2})', time_text)
                    if time_match:
                        hour = int(time_match.group(1))
                        minute = int(time_match.group(2))
                        try:
                            concert_date = concert_date.replace(hour=hour, minute=minute)
                        except ValueError:
                            pass  # Invalid time
                
                # Extract performers
                performers = self.extract_performers(program_text)
                
                # Extract program
                pieces = self.extract_program(program_text, composers)
                
                # Enhance the title if it's too generic
                if title in ["Koncert", "Koncert Filharmonii Narodowej"] and venue_text:
                    title = f"{title} - {venue_text}"
                
                # If we found a specific ensemble or performer, add it to the title
                performer_names = [p['name'] for p in performers if p['name'] not in ['Orkiestra Filharmonii Narodowej', 'Chór Filharmonii Narodowej']]
                if performer_names and len(title) < 30:
                    title = f"{title}: {performer_names[0]}"
                
                # Save concert to database
                external_url = concert_link if concert_link else self.base_url
                self._save_concert(title, concert_date, external_url, performers, pieces)
                concert_count += 1
                
            except Exception as e:
                logger.error(f"Error processing Filharmonia Narodowa concert element: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
                continue
        
        # Update venue's last_scraped timestamp
        self.venue.last_scraped = datetime.utcnow()
        db.session.commit()
        
        return concert_count > 0

# Factory to get the appropriate scraper
def get_scraper(venue):
    """Factory function to return the appropriate scraper for the venue"""
    scraper_map = {
        'generic': GenericScraper,
        'classical': ClassicalMusicScraper,
        'filharmonia_narodowa': FilharmoniaNarodowaScraper,
        # Add more specialized scrapers here as needed
    }
    
    # Special domain-based scrapers - automatically select specialized scraper based on domain
    if 'filharmonia.pl' in venue.url.lower():
        return FilharmoniaNarodowaScraper(venue)
    
    scraper_class = scraper_map.get(venue.scraper_type, GenericScraper)
    return scraper_class(venue)


def scrape_venue(venue_id):
    """Scrape concert information for a specific venue"""
    venue = Venue.query.get(venue_id)
    if not venue:
        logger.error(f"Venue with ID {venue_id} not found")
        return False
    
    scraper = get_scraper(venue)
    success = scraper.scrape()
    
    return success


def scrape_all_venues():
    """Scrape concert information for all venues"""
    venues = Venue.query.all()
    results = {}
    
    for venue in venues:
        logger.info(f"Scraping venue: {venue.name}")
        try:
            scraper = get_scraper(venue)
            success = scraper.scrape()
            results[venue.id] = success
        except Exception as e:
            logger.error(f"Error scraping venue {venue.name}: {str(e)}")
            results[venue.id] = False
    
    return results
