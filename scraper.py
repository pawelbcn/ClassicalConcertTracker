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
    
    def scrape(self):
        """Scrape concerts from Filharmonia Narodowa website"""
        html = self._get_html(self.base_url)
        if not html:
            return False
        
        soup = BeautifulSoup(html, 'html.parser')
        concert_count = 0
        
        # Find all concert elements - they are usually in items with class "item-calendar"
        concert_elements = soup.find_all(['div', 'article'], class_=lambda c: c and 'item-calendar' in str(c))
        
        # If we can't find items with that class, look for elements with the date format used on the site
        if not concert_elements:
            # Look for event date elements
            date_elements = soup.find_all(['div', 'span', 'time'], class_=lambda c: c and any(date_class in str(c) for date_class in ['event-date', 'date', 'calendar-date']))
            
            if date_elements:
                concert_elements = []
                for date_elem in date_elements:
                    # Get parent element that should contain the concert details
                    parent = date_elem.parent
                    if parent:
                        # Try to get a larger container by going up one more level
                        container = parent.parent if parent.parent else parent
                        concert_elements.append(container)
        
        # If still no elements found, try looking for the repertoire structure on the page
        if not concert_elements:
            # Look for month sections or event lists
            month_sections = soup.find_all(['section', 'div'], class_=lambda c: c and any(term in str(c) for term in ['month-events', 'repertuar', 'koncerty', 'events-list']))
            
            if month_sections:
                concert_elements = []
                for section in month_sections:
                    # Find individual concert items within the section
                    items = section.find_all(['div', 'article', 'li'], class_=lambda c: c and any(term in str(c) for term in ['event', 'concert', 'item']))
                    concert_elements.extend(items)
        
        # Special fallback for Filharmonia Narodowa structure
        if not concert_elements:
            # Their site often has program details in elements with calendar-related classes
            calendar_elements = soup.find_all(['div', 'ul', 'ol'], class_=lambda c: c and any(cal_term in str(c) for cal_term in ['calendar', 'repertuar', 'program', 'events']))
            
            for cal_elem in calendar_elements:
                event_items = cal_elem.find_all(['li', 'div', 'article'], recursive=True)
                if event_items:
                    concert_elements.extend(event_items)
        
        # Process discovered concert elements
        for element in concert_elements[:20]:  # Limit to prevent overload
            try:
                # Extract concert title
                title = "Koncert Filharmonii Narodowej"  # Default title in Polish
                title_elem = element.find(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'strong', 'b'], class_=lambda c: c and any(title_term in str(c) for title_term in ['title', 'name', 'heading']))
                
                if title_elem:
                    title = title_elem.get_text().strip()
                else:
                    # Try finding any heading without class specification
                    title_elem = element.find(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'strong', 'b'])
                    if title_elem:
                        title = title_elem.get_text().strip()
                
                # Extract date - look for Polish date formats
                date_text = None
                # Look for date elements with specific classes
                date_elem = element.find(['div', 'span', 'time'], class_=lambda c: c and any(date_term in str(c) for date_term in ['date', 'day', 'month', 'time']))
                
                if date_elem:
                    date_text = date_elem.get_text().strip()
                else:
                    # Look for Polish date patterns in the text
                    # Polish months: stycznia, lutego, marca, kwietnia, maja, czerwca, lipca, sierpnia, września, października, listopada, grudnia
                    polish_date_pattern = r'\d{1,2}\s+(?:stycznia|lutego|marca|kwietnia|maja|czerwca|lipca|sierpnia|września|października|listopada|grudnia)\s+\d{4}'
                    polish_date_match = re.search(polish_date_pattern, element.get_text())
                    
                    if polish_date_match:
                        date_text = polish_date_match.group(0)
                    else:
                        # Try standard date patterns
                        date_pattern = r'\d{1,2}[\./]\d{1,2}[\./]\d{4}|\d{4}-\d{1,2}-\d{1,2}'
                        date_match = re.search(date_pattern, element.get_text())
                        if date_match:
                            date_text = date_match.group(0)
                
                # Parse the date
                date = datetime.now()  # Default to current date
                if date_text:
                    try:
                        # Handle Polish date format (e.g., "15 maja 2023")
                        polish_months = {
                            'stycznia': '01', 'lutego': '02', 'marca': '03', 'kwietnia': '04',
                            'maja': '05', 'czerwca': '06', 'lipca': '07', 'sierpnia': '08',
                            'września': '09', 'października': '10', 'listopada': '11', 'grudnia': '12'
                        }
                        
                        for pl_month, month_num in polish_months.items():
                            if pl_month in date_text.lower():
                                # Extract day and year
                                day_match = re.search(r'(\d{1,2})\s+', date_text)
                                year_match = re.search(r'\s+(\d{4})', date_text)
                                
                                if day_match and year_match:
                                    day = day_match.group(1).zfill(2)  # Pad with leading zero if needed
                                    year = year_match.group(1)
                                    date_string = f"{year}-{month_num}-{day}"
                                    date = datetime.strptime(date_string, '%Y-%m-%d')
                                    break
                        
                        # If not parsed with Polish months, try standard formats
                        if date == datetime.now():
                            for fmt in ['%d/%m/%Y', '%d.%m.%Y', '%Y-%m-%d']:
                                try:
                                    date = datetime.strptime(date_text, fmt)
                                    break
                                except ValueError:
                                    continue
                    except Exception as e:
                        logger.warning(f"Could not parse date '{date_text}': {str(e)}")
                
                # Get link to full concert page
                external_url = self.base_url
                link_elem = element.find('a')
                if link_elem and 'href' in link_elem.attrs:
                    external_url = urljoin(self.base_url, link_elem['href'])
                
                # Extract performers - look for conductor and soloists
                performers = []
                
                # Look for elements containing performer info
                performer_elem = element.find(['div', 'p', 'span'], class_=lambda c: c and any(perf_term in str(c) for perf_term in ['performers', 'artists', 'conductor', 'soloists']))
                
                if performer_elem:
                    performer_text = performer_elem.get_text()
                    
                    # Look for conductor
                    conductor_patterns = [
                        r'dyrygent:?\s*([\w\s\-\.]+)', # Polish: dyrygent
                        r'conductor:?\s*([\w\s\-\.]+)'
                    ]
                    
                    for pattern in conductor_patterns:
                        conductor_match = re.search(pattern, performer_text, re.IGNORECASE)
                        if conductor_match:
                            performers.append({
                                'name': conductor_match.group(1).strip(),
                                'role': 'conductor'
                            })
                    
                    # Look for soloists (Polish terms)
                    soloist_patterns = [
                        r'solista:?\s*([\w\s\-\.]+)', # Polish: solista
                        r'skrzypce:?\s*([\w\s\-\.]+)', # Polish: violin
                        r'fortepian:?\s*([\w\s\-\.]+)', # Polish: piano
                        r'wiolonczela:?\s*([\w\s\-\.]+)', # Polish: cello
                        r'altówka:?\s*([\w\s\-\.]+)', # Polish: viola
                        r'flet:?\s*([\w\s\-\.]+)', # Polish: flute
                        r'obój:?\s*([\w\s\-\.]+)', # Polish: oboe
                        r'klarnet:?\s*([\w\s\-\.]+)', # Polish: clarinet
                        r'fagot:?\s*([\w\s\-\.]+)', # Polish: bassoon
                        r'trąbka:?\s*([\w\s\-\.]+)', # Polish: trumpet
                        r'róg:?\s*([\w\s\-\.]+)', # Polish: horn
                        r'puzon:?\s*([\w\s\-\.]+)', # Polish: trombone
                        r'harfa:?\s*([\w\s\-\.]+)', # Polish: harp
                        r'perkusja:?\s*([\w\s\-\.]+)' # Polish: percussion
                    ]
                    
                    for pattern in soloist_patterns:
                        matches = re.finditer(pattern, performer_text, re.IGNORECASE)
                        for match in matches:
                            role = match.group(0).split(':')[0].strip().lower()
                            performers.append({
                                'name': match.group(1).strip(),
                                'role': role
                            })
                
                # If no performers found, look for capitalized names
                if not performers:
                    # Look for patterns that might indicate performers
                    names = re.findall(r'([A-Z][a-zA-Z]+\s+[A-Z][a-zA-Z\-]+)', element.get_text())
                    for name in names:
                        # Filter out common words that aren't likely to be performer names
                        if name not in ['Filharmonia Narodowa', 'Sala Koncertowa', 'Sala Kameralna']:
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
                
                # Extract program/repertoire
                pieces = []
                program_elem = element.find(['div', 'p', 'span'], class_=lambda c: c and any(prog_term in str(c) for prog_term in ['program', 'repertoire', 'pieces', 'works']))
                
                if program_elem:
                    program_text = program_elem.get_text()
                    
                    # List of common Polish and international composers
                    composers = [
                        'Mozart', 'Beethoven', 'Bach', 'Chopin', 'Szymanowski', 'Moniuszko', 'Wieniawski', 'Lutosławski',
                        'Penderecki', 'Górecki', 'Kilar', 'Tchaikovsky', 'Brahms', 'Mahler', 'Schumann', 'Schubert', 
                        'Debussy', 'Ravel', 'Shostakovich', 'Prokofiev', 'Stravinsky', 'Dvořák', 'Bartók', 'Rachmaninoff'
                    ]
                    
                    # Look for composer names in the program text
                    for composer in composers:
                        if composer in program_text:
                            # Try to extract the piece title that follows the composer name
                            composer_pattern = re.escape(composer) + r'[\s\:\–\-]+(.*?)(?:\n|$|\.|\,|\;|\(|\[)'
                            piece_match = re.search(composer_pattern, program_text)
                            
                            if piece_match:
                                title = piece_match.group(1).strip()
                                pieces.append({
                                    'composer': composer,
                                    'title': title
                                })
                            else:
                                pieces.append({
                                    'composer': composer,
                                    'title': 'Dzieło' # Polish for 'Work'
                                })
                
                # If no pieces found, try more generic approach looking for common terms in classical music
                if not pieces:
                    piece_terms = ['symphony', 'concerto', 'sonata', 'quartet', 'symfonii', 'koncert', 'sonata']
                    
                    for term in piece_terms:
                        if term.lower() in element.get_text().lower():
                            # Try to find a nearby composer name
                            for composer in composers:
                                if composer in element.get_text():
                                    pieces.append({
                                        'composer': composer,
                                        'title': f"{term.title()}"
                                    })
                                    break
                            
                            # If term found but no composer identified
                            if not pieces:
                                pieces.append({
                                    'composer': "Kompozytor", # Polish for 'Composer'
                                    'title': f"{term.title()}"
                                })
                
                # If still no pieces found, add placeholder
                if not pieces:
                    pieces.append({
                        'composer': 'W programie', # Polish for 'In the program'
                        'title': 'Repertuar do potwierdzenia' # 'Repertoire to be confirmed'
                    })
                
                # Save concert to database
                self._save_concert(title, date, external_url, performers, pieces)
                concert_count += 1
                
            except Exception as e:
                logger.error(f"Error processing Filharmonia Narodowa concert element: {str(e)}")
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
