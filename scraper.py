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
        return self._save_concert_with_city(title, date, external_url, performers, pieces, None)
        
    def _save_concert_with_city(self, title, date, external_url, performers, pieces, city=None):
        """Save concert and related data to database with city information"""
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
                
                # Update city if provided
                if city:
                    existing_concert.city = city
                
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
                
                # Set city if provided
                if city:
                    concert.city = city
                    
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
            logger.info(f"Saved concert: {title} in {city if city else 'unknown city'}")
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
            ]) and not any(exclude in str(c).lower() for exclude in [
                'nav', 'menu', 'header', 'footer', 'sidebar', 'breadcrumb',
                'search', 'filter', 'pagination', 'social', 'share'
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
            
            # Skip elements that are too short or look like navigation
            if len(element_content) < 20:
                continue
                
            # Skip elements that contain navigation-like text
            navigation_keywords = ['home', 'about', 'contact', 'login', 'register', 'search', 
                                 'menu', 'navigation', 'breadcrumb', 'social media', 'follow us',
                                 'subscribe', 'newsletter', 'privacy', 'terms', 'cookie']
            if any(keyword in element_content.lower() for keyword in navigation_keywords):
                continue
                
            # Skip if we've already processed an identical or very similar element
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
                title = None
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
                
                # If no good title found, skip this element as it's likely not a concert
                if not title or len(title) < 10 or any(generic in title.lower() for generic in 
                    ['digital concert hall', 'calendar', 'subscriptions', 'vouchers', 'ticket information', 
                     'season highlights', 'tours', 'cinema', 'radio', 'tv', 'home', 'about', 'contact']):
                    continue
                
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
    
    def __init__(self, venue):
        super().__init__(venue)
        self.is_symphonic = False  # Flag to indicate if we're scraping the symphonic concerts page
        self.city = 'Warsaw'  # All Filharmonia Narodowa concerts are in Warsaw
        
        # Auto-detect if it's a symphonic concerts page based on URL
        if 'koncert-symfoniczny' in venue.url.lower():
            self.is_symphonic = True
            logger.info(f"Detected symphonic concerts page: {venue.url}")

    def scrape(self):
        """Scrape concerts from Filharmonia Narodowa website"""
        try:
            print("=== SCRAPER METHOD CALLED ===")
            logger.info(f"Scraping Filharmonia Narodowa website: {self.base_url}")
            if self.is_symphonic:
                logger.info("Detected symphonic concerts specific page")
                
            # Get HTML content with proper error handling
            html = self._get_html(self.base_url)
            if not html:
                logger.error(f"Failed to get HTML content from {self.base_url}")
                return False
            
            soup = BeautifulSoup(html, 'html.parser')
            concert_count = 0
            
            print("=== ABOUT TO SEARCH FOR SYMPHONIC CONCERTS ===")
            # Simple approach: Look for all text containing "Symphonic Concert" and try to find nearby dates
            symphonic_elements = soup.find_all(string=lambda text: text and 'Symphonic Concert' in text)
            print(f"DEBUG: Found {len(symphonic_elements)} Symphonic Concert elements")
            logger.info(f"Found {len(symphonic_elements)} Symphonic Concert elements")
            
            for i, symphonic_text in enumerate(symphonic_elements):
                try:
                    title = symphonic_text.strip()
                    print(f"DEBUG: Processing concert {i+1}: {title}")
                    logger.info(f"Processing concert {i+1}: {title}")
                    
                    # Find the parent element
                    parent = symphonic_text.parent
                    while parent and parent.name not in ['div', 'article', 'section', 'td', 'li']:
                        parent = parent.parent
                    
                    if not parent:
                        logger.warning(f"No suitable parent found for: {title}")
                        continue
                    
                    # Look for date in the same parent or nearby elements
                    date_text = None
                    time_text = None
                    
                    # Search in the parent element and its siblings
                    search_elements = [parent] + list(parent.find_next_siblings()) + list(parent.find_previous_siblings())
                    
                    for elem in search_elements:
                        if not elem:
                            continue
                            
                        # Look for date pattern (DD.MM)
                        date_match = re.search(r'(\d{1,2})\.(\d{1,2})', elem.get_text())
                        if date_match and not date_text:
                            date_text = date_match.group(0)
                            logger.info(f"Found date: {date_text}")
                        
                        # Look for time pattern (HH:MM)
                        time_match = re.search(r'(\d{1,2}):(\d{2})', elem.get_text())
                        if time_match and not time_text:
                            time_text = time_match.group(0)
                            logger.info(f"Found time: {time_text}")
                    
                    # If no date found in nearby elements, try a broader search
                    if not date_text:
                        all_text = parent.get_text()
                        date_match = re.search(r'(\d{1,2})\.(\d{1,2})', all_text)
                        if date_match:
                            date_text = date_match.group(0)
                            logger.info(f"Found date in parent text: {date_text}")
                    
                    if not date_text:
                        logger.warning(f"No date found for concert: {title}")
                        continue
                    
                    # Parse the date
                    concert_date = self._parse_filharmonia_date(date_text, time_text)
                    if not concert_date:
                        logger.warning(f"Could not parse date for concert: {title}")
                        continue
                    
                    logger.info(f"Parsed date: {concert_date}")
                    
                    # Create external URL
                    external_url = self.base_url
                    
                    # Simple performer and piece extraction
                    performers = []
                    pieces = []
                    
                    # Look for composer names in the surrounding text
                    all_text = parent.get_text()
                    composers = ['Mozart', 'Beethoven', 'Bach', 'Chopin', 'Szymanowski', 'Moniuszko', 'Wieniawski', 'Lutosławski',
                               'Penderecki', 'Górecki', 'Kilar', 'Tchaikovsky', 'Brahms', 'Mahler', 'Schumann', 'Schubert', 
                               'Debussy', 'Ravel', 'Shostakovich', 'Prokofiev', 'Stravinsky', 'Dvořák', 'Bartók', 'Rachmaninoff']
                    
                    for composer in composers:
                        if composer.lower() in all_text.lower():
                            pieces.append(f"Works by {composer}")
                    
                    # Save the concert
                    if self._save_concert_with_city(title, concert_date, external_url, performers, pieces, self.city):
                        concert_count += 1
                        logger.info(f"Saved concert: {title} on {concert_date.strftime('%Y-%m-%d')}")
                    else:
                        logger.warning(f"Failed to save concert: {title}")
                    
                except Exception as e:
                    logger.error(f"Error processing concert element: {str(e)}")
                    import traceback
                    logger.error(traceback.format_exc())
                    continue
            
            # Mark the venue as scraped
            self.venue.last_scraped = datetime.now()
            db.session.commit()
            
            logger.info(f"Successfully scraped {concert_count} concerts from Filharmonia Narodowa")
            return concert_count > 0
            
        except Exception as e:
            logger.error(f"Error scraping Filharmonia Narodowa: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def _parse_filharmonia_date(self, date_text, time_text):
        """Parse date and time from Filharmonia Narodowa website"""
        try:
            from dateutil import parser
            import re
            
            if not date_text:
                return None
            
            # Clean up the date text
            date_text = date_text.strip()
            
            # Try to extract day and month from the date text
            # Look for patterns like "30.10", "2.10", etc.
            date_match = re.search(r'(\d{1,2})\.(\d{1,2})', date_text)
            if date_match:
                day = int(date_match.group(1))
                month = int(date_match.group(2))
                current_year = datetime.now().year
                
                # Create the date
                concert_date = datetime(current_year, month, day)
                
                # If the date is in the past, assume it's next year
                if concert_date < datetime.now():
                    concert_date = datetime(current_year + 1, month, day)
                
                # Add time if available
                if time_text:
                    time_match = re.search(r'(\d{1,2}):(\d{2})', time_text)
                    if time_match:
                        hour = int(time_match.group(1))
                        minute = int(time_match.group(2))
                        concert_date = concert_date.replace(hour=hour, minute=minute)
                    else:
                        # Default to evening concert time
                        concert_date = concert_date.replace(hour=19, minute=30)
                else:
                    # Default to evening concert time
                    concert_date = concert_date.replace(hour=19, minute=30)
                
                return concert_date
            
            # Try parsing with dateutil as fallback
            try:
                parsed_date = parser.parse(date_text, fuzzy=True)
                if parsed_date:
                    return parsed_date
            except:
                pass
            
            return None
            
        except Exception as e:
            logger.error(f"Error parsing date '{date_text}': {str(e)}")
            return None
    
    def get_concert_details(self, url):
        """Get detailed concert information from the concert's dedicated page"""
        try:
            logger.info(f"Fetching detailed concert information from: {url}")
            html = self._get_html(url)
            if not html:
                logger.error(f"Failed to fetch HTML from {url}")
                return None
                
            soup = BeautifulSoup(html, 'html.parser')
            details = {}
            
            # Set city location to Warsaw for all Filharmonia Narodowa concerts
            details['city'] = 'Warsaw'
            
            # TITLE: Get using the specific selector
            # title-attr title-in-sidebar display-1 col-fn-s
            title_elem = soup.find(class_='title-in-sidebar')
            if not title_elem:
                title_elem = soup.find(class_='display-1')
            if not title_elem:
                title_elem = soup.find(class_='title-attr')
            
            if title_elem:
                title_text = title_elem.get_text().strip()
                logger.info(f"Found title: {title_text}")
                details['title'] = title_text
            else:
                # Fallback for title
                title_elem = soup.find(['h1', 'h2'], class_=lambda c: c and any(title_class in str(c) for title_class in ['title', 'heading', 'display-1']))
                if title_elem:
                    details['title'] = title_elem.get_text().strip()
            
            # DATE: Using class="event-date d-flex align-items-center h3 mr-3 mb-sm-0"
            date_elem = soup.find('div', class_='event-date')
            if date_elem:
                # Remove any inner divs and get just the text
                date_text = ''.join(s for s in date_elem.strings).strip()
                logger.info(f"Found date: {date_text}")
                details['date_text'] = date_text
                
                # Try to find the date in inner div with class="inner" as a fallback
                if not date_text:
                    inner_elem = date_elem.find('div', class_='inner')
                    if inner_elem:
                        details['date_text'] = inner_elem.get_text().strip()
            
            # WEEKDAY/TIME: <div class="day-time"> with span class="time"
            day_time_elem = soup.find('div', class_='day-time')
            if day_time_elem:
                # Extract the day of week and time
                day_time_text = day_time_elem.get_text().strip()
                logger.info(f"Found day/time: {day_time_text}")
                
                # Split into day and time
                if '/' in day_time_text:
                    day_part, time_part = day_time_text.split('/', 1)
                    details['day'] = day_part.strip()
                    details['day_time'] = day_time_text
                
                # Get time specifically from the span
                time_elem = day_time_elem.find('span', class_='time')
                if time_elem:
                    details['time'] = time_elem.get_text().strip()
                else:
                    # If no specific span, extract time using regex
                    time_match = re.search(r'(\d{1,2})\s*[:\.](\d{2})', day_time_text)
                    if time_match:
                        details['time'] = f"{time_match.group(1)}:{time_match.group(2)}"
            
            # VENUE: Look for venue text
            venue_texts = ['Sala Koncertowa', 'Sala Kameralna']
            for venue_text in venue_texts:
                # Try to find as a standalone element
                venue_elem = soup.find(string=lambda s: s and s.strip() == venue_text)
                if venue_elem:
                    details['venue'] = venue_text
                    logger.info(f"Found venue: {venue_text}")
                    break
            
            # If venue not found, look in any element containing venue text
            if 'venue' not in details:
                for elem in soup.find_all(['div', 'span', 'p']):
                    elem_text = elem.get_text().strip()
                    for venue_text in venue_texts:
                        if venue_text in elem_text and len(elem_text) < len(venue_text) + 10:
                            details['venue'] = venue_text
                            logger.info(f"Found venue in text: {venue_text}")
                            break
                    if 'venue' in details:
                        break
            
            # TICKET LINK: class="tickets-wrapper ml-sm-auto text-right text-sm-left"
            tickets_elem = soup.find('div', class_='tickets-wrapper')
            if tickets_elem:
                ticket_link = tickets_elem.find('a')
                if ticket_link and 'href' in ticket_link.attrs:
                    ticket_url = ticket_link['href']
                    if ticket_url and not ticket_url.startswith('#'):
                        details['ticket_url'] = ticket_url
                        logger.info(f"Found ticket URL: {ticket_url}")
            
            # PERFORMERS: class="performers-wrapper"
            performers_elem = soup.find('div', class_='performers-wrapper')
            if performers_elem:
                performer_lines = []
                # Each performer is likely in a separate element or line
                for elem in performers_elem.find_all(['p', 'div', 'span', 'li']):
                    performer_text = elem.get_text().strip()
                    if performer_text:
                        performer_lines.append(performer_text)
                
                if performer_lines:
                    details['performers_list'] = performer_lines
                    logger.info(f"Found performers: {', '.join(performer_lines[:3])}{'...' if len(performer_lines) > 3 else ''}")
                else:
                    # If no specific elements, use the entire text
                    details['performers_text'] = performers_elem.get_text().strip()
            
            # REPERTOIRE: event-meta-composer meta-area py-3 border-bottom
            repertoire_elem = soup.find('div', class_='event-meta-composer')
            if repertoire_elem:
                repertoire_text = repertoire_elem.get_text().strip()
                details['repertoire'] = repertoire_text
                logger.info(f"Found repertoire: {repertoire_text[:50]}{'...' if len(repertoire_text) > 50 else ''}")
                
                # Try to extract individual pieces from specific elements
                pieces = []
                for piece_elem in repertoire_elem.find_all(['p', 'div', 'li']):
                    piece_text = piece_elem.get_text().strip()
                    if piece_text and len(piece_text) > 5:
                        pieces.append(piece_text)
                
                if pieces:
                    details['repertoire_pieces'] = pieces
            
            # CATEGORIES
            categories_elem = soup.find('div', class_='event-meta-categories')
            if categories_elem:
                details['categories'] = categories_elem.get_text().strip()
            
            # DESCRIPTION
            description_elem = soup.find('div', class_='event-meta-info')
            if description_elem:
                details['description'] = description_elem.get_text().strip()
            
            return details
            
        except Exception as e:
            logger.error(f"Error fetching concert details from {url}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
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
    
        
        # EXACTLY use the specific selector provided
        # Find all concert items with the event-date class that are in the repertuar section
        event_items = []
        
        # APPROACH 1: Look for event-date elements specifically as per the selector
        date_elements = soup.find_all('div', class_='event-date')
        
        for date_elem in date_elements:
            # Get the parent elements that should contain the concert info
            parent_article = None
            current = date_elem.parent
            while current and current.name != 'article' and current.name != 'body':
                current = current.parent
            
            if current and current.name == 'article':
                parent_article = current
                if parent_article not in event_items:
                    event_items.append(parent_article)
        
        # APPROACH 2: Look directly for article elements with class='item item-calendar'
        if not event_items:
            calendar_items = soup.find_all('article', class_='item-calendar')
            for item in calendar_items:
                if item not in event_items:
                    event_items.append(item)
        
        # APPROACH 3: Look for event links and find their parent articles
        if not event_items:
            event_links = soup.find_all('a', class_='event-link')
            for link in event_links:
                parent = link.parent
                while parent and parent.name != 'article' and parent.name != 'body':
                    parent = parent.parent
                if parent and parent.name == 'article' and parent not in event_items:
                    event_items.append(parent)
        
        # APPROACH 4: For Symphonic Concerts page, find concert items by layout structure
        if self.is_symphonic and not event_items:
            # Look for the main container that holds concert listings
            main_container = soup.find('div', class_='calendar-main')
            if main_container:
                # Find rows that contain concert information 
                rows = main_container.find_all('div', class_='row')
                for row in rows:
                    # Each row could be a concert
                    if row not in event_items:
                        event_items.append(row)
        
        # Log how many event items we found
        logger.info(f"Found {len(event_items)} potential concert events to process")
        
        # Process each concert
        for item in event_items:
            try:
                # Initialize default values
                title = "Koncert Filharmonii Narodowej"
                date_text = None
                time_text = None
                venue_text = None
                program_text = ""
                
                # Debug what we're working with
                item_text = item.get_text().strip()
                logger.info(f"Processing item: {item_text[:50]}...")
                
                # Get the concert URL - needed for detailed information
                concert_link = None
                link_elem = item.find('a', class_='event-link')
                if link_elem and 'href' in link_elem.attrs:
                    href = link_elem['href']
                    if href and href != '#' and not href.startswith('javascript'):
                        concert_link = urljoin(self.base_url, href)
                
                # EXTRACT DATE using the exact selector
                date_elem = item.find('div', class_='event-date')
                if date_elem:
                    inner_elem = date_elem.find('div', class_='inner')
                    if inner_elem:
                        date_text = inner_elem.get_text().strip()
                        logger.info(f"Found date text: {date_text}")
                
                # If we're on a symphonic concert page, look for date in standard div formats
                if not date_text and self.is_symphonic:
                    # They might use different date formatting on this page
                    date_pattern = r'\d{1,2}\.\d{1,2}'
                    date_matches = re.findall(date_pattern, item.get_text())
                    if date_matches:
                        date_text = date_matches[0]
                        logger.info(f"Found date using pattern: {date_text}")
                
                # EXTRACT DAY AND TIME using the exact selector
                day_time_elem = item.find('div', class_='day-time')
                if day_time_elem:
                    # Extract the day of week
                    day_elem = day_time_elem.find('div', class_='day')
                    day_of_week = day_elem.get_text().strip() if day_elem else ""
                    
                    # Extract the time
                    time_elem = day_time_elem.find('div', class_='time')
                    if time_elem:
                        time_text = time_elem.get_text().strip()
                
                # EXTRACT VENUE
                venue_elem = item.find('div', string=lambda s: s and ('Sala Koncertowa' in s or 'Sala Kameralna' in s))
                if venue_elem:
                    venue_text = venue_elem.get_text().strip()
                
                # EXTRACT TITLE using specific elements
                title_elem = item.find('div', class_='event-title')
                if title_elem:
                    title = title_elem.get_text().strip()
                
                # EXTRACT CATEGORIES using the exact selector
                categories_elem = item.find('div', class_='event-meta-categories')
                categories_text = ""
                if categories_elem:
                    categories_text = categories_elem.get_text().strip()
                    # If the title is generic, use categories to enhance it
                    if len(title) < 30 and categories_text:
                        title = f"{title} ({categories_text})"
                
                # EXTRACT DESCRIPTION using the exact selector
                description_elem = item.find('div', class_='event-meta-info')
                if description_elem:
                    program_text = description_elem.get_text().strip()
                
                # If we have a link to the concert page, get additional details
                event_details = None
                if concert_link:
                    event_details = self.get_concert_details(concert_link)
                    
                    # Merge information from the concert details
                    if event_details:
                        # Use better title if available
                        if 'title' in event_details and event_details['title'] and len(event_details['title']) > len(title):
                            title = event_details['title']
                            
                        # Better date information if available
                        if 'date_text' in event_details and event_details['date_text'] and not date_text:
                            date_text = event_details['date_text']
                            
                        # Better time information if available
                        if 'time' in event_details and event_details['time'] and not time_text:
                            time_text = event_details['time']
                            
                        # Better venue information if available
                        if 'venue' in event_details and event_details['venue'] and not venue_text:
                            venue_text = event_details['venue']
                            
                        # Better description if available
                        if 'description' in event_details and event_details['description']:
                            program_text = event_details['description']
                            
                        # Additional categories if available
                        if 'categories' in event_details and event_details['categories']:
                            if not categories_text:
                                categories_text = event_details['categories']
                            # If the title is still generic, enhance it with categories
                            if len(title) < 30 and categories_text and categories_text not in title:
                                title = f"{title} ({categories_text})"
                
                # Clean up and parse the date
                if date_text:
                    # Clean up the date text - remove non-date content
                    date_text = re.sub(r'[^\d\. \-/a-zA-Ząęćżźńóśłł]', '', date_text)
                    
                    # Parse the date
                    concert_date = self.extract_date_from_text(date_text, current_year)
                    
                    # If time information is available, add it to the date
                    if time_text:
                        time_match = re.search(r'(\d{1,2})[\.:]?(\d{2})', time_text)
                        if time_match:
                            hour = int(time_match.group(1))
                            minute = int(time_match.group(2))
                            try:
                                concert_date = concert_date.replace(hour=hour, minute=minute)
                            except ValueError:
                                pass
                else:
                    # Default to current datetime if no date could be parsed
                    concert_date = datetime.now()
                
                # Extract performers from the program text
                performers = self.extract_performers(program_text)
                
                # Look for specific ensembles mentioned in the title or description
                if "FudalaRot Duo" in (title + " " + program_text):
                    # Add the specific duo
                    performers = [{
                        'name': 'FudalaRot Duo',
                        'role': 'ensemble'
                    }]
                    
                    # Add individual instruments they play
                    if "wiolonczel" in program_text.lower():
                        performers.append({
                            'name': 'Fudala',  # Assuming the first member plays cello
                            'role': 'wiolonczela'
                        })
                    if "fortepian" in program_text.lower():
                        performers.append({
                            'name': 'Rot',  # Assuming the second member plays piano
                            'role': 'fortepian'
                        })
                
                # Extract program information - making sure it's not too long for the database
                # Extract from the detailed text but keep it shorter than 250 chars
                pieces = []
                if 'w repertuarze na wiolonczelę i fortepian' in program_text.lower():
                    pieces.append({
                        'composer': 'W programie',
                        'title': 'Utwory na wiolonczelę i fortepian'  # Keep it short
                    })
                else:
                    pieces = self.extract_program(program_text, composers)
                    # Ensure titles aren't too long for DB
                    for piece in pieces:
                        if len(piece['title']) > 250:
                            piece['title'] = piece['title'][:247] + '...'
                
                # Keep the title to a reasonable length for display
                if len(title) > 100:
                    title = title[:97] + '...'
                
                # Construct a good URL
                external_url = concert_link if concert_link else self.base_url
                
                # Finally, save the concert with city information for Filharmonia Narodowa
                if 'filharmonia.pl' in self.base_url.lower():
                    city = 'Warsaw'
                    self._save_concert_with_city(title, concert_date, external_url, performers, pieces, city)
                else:
                    self._save_concert_with_city(title, concert_date, external_url, performers, pieces, self.city)
                concert_count += 1
                
            except Exception as e:
                logger.error(f"Error processing Filharmonia Narodowa concert: {str(e)}")
                continue
        
        # Update venue's last_scraped timestamp
        self.venue.last_scraped = datetime.utcnow()
        db.session.commit()
        
        # Additional verification - check what's actually in the database
        try:
            from app import Concert
            total_concerts = Concert.query.filter_by(venue_id=self.venue_id).count()
            print(f"DEBUG: Total concerts in database for this venue: {total_concerts}")
            logger.info(f"Total concerts in database for this venue: {total_concerts}")
        except Exception as e:
            print(f"DEBUG: Error checking database: {e}")
            logger.error(f"Error checking database: {e}")
        
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
        # Check if it's specifically a symphonic concert page
        if 'koncert-symfoniczny' in venue.url.lower():
            # Create a specialized instance for symphonic concerts
            scraper = FilharmoniaNarodowaScraper(venue)
            scraper.is_symphonic = True
            return scraper
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
