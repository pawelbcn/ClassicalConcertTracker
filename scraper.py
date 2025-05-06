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
        
        soup = BeautifulSoup(html, 'html.parser')
        concert_count = 0
        
        # Look for common concert listing patterns
        # Often concerts are in divs with certain classes
        concert_elements = soup.find_all(['div', 'article', 'section'], 
                                        class_=lambda c: c and any(term in str(c).lower() 
                                                                for term in ['concert', 'event', 'performance']))
        
        if not concert_elements:
            # Try finding elements by headings
            concert_elements = soup.find_all(['h1', 'h2', 'h3', 'h4'], 
                                            string=lambda s: s and any(term in s.lower() 
                                                                    for term in ['concert', 'symphony', 'orchestra']))
            # Get parent containers of these headings
            if concert_elements:
                concert_elements = [h.parent for h in concert_elements]
        
        for element in concert_elements[:10]:  # Limit to first 10 to prevent overloading
            try:
                # Extract concert details
                title_elem = element.find(['h1', 'h2', 'h3', 'h4', 'h5'])
                title = title_elem.text.strip() if title_elem else "Unknown Concert"
                
                # Look for date patterns
                date_text = None
                date_elem = element.find(string=re.compile(r'\d{1,2}[/\.-]\d{1,2}[/\.-]\d{2,4}|\d{4}[/\.-]\d{1,2}[/\.-]\d{1,2}'))
                if date_elem:
                    date_text = date_elem.strip()
                
                if not date_text:
                    # Look for elements with date-related classes or IDs
                    date_elem = element.find(['span', 'div', 'p'], class_=lambda c: c and any(term in str(c).lower() for term in ['date', 'time']))
                    if date_elem:
                        date_text = date_elem.text.strip()
                
                # Parse date - try common formats
                date = datetime.now()  # Default to current date if parsing fails
                if date_text:
                    date_formats = [
                        '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%d.%m.%Y',
                        '%B %d, %Y', '%d %B %Y'
                    ]
                    
                    for fmt in date_formats:
                        try:
                            date = datetime.strptime(date_text, fmt)
                            break
                        except ValueError:
                            continue
                
                # Get link to full concert page
                link_elem = element.find('a')
                external_url = None
                if link_elem and 'href' in link_elem.attrs:
                    external_url = urljoin(self.base_url, link_elem['href'])
                else:
                    external_url = self.base_url
                
                # Try to extract performers and repertoire from the summary
                # This is a simplified approach - the real implementation would be more sophisticated
                performers = []
                pieces = []
                
                # Extract text that might contain performer info
                performer_text = None
                performer_elem = element.find(['p', 'div'], string=lambda s: s and any(term in s.lower() for term in ['conductor', 'soloist', 'pianist', 'violinist']))
                if performer_elem:
                    performer_text = performer_elem.text.strip()
                    # Very basic extraction - in a real implementation, use NLP or more sophisticated regex
                    conductor_match = re.search(r'conductor:?\s*([\w\s]+)', performer_text, re.IGNORECASE)
                    if conductor_match:
                        performers.append({'name': conductor_match.group(1).strip(), 'role': 'conductor'})
                    
                    # Look for common soloist instruments
                    for instrument in ['piano', 'violin', 'cello', 'flute', 'clarinet']:
                        match = re.search(rf'{instrument}:?\s*([\w\s]+)', performer_text, re.IGNORECASE)
                        if match:
                            performers.append({'name': match.group(1).strip(), 'role': instrument})
                
                # If no performers found, add a placeholder
                if not performers:
                    performers.append({'name': 'Unknown', 'role': 'performer'})
                
                # Extract text that might contain repertoire info
                repertoire_text = None
                repertoire_elem = element.find(['p', 'div'], string=lambda s: s and any(term in s.lower() for term in ['program', 'repertoire', 'works', 'pieces']))
                if repertoire_elem:
                    repertoire_text = repertoire_elem.text.strip()
                    
                    # Very basic composer detection - in a real implementation, use a database of composer names
                    for composer in ['Mozart', 'Beethoven', 'Bach', 'Tchaikovsky', 'Brahms', 'Chopin']:
                        if composer in repertoire_text:
                            # Try to find work title near composer name
                            match = re.search(rf'{composer}[\'s]?\s*([\w\s\.]+)', repertoire_text)
                            if match:
                                pieces.append({'composer': composer, 'title': match.group(1).strip()})
                            else:
                                pieces.append({'composer': composer, 'title': 'Unknown Work'})
                
                # If no pieces found, add a placeholder
                if not pieces:
                    pieces.append({'composer': 'Unknown', 'title': 'TBA'})
                
                # Save concert to database
                self._save_concert(title, date, external_url, performers, pieces)
                concert_count += 1
                
            except Exception as e:
                logger.error(f"Error processing concert element: {str(e)}")
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
        # Add more specialized scrapers here as needed
    }
    
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
