import logging
import threading
import time
from datetime import datetime, timedelta
from flask import render_template, request, jsonify, flash, redirect, url_for, send_from_directory
from sqlalchemy import or_
from app import app, db
from models import Concert, Performer, Piece, Venue
from scraper import scrape_venue, scrape_all_venues

# Global progress tracking
scraping_progress = {}

logger = logging.getLogger(__name__)

@app.route('/static/<path:filename>')
def static_files(filename):
    """Serve static files"""
    return send_from_directory('static', filename)

@app.route('/')
def index():
    """Home page with concert listings and filters"""
    # Get filter parameters
    time_period = request.args.get('time_period', '')
    venue_id = request.args.get('venue_id', '')
    performer = request.args.get('performer', '')
    
    # Base query
    query = Concert.query
    
    # Apply time period filter
    now = datetime.now()
    if time_period == 'today':
        start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = now.replace(hour=23, minute=59, second=59, microsecond=999999)
        query = query.filter(Concert.date >= start_of_day, Concert.date <= end_of_day)
    elif time_period == 'this_week':
        # Get start of current week (Monday)
        days_since_monday = now.weekday()
        start_of_week = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days_since_monday)
        end_of_week = start_of_week + timedelta(days=6, hours=23, minutes=59, seconds=59)
        query = query.filter(Concert.date >= start_of_week, Concert.date <= end_of_week)
    elif time_period == 'this_month':
        start_of_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if now.month == 12:
            end_of_month = now.replace(year=now.year + 1, month=1, day=1) - timedelta(microseconds=1)
        else:
            end_of_month = now.replace(month=now.month + 1, day=1) - timedelta(microseconds=1)
        query = query.filter(Concert.date >= start_of_month, Concert.date <= end_of_month)
    elif time_period == 'next_month':
        if now.month == 12:
            start_of_next_month = now.replace(year=now.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
            end_of_next_month = now.replace(year=now.year + 1, month=2, day=1) - timedelta(microseconds=1)
        else:
            start_of_next_month = now.replace(month=now.month + 1, day=1, hour=0, minute=0, second=0, microsecond=0)
            if now.month == 11:
                end_of_next_month = now.replace(year=now.year + 1, month=1, day=1) - timedelta(microseconds=1)
            else:
                end_of_next_month = now.replace(month=now.month + 2, day=1) - timedelta(microseconds=1)
        query = query.filter(Concert.date >= start_of_next_month, Concert.date <= end_of_next_month)
    
    # Apply other filters
    if venue_id:
        query = query.filter(Concert.venue_id == venue_id)
    
    if performer:
        query = query.join(Concert.performers).filter(Performer.name.ilike(f'%{performer}%'))
    
    # Execute query and get results
    concerts = query.order_by(Concert.date).all()
    
    # Get all venues for the filter dropdown
    venues = Venue.query.order_by(Venue.name).all()
    
    # Get all performers for the filter dropdown (distinct by name)
    performers = Performer.query.distinct(Performer.name).order_by(Performer.name).all()
    
    return render_template('index.html', 
                           concerts=concerts, 
                           venues=venues,
                           performers=performers,
                           filters={
                               'time_period': time_period,
                               'venue_id': venue_id,
                               'performer': performer
                           })

@app.route('/venues/add', methods=['GET', 'POST'])
def add_venue():
    """Add a new venue to monitor"""
    if request.method == 'POST':
        name = request.form.get('name')
        url = request.form.get('url')
        scraper_type = request.form.get('scraper_type', 'generic')
        
        # Basic validation
        if not name or not url:
            flash('Both name and URL are required!', 'danger')
            return redirect(url_for('add_venue'))
        
        # Normalize URL if needed
        if not url.startswith('http'):
            url = f"https://{url}"
        
        # Check if this is Filharmonia Narodowa's symphonic concerts page
        if 'filharmonia.pl' in url.lower() and 'koncert-symfoniczny' in url.lower():
            # Set a specific name to indicate it's the symphonic concerts page
            if 'symphonic' not in name.lower():
                name = f"{name} - Symphonic Concerts"
        
        try:
            venue = Venue(name=name, url=url, scraper_type=scraper_type)
            db.session.add(venue)
            db.session.commit()
            flash(f'Venue "{name}" added successfully. Use the scrape button to collect concert information.', 'success')
            return redirect(url_for('index'))
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error adding venue: {str(e)}")
            flash(f'Error adding venue: {str(e)}', 'danger')
    
    return render_template('add_venue.html')

@app.route('/api/venues/<int:venue_id>/scrape', methods=['POST'])
def api_scrape_venue(venue_id):
    """API endpoint to scrape a specific venue with progress tracking"""
    try:
        # Initialize progress tracking
        scraping_progress[venue_id] = {
            'status': 'starting',
            'current': 0,
            'total': 0,
            'message': 'Initializing scraper...',
            'error': None
        }
        
        # Start scraping in a separate thread
        def scrape_with_progress():
            try:
                # Set up Flask application context for the thread
                with app.app_context():
                    scraping_progress[venue_id]['status'] = 'running'
                    scraping_progress[venue_id]['message'] = 'Fetching concert data...'
                    
                    # Get venue info
                    venue = Venue.query.get(venue_id)
                    if not venue:
                        scraping_progress[venue_id]['status'] = 'error'
                        scraping_progress[venue_id]['error'] = 'Venue not found'
                        return
                    
                    # Import scraper here to avoid circular imports
                    from scraper import get_scraper
                    scraper = get_scraper(venue)
                    
                    # Override the scraper's progress tracking
                    original_scrape = scraper.scrape
                    
                    def progress_wrapper():
                        try:
                            # Get the main repertoire page first to count items
                            html = scraper._get_html(scraper.base_url)
                            if not html:
                                scraping_progress[venue_id]['status'] = 'error'
                                scraping_progress[venue_id]['error'] = 'Failed to fetch venue page'
                                return False
                            
                            from bs4 import BeautifulSoup
                            soup = BeautifulSoup(html, 'html.parser')
                            
                            # Count items based on venue type, but limit to 5 for testing
                            if 'filharmonia.pl' in venue.url.lower():
                                items = soup.find_all('a', class_='event-list-chocolate')
                            elif 'nfm.wroclaw.pl' in venue.url.lower():
                                items = soup.find_all('div', class_='nfmELItem')
                            elif 'nospr.org.pl' in venue.url.lower():
                                all_rows = soup.find_all('div', class_='calendar__row')
                                items = [row for row in all_rows if row.find('div', class_='tile tile--calendar')]
                            else:
                                items = soup.find_all(['div', 'article'], class_=lambda x: x and 'concert' in x.lower())
                            
                            # Limit to 5 concerts for testing purposes
                            total_items = min(5, len(items))
                            scraping_progress[venue_id]['total'] = total_items
                            scraping_progress[venue_id]['message'] = f'Found {len(items)} concerts, processing first {total_items}'
                            
                            # Now run the actual scraper
                            result = original_scrape()
                            
                            if result:
                                scraping_progress[venue_id]['status'] = 'completed'
                                scraping_progress[venue_id]['message'] = f'Successfully scraped {total_items} concerts'
                                scraping_progress[venue_id]['current'] = total_items
                            else:
                                scraping_progress[venue_id]['status'] = 'error'
                                scraping_progress[venue_id]['error'] = 'Scraping failed'
                            
                            return result
                            
                        except Exception as e:
                            scraping_progress[venue_id]['status'] = 'error'
                            scraping_progress[venue_id]['error'] = str(e)
                            logger.error(f"Error in progress wrapper: {str(e)}")
                            return False
                    
                    # Run the scraper with progress tracking
                    progress_wrapper()
                
            except Exception as e:
                scraping_progress[venue_id]['status'] = 'error'
                scraping_progress[venue_id]['error'] = str(e)
                logger.error(f"Error in scraping thread: {str(e)}")
        
        # Start the scraping thread
        thread = threading.Thread(target=scrape_with_progress)
        thread.daemon = True
        thread.start()
        
        return jsonify({'status': 'started', 'message': 'Scraping started'})
        
    except Exception as e:
        logger.error(f"Error starting scrape for venue {venue_id}: {str(e)}")
        return jsonify({'status': 'error', 'message': f"Error: {str(e)}"}), 500

@app.route('/api/venues/<int:venue_id>/progress', methods=['GET'])
def api_get_scraping_progress(venue_id):
    """API endpoint to get scraping progress for a venue"""
    progress = scraping_progress.get(venue_id, {
        'status': 'not_found',
        'current': 0,
        'total': 0,
        'message': 'No scraping in progress',
        'error': None
    })
    
    return jsonify(progress)

@app.route('/api/venues/scrape-all', methods=['POST'])
def api_scrape_all_venues():
    """API endpoint to scrape all venues"""
    try:
        results = scrape_all_venues()
        success_count = sum(1 for success in results.values() if success)
        total = len(results)
        
        if success_count > 0:
            return jsonify({
                'status': 'success', 
                'message': f'Successfully scraped {success_count} out of {total} venues'
            })
        else:
            return jsonify({
                'status': 'warning', 
                'message': f'Failed to scrape any venues successfully'
            }), 400
    except Exception as e:
        logger.error(f"Error scraping all venues: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/venues')
def list_venues():
    """List all venues with their last scraped time"""
    venues = Venue.query.order_by(Venue.name).all()
    return render_template('index.html', venues=venues, show_venues_tab=True)

@app.route('/api/venues/<int:venue_id>/delete', methods=['POST'])
def delete_venue(venue_id):
    """Delete a venue and all its associated concerts"""
    try:
        venue = Venue.query.get_or_404(venue_id)
        name = venue.name
        db.session.delete(venue)
        db.session.commit()
        flash(f'Venue "{name}" and all associated concerts deleted successfully', 'success')
        return jsonify({'status': 'success'})
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error deleting venue {venue_id}: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500
