import logging
from datetime import datetime
from flask import render_template, request, jsonify, flash, redirect, url_for
from sqlalchemy import or_
from app import app, db
from models import Concert, Performer, Piece, Venue
from scraper import scrape_venue, scrape_all_venues

logger = logging.getLogger(__name__)

@app.route('/')
def index():
    """Home page with concert listings and filters"""
    # Get filter parameters
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    venue_id = request.args.get('venue_id', '')
    performer = request.args.get('performer', '')
    repertoire = request.args.get('repertoire', '')
    
    # Base query
    query = Concert.query
    
    # Apply filters
    if date_from:
        try:
            from_date = datetime.strptime(date_from, '%Y-%m-%d')
            query = query.filter(Concert.date >= from_date)
        except ValueError:
            flash('Invalid from date format. Please use YYYY-MM-DD', 'warning')
    
    if date_to:
        try:
            to_date = datetime.strptime(date_to, '%Y-%m-%d')
            query = query.filter(Concert.date <= to_date)
        except ValueError:
            flash('Invalid to date format. Please use YYYY-MM-DD', 'warning')
    
    if venue_id:
        query = query.filter(Concert.venue_id == venue_id)
    
    if performer:
        query = query.join(Concert.performers).filter(
            or_(
                Performer.name.ilike(f'%{performer}%'),
                Performer.role.ilike(f'%{performer}%')
            )
        )
    
    if repertoire:
        query = query.join(Concert.pieces).filter(
            or_(
                Piece.title.ilike(f'%{repertoire}%'),
                Piece.composer.ilike(f'%{repertoire}%')
            )
        )
    
    # Execute query and get results
    concerts = query.order_by(Concert.date).all()
    
    # Get all venues for the filter dropdown
    venues = Venue.query.order_by(Venue.name).all()
    
    return render_template('index.html', 
                           concerts=concerts, 
                           venues=venues,
                           filters={
                               'date_from': date_from,
                               'date_to': date_to,
                               'venue_id': venue_id,
                               'performer': performer,
                               'repertoire': repertoire
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
    """API endpoint to scrape a specific venue"""
    try:
        success = scrape_venue(venue_id)
        if success:
            return jsonify({'status': 'success', 'message': 'Venue scraped successfully'})
        else:
            return jsonify({'status': 'error', 'message': 'Failed to scrape venue'}), 400
    except Exception as e:
        logger.error(f"Error scraping venue {venue_id}: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return jsonify({'status': 'error', 'message': f"Error: {str(e)}"}), 500

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
