from datetime import datetime
from app import db


class Venue(db.Model):
    """Model representing a concert venue/organizer"""
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    url = db.Column(db.String(512), nullable=False)
    scraper_type = db.Column(db.String(50), nullable=False)  # Identifies the scraper to use
    last_scraped = db.Column(db.DateTime, nullable=True)
    
    # Relationship with concerts
    concerts = db.relationship('Concert', backref='venue', lazy=True, cascade="all, delete-orphan")
    
    def __repr__(self):
        return f'<Venue {self.name}>'


class Concert(db.Model):
    """Model representing a concert event"""
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(255), nullable=False)
    date = db.Column(db.DateTime, nullable=False)
    venue_id = db.Column(db.Integer, db.ForeignKey('venue.id'), nullable=False)
    external_url = db.Column(db.String(512), nullable=True)  # Link to the original concert page
    city = db.Column(db.String(100), nullable=True)  # City where the concert takes place
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships with performers and repertoire
    performers = db.relationship('Performer', secondary='concert_performer', backref='concerts', lazy=True)
    pieces = db.relationship('Piece', secondary='concert_piece', backref='concerts', lazy=True)
    
    def __repr__(self):
        return f'<Concert {self.title} on {self.date.strftime("%Y-%m-%d")}>'


class Performer(db.Model):
    """Model representing a performer (conductor, soloist, etc.)"""
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    role = db.Column(db.String(100), nullable=True)  # e.g., 'conductor', 'pianist', etc.
    
    def __repr__(self):
        return f'<Performer {self.name} ({self.role})>'


class Piece(db.Model):
    """Model representing a musical piece/composition"""
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(255), nullable=False)
    composer = db.Column(db.String(255), nullable=False)
    
    def __repr__(self):
        return f'<Piece {self.title} by {self.composer}>'


# Association tables
concert_performer = db.Table('concert_performer',
    db.Column('concert_id', db.Integer, db.ForeignKey('concert.id'), primary_key=True),
    db.Column('performer_id', db.Integer, db.ForeignKey('performer.id'), primary_key=True)
)

concert_piece = db.Table('concert_piece',
    db.Column('concert_id', db.Integer, db.ForeignKey('concert.id'), primary_key=True),
    db.Column('piece_id', db.Integer, db.ForeignKey('piece.id'), primary_key=True)
)
