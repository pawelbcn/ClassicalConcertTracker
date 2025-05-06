from app import app, db

with app.app_context():
    # Apply all model changes to the database
    db.create_all()
    print("Database schema updated successfully!")
