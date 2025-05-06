import logging
from app import app
from routes import *  # Import all route definitions

# Configure logging
logging.basicConfig(level=logging.DEBUG)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
