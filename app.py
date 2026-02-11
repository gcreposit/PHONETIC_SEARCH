import os
from dotenv import load_dotenv
from flask_cors import CORS

from config import create_app, db
from Controller.PageController import page_bp
from Controller.ApiController import api_bp
from Controller.PhoneticPythonController import phonetic_py_bp

# Load environment variables
load_dotenv()

# Create Flask app
app = create_app()

# Enable CORS (optional)
CORS(app, origins="*", supports_credentials=True)

# Register Blueprints
app.register_blueprint(page_bp, url_prefix='/')
app.register_blueprint(api_bp, url_prefix='/api')
app.register_blueprint(phonetic_py_bp)

# Run Application
if __name__ == "__main__":
    port = int(os.getenv("PORT", 7777))

    # Create DB tables (only if you need DB in this project)
    with app.app_context():
        db.create_all()
        print("✅ Database initialized")

    print(f"🚀 Server running on http://localhost:{port}")

    app.run(
        host="0.0.0.0",
        port=port,
        debug=True
    )
