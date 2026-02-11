import os
import urllib.parse
from dotenv import load_dotenv
from flask import Flask
from flask_sqlalchemy import SQLAlchemy

load_dotenv()

db = SQLAlchemy()


def create_app():
    app = Flask(__name__)

    # Build DB connection URL using PyMySQL
    DB_HOST = os.getenv("DB_HOST")
    DB_USER = os.getenv("DB_USER")
    DB_PORT = os.getenv("DB_PORT", "3306")  # ✅ default to 3306 if not provided

    DB_PASS = urllib.parse.quote_plus(os.getenv("DB_PASS"))
    DB_NAME = os.getenv("DB_NAME")

    # app.config["SQLALCHEMY_DATABASE_URI"] = (
    #     f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"
    # )

    app.config["SQLALCHEMY_DATABASE_URI"] = (
        f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

    # ✅ Pooling from ENV
    app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
        "pool_size": int(os.getenv("DB_POOL_SIZE", 20)),
        "max_overflow": int(os.getenv("DB_MAX_OVERFLOW", 30)),
        "pool_timeout": int(os.getenv("DB_POOL_TIMEOUT", 60)),
        "pool_recycle": int(os.getenv("DB_POOL_RECYCLE", 1800)),
        "pool_pre_ping": True
    }

    # # Twitter API
    # TW_BEARER_TOKEN = os.getenv("TW_BEARER_TOKEN")
    # if not TW_BEARER_TOKEN:
    #     raise RuntimeError("Missing TW_BEARER_TOKEN in .env")

    db.init_app(app)
    return app
