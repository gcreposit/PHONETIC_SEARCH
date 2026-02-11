import os
from flask import Blueprint, request, jsonify
from sqlalchemy import text
from config import db

api_bp = Blueprint("api_bp", __name__)
DB_NAME = os.getenv("DB_NAME")


# ----------------------------------------
# Helper for Paginated Fetch
# ----------------------------------------
def get_paginated_data(table_name, page, limit):
    offset = (page - 1) * limit

    # 1. Get total count for frontend progress bars/pagination UI
    count_query = f"SELECT COUNT(*) as total FROM {DB_NAME}.{table_name}"
    total_count = db.session.execute(text(count_query)).fetchone()[0]

    # 2. Get the actual chunk of data
    data_query = f"SELECT * FROM {DB_NAME}.{table_name} LIMIT :limit OFFSET :offset"
    result = db.session.execute(text(data_query), {"limit": limit, "offset": offset})

    rows = [dict(row._mapping) for row in result]

    return rows, total_count


# ----------------------------------------
# Nagar Nigam
# ----------------------------------------
@api_bp.route("/nagar-nigam", methods=["GET"])
def get_nagar_nigam():
    # Default to page 1 and limit 1000 if not provided
    page = int(request.args.get("page", 1))
    limit = int(request.args.get("limit", 1000))

    try:
        data, total = get_paginated_data("nagar_nigam", page, limit)
        return jsonify({
            "page": page,
            "limit": limit,
            "total_records": total,
            "data": data
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ----------------------------------------
# Gram Panchayat Voters
# ----------------------------------------
@api_bp.route("/gram-panchayat-voters", methods=["GET"])
def get_gram_panchayat_voters():
    page = int(request.args.get("page", 1))
    limit = int(request.args.get("limit", 1000))

    try:
        data, total = get_paginated_data("gram_panchayat_voters", page, limit)
        return jsonify({
            "page": page,
            "limit": limit,
            "total_records": total,
            "data": data
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ----------------------------------------
# Voters PDF Extract
# ----------------------------------------
@api_bp.route("/voters-pdf-extract", methods=["GET"])
def get_voters_pdf_extract():
    page = int(request.args.get("page", 1))
    limit = int(request.args.get("limit", 1000))

    try:
        data, total = get_paginated_data("voters_pdf_extract", page, limit)
        return jsonify({
            "page": page,
            "limit": limit,
            "total_records": total,
            "data": data
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ----------------------------------------
# Voters Data
# ----------------------------------------
@api_bp.route("/voters-data", methods=["GET"])
def get_voters_data():
    page = int(request.args.get("page", 1))
    limit = int(request.args.get("limit", 1000))

    try:
        data, total = get_paginated_data("voter_data", page, limit)
        return jsonify({
            "page": page,
            "limit": limit,
            "total_records": total,
            "data": data
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500