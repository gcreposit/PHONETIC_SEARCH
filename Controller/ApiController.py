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


# TESTING

# @api_bp.route("/testing-data", methods=["GET"])
# def getTestingData():
#     page = int(request.args.get("page", 1))
#     limit = int(request.args.get("limit", 1000))
#
#     try:
#         data, total = get_paginated_data("testing", page, limit)
#         return jsonify({
#             "page": page,
#             "limit": limit,
#             "total_records": total,
#             "data": data
#         })
#     except Exception as e:
#         return jsonify({"error": str(e)}), 500

@api_bp.route("/testing-data", methods=["GET"])
def getTestingData():
    page = int(request.args.get("page", 1))
    limit = int(request.args.get("limit", 1000))

    offset = (page - 1) * limit

    try:
        # 🔥 Count ONLY ACTIVE records
        count_query = f"""
            SELECT COUNT(*) 
            FROM {DB_NAME}.testing 
            WHERE status = 'ACTIVE'
        """
        total_count = db.session.execute(text(count_query)).fetchone()[0]

        # 🔥 Fetch ONLY ACTIVE records
        data_query = f"""
            SELECT * 
            FROM {DB_NAME}.testing 
            WHERE status = 'ACTIVE'
            LIMIT :limit OFFSET :offset
        """

        result = db.session.execute(
            text(data_query),
            {"limit": limit, "offset": offset}
        )

        rows = [dict(row._mapping) for row in result]

        return jsonify({
            "page": page,
            "limit": limit,
            "total_records": total_count,
            "data": rows
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ----------------------------------------
# Toggle Status (Testing Table)
# ----------------------------------------
# @api_bp.route("/testing-data/check-toggle/<int:id>", methods=["POST"])
# def checkedUncheckStatusChange(id):
#     try:
#         # 🔥 1️⃣ Toggle status
#         toggle_query = f"""
#             UPDATE {DB_NAME}.testing
#             SET check_status = CASE
#                             WHEN status = 'CHECKED' THEN 'UNCHECKED'
#                             ELSE 'CHECKED'
#                          END
#             WHERE id = :id
#         """
#
#         result = db.session.execute(text(toggle_query), {"id": id})
#
#         # If no row updated
#         if result.rowcount == 0:
#             return jsonify({"error": "ID not found"}), 404
#
#         db.session.commit()
#
#         # 🔥 2️⃣ Fetch updated status
#         fetch_query = f"""
#             SELECT id, status
#             FROM {DB_NAME}.testing
#             WHERE id = :id
#         """
#
#         updated_row = db.session.execute(
#             text(fetch_query),
#             {"id": id}
#         ).fetchone()
#
#         return jsonify({
#             "message": "Status toggled successfully",
#             "id": updated_row._mapping["id"],
#             "new_status": updated_row._mapping["status"]
#         })
#
#     except Exception as e:
#         db.session.rollback()
#         return jsonify({"error": str(e)}), 500
#
#     @api_bp.route("/testing-data/toggle-status/<int:id>", methods=["POST"])
#     def toggle_testing_status(id):
#         try:
#             # 🔥 1️⃣ Toggle status
#             toggle_query = f"""
#                 UPDATE {DB_NAME}.testing
#                 SET status = CASE
#                                 WHEN status = 'ACTIVE' THEN 'INACTIVE'
#                                 ELSE 'ACTIVE'
#                              END
#                 WHERE id = :id
#             """
#
#             result = db.session.execute(text(toggle_query), {"id": id})
#
#             # If no row updated
#             if result.rowcount == 0:
#                 return jsonify({"error": "ID not found"}), 404
#
#             db.session.commit()
#
#             # 🔥 2️⃣ Fetch updated status
#             fetch_query = f"""
#                 SELECT id, status
#                 FROM {DB_NAME}.testing
#                 WHERE id = :id
#             """
#
#             updated_row = db.session.execute(
#                 text(fetch_query),
#                 {"id": id}
#             ).fetchone()
#
#             return jsonify({
#                 "message": "Status toggled successfully",
#                 "id": updated_row._mapping["id"],
#                 "new_status": updated_row._mapping["status"]
#             })
#
#         except Exception as e:
#             db.session.rollback()
#             return jsonify({"error": str(e)}), 500


@api_bp.route("/testing-data/check-toggle/<int:id>", methods=["POST"])
def checkedUncheckStatusChange(id):
    try:
        # 🔥 1️⃣ Get current check_status
        fetch_query = f"""
            SELECT check_status
            FROM {DB_NAME}.testing
            WHERE id = :id
        """

        row = db.session.execute(
            text(fetch_query),
            {"id": id}
        ).fetchone()

        if not row:
            return jsonify({"error": "ID not found"}), 404

        current_status = row._mapping["check_status"]

        # 🔥 2️⃣ Decide new status
        if current_status == "CHECKED":
            new_status = "UNCHECKED"
        else:
            new_status = "CHECKED"

        # 🔥 3️⃣ Update
        update_query = f"""
            UPDATE {DB_NAME}.testing
            SET check_status = :new_status
            WHERE id = :id
        """

        db.session.execute(
            text(update_query),
            {"new_status": new_status, "id": id}
        )

        db.session.commit()

        return jsonify({
            "message": "Check status toggled successfully",
            "id": id,
            "new_status": new_status
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500

# @api_bp.route("/testing-data/toggle-status/<int:id>", methods=["POST"])
# def toggle_testing_status(id):
#     try:
#         toggle_query = f"""
#             UPDATE {DB_NAME}.testing
#             SET status = CASE
#                             WHEN status = 'ACTIVE' THEN 'INACTIVE'
#                             ELSE 'ACTIVE'
#                          END
#             WHERE id = :id
#             RETURNING id, status
#         """
#
#         result = db.session.execute(text(toggle_query), {"id": id})
#         updated_row = result.fetchone()
#
#         if not updated_row:
#             return jsonify({"error": "ID not found"}), 404
#
#         db.session.commit()
#
#         return jsonify({
#             "message": "Status toggled successfully",
#             "id": updated_row._mapping["id"],
#             "new_status": updated_row._mapping["status"]
#         })
#
#     except Exception as e:
#         db.session.rollback()
#         return jsonify({"error": str(e)}), 500
