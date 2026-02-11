import os
import re
from flask import Blueprint, request, jsonify
from sqlalchemy import text
from config import db
from rapidfuzz import fuzz
from metaphone import doublemetaphone
from indic_transliteration import sanscript
from indic_transliteration.sanscript import transliterate

phonetic_py_bp = Blueprint("phonetic_py_bp", __name__, url_prefix="/api/pysearch")
DB_NAME = os.getenv("DB_NAME")


def get_universal_skeleton(text_val):
    if not text_val: return "", "", ""
    # Normalize Hindi scripts
    norm = text_val.replace('ं', 'न्').replace('ँ', 'न्').replace('व', 'ब').replace('v', 'b')
    norm = norm.replace('श', 'स').replace('ष', 'स')
    # Transliterate to Latin
    lat = transliterate(norm, sanscript.DEVANAGARI, sanscript.ITRANS).lower()
    # Create Consonant Skeleton
    skel = re.sub(r'[aeiouy]', '', lat)
    # Double Metaphone Hash
    meta = doublemetaphone(lat)[0]
    return lat, skel, meta


def calculate_best_score(row, q_lat, q_skel, q_meta):
    """Internal helper to score a row based on multiple fields."""
    fields_to_check = ["voter_name", "father_husband_mother_name"]
    best_score = 0

    for field in fields_to_check:
        val = row.get(field) or ""
        t_lat, t_skel, t_meta = get_universal_skeleton(val)

        skel_score = 100 if (q_skel == t_skel and q_skel != "") else 0
        ratio = fuzz.ratio(q_lat, t_lat)
        partial = fuzz.partial_ratio(q_lat, t_lat)
        phonetic = (q_meta == t_meta and q_meta != "")

        current_score = (skel_score * 0.4) + (partial * 0.4) + (ratio * 0.1)
        if phonetic: current_score += 10

        if current_score > best_score:
            best_score = current_score

    return round(best_score, 2)


def execute_phonetic_search(table_name, query_text, search_fields):
    # 🔹 Split comma separated queries
    query_list = [q.strip() for q in query_text.split(",") if q.strip()]
    if not query_list:
        return []

    # 🔹 Precompute query skeletons ONCE
    query_data = []
    first_chars = set()

    for q in query_list:
        q_lat, q_skel, q_meta = get_universal_skeleton(q)
        query_data.append((q_lat, q_skel, q_meta))
        first_chars.add(q[0])

    # 🔹 Build LIKE conditions dynamically
    like_conditions = []
    params = {}

    for i, ch in enumerate(first_chars):
        key = f"lk{i}"
        field_conditions = " OR ".join([f"{field} LIKE :{key}" for field in search_fields])
        like_conditions.append(f"({field_conditions})")
        params[key] = f"{ch}%"

    where_clause = " OR ".join(like_conditions)

    sql = f"""
        SELECT *
        FROM {DB_NAME}.{table_name}
        WHERE {where_clause}
    """

    result = db.session.execute(text(sql), params)
    rows = [dict(row._mapping) for row in result]

    unique_matches = {}

    for row in rows:
        row_id = row.get("id")
        if not row_id:
            continue

        # 🔹 Precompute row skeleton once per field
        row_data = []
        for field in search_fields:
            val = row.get(field) or ""
            row_data.append(get_universal_skeleton(val))

        best_score = 0

        for q_lat, q_skel, q_meta in query_data:
            for t_lat, t_skel, t_meta in row_data:

                skel_score = 100 if (q_skel == t_skel and q_skel != "") else 0
                ratio = fuzz.ratio(q_lat, t_lat)
                partial = fuzz.partial_ratio(q_lat, t_lat)
                phonetic = (q_meta == t_meta and q_meta != "")

                score = (skel_score * 0.4) + (partial * 0.4) + (ratio * 0.1)
                if phonetic:
                    score += 10

                if score > best_score:
                    best_score = score

        if best_score >= 55:
            row["match_score"] = round(best_score, 2)
            if row_id not in unique_matches or best_score > unique_matches[row_id]["match_score"]:
                unique_matches[row_id] = row

    return sorted(unique_matches.values(), key=lambda x: x["match_score"], reverse=True)


@phonetic_py_bp.route("/nagar-nigam", methods=["GET"])
def search_nagar_nigam():
    q = request.args.get("q", "").strip()
    if not q: return jsonify({"error": "Query required"}), 400
    # results = execute_phonetic_search("nagar_nigam", q)
    results = execute_phonetic_search(
        "nagar_nigam",
        q,
        ["voter_name", "father_husband_mother_name"]
    )

    return jsonify({"query": q, "total": len(results), "data": results})


@phonetic_py_bp.route("/gram-panchayat", methods=["GET"])
def search_gram_panchayat():
    q = request.args.get("q", "").strip()
    if not q: return jsonify({"error": "Query required"}), 400
    # results = execute_phonetic_search("gram_panchayat_voters", q)
    results = execute_phonetic_search(
        "gram_panchayat_voters",
        q,
        ["voter_name", "father_husband_mother_name"]
    )
    return jsonify({"query": q, "total": len(results), "data": results})


@phonetic_py_bp.route("/voter-pdf", methods=["GET"])
def search_voter_pdf():
    q = request.args.get("q", "").strip()
    if not q: return jsonify({"error": "Query required"}), 400
    # results = execute_phonetic_search("voters_pdf_extract", q)
    results = execute_phonetic_search(
        "voters_pdf_extract",
        q,
        ["voter_name", "father_husband_mother_name"]
    )
    return jsonify({"query": q, "total": len(results), "data": results})


@phonetic_py_bp.route("/voters-data", methods=["GET"])
def search_voter_data():
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"error": "Query required"}), 400

    results = execute_phonetic_search(
        "voter_data",
        q,
        ["e_name", "rel_name", "e_name_eng", "rel_name_eng"]
    )

    return jsonify({
        "query": q,
        "total": len(results),
        "data": results
    })
