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


def get_universal_skeleton(text_val):
    if not text_val: return "", "", ""

    # 1. Normalize Devanagari specifically for common sound-alikes
    norm = text_val.strip().lower()
    replacements = {
        'v': 'b', 'w': 'b',
        'sh': 's', 'shh': 's', 'z': 'j',
        'ph': 'f', 'ee': 'i', 'oo': 'u',
        'ं': 'n', 'ः': 'h', 'व': 'ब', 'श': 'स', 'ष': 'स'
    }
    for old, new in replacements.items():
        norm = norm.replace(old, new)

    # 2. Transliterate to Latin (ITRANS is good, but keep it simple)
    lat = transliterate(norm, sanscript.DEVANAGARI, sanscript.ITRANS).lower()

    # 3. Consonant Skeleton (stripping vowels makes 'Amit' and 'Amut' identical)
    skel = re.sub(r'[aeiouy]', '', lat)

    # 4. Double Metaphone (The actual phonetic engine)
    # This returns a tuple (primary, secondary). We use both.
    meta_primary, meta_secondary = doublemetaphone(lat)

    return lat, skel, meta_primary or ""


def calculate_phonetic_score(q_data, t_val):
    q_lat, q_skel, q_meta = q_data
    t_lat, t_skel, t_meta = get_universal_skeleton(t_val)

    if not t_lat: return 0

    # PHONETIC MATCH (Huge weight)
    # If the sound-code matches exactly, it's likely the same name
    phonetic_match = 100 if (q_meta == t_meta and q_meta != "") else 0

    # CONSONANT MATCH (High weight)
    # Matches 'vks' (Vikas) with 'bks' (Bikas) after normalization
    skel_match = 100 if (q_skel == t_skel and q_skel != "") else 0

    # FUZZY MATCH (Token based for name reversals)
    # Use token_sort_ratio so "Anil Kumar" matches "Kumar Anil"
    fuzzy_score = fuzz.token_sort_ratio(q_lat, t_lat)

    # FINAL CALCULATION
    # We prioritize the "Sound" (Phonetic + Skeleton) over the "Spelling" (Fuzzy)
    score = (phonetic_match * 0.45) + (skel_match * 0.35) + (fuzzy_score * 0.20)

    return round(score, 2)


def execute_phonetic_search(table_name, query_text, search_fields):
    query_list = [q.strip() for q in query_text.split(",") if q.strip()]
    if not query_list:
        return []

    # 🔹 Build SQL LIKE filter (broad match)
    like_conditions = []
    params = {}

    for i, q in enumerate(query_list):
        key = f"q{i}"
        field_conditions = " OR ".join(
            [f"{field} LIKE :{key}" for field in search_fields]
        )
        like_conditions.append(f"({field_conditions})")
        params[key] = f"%{q}%"

    where_clause = " OR ".join(like_conditions)

    sql = f"""
        SELECT *
        FROM {DB_NAME}.{table_name}
        WHERE {where_clause}
    """

    result = db.session.execute(text(sql), params)
    rows = [dict(row._mapping) for row in result]

    if not rows:
        return []

    # 🔹 Precompute query skeletons
    query_data = [get_universal_skeleton(q) for q in query_list]

    unique_matches = {}

    for row in rows:
        row_id = row.get("id")
        if not row_id:
            continue

        best_score = 0

        for field in search_fields:
            val = row.get(field) or ""
            t_lat, t_skel, t_meta = get_universal_skeleton(val)

            for q_lat, q_skel, q_meta in query_data:

                token_match = any(token in t_lat for token in q_lat.split())

                skel_score = 100 if (q_skel == t_skel and q_skel != "") else 0
                ratio = fuzz.ratio(q_lat, t_lat)
                partial = fuzz.partial_ratio(q_lat, t_lat)
                phonetic = (q_meta == t_meta and q_meta != "")

                score = (skel_score * 0.3) + (partial * 0.5) + (ratio * 0.1)

                if phonetic:
                    score += 10

                if token_match:
                    score += 20

                if score > best_score:
                    best_score = score

        if best_score >= 35:  # 🔥 lower threshold
            row["match_score"] = round(best_score, 2)
            unique_matches[row_id] = row

    return sorted(unique_matches.values(), key=lambda x: x["match_score"], reverse=True)


# def execute_sequential_search(table_name, query_text, voter_fields, father_fields):
#     query_list = [q.strip() for q in query_text.split(",") if q.strip()]
#     if len(query_list) != 2:
#         return []
#
#     first_query, second_query = query_list
#
#     # Step 1: broad LIKE on voter fields
#     like_conditions = []
#     params = {}
#
#     for i, field in enumerate(voter_fields):
#         key = f"v{i}"
#         like_conditions.append(f"{field} LIKE :{key}")
#         params[key] = f"%{first_query}%"
#
#     where_clause = " OR ".join(like_conditions)
#
#     sql = f"""
#         SELECT *
#         FROM {DB_NAME}.{table_name}
#         WHERE {where_clause}
#     """
#
#     result = db.session.execute(text(sql), params)
#     rows = [dict(row._mapping) for row in result]
#
#     if not rows:
#         return []
#
#     filtered_results = []
#
#     q_lat, q_skel, q_meta = get_universal_skeleton(second_query)
#
#     for row in rows:
#         best_score = 0
#
#         for field in father_fields:
#             val = row.get(field) or ""
#             t_lat, t_skel, t_meta = get_universal_skeleton(val)
#
#             token_match = any(token in t_lat for token in q_lat.split())
#
#             skel_score = 100 if (q_skel == t_skel and q_skel != "") else 0
#             ratio = fuzz.ratio(q_lat, t_lat)
#             partial = fuzz.partial_ratio(q_lat, t_lat)
#             phonetic = (q_meta == t_meta and q_meta != "")
#
#             score = (skel_score * 0.3) + (partial * 0.5) + (ratio * 0.1)
#
#             if phonetic:
#                 score += 10
#
#             if token_match:
#                 score += 20
#
#             if score > best_score:
#                 best_score = score
#
#         if best_score >= 30:
#             row["match_score"] = round(best_score, 2)
#             filtered_results.append(row)
#
#     return sorted(filtered_results, key=lambda x: x["match_score"], reverse=True)
def execute_sequential_search(table_name, query_text, voter_fields, father_fields):
    query_list = [q.strip() for q in query_text.split(",") if q.strip()]
    if len(query_list) != 2:
        return []

    first_query, second_query = query_list

    # Step 1: broad LIKE on voter fields
    like_conditions = []
    params = {}

    for i, field in enumerate(voter_fields):
        key = f"v{i}"
        like_conditions.append(f"{field} LIKE :{key}")
        params[key] = f"%{first_query}%"

    where_clause = " OR ".join(like_conditions)

    sql = f"""
        SELECT *
        FROM {DB_NAME}.{table_name}
        WHERE {where_clause}
    """

    result = db.session.execute(text(sql), params)
    rows = [dict(row._mapping) for row in result]

    if not rows:
        return []

    filtered_results = []

    # 🔥 Precompute second query skeleton
    q_lat, q_skel, q_meta = get_universal_skeleton(second_query)
    query_tokens = q_lat.split()

    for row in rows:
        best_score = 0

        for field in father_fields:
            val = row.get(field) or ""
            t_lat, t_skel, t_meta = get_universal_skeleton(val)

            target_tokens = t_lat.split()

            # 🔥 Full skeleton match (highest priority)
            full_name_match = (q_skel == t_skel and q_skel != "")

            # 🔥 Token match ratio
            matched_tokens = sum(1 for token in query_tokens if token in target_tokens)
            token_ratio = matched_tokens / len(query_tokens) if query_tokens else 0

            ratio = fuzz.ratio(q_lat, t_lat)
            partial = fuzz.partial_ratio(q_lat, t_lat)
            phonetic = (q_meta == t_meta and q_meta != "")

            score = 0

            # 🔥 Strong boost for full name match
            if full_name_match:
                score += 60

            # 🔥 Majority token match required
            if token_ratio >= 0.7:
                score += 30
            elif token_ratio >= 0.4:
                score += 15

            # 🔥 Fuzzy scoring
            score += partial * 0.3
            score += ratio * 0.1

            if phonetic:
                score += 10

            if score > best_score:
                best_score = score

        if best_score >= 35:
            row["match_score"] = round(best_score, 2)
            filtered_results.append(row)

    return sorted(filtered_results, key=lambda x: x["match_score"], reverse=True)


@phonetic_py_bp.route("/nagar-nigam", methods=["GET"])
def search_nagar_nigam():
    q = request.args.get("q", "").strip()
    search_type = request.args.get("type", "normal").strip().lower()

    if not q:
        return jsonify({"error": "Query required"}), 400

    if search_type == "voter_father" and "," in q:
        results = execute_sequential_search(
            "nagar_nigam",
            q,
            ["voter_name"],
            ["father_husband_mother_name"]
        )
    else:
        results = execute_phonetic_search(
            "nagar_nigam",
            q,
            ["voter_name", "father_husband_mother_name"]
        )

    return jsonify({
        "query": q,
        "type": search_type,
        "total": len(results),
        "data": results
    })


@phonetic_py_bp.route("/gram-panchayat", methods=["GET"])
def search_gram_panchayat():
    q = request.args.get("q", "").strip()
    search_type = request.args.get("type", "normal").strip().lower()

    if not q:
        return jsonify({"error": "Query required"}), 400

    if search_type == "voter_father" and "," in q:
        results = execute_sequential_search(
            "gram_panchayat_voters",
            q,
            ["voter_name"],
            ["father_husband_mother_name"]
        )
    else:
        results = execute_phonetic_search(
            "gram_panchayat_voters",
            q,
            ["voter_name", "father_husband_mother_name"]
        )

    return jsonify({
        "query": q,
        "type": search_type,
        "total": len(results),
        "data": results
    })


@phonetic_py_bp.route("/voter-pdf", methods=["GET"])
def search_voter_pdf():
    q = request.args.get("q", "").strip()
    search_type = request.args.get("type", "normal").strip().lower()

    if not q:
        return jsonify({"error": "Query required"}), 400

    if search_type == "voter_father" and "," in q:
        results = execute_sequential_search(
            "voters_pdf_extract",
            q,
            ["voter_name"],
            ["father_husband_mother_name"]
        )
    else:
        results = execute_phonetic_search(
            "voters_pdf_extract",
            q,
            ["voter_name", "father_husband_mother_name"]
        )

    return jsonify({
        "query": q,
        "type": search_type,
        "total": len(results),
        "data": results
    })


@phonetic_py_bp.route("/voters-data", methods=["GET"])
def search_voter_data():
    q = request.args.get("q", "").strip()
    search_type = request.args.get("type", "normal").strip().lower()

    if not q:
        return jsonify({"error": "Query required"}), 400

    # 🔹 Sequential voter + father search
    if search_type == "voter_father" and "," in q:
        results = execute_sequential_search(
            "voter_data",
            q,
            ["e_name", "e_name_eng"],  # voter fields
            ["rel_name", "rel_name_eng"]  # father fields
        )

    else:
        # Normal search (single or multi OR search)
        results = execute_phonetic_search(
            "voter_data",
            q,
            ["e_name", "rel_name", "e_name_eng", "rel_name_eng"]
        )

    return jsonify({
        "query": q,
        "type": search_type,
        "total": len(results),
        "data": results
    })


@phonetic_py_bp.route("/testing-data", methods=["GET"])
def search_testing_data():
    q = request.args.get("q", "").strip()
    search_type = request.args.get("type", "normal").strip().lower()

    if not q:
        return jsonify({"error": "Query required"}), 400

    if search_type == "voter_father" and "," in q:
        results = execute_sequential_search(
            "testing",
            q,
            ["voter_name"],
            ["father_husband_mother_name"]
        )
    else:
        results = execute_phonetic_search(
            "testing",
            q,
            ["voter_name", "father_husband_mother_name"]
        )

    return jsonify({
        "query": q,
        "type": search_type,
        "total": len(results),
        "data": results
    })
