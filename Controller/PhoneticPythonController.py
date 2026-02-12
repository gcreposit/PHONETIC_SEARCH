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


#=============================================================================================#
# NEW CODE IMPL #

@phonetic_py_bp.route("/deduplicate-voters", methods=["POST"])
def deduplicate_voters():
    """
    Find duplicate voter records based on phonetic matching of:
    1. voter_name
    2. father_husband_mother_name

    Mark duplicates as INACTIVE in status column
    """
    table_name = request.json.get("table_name", "gram_panchayat_voters")
    dry_run = request.json.get("dry_run", True)  # Safety: preview before actual update
    similarity_threshold = request.json.get("threshold", 85)  # Phonetic match threshold

    try:
        # Step 1: Fetch all active voter records
        sql = f"""
            SELECT id, voter_name, father_husband_mother_name, status
            FROM {DB_NAME}.{table_name}
            WHERE status IS NULL OR status != 'INACTIVE'
            ORDER BY id ASC
        """

        result = db.session.execute(text(sql))
        rows = [dict(row._mapping) for row in result]

        if not rows:
            return jsonify({
                "message": "No active records found",
                "total_processed": 0,
                "duplicates_found": 0
            })

        # Step 2: Group records by phonetic similarity
        duplicate_groups = find_duplicate_groups(rows, similarity_threshold)

        # Step 3: Prepare update list (keep first, mark rest INACTIVE)
        records_to_deactivate = []

        for group in duplicate_groups:
            if len(group) > 1:
                # Keep first record (lowest ID), mark rest as duplicates
                primary_record = group[0]
                duplicates = group[1:]

                for dup in duplicates:
                    records_to_deactivate.append({
                        "id": dup["id"],
                        "voter_name": dup["voter_name"],
                        "father_name": dup["father_husband_mother_name"],
                        "duplicate_of": primary_record["id"],
                        "match_score": dup.get("match_score", 0)
                    })

        # Step 4: Execute updates (if not dry run)
        if not dry_run and records_to_deactivate:
            deactivate_duplicate_records(table_name, records_to_deactivate)

        return jsonify({
            "success": True,
            "dry_run": dry_run,
            "total_records_processed": len(rows),
            "duplicate_groups_found": len(duplicate_groups),
            "records_to_deactivate": len(records_to_deactivate),
            "details": records_to_deactivate[:100],  # Show first 100
            "message": "Dry run completed" if dry_run else "Duplicates marked as INACTIVE"
        })

    except Exception as e:
        return jsonify({
            "error": str(e),
            "success": False
        }), 500



def cluster_by_similarity(records, threshold):
    """
    Clusters records using phonetic + fuzzy matching
    Returns list of duplicate groups
    """
    groups = []
    processed = set()

    for i, rec1 in enumerate(records):
        if rec1["id"] in processed:
            continue

        current_group = [rec1]
        processed.add(rec1["id"])

        for j, rec2 in enumerate(records):
            if i == j or rec2["id"] in processed:
                continue

            # Calculate similarity between rec1 and rec2
            voter_score = calculate_name_similarity(
                (rec1["_v_lat"], rec1["_v_skel"], rec1["_v_meta"]),
                (rec2["_v_lat"], rec2["_v_skel"], rec2["_v_meta"])
            )

            father_score = calculate_name_similarity(
                (rec1["_f_lat"], rec1["_f_skel"], rec1["_f_meta"]),
                (rec2["_f_lat"], rec2["_f_skel"], rec2["_f_meta"])
            )

            # Both voter and father should match
            if voter_score >= threshold and father_score >= threshold:
                rec2["match_score"] = round((voter_score + father_score) / 2, 2)
                current_group.append(rec2)
                processed.add(rec2["id"])

        if len(current_group) > 1:
            groups.append(current_group)

    return groups


# Replace these 3 functions completely in your PhoneticPythonController.py

def find_duplicate_groups(rows, threshold=85):
    """
    Groups records that are phonetically similar based on:
    1. voter_name match
    2. father_husband_mother_name match

    Returns: List of duplicate groups
    """

    # Precompute phonetic data for all records
    valid_rows = []
    for row in rows:
        voter_name = row.get("voter_name") or ""
        father_name = row.get("father_husband_mother_name") or ""

        voter_name = voter_name.strip() if voter_name else ""
        father_name = father_name.strip() if father_name else ""

        if not voter_name:
            continue

        # Generate phonetic signatures
        v_lat, v_skel, v_meta = get_universal_skeleton(voter_name)
        f_lat, f_skel, f_meta = get_universal_skeleton(father_name)

        row["_v_lat"] = v_lat
        row["_v_skel"] = v_skel
        row["_v_meta"] = v_meta
        row["_f_lat"] = f_lat
        row["_f_skel"] = f_skel
        row["_f_meta"] = f_meta

        valid_rows.append(row)

    # 🔥 Direct comparison without bucketing - more accurate
    duplicate_groups = cluster_by_similarity(valid_rows, threshold)

    return duplicate_groups


def calculate_name_similarity(q_data, t_data):
    """
    Calculate phonetic similarity between two names
    Returns: Score (0-100)
    """
    q_lat, q_skel, q_meta = q_data
    t_lat, t_skel, t_meta = t_data

    # Handle empty strings
    if not q_lat and not t_lat:
        return 100  # Both empty = match

    if not q_lat or not t_lat:
        return 0  # One empty, one not = no match

    # 1. Phonetic match (metaphone) - exact sound match
    phonetic_match = 100 if (q_meta == t_meta and q_meta != "") else 0

    # 2. Skeleton match (consonants only) - handles vowel variations
    skel_match = 100 if (q_skel == t_skel and q_skel != "") else 0

    # 3. Partial skeleton match - for cases like "shn" vs "shng" or "sinh" vs "singh"
    if not skel_match and q_skel and t_skel:
        # Check if one skeleton is substring of another
        if q_skel in t_skel or t_skel in q_skel:
            skel_match = 80
        else:
            # Check character overlap percentage
            common_chars = len(set(q_skel) & set(t_skel))
            total_chars = max(len(q_skel), len(t_skel))
            if total_chars > 0:
                overlap_ratio = common_chars / total_chars
                if overlap_ratio > 0.7:
                    skel_match = 60

    # 4. Fuzzy string matching - token sort handles word reordering
    fuzzy_score = fuzz.token_sort_ratio(q_lat, t_lat)

    # 5. Partial ratio - substring matching for partial names
    partial_score = fuzz.partial_ratio(q_lat, t_lat)

    # 6. Token overlap - handles name order changes
    q_tokens = set(q_lat.split())
    t_tokens = set(t_lat.split())
    if q_tokens or t_tokens:
        token_overlap = len(q_tokens & t_tokens) / max(len(q_tokens), len(t_tokens)) * 100
    else:
        token_overlap = 0

    # 🔥 Enhanced weighted score with multiple signals
    score = (
            (phonetic_match * 0.25) +  # Metaphone code match
            (skel_match * 0.25) +  # Consonant skeleton match
            (fuzzy_score * 0.20) +  # Token sort ratio
            (partial_score * 0.15) +  # Partial string match
            (token_overlap * 0.15)  # Token overlap ratio
    )

    return round(score, 2)


def get_universal_skeleton(text_val):
    """
    Enhanced phonetic normalization for Hindi/English names
    """
    if not text_val:
        return "", "", ""

    # 1. Normalize Devanagari specifically for common sound-alikes
    norm = text_val.strip().lower()

    # 🔥 Enhanced replacements - handle more variations
    replacements = {
        # Anusvara and visarga normalization
        'ं': 'n',  # Anusvara to 'n'
        'ँ': 'n',  # Chandrabindu to 'n'
        'ः': 'h',  # Visarga to 'h'
        # Consonant normalization
        'व': 'ब',  # va to ba
        'श': 'स',  # sha to sa
        'ष': 'स',  # shha to sa
        'ण': 'न',  # Na to na
        'ढ': 'ड',  # dha to da
        # Latin normalization
        'v': 'b',
        'w': 'b',
        'sh': 's',
        'shh': 's',
        'z': 'j',
        'ph': 'f',
        'ee': 'i',
        'oo': 'u',
    }

    for old, new in replacements.items():
        norm = norm.replace(old, new)

    # 2. Transliterate to Latin (ITRANS)
    try:
        lat = transliterate(norm, sanscript.DEVANAGARI, sanscript.ITRANS).lower()
    except:
        lat = norm  # Fallback if transliteration fails

    # Clean up transliteration artifacts
    lat = lat.replace('~', 'n')  # Handle tilde from anusvara
    lat = lat.replace('M', 'n')  # Handle capital M from anusvara
    lat = lat.replace('.', '')  # Remove dots

    # 3. Consonant Skeleton (stripping vowels)
    skel = re.sub(r'[aeiouy]', '', lat)

    # 4. Double Metaphone (The actual phonetic engine)
    meta_primary, meta_secondary = doublemetaphone(lat)

    return lat, skel, meta_primary or ""





# 🔥 ADD THIS NEW DEBUG ENDPOINT
@phonetic_py_bp.route("/test-specific", methods=["GET"])
def test_specific():
    """Test the specific case you mentioned"""
    name1_v = "अखिलेश प्रताप सिहं"
    name1_f = "विजय कुमार सिंह"

    name2_v = "अखिलेश प्रताप सिंह"
    name2_f = "विजय कुमार सिंह"

    v1_lat, v1_skel, v1_meta = get_universal_skeleton(name1_v)
    v2_lat, v2_skel, v2_meta = get_universal_skeleton(name2_v)

    f1_lat, f1_skel, f1_meta = get_universal_skeleton(name1_f)
    f2_lat, f2_skel, f2_meta = get_universal_skeleton(name2_f)

    voter_score = calculate_name_similarity(
        (v1_lat, v1_skel, v1_meta),
        (v2_lat, v2_skel, v2_meta)
    )

    father_score = calculate_name_similarity(
        (f1_lat, f1_skel, f1_meta),
        (f2_lat, f2_skel, f2_meta)
    )

    return jsonify({
        "test_case": {
            "voter1": name1_v,
            "voter2": name2_v,
            "father1": name1_f,
            "father2": name2_f
        },
        "voter1_phonetics": {
            "latin": v1_lat,
            "skeleton": v1_skel,
            "metaphone": v1_meta
        },
        "voter2_phonetics": {
            "latin": v2_lat,
            "skeleton": v2_skel,
            "metaphone": v2_meta
        },
        "father1_phonetics": {
            "latin": f1_lat,
            "skeleton": f1_skel,
            "metaphone": f1_meta
        },
        "father2_phonetics": {
            "latin": f2_lat,
            "skeleton": f2_skel,
            "metaphone": f2_meta
        },
        "scores": {
            "voter_similarity": voter_score,
            "father_similarity": father_score,
            "combined_average": round((voter_score + father_score) / 2, 2)
        },
        "matching_results": {
            "would_match_at_threshold_85": voter_score >= 85 and father_score >= 85,
            "would_match_at_threshold_80": voter_score >= 80 and father_score >= 80,
            "would_match_at_threshold_70": voter_score >= 70 and father_score >= 70
        }
    })


# 🔥 ADD THIS DEBUG ENDPOINT FOR ANY TWO NAMES
@phonetic_py_bp.route("/debug-phonetic", methods=["GET"])
def debug_phonetic():
    """
    Debug phonetic matching for any two names
    Usage: /debug-phonetic?name1=राम&name2=Ram
    """
    name1 = request.args.get("name1", "")
    name2 = request.args.get("name2", "")

    if not name1 or not name2:
        return jsonify({
            "error": "Please provide both name1 and name2 parameters",
            "example": "/debug-phonetic?name1=अखिलेश&name2=अकिलेश"
        }), 400

    lat1, skel1, meta1 = get_universal_skeleton(name1)
    lat2, skel2, meta2 = get_universal_skeleton(name2)

    similarity_score = calculate_name_similarity(
        (lat1, skel1, meta1),
        (lat2, skel2, meta2)
    )

    return jsonify({
        "name1": {
            "original": name1,
            "latin": lat1,
            "skeleton": skel1,
            "metaphone": meta1
        },
        "name2": {
            "original": name2,
            "latin": lat2,
            "skeleton": skel2,
            "metaphone": meta2
        },
        "comparison": {
            "similarity_score": similarity_score,
            "skeleton_match": skel1 == skel2,
            "metaphone_match": meta1 == meta2,
            "would_match_at_85": similarity_score >= 85,
            "would_match_at_80": similarity_score >= 80,
            "would_match_at_70": similarity_score >= 70
        }
    })


def deactivate_duplicate_records(table_name, records_to_deactivate):
    """
    Mark duplicate records as INACTIVE
    Also store reference to primary record in similar_too column
    """
    if not records_to_deactivate:
        return

    # Batch update for efficiency
    ids_to_deactivate = [rec["id"] for rec in records_to_deactivate]

    # Create a mapping of duplicate_id -> primary_id
    similar_mapping = {rec["id"]: rec["duplicate_of"] for rec in records_to_deactivate}

    # Update in batches of 1000
    batch_size = 1000
    for i in range(0, len(ids_to_deactivate), batch_size):
        batch = ids_to_deactivate[i:i + batch_size]

        # Build CASE statement for similar_too
        case_statements = []
        for rec_id in batch:
            primary_id = similar_mapping[rec_id]
            case_statements.append(f"WHEN id = {rec_id} THEN {primary_id}")

        case_sql = " ".join(case_statements)

        sql = f"""
            UPDATE {DB_NAME}.{table_name}
            SET status = 'INACTIVE',
                similar_too = CASE {case_sql} END,
                check_status = 'DUPLICATE_DETECTED'
            WHERE id IN :ids
        """

        db.session.execute(text(sql), {"ids": tuple(batch)})

    db.session.commit()


# Additional utility endpoint to preview duplicates before deactivation
@phonetic_py_bp.route("/preview-duplicates", methods=["GET"])
def preview_duplicates():
    """
    Preview duplicate records without making any changes
    """
    table_name = request.args.get("table", "gram_panchayat_voters")
    limit = int(request.args.get("limit", 50))
    threshold = int(request.args.get("threshold", 85))

    sql = f"""
        SELECT id, voter_name, father_husband_mother_name, status
        FROM {DB_NAME}.{table_name}
        WHERE status IS NULL OR status != 'INACTIVE'
        ORDER BY id ASC
        LIMIT 5000
    """

    result = db.session.execute(text(sql))
    rows = [dict(row._mapping) for row in result]

    duplicate_groups = find_duplicate_groups(rows, threshold)

    # Format for easy viewing
    preview_data = []
    for group in duplicate_groups[:limit]:
        if len(group) > 1:
            preview_data.append({
                "primary_record": {
                    "id": group[0]["id"],
                    "voter_name": group[0]["voter_name"],
                    "father_name": group[0]["father_husband_mother_name"]
                },
                "duplicates": [
                    {
                        "id": rec["id"],
                        "voter_name": rec["voter_name"],
                        "father_name": rec["father_husband_mother_name"],
                        "match_score": rec.get("match_score", 0)
                    }
                    for rec in group[1:]
                ]
            })

    return jsonify({
        "total_duplicate_groups": len(duplicate_groups),
        "preview_count": len(preview_data),
        "threshold_used": threshold,
        "data": preview_data
    })


# Add this to your phonetic_py_bp routes


@phonetic_py_bp.route("/statistics", methods=["GET"])
def get_statistics():
    """
    Get statistics about records in a table
    """
    table_name = request.args.get("table", "gram_panchayat_voters")

    try:
        sql = f"""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN status = 'INACTIVE' THEN 1 ELSE 0 END) as inactive,
                SUM(CASE WHEN status IS NULL OR status != 'INACTIVE' THEN 1 ELSE 0 END) as active,
                SUM(CASE WHEN check_status = 'DUPLICATE_DETECTED' THEN 1 ELSE 0 END) as duplicates
            FROM {DB_NAME}.{table_name}
        """

        result = db.session.execute(text(sql))
        row = dict(result.fetchone()._mapping)

        total = row.get('total', 0)
        inactive = row.get('inactive', 0)
        active = row.get('active', 0)
        duplicates = row.get('duplicates', 0)

        duplicate_percentage = round((inactive / total * 100), 2) if total > 0 else 0

        return jsonify({
            "table_name": table_name,
            "total_records": total,
            "active_records": active,
            "inactive_records": inactive,
            "duplicate_records": duplicates,
            "duplicate_percentage": duplicate_percentage
        })

    except Exception as e:
        return jsonify({
            "error": str(e),
            "success": False
        }), 500


@phonetic_py_bp.route("/reset-to-active", methods=["POST"])
def reset_to_active():
    """
    Reset all records back to ACTIVE status
    Clear status, similar_too, and check_status columns
    """
    table_name = request.json.get("table_name", "gram_panchayat_voters")

    try:
        # Count records before reset
        count_sql = f"""
            SELECT COUNT(*) as count
            FROM {DB_NAME}.{table_name}
            WHERE status = 'INACTIVE' OR check_status IS NOT NULL OR similar_too IS NOT NULL
        """
        result = db.session.execute(text(count_sql))
        records_to_reset = result.fetchone()[0]

        # Reset all records
        reset_sql = f"""
            UPDATE {DB_NAME}.{table_name}
            SET status = NULL,
                similar_too = NULL,
                check_status = NULL
            WHERE status = 'INACTIVE' 
               OR check_status IS NOT NULL 
               OR similar_too IS NOT NULL
        """

        db.session.execute(text(reset_sql))
        db.session.commit()

        return jsonify({
            "success": True,
            "message": "All records reset to ACTIVE",
            "records_reset": records_to_reset,
            "table_name": table_name
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({
            "error": str(e),
            "success": False
        }), 500



# ======================================================================================== #







# ======================================================================================== #
# SEPARATE CHEKC CODES

@phonetic_py_bp.route("/find-duplicate-voter-names", methods=["GET"])
def find_duplicate_voter_names():
    """
    Find records with duplicate VOTER NAMES only (ignore father names)
    Groups by phonetically similar voter names for cross-checking
    """
    table_name = request.args.get("table", "gram_panchayat_voters")
    threshold = int(request.args.get("threshold", 85))
    limit = int(request.args.get("limit", 100))

    try:
        pk_column = "id"

        # Fetch all active records
        sql = f"""
            SELECT {pk_column}, voter_name, father_husband_mother_name, status
            FROM {DB_NAME}.{table_name}
            WHERE status IS NULL OR status != 'INACTIVE'
            ORDER BY {pk_column} ASC
            LIMIT 10000
        """

        result = db.session.execute(text(sql))
        rows = [dict(row._mapping) for row in result]

        # Normalize primary key
        for row in rows:
            if pk_column != 'id':
                row['id'] = row.get(pk_column)
            row['voter_name'] = row.get('voter_name') or ""
            row['father_husband_mother_name'] = row.get('father_husband_mother_name') or ""

        # Group by voter name similarity ONLY
        voter_groups = group_by_voter_name_only(rows, threshold)

        # Format results
        result_data = []
        for group in voter_groups[:limit]:
            if len(group) < 2:
                continue

            result_data.append({
                "voter_name": group[0]['voter_name'],
                "total_matches": len(group),
                "records": [
                    {
                        "id": rec['id'],
                        "voter_name": rec['voter_name'],
                        "father_name": rec['father_husband_mother_name'],
                        "match_score": rec.get('voter_score', 0)
                    }
                    for rec in group
                ]
            })

        return jsonify({
            "success": True,
            "table_name": table_name,
            "threshold": threshold,
            "total_groups": len(voter_groups),
            "returned_groups": len(result_data),
            "data": result_data
        })

    except Exception as e:
        import traceback
        return jsonify({
            "error": str(e),
            "traceback": traceback.format_exc(),
            "success": False
        }), 500


def group_by_voter_name_only(rows, threshold=85):
    """
    Group records by voter name similarity ONLY
    Returns list of groups where voter names are similar
    """
    from collections import defaultdict

    # Precompute phonetics for all voter names
    for row in rows:
        voter_name = row.get('voter_name', '').strip()
        if not voter_name:
            continue

        v_lat, v_skel, v_meta = get_universal_skeleton(voter_name)
        row['_v_lat'] = v_lat
        row['_v_skel'] = v_skel
        row['_v_meta'] = v_meta

    # Group by phonetic similarity
    groups = []
    processed = set()

    for i, rec1 in enumerate(rows):
        if rec1['id'] in processed or not rec1.get('_v_lat'):
            continue

        current_group = [rec1]
        processed.add(rec1['id'])

        for j, rec2 in enumerate(rows):
            if i == j or rec2['id'] in processed or not rec2.get('_v_lat'):
                continue

            # Calculate voter name similarity
            voter_score = calculate_name_similarity(
                (rec1['_v_lat'], rec1['_v_skel'], rec1['_v_meta']),
                (rec2['_v_lat'], rec2['_v_skel'], rec2['_v_meta'])
            )

            if voter_score >= threshold:
                rec2['voter_score'] = voter_score
                current_group.append(rec2)
                processed.add(rec2['id'])

        if len(current_group) > 1:
            groups.append(current_group)

    return groups


@phonetic_py_bp.route("/analyze-duplicates-strict", methods=["GET"])
def analyze_duplicates_strict():
    """
    STRICT duplicate detection with multiple verification layers
    Only returns high-confidence duplicates
    """
    table_name = request.args.get("table", "gram_panchayat_voters")
    min_voter_threshold = int(request.args.get("voter_threshold", 90))
    min_father_threshold = int(request.args.get("father_threshold", 85))
    limit = int(request.args.get("limit", 100))

    try:
        pk_column = "id"

        sql = f"""
            SELECT {pk_column}, voter_name, father_husband_mother_name, status
            FROM {DB_NAME}.{table_name}
            WHERE status IS NULL OR status != 'INACTIVE'
            ORDER BY {pk_column} ASC
            LIMIT 10000
        """

        result = db.session.execute(text(sql))
        rows = [dict(row._mapping) for row in result]

        # Normalize
        for row in rows:
            if pk_column != 'id':
                row['id'] = row.get(pk_column)
            row['voter_name'] = row.get('voter_name') or ""
            row['father_husband_mother_name'] = row.get('father_husband_mother_name') or ""

        # Find strict duplicates
        strict_duplicates = find_strict_duplicates(
            rows,
            min_voter_threshold,
            min_father_threshold
        )

        # Format results
        result_data = []
        for group in strict_duplicates[:limit]:
            if len(group) < 2:
                continue

            result_data.append({
                "primary_record": {
                    "id": group[0]['id'],
                    "voter_name": group[0]['voter_name'],
                    "father_name": group[0]['father_husband_mother_name']
                },
                "duplicates": [
                    {
                        "id": rec['id'],
                        "voter_name": rec['voter_name'],
                        "father_name": rec['father_husband_mother_name'],
                        "voter_score": rec.get('voter_score', 0),
                        "father_score": rec.get('father_score', 0),
                        "combined_score": rec.get('combined_score', 0)
                    }
                    for rec in group[1:]
                ]
            })

        return jsonify({
            "success": True,
            "table_name": table_name,
            "voter_threshold": min_voter_threshold,
            "father_threshold": min_father_threshold,
            "total_duplicate_groups": len(strict_duplicates),
            "returned_groups": len(result_data),
            "data": result_data
        })

    except Exception as e:
        import traceback
        return jsonify({
            "error": str(e),
            "traceback": traceback.format_exc(),
            "success": False
        }), 500


def find_strict_duplicates(rows, min_voter_threshold, min_father_threshold):
    """
    Find duplicates with STRICT criteria:
    - Both voter name AND father name must match above threshold
    - Applies multiple verification layers
    """
    # Precompute phonetics
    for row in rows:
        voter_name = row.get('voter_name', '').strip()
        father_name = row.get('father_husband_mother_name', '').strip()

        if not voter_name:
            continue

        v_lat, v_skel, v_meta = get_universal_skeleton(voter_name)
        f_lat, f_skel, f_meta = get_universal_skeleton(father_name)

        row['_v_lat'] = v_lat
        row['_v_skel'] = v_skel
        row['_v_meta'] = v_meta
        row['_f_lat'] = f_lat
        row['_f_skel'] = f_skel
        row['_f_meta'] = f_meta

    # Find duplicates
    groups = []
    processed = set()

    for i, rec1 in enumerate(rows):
        if rec1['id'] in processed or not rec1.get('_v_lat'):
            continue

        current_group = [rec1]
        processed.add(rec1['id'])

        for j, rec2 in enumerate(rows):
            if i == j or rec2['id'] in processed or not rec2.get('_v_lat'):
                continue

            # Calculate both scores
            voter_score = calculate_name_similarity(
                (rec1['_v_lat'], rec1['_v_skel'], rec1['_v_meta']),
                (rec2['_v_lat'], rec2['_v_skel'], rec2['_v_meta'])
            )

            father_score = calculate_name_similarity(
                (rec1['_f_lat'], rec1['_f_skel'], rec1['_f_meta']),
                (rec2['_f_lat'], rec2['_f_skel'], rec2['_f_meta'])
            )

            # STRICT: Both must pass threshold
            if voter_score >= min_voter_threshold and father_score >= min_father_threshold:
                rec2['voter_score'] = voter_score
                rec2['father_score'] = father_score
                rec2['combined_score'] = round((voter_score + father_score) / 2, 2)
                current_group.append(rec2)
                processed.add(rec2['id'])

        if len(current_group) > 1:
            groups.append(current_group)

    return groups


@phonetic_py_bp.route("/compare-two-records", methods=["GET"])
def compare_two_records():
    """
    Compare two specific records by ID
    Useful for manual verification
    """
    table_name = request.args.get("table", "gram_panchayat_voters")
    id1 = request.args.get("id1")
    id2 = request.args.get("id2")

    if not id1 or not id2:
        return jsonify({
            "error": "Both id1 and id2 are required",
            "example": "/compare-two-records?table=testing&id1=123&id2=456"
        }), 400

    try:
        pk_column = "id"

        sql = f"""
            SELECT {pk_column}, voter_name, father_husband_mother_name
            FROM {DB_NAME}.{table_name}
            WHERE {pk_column} IN (:id1, :id2)
        """

        result = db.session.execute(text(sql), {"id1": id1, "id2": id2})
        rows = [dict(row._mapping) for row in result]

        if len(rows) != 2:
            return jsonify({
                "error": f"Expected 2 records, found {len(rows)}"
            }), 404

        rec1, rec2 = rows[0], rows[1]

        # Get phonetics
        v1_lat, v1_skel, v1_meta = get_universal_skeleton(rec1['voter_name'] or "")
        v2_lat, v2_skel, v2_meta = get_universal_skeleton(rec2['voter_name'] or "")

        f1_lat, f1_skel, f1_meta = get_universal_skeleton(rec1['father_husband_mother_name'] or "")
        f2_lat, f2_skel, f2_meta = get_universal_skeleton(rec2['father_husband_mother_name'] or "")

        # Calculate scores
        voter_score = calculate_name_similarity(
            (v1_lat, v1_skel, v1_meta),
            (v2_lat, v2_skel, v2_meta)
        )

        father_score = calculate_name_similarity(
            (f1_lat, f1_skel, f1_meta),
            (f2_lat, f2_skel, f2_meta)
        )

        return jsonify({
            "success": True,
            "record1": {
                "id": rec1.get(pk_column),
                "voter_name": rec1['voter_name'],
                "father_name": rec1['father_husband_mother_name'],
                "voter_phonetics": {
                    "latin": v1_lat,
                    "skeleton": v1_skel,
                    "metaphone": v1_meta
                },
                "father_phonetics": {
                    "latin": f1_lat,
                    "skeleton": f1_skel,
                    "metaphone": f1_meta
                }
            },
            "record2": {
                "id": rec2.get(pk_column),
                "voter_name": rec2['voter_name'],
                "father_name": rec2['father_husband_mother_name'],
                "voter_phonetics": {
                    "latin": v2_lat,
                    "skeleton": v2_skel,
                    "metaphone": v2_meta
                },
                "father_phonetics": {
                    "latin": f2_lat,
                    "skeleton": f2_skel,
                    "metaphone": f2_meta
                }
            },
            "similarity": {
                "voter_score": voter_score,
                "father_score": father_score,
                "combined_score": round((voter_score + father_score) / 2, 2),
                "is_duplicate_at_85": voter_score >= 85 and father_score >= 85,
                "is_duplicate_at_90": voter_score >= 90 and father_score >= 90
            }
        })

    except Exception as e:
        import traceback
        return jsonify({
            "error": str(e),
            "traceback": traceback.format_exc(),
            "success": False
        }), 500