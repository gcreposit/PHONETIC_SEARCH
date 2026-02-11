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


def execute_phonetic_search(table_name, query_text):
    """Universal function to handle DB fetch and unique scoring."""
    q_lat, q_skel, q_meta = get_universal_skeleton(query_text)
    first_char = query_text[0]

    sql = f"SELECT * FROM {DB_NAME}.{table_name} WHERE voter_name LIKE :lk OR father_husband_mother_name LIKE :lk"
    result = db.session.execute(text(sql), {"lk": f"{first_char}%"})
    rows = [dict(row._mapping) for row in result]

    unique_matches = {}

    for row in rows:
        row_id = row.get("id")
        # Ensure we have a valid key for the dictionary
        if row_id is None:
            continue

        score = calculate_best_score(row, q_lat, q_skel, q_meta)

        if score >= 55:
            row["match_score"] = score
            # If ID is new, OR if this version of the ID has a better score, update it
            if row_id not in unique_matches or score > unique_matches[row_id]["match_score"]:
                unique_matches[row_id] = row

    # Convert dictionary back to a list and sort
    final_results = sorted(unique_matches.values(), key=lambda x: x["match_score"], reverse=True)
    return final_results


@phonetic_py_bp.route("/nagar-nigam", methods=["GET"])
def search_nagar_nigam():
    q = request.args.get("q", "").strip()
    if not q: return jsonify({"error": "Query required"}), 400
    results = execute_phonetic_search("nagar_nigam", q)
    return jsonify({"query": q, "total": len(results), "data": results})


@phonetic_py_bp.route("/gram-panchayat", methods=["GET"])
def search_gram_panchayat():
    q = request.args.get("q", "").strip()
    if not q: return jsonify({"error": "Query required"}), 400
    results = execute_phonetic_search("gram_panchayat_voters", q)
    return jsonify({"query": q, "total": len(results), "data": results})


@phonetic_py_bp.route("/voter-pdf", methods=["GET"])
def search_voter_pdf():
    q = request.args.get("q", "").strip()
    if not q: return jsonify({"error": "Query required"}), 400
    results = execute_phonetic_search("voters_pdf_extract", q)
    return jsonify({"query": q, "total": len(results), "data": results})


# import os
# import re
# from flask import Blueprint, request, jsonify
# from sqlalchemy import text
# from config import db
# from rapidfuzz import fuzz
# from metaphone import doublemetaphone
# from indic_transliteration import sanscript
# from indic_transliteration.sanscript import transliterate
#
# phonetic_py_bp = Blueprint("phonetic_py_bp", __name__, url_prefix="/api/pysearch")
# DB_NAME = os.getenv("DB_NAME")
#
#
# def get_universal_skeleton(text_val):
#     if not text_val: return "", "", ""
#     norm = text_val.replace('ं', 'न्').replace('ँ', 'न्').replace('व', 'ब').replace('v', 'b')
#     norm = norm.replace('श', 'स').replace('ष', 'स')
#     lat = transliterate(norm, sanscript.DEVANAGARI, sanscript.ITRANS).lower()
#     skel = re.sub(r'[aeiouy]', '', lat)
#     meta = doublemetaphone(lat)[0]
#     return lat, skel, meta
#
#
# def calculate_best_score(row, q_lat, q_skel, q_meta):
#     fields_to_check = ["voter_name", "father_husband_mother_name"]
#     best_score = 0
#     for field in fields_to_check:
#         val = row.get(field) or ""
#         t_lat, t_skel, t_meta = get_universal_skeleton(val)
#         skel_score = 100 if (q_skel == t_skel and q_skel != "") else 0
#         ratio = fuzz.ratio(q_lat, t_lat)
#         partial = fuzz.partial_ratio(q_lat, t_lat)
#         phonetic = (q_meta == t_meta and q_meta != "")
#         current_score = (skel_score * 0.4) + (partial * 0.4) + (ratio * 0.1)
#         if phonetic: current_score += 10
#         if current_score > best_score:
#             best_score = current_score
#     return round(best_score, 2)
#
#
# def execute_phonetic_search(table_name, raw_query_text):
#     """Handles multi-term search by splitting on commas."""
#     # 1. Split query by commas and clean whitespace
#     search_terms = [t.strip() for t in raw_query_text.split(',') if t.strip()]
#
#     # Store results across all terms to ensure ID uniqueness
#     unique_matches = {}
#
#     for term in search_terms:
#         q_lat, q_skel, q_meta = get_universal_skeleton(term)
#         first_char = term[0]
#
#         # Fetch from DB for this specific term
#         sql = f"SELECT * FROM {DB_NAME}.{table_name} WHERE voter_name LIKE :lk OR father_husband_mother_name LIKE :lk"
#         result = db.session.execute(text(sql), {"lk": f"{first_char}%"})
#         rows = [dict(row._mapping) for row in result]
#
#         for row in rows:
#             row_id = row.get("id")
#             if row_id is None: continue
#
#             score = calculate_best_score(row, q_lat, q_skel, q_meta)
#
#             if score >= 55:
#                 row["match_score"] = score
#                 # If ID exists, keep the one with the HIGHER score
#                 if row_id not in unique_matches or score > unique_matches[row_id]["match_score"]:
#                     unique_matches[row_id] = row
#
#     # Sort final combined results
#     final_results = sorted(unique_matches.values(), key=lambda x: x["match_score"], reverse=True)
#     return final_results
#
#
# # Routes remain clean because logic is in execute_phonetic_search
# @phonetic_py_bp.route("/nagar-nigam", methods=["GET"])
# def search_nagar_nigam():
#     q = request.args.get("q", "").strip()
#     if not q: return jsonify({"error": "Query required"}), 400
#     results = execute_phonetic_search("nagar_nigam", q)
#     return jsonify({"query": q, "total": len(results), "data": results})
#
#
# @phonetic_py_bp.route("/gram-panchayat", methods=["GET"])
# def search_gram_panchayat():
#     q = request.args.get("q", "").strip()
#     if not q: return jsonify({"error": "Query required"}), 400
#     results = execute_phonetic_search("gram_panchayat_voters", q)
#     return jsonify({"query": q, "total": len(results), "data": results})
#
#
# @phonetic_py_bp.route("/voter-pdf", methods=["GET"])
# def search_voter_pdf():
#     q = request.args.get("q", "").strip()
#     if not q: return jsonify({"error": "Query required"}), 400
#     results = execute_phonetic_search("voters_pdf_extract", q)
#     return jsonify({"query": q, "total": len(results), "data": results})

# old working code
# import os
# import re
# from flask import Blueprint, request, jsonify
# from sqlalchemy import text
# from config import db
# from rapidfuzz import fuzz
# from metaphone import doublemetaphone
# from indic_transliteration import sanscript
# from indic_transliteration.sanscript import transliterate
#
# phonetic_py_bp = Blueprint("phonetic_py_bp", __name__, url_prefix="/api/pysearch")
# DB_NAME = os.getenv("DB_NAME")
#
#
# def get_universal_skeleton(text_val):
#     """
#     Standardizes ANY Hindi/English input into a phonetic 'skeleton'
#     to bridge the gap between spelling variations.
#     """
#     if not text_val: return "", "", ""
#
#     # 1. Normalize Hindi scripts (Dot vs Half-N, V vs B, Sh vs S)
#     # This handles common phonetic overlaps across all Indian names
#     norm = text_val.replace('ं', 'न्').replace('ँ', 'न्').replace('व', 'ब').replace('v', 'b')
#     norm = norm.replace('श', 'स').replace('ष', 'स')
#
#     # 2. Transliterate to a stable Latin format (ITRANS)
#     lat = transliterate(norm, sanscript.DEVANAGARI, sanscript.ITRANS).lower()
#
#     # 3. Create a Consonant Skeleton (strips all vowels)
#     # 'pankaj' -> 'pnkj', 'panakaja' -> 'pnkj', 'amit' -> 'mt'
#     skel = re.sub(r'[aeiouy]', '', lat)
#
#     # 4. English Phonetic Hash (Double Metaphone)
#     meta = doublemetaphone(lat)[0]
#
#     return lat, skel, meta
#
#
# @phonetic_py_bp.route("/nagar-nigam", methods=["GET"])
# def phonetic_python_search():
#     query_text = request.args.get("q", "").strip()
#     if not query_text:
#         return jsonify({"error": "Query required"}), 400
#
#     # Process Query into 3 layers: Latin, Skeleton, and Metaphone
#     q_lat, q_skel, q_meta = get_universal_skeleton(query_text)
#
#     print(f"\n--- NEW SEARCH: {query_text} ---")
#     print(f"DEBUG: Latin: {q_lat} | Skeleton: {q_skel} | Meta: {q_meta}")
#
#     # Broad fetch: pull everything starting with the same first letter
#     # This ensures we don't miss rows due to 2nd/3rd letter typos
#     first_char = query_text[0]
#     sql_query = f"SELECT * FROM {DB_NAME}.nagar_nigam WHERE voter_name LIKE :lk or father_husband_mother_name LIKE :lk"
#     result = db.session.execute(text(sql_query), {"lk": f"{first_char}%"})
#     rows = [dict(row._mapping) for row in result]
#
#     unique_matches = {}
#
#     for row in rows:
#         row_id = row.get("id")
#
#         voter_name = row.get("voter_name") or ""
#         father_name = row.get("father_husband_mother_name") or ""
#
#         # Process voter_name
#         v_lat, v_skel, v_meta = get_universal_skeleton(voter_name)
#
#         # Process father_name
#         f_lat, f_skel, f_meta = get_universal_skeleton(father_name)
#
#         # -----------------------------
#         # SCORE voter_name
#         # -----------------------------
#         v_skel_score = 100 if (q_skel == v_skel and q_skel != "") else 0
#         v_ratio = fuzz.ratio(q_lat, v_lat)
#         v_partial = fuzz.partial_ratio(q_lat, v_lat)
#         v_phonetic = (q_meta == v_meta and q_meta != "")
#
#         v_score = (v_skel_score * 0.4) + (v_partial * 0.4) + (v_ratio * 0.1)
#         if v_phonetic:
#             v_score += 10
#
#         # -----------------------------
#         # SCORE father_name
#         # -----------------------------
#         f_skel_score = 100 if (q_skel == f_skel and q_skel != "") else 0
#         f_ratio = fuzz.ratio(q_lat, f_lat)
#         f_partial = fuzz.partial_ratio(q_lat, f_lat)
#         f_phonetic = (q_meta == f_meta and q_meta != "")
#
#         f_score = (f_skel_score * 0.4) + (f_partial * 0.4) + (f_ratio * 0.1)
#         if f_phonetic:
#             f_score += 10
#
#         # -----------------------------
#         # FINAL BEST SCORE
#         # -----------------------------
#         final_score = max(v_score, f_score)
#
#         if final_score >= 55:
#             row["match_score"] = round(final_score, 2)
#
#             # Keep only highest score per ID
#             if row_id not in unique_matches:
#                 unique_matches[row_id] = row
#             else:
#                 # If already exists, keep better score
#                 if final_score > unique_matches[row_id]["match_score"]:
#                     unique_matches[row_id] = row
#
#     # Sort: Best matches at the top
#     # matches = sorted(matches, key=lambda x: x["match_score"], reverse=True)
#     matches = list(unique_matches.values())
#     matches = sorted(matches, key=lambda x: x["match_score"], reverse=True)
#     # matches = []
#     # for row in rows:
#     #     v_name = row.get("voter_name") or ""
#     #     v_lat, v_skel, v_meta = get_universal_skeleton(v_name)
#     #
#     #     # LAYER 1: The Skeleton Match (Best for spelling mistakes)
#     #     # If consonants match exactly, it's a very high probability match
#     #     skel_score = 100 if (q_skel == v_skel and q_skel != "") else 0
#     #
#     #     # LAYER 2: Fuzzy Ratios (Best for partial names)
#     #     # partial_ratio handles "Pankaj" vs "Pankaj Kumar"
#     #     ratio = fuzz.ratio(q_lat, v_lat)
#     #     partial = fuzz.partial_ratio(q_lat, v_lat)
#     #
#     #     # LAYER 3: Phonetic (Double Metaphone)
#     #     phonetic_match = (q_meta == v_meta and q_meta != "")
#     #
#     #     # UNIVERSAL SCORING FORMULA
#     #     # We give the most weight to the Skeleton and Partial matches
#     #     final_score = (skel_score * 0.4) + (partial * 0.4) + (ratio * 0.1)
#     #     if phonetic_match: final_score += 10
#     #
#     #     # Log significant matches for debugging
#     #     # if final_score >= 50:
#     #     #     print(f"DEBUG: Match -> {v_name} [Score: {round(final_score, 2)}]")
#     #
#     #     if final_score >= 55:  # Threshold for result inclusion
#     #         row["match_score"] = round(final_score, 2)
#     #         matches.append(row)
#     #
#     # # Sort: Best matches at the top
#     # matches = sorted(matches, key=lambda x: x["match_score"], reverse=True)
#
#     print(f"--- DONE: {len(matches)} results found ---\n")
#     return jsonify({"query": query_text, "total": len(matches), "data": matches})
#
#
# @phonetic_py_bp.route("/gram-panchayat", methods=["GET"])
# def gram_panchayat_search():
#     query_text = request.args.get("q", "").strip()
#     if not query_text:
#         return jsonify({"error": "Query required"}), 400
#
#     # Process Query into 3 layers: Latin, Skeleton, and Metaphone
#     q_lat, q_skel, q_meta = get_universal_skeleton(query_text)
#
#     print(f"\n--- NEW SEARCH: {query_text} ---")
#     print(f"DEBUG: Latin: {q_lat} | Skeleton: {q_skel} | Meta: {q_meta}")
#
#     # Broad fetch: pull everything starting with the same first letter
#     # This ensures we don't miss rows due to 2nd/3rd letter typos
#     first_char = query_text[0]
#     sql_query = f"SELECT * FROM {DB_NAME}.gram_panchayat_voters WHERE voter_name LIKE :lk or father_husband_mother_name LIKE :lk"
#     result = db.session.execute(text(sql_query), {"lk": f"{first_char}%"})
#     rows = [dict(row._mapping) for row in result]
#     unique_matches = {}
#
#     for row in rows:
#         row_id = row.get("id")
#
#         voter_name = row.get("voter_name") or ""
#         father_name = row.get("father_husband_mother_name") or ""
#
#         # Process voter_name
#         v_lat, v_skel, v_meta = get_universal_skeleton(voter_name)
#
#         # Process father_name
#         f_lat, f_skel, f_meta = get_universal_skeleton(father_name)
#
#         # -----------------------------
#         # SCORE voter_name
#         # -----------------------------
#         v_skel_score = 100 if (q_skel == v_skel and q_skel != "") else 0
#         v_ratio = fuzz.ratio(q_lat, v_lat)
#         v_partial = fuzz.partial_ratio(q_lat, v_lat)
#         v_phonetic = (q_meta == v_meta and q_meta != "")
#
#         v_score = (v_skel_score * 0.4) + (v_partial * 0.4) + (v_ratio * 0.1)
#         if v_phonetic:
#             v_score += 10
#
#         # -----------------------------
#         # SCORE father_name
#         # -----------------------------
#         f_skel_score = 100 if (q_skel == f_skel and q_skel != "") else 0
#         f_ratio = fuzz.ratio(q_lat, f_lat)
#         f_partial = fuzz.partial_ratio(q_lat, f_lat)
#         f_phonetic = (q_meta == f_meta and q_meta != "")
#
#         f_score = (f_skel_score * 0.4) + (f_partial * 0.4) + (f_ratio * 0.1)
#         if f_phonetic:
#             f_score += 10
#
#         # -----------------------------
#         # FINAL BEST SCORE
#         # -----------------------------
#         final_score = max(v_score, f_score)
#
#         if final_score >= 55:
#             row["match_score"] = round(final_score, 2)
#
#             # Keep only highest score per ID
#             if row_id not in unique_matches:
#                 unique_matches[row_id] = row
#             else:
#                 # If already exists, keep better score
#                 if final_score > unique_matches[row_id]["match_score"]:
#                     unique_matches[row_id] = row
#
#     # Sort: Best matches at the top
#     # matches = sorted(matches, key=lambda x: x["match_score"], reverse=True)
#     matches = list(unique_matches.values())
#     matches = sorted(matches, key=lambda x: x["match_score"], reverse=True)
#     # matches = []
#     # for row in rows:
#     #     v_name = row.get("voter_name") or ""
#     #     v_lat, v_skel, v_meta = get_universal_skeleton(v_name)
#     #
#     #     # LAYER 1: The Skeleton Match (Best for spelling mistakes)
#     #     # If consonants match exactly, it's a very high probability match
#     #     skel_score = 100 if (q_skel == v_skel and q_skel != "") else 0
#     #
#     #     # LAYER 2: Fuzzy Ratios (Best for partial names)
#     #     # partial_ratio handles "Pankaj" vs "Pankaj Kumar"
#     #     ratio = fuzz.ratio(q_lat, v_lat)
#     #     partial = fuzz.partial_ratio(q_lat, v_lat)
#     #
#     #     # LAYER 3: Phonetic (Double Metaphone)
#     #     phonetic_match = (q_meta == v_meta and q_meta != "")
#     #
#     #     # UNIVERSAL SCORING FORMULA
#     #     # We give the most weight to the Skeleton and Partial matches
#     #     final_score = (skel_score * 0.4) + (partial * 0.4) + (ratio * 0.1)
#     #     if phonetic_match: final_score += 10
#     #
#     #     # Log significant matches for debugging
#     #     # if final_score >= 50:
#     #     #     print(f"DEBUG: Match -> {v_name} [Score: {round(final_score, 2)}]")
#     #
#     #     if final_score >= 55:  # Threshold for result inclusion
#     #         print(f"DEBUG: Match -> {v_name} [Score: {round(final_score, 2)}]")
#     #         row["match_score"] = round(final_score, 2)
#     #         matches.append(row)
#     #
#     # # Sort: Best matches at the top
#     # matches = sorted(matches, key=lambda x: x["match_score"], reverse=True)
#
#     print(f"--- DONE: {len(matches)} results found ---\n")
#     return jsonify({"query": query_text, "total": len(matches), "data": matches})
#
#
# @phonetic_py_bp.route("/voter-pdf", methods=["GET"])
# def voter_pdf_search():
#     query_text = request.args.get("q", "").strip()
#     if not query_text:
#         return jsonify({"error": "Query required"}), 400
#
#     # Process Query into 3 layers: Latin, Skeleton, and Metaphone
#     q_lat, q_skel, q_meta = get_universal_skeleton(query_text)
#
#     print(f"\n--- NEW SEARCH: {query_text} ---")
#     print(f"DEBUG: Latin: {q_lat} | Skeleton: {q_skel} | Meta: {q_meta}")
#
#     # Broad fetch: pull everything starting with the same first letter
#     # This ensures we don't miss rows due to 2nd/3rd letter typos
#     first_char = query_text[0]
#     sql_query = f"SELECT * FROM {DB_NAME}.voters_pdf_extract WHERE voter_name LIKE :lk or father_husband_mother_name LIKE :lk"
#     result = db.session.execute(text(sql_query), {"lk": f"{first_char}%"})
#     rows = [dict(row._mapping) for row in result]
#
#
#     # matches = []
#     # for row in rows:
#     #     v_name = row.get("voter_name") or ""
#     #     v_lat, v_skel, v_meta = get_universal_skeleton(v_name)
#     #
#     #     # LAYER 1: The Skeleton Match (Best for spelling mistakes)
#     #     # If consonants match exactly, it's a very high probability match
#     #     skel_score = 100 if (q_skel == v_skel and q_skel != "") else 0
#     #
#     #     # LAYER 2: Fuzzy Ratios (Best for partial names)
#     #     # partial_ratio handles "Pankaj" vs "Pankaj Kumar"
#     #     ratio = fuzz.ratio(q_lat, v_lat)
#     #     partial = fuzz.partial_ratio(q_lat, v_lat)
#     #
#     #     # LAYER 3: Phonetic (Double Metaphone)
#     #     phonetic_match = (q_meta == v_meta and q_meta != "")
#     #
#     #     # UNIVERSAL SCORING FORMULA
#     #     # We give the most weight to the Skeleton and Partial matches
#     #     final_score = (skel_score * 0.4) + (partial * 0.4) + (ratio * 0.1)
#     #     if phonetic_match: final_score += 10
#     #
#     #     # Log significant matches for debugging
#     #     # if final_score >= 50:
#     #     #     print(f"DEBUG: Match -> {v_name} [Score: {round(final_score, 2)}]")
#     #
#     #     if final_score >= 55:  # Threshold for result inclusion
#     #         print(f"DEBUG: Match -> {v_name} [Score: {round(final_score, 2)}]")
#     #         row["match_score"] = round(final_score, 2)
#     #         matches.append(row)
#     unique_matches = {}
#
#     for row in rows:
#         row_id = row.get("id")
#
#         voter_name = row.get("voter_name") or ""
#         father_name = row.get("father_husband_mother_name") or ""
#
#         # Process voter_name
#         v_lat, v_skel, v_meta = get_universal_skeleton(voter_name)
#
#         # Process father_name
#         f_lat, f_skel, f_meta = get_universal_skeleton(father_name)
#
#         # -----------------------------
#         # SCORE voter_name
#         # -----------------------------
#         v_skel_score = 100 if (q_skel == v_skel and q_skel != "") else 0
#         v_ratio = fuzz.ratio(q_lat, v_lat)
#         v_partial = fuzz.partial_ratio(q_lat, v_lat)
#         v_phonetic = (q_meta == v_meta and q_meta != "")
#
#         v_score = (v_skel_score * 0.4) + (v_partial * 0.4) + (v_ratio * 0.1)
#         if v_phonetic:
#             v_score += 10
#
#         # -----------------------------
#         # SCORE father_name
#         # -----------------------------
#         f_skel_score = 100 if (q_skel == f_skel and q_skel != "") else 0
#         f_ratio = fuzz.ratio(q_lat, f_lat)
#         f_partial = fuzz.partial_ratio(q_lat, f_lat)
#         f_phonetic = (q_meta == f_meta and q_meta != "")
#
#         f_score = (f_skel_score * 0.4) + (f_partial * 0.4) + (f_ratio * 0.1)
#         if f_phonetic:
#             f_score += 10
#
#         # -----------------------------
#         # FINAL BEST SCORE
#         # -----------------------------
#         final_score = max(v_score, f_score)
#
#         if final_score >= 55:
#             row["match_score"] = round(final_score, 2)
#
#             # Keep only highest score per ID
#             if row_id not in unique_matches:
#                 unique_matches[row_id] = row
#             else:
#                 # If already exists, keep better score
#                 if final_score > unique_matches[row_id]["match_score"]:
#                     unique_matches[row_id] = row
#
#     # Sort: Best matches at the top
#     # matches = sorted(matches, key=lambda x: x["match_score"], reverse=True)
#     matches = list(unique_matches.values())
#     matches = sorted(matches, key=lambda x: x["match_score"], reverse=True)
#
#     print(f"--- DONE: {len(matches)} results found ---\n")
#     return jsonify({"query": query_text, "total": len(matches), "data": matches})
