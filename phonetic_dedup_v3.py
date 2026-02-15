"""
Enhanced Voter Deduplication System v3.0
- Gram Panchayat based grouping (MAJOR PERFORMANCE BOOST)
- Sorted + Adaptive Window algorithm per GP
- Real-time progress tracking
- Gender validation
- Robust Hindi phonetic matching
"""

from flask import Blueprint, request, jsonify, Response
from sqlalchemy import text
import re
import time
import json
from collections import defaultdict
from fuzzywuzzy import fuzz
from metaphone import doublemetaphone
from indic_transliteration import sanscript
from indic_transliteration.sanscript import transliterate

# Import from config to avoid circular imports
from config import db, Config
DB_NAME = Config.DB_NAME  # Use the database name from config

phonetic_v3_bp = Blueprint('phonetic_v3', __name__)

# Global progress tracker
progress_tracker = {
    'status': 'idle',
    'current_step': '',
    'records_processed': 0,
    'total_records': 0,
    'duplicates_found': 0,
    'gps_processed': 0,
    'total_gps': 0,
    'current_gp': '',
    'start_time': 0,
    'estimated_seconds_remaining': 0,
    'percentage': 0
}


def normalize_gender(gender_value):
    """
    Normalize messy gender data to standard format
    
    Returns: 'MALE', 'FEMALE', 'OTHER', or 'UNKNOWN'
    """
    if not gender_value or gender_value == 'NULL':
        return 'UNKNOWN'
    
    gender_str = str(gender_value).strip().lower()
    
    # Female patterns
    female_patterns = ['म', 'महिला', 'मह', 'female', 'f', 'w']
    if any(pattern in gender_str for pattern in female_patterns):
        return 'FEMALE'
    
    # Male patterns  
    male_patterns = ['पु', 'पुरुष', 'male', 'm']
    if any(pattern in gender_str for pattern in male_patterns):
        return 'MALE'
    
    # Third gender
    if 'तृतीय' in gender_str or 'third' in gender_str:
        return 'OTHER'
    
    return 'UNKNOWN'


def genders_compatible(gender1, gender2):
    """
    Check if two gender values are compatible for duplicate matching
    
    Rules:
    - MALE only matches MALE or UNKNOWN
    - FEMALE only matches FEMALE or UNKNOWN
    - UNKNOWN matches anything
    - OTHER matches OTHER or UNKNOWN
    """
    g1 = normalize_gender(gender1)
    g2 = normalize_gender(gender2)
    
    if g1 == 'UNKNOWN' or g2 == 'UNKNOWN':
        return True
    
    return g1 == g2


def aggressive_normalize_for_sorting(name):
    """
    Aggressive normalization for sorting similar names together
    Removes vowel variations, titles, and minor differences
    NOW: Only removes titles/surnames from END of name (not middle!)
    """
    if not name:
        return ""

    name = str(name).strip().lower()

    # Common titles and surnames to remove (ONLY from end)
    suffixes_to_remove = [
        'कुमार', 'कुमारी', 'देवी', 'सिंह', 'प्रसाद', 'यादव', 'पाल',
        'शर्मा', 'वर्मा', 'गुप्ता', 'राजपूत', 'खान', 'अली', 'बेगम',
        'श्री', 'श्रीमती', 'कुँवर', 'बाबू', 'लाल'
    ]

    # Remove suffixes only if they appear at the END (after space)
    words = name.split()
    if len(words) > 1:
        # Check if last word is a common suffix
        last_word = words[-1]
        if last_word in suffixes_to_remove:
            words = words[:-1]  # Remove last word only
        # Check if second-to-last word is suffix (for cases like "कृष्ण कुमार")
        if len(words) > 1 and words[-1] in suffixes_to_remove:
            words = words[:-1]

    name = ' '.join(words)

    # Aggressive vowel normalization
    vowel_map = {
        'आ': 'अ', 'ा': '', 'ी': 'ि', 'ू': 'ु',
        'ए': 'अ', 'ै': 'अ', 'ओ': 'अ', 'ौ': 'अ',
        'ं': '', 'ँ': '', 'ः': '', '़': ''
    }

    for old, new in vowel_map.items():
        name = name.replace(old, new)

    # Consonant normalization
    consonant_map = {
        'व': 'ब', 'श': 'स', 'ष': 'स', 'ण': 'न',
        'ढ': 'ड', 'ढ़': 'ड', 'ऱ': 'र', 'क़': 'क',
        'ख़': 'ख', 'ग़': 'ग', 'ज़': 'ज', 'फ़': 'फ'
    }

    for old, new in consonant_map.items():
        name = name.replace(old, new)

    # Remove extra spaces
    name = re.sub(r'\s+', '', name)

    return name


def get_enhanced_phonetic_signature(text_val):
    """
    Enhanced phonetic signature generation for Hindi/English names
    Returns: (latin, skeleton, metaphone, normalized)
    """
    if not text_val:
        return "", "", "", ""

    norm = str(text_val).strip().lower()

    # Enhanced Devanagari normalization
    replacements = {
        'ं': 'n', 'ँ': 'n', 'ः': 'h',
        'व': 'ब', 'श': 'स', 'ष': 'स', 'ण': 'न',
        'ढ': 'ड', 'ढ़': 'ड', 'ऱ': 'र',
        'क़': 'क', 'ख़': 'ख', 'ग़': 'ग', 'ज़': 'ज', 'फ़': 'फ',
        'v': 'b', 'w': 'b', 'ph': 'f', 'z': 'j'
    }

    for old, new in replacements.items():
        norm = norm.replace(old, new)

    # Transliterate to Latin
    try:
        lat = transliterate(norm, sanscript.DEVANAGARI, sanscript.ITRANS).lower()
    except:
        lat = norm

    # Clean transliteration artifacts
    lat = lat.replace('~', 'n').replace('M', 'n').replace('m', 'n')
    lat = lat.replace('.', '').replace('H', 'h').replace('|', '')
    lat = re.sub(r'n\s*g', 'ng', lat)
    lat = re.sub(r'n\s*h', 'nh', lat)

    # Consonant skeleton
    skel = re.sub(r'[aeiouy]', '', lat)

    # Double Metaphone
    meta_primary, meta_secondary = doublemetaphone(lat)

    # Normalized version
    normalized = aggressive_normalize_for_sorting(text_val)

    return lat, skel, meta_primary or "", normalized


def calculate_enhanced_similarity(sig1, sig2):
    """
    Calculate similarity between two phonetic signatures

    Args:
        sig1, sig2: Tuples of (latin, skeleton, metaphone, normalized)

    Returns: Score (0-100)
    """
    lat1, skel1, meta1, norm1 = sig1
    lat2, skel2, meta2, norm2 = sig2

    # Handle empty strings
    if not lat1 and not lat2:
        return 100
    if not lat1 or not lat2:
        return 0

    # 1. Exact normalized match
    if norm1 and norm2 and norm1 == norm2:
        return 100

    # 2. Phonetic match
    phonetic_score = 100 if (meta1 == meta2 and meta1 != "") else 0

    # 3. Skeleton match
    skel_score = 0
    if skel1 and skel2:
        if skel1 == skel2:
            skel_score = 100
        elif skel1 in skel2 or skel2 in skel1:
            # TIGHTENED: Only give partial credit if length difference is small
            len_diff = abs(len(skel1) - len(skel2))
            if len_diff <= 2:  # Maximum 2 character difference
                skel_score = 70  # Reduced from 80
            else:
                skel_score = 0  # Too different, don't match
        else:
            common = len(set(skel1) & set(skel2))
            total = max(len(skel1), len(skel2))
            if total > 0:
                overlap_ratio = common / total
                if overlap_ratio > 0.8:  # Increased from 0.7
                    skel_score = 50  # Reduced from 60

    # 4. Fuzzy string matching
    fuzzy_score = fuzz.token_sort_ratio(lat1, lat2)
    partial_score = fuzz.partial_ratio(lat1, lat2)

    # 5. Token overlap
    tokens1 = set(lat1.split())
    tokens2 = set(lat2.split())
    if tokens1 or tokens2:
        token_overlap = len(tokens1 & tokens2) / max(len(tokens1), len(tokens2)) * 100
    else:
        token_overlap = 0

    # Weighted combination
    final_score = (
        (phonetic_score * 0.25) +
        (skel_score * 0.25) +
        (fuzzy_score * 0.20) +
        (partial_score * 0.15) +
        (token_overlap * 0.15)
    )

    return round(final_score, 2)


def update_progress(status, step, processed, total, duplicates, gps_processed=0, total_gps=0, current_gp=''):
    """Update global progress tracker"""
    global progress_tracker

    progress_tracker['status'] = status
    progress_tracker['current_step'] = step
    progress_tracker['records_processed'] = processed
    progress_tracker['total_records'] = total
    progress_tracker['duplicates_found'] = duplicates
    progress_tracker['gps_processed'] = gps_processed
    progress_tracker['total_gps'] = total_gps
    progress_tracker['current_gp'] = current_gp

    if total > 0:
        progress_tracker['percentage'] = round((processed / total) * 100, 2)

    # Estimate time remaining
    if processed > 0 and progress_tracker['start_time'] > 0:
        elapsed = time.time() - progress_tracker['start_time']
        rate = processed / elapsed
        remaining = total - processed
        if rate > 0:
            progress_tracker['estimated_seconds_remaining'] = int(remaining / rate)


def find_duplicates_in_gp(records, voter_threshold=85, father_threshold=80,
                          use_gender=True, max_window=200):
    """
    Find duplicates within a single Gram Panchayat using Sorted + Adaptive Window

    Args:
        records: List of voter records from same GP
        voter_threshold: Minimum voter name match score
        father_threshold: Minimum father name match score
        use_gender: Whether to validate gender compatibility
        max_window: Maximum lookahead window size

    Returns: List of duplicate groups
    """
    if not records or len(records) < 2:
        return []

    # Preprocess - Generate phonetic signatures
    for record in records:
        voter_name = (record.get('voter_name') or "").strip()
        father_name = (record.get('father_husband_mother_name') or "").strip()

        v_sig = get_enhanced_phonetic_signature(voter_name)
        f_sig = get_enhanced_phonetic_signature(father_name)

        record['_v_sig'] = v_sig
        record['_f_sig'] = f_sig
        record['_sort_key'] = v_sig[3]  # normalized version
        record['_gender'] = normalize_gender(record.get('gender'))

    # Sort by normalized voter name
    records_sorted = sorted(records, key=lambda x: x.get('_sort_key', ''))

    # Adaptive window comparison
    duplicate_groups = []
    processed_ids = set()

    for i in range(len(records_sorted)):
        rec_id = records_sorted[i].get('id')

        if rec_id in processed_ids:
            continue

        current_group = [records_sorted[i]]
        processed_ids.add(rec_id)

        # Adaptive window
        j = i + 1
        checked = 0

        while j < len(records_sorted) and checked < max_window:
            if records_sorted[j].get('id') in processed_ids:
                j += 1
                continue

            # Quick pre-filter
            norm_i = records_sorted[i].get('_sort_key', '')
            norm_j = records_sorted[j].get('_sort_key', '')

            # Early termination if names diverge
            if norm_i and norm_j and len(norm_i) >= 3 and len(norm_j) >= 3:
                if norm_i[:3] != norm_j[:3]:
                    break

            # Full phonetic comparison - Voter name
            voter_score = calculate_enhanced_similarity(
                records_sorted[i]['_v_sig'],
                records_sorted[j]['_v_sig']
            )

            if voter_score >= voter_threshold:
                # Father name comparison
                father_score = calculate_enhanced_similarity(
                    records_sorted[i]['_f_sig'],
                    records_sorted[j]['_f_sig']
                )

                if father_score >= father_threshold:
                    # Gender validation
                    gender_match = True
                    if use_gender:
                        gender_match = genders_compatible(
                            records_sorted[i].get('_gender'),
                            records_sorted[j].get('_gender')
                        )

                    if gender_match:
                        combined_score = round((voter_score + father_score) / 2, 2)

                        records_sorted[j]['voter_score'] = voter_score
                        records_sorted[j]['father_score'] = father_score
                        records_sorted[j]['combined_score'] = combined_score

                        current_group.append(records_sorted[j])
                        processed_ids.add(records_sorted[j].get('id'))

            j += 1
            checked += 1

        if len(current_group) > 1:
            duplicate_groups.append(current_group)

    return duplicate_groups


@phonetic_v3_bp.route("/get-gram-panchayats", methods=["GET"])
def get_gram_panchayats():
    """
    Get list of all distinct Gram Panchayats with voter counts
    """
    table_name = request.args.get("table", "gram_panchayat_voters")
    gp_column = request.args.get("gp_column", "gram_panchayat")

    try:
        sql = f"""
            SELECT 
                {gp_column} as gp_name,
                COUNT(*) as voter_count
            FROM {DB_NAME}.{table_name}
            WHERE status IS NULL OR status != 'INACTIVE'
            GROUP BY {gp_column}
            ORDER BY voter_count DESC
        """

        result = db.session.execute(text(sql))
        gps = [dict(row._mapping) for row in result]

        return jsonify({
            "success": True,
            "total_gps": len(gps),
            "gram_panchayats": gps
        })

    except Exception as e:
        import traceback
        return jsonify({
            "error": str(e),
            "traceback": traceback.format_exc(),
            "success": False
        }), 500


@phonetic_v3_bp.route("/preview-duplicates-v3", methods=["GET"])
def preview_duplicates_v3():
    """
    Preview duplicates with GP-based grouping
    """
    table_name = request.args.get("table", "gram_panchayat_voters")
    gp_column = request.args.get("gp_column", "gram_panchayat")
    gp_filter = request.args.get("gp_filter", "")  # Specific GP or empty for all
    voter_threshold = int(request.args.get("voter_threshold", 85))
    father_threshold = int(request.args.get("father_threshold", 80))
    use_gender = request.args.get("use_gender", "true").lower() == "true"
    limit = int(request.args.get("limit", 50))
    preview_size = int(request.args.get("preview_size", 50000))  # NEW: Configurable preview size

    global progress_tracker
    progress_tracker['start_time'] = time.time()
    progress_tracker['status'] = 'processing'

    try:
        # Build GP filter
        gp_where = ""
        if gp_filter:
            gp_where = f"AND {gp_column} = '{gp_filter}'"

        # Fetch active records - Use preview_size instead of hardcoded 10000
        sql = f"""
            SELECT id, voter_name, father_husband_mother_name, gender, 
                   {gp_column} as gram_panchayat, status
            FROM {DB_NAME}.{table_name}
            WHERE (status IS NULL OR status != 'INACTIVE') {gp_where}
            ORDER BY {gp_column}, id ASC
            LIMIT {preview_size}
        """

        result = db.session.execute(text(sql))
        rows = [dict(row._mapping) for row in result]

        if not rows:
            return jsonify({
                "success": True,
                "message": "No active records found",
                "total_duplicate_groups": 0,
                "data": []
            })

        # Group by GP
        gp_groups = defaultdict(list)
        for row in rows:
            gp_name = row.get('gram_panchayat') or 'UNKNOWN'
            gp_groups[gp_name].append(row)

        # Process each GP
        all_duplicate_groups = []
        total_processed = 0

        update_progress('processing', 'Processing Gram Panchayats...', 0, len(rows), 0,
                       0, len(gp_groups), '')

        for gp_idx, (gp_name, gp_records) in enumerate(gp_groups.items(), 1):
            update_progress('processing', f'Processing GP: {gp_name}',
                          total_processed, len(rows), len(all_duplicate_groups),
                          gp_idx, len(gp_groups), gp_name)

            # Find duplicates within this GP
            gp_duplicates = find_duplicates_in_gp(
                gp_records, voter_threshold, father_threshold, use_gender
            )

            # Add GP info to each group
            for group in gp_duplicates:
                for record in group:
                    record['_gp'] = gp_name

            all_duplicate_groups.extend(gp_duplicates)
            total_processed += len(gp_records)

        # Format for preview
        preview_data = []
        for group in all_duplicate_groups[:limit]:
            if len(group) > 1:
                gp_name = group[0].get('_gp', 'UNKNOWN')
                preview_data.append({
                    "gram_panchayat": gp_name,
                    "primary_record": {
                        "id": group[0]["id"],
                        "voter_name": group[0].get("voter_name") or "(empty)",
                        "father_name": group[0].get("father_husband_mother_name") or "(empty)",
                        "gender": group[0].get("_gender", "UNKNOWN")
                    },
                    "duplicates": [
                        {
                            "id": rec["id"],
                            "voter_name": rec.get("voter_name") or "(empty)",
                            "father_name": rec.get("father_husband_mother_name") or "(empty)",
                            "gender": rec.get("_gender", "UNKNOWN"),
                            "voter_score": rec.get("voter_score", 0),
                            "father_score": rec.get("father_score", 0),
                            "combined_score": rec.get("combined_score", 0)
                        }
                        for rec in group[1:]
                    ]
                })

        update_progress('completed', 'Preview completed', len(rows), len(rows),
                       len(all_duplicate_groups), len(gp_groups), len(gp_groups), '')

        return jsonify({
            "success": True,
            "table_name": table_name,
            "voter_threshold": voter_threshold,
            "father_threshold": father_threshold,
            "use_gender_validation": use_gender,
            "gps_processed": len(gp_groups),
            "records_analyzed": len(rows),
            "total_duplicate_groups": len(all_duplicate_groups),
            "preview_count": len(preview_data),
            "data": preview_data
        })

    except Exception as e:
        import traceback
        progress_tracker['status'] = 'error'
        return jsonify({
            "error": str(e),
            "traceback": traceback.format_exc(),
            "success": False
        }), 500


@phonetic_v3_bp.route("/deduplicate-voters-v3", methods=["POST"])
def deduplicate_voters_v3():
    """
    Full deduplication with GP-based grouping
    """
    table_name = request.json.get("table_name", "gram_panchayat_voters")
    gp_column = request.json.get("gp_column", "gram_panchayat")
    voter_threshold = request.json.get("voter_threshold", 85)
    father_threshold = request.json.get("father_threshold", 80)
    use_gender = request.json.get("use_gender", True)
    dry_run = request.json.get("dry_run", True)

    global progress_tracker
    progress_tracker['start_time'] = time.time()
    progress_tracker['status'] = 'processing'

    try:
        pk_column = "id"

        # Get total count and GP count
        count_sql = f"""
            SELECT 
                COUNT(*) as total,
                COUNT(DISTINCT {gp_column}) as gp_count
            FROM {DB_NAME}.{table_name}
            WHERE status IS NULL OR status != 'INACTIVE'
        """
        result = db.session.execute(text(count_sql))
        counts = dict(result.fetchone()._mapping)
        total_records = counts['total']
        total_gps = counts['gp_count']

        update_progress('processing', f'Starting deduplication of {total_records} records across {total_gps} GPs...',
                       0, total_records, 0, 0, total_gps, '')

        # Get list of distinct GPs
        gp_sql = f"""
            SELECT DISTINCT {gp_column} as gp_name
            FROM {DB_NAME}.{table_name}
            WHERE (status IS NULL OR status != 'INACTIVE')
              AND {gp_column} IS NOT NULL
            ORDER BY {gp_column}
        """
        result = db.session.execute(text(gp_sql))
        gp_list = [row[0] for row in result]

        # Process each GP
        all_records_to_deactivate = []
        total_processed = 0

        for gp_idx, gp_name in enumerate(gp_list, 1):
            update_progress('processing', f'Processing GP {gp_idx}/{total_gps}: {gp_name}',
                          total_processed, total_records, len(all_records_to_deactivate),
                          gp_idx, total_gps, gp_name)

            # Fetch voters in this GP
            sql = f"""
                SELECT {pk_column} as id, voter_name, father_husband_mother_name, 
                       gender, status
                FROM {DB_NAME}.{table_name}
                WHERE (status IS NULL OR status != 'INACTIVE')
                  AND {gp_column} = :gp_name
                ORDER BY {pk_column} ASC
            """

            result = db.session.execute(text(sql), {"gp_name": gp_name})
            gp_records = [dict(row._mapping) for row in result]

            if not gp_records:
                continue

            # Find duplicates in this GP
            duplicate_groups = find_duplicates_in_gp(
                gp_records, voter_threshold, father_threshold, use_gender
            )

            # Prepare deactivation list
            for group in duplicate_groups:
                if len(group) > 1:
                    primary_record = group[0]
                    duplicates = group[1:]

                    for dup in duplicates:
                        all_records_to_deactivate.append({
                            "id": dup["id"],
                            "voter_name": dup.get("voter_name") or "(empty)",
                            "father_name": dup.get("father_husband_mother_name") or "(empty)",
                            "gender": dup.get("_gender", "UNKNOWN"),
                            "gram_panchayat": gp_name,
                            "duplicate_of": primary_record["id"],
                            "voter_score": dup.get("voter_score", 0),
                            "father_score": dup.get("father_score", 0),
                            "combined_score": dup.get("combined_score", 0)
                        })

            total_processed += len(gp_records)

        # Execute updates (if not dry run)
        if not dry_run and all_records_to_deactivate:
            update_progress('processing', 'Marking duplicates as INACTIVE...',
                          total_records, total_records, len(all_records_to_deactivate),
                          total_gps, total_gps, '')

            deactivate_records_batch(table_name, pk_column, all_records_to_deactivate)

        update_progress('completed', 'Deduplication completed',
                       total_records, total_records, len(all_records_to_deactivate),
                       total_gps, total_gps, '')

        return jsonify({
            "success": True,
            "dry_run": dry_run,
            "voter_threshold": voter_threshold,
            "father_threshold": father_threshold,
            "use_gender_validation": use_gender,
            "total_records_processed": total_processed,
            "total_gps_processed": len(gp_list),
            "duplicate_groups_found": len([g for r in all_records_to_deactivate for g in [r] if r]),
            "records_to_deactivate": len(all_records_to_deactivate),
            "details": all_records_to_deactivate[:100],
            "message": "Dry run completed" if dry_run else "Duplicates marked as INACTIVE"
        })

    except Exception as e:
        import traceback
        progress_tracker['status'] = 'error'
        return jsonify({
            "error": str(e),
            "traceback": traceback.format_exc(),
            "success": False
        }), 500


def deactivate_records_batch(table_name, pk_column, records_to_deactivate):
    """Mark duplicate records as INACTIVE in batches"""
    if not records_to_deactivate:
        return

    ids_to_deactivate = [rec["id"] for rec in records_to_deactivate]
    similar_mapping = {rec["id"]: rec["duplicate_of"] for rec in records_to_deactivate}

    batch_size = 1000
    for i in range(0, len(ids_to_deactivate), batch_size):
        batch = ids_to_deactivate[i:i + batch_size]

        case_statements = []
        for rec_id in batch:
            primary_id = similar_mapping[rec_id]
            case_statements.append(f"WHEN {pk_column} = {rec_id} THEN {primary_id}")

        case_sql = " ".join(case_statements)
        placeholders = ','.join([str(id) for id in batch])

        sql = f"""
            UPDATE {DB_NAME}.{table_name}
            SET status = 'INACTIVE',
                similar_too = CASE {case_sql} END,
                check_status = 'DUPLICATE_DETECTED'
            WHERE {pk_column} IN ({placeholders})
        """

        db.session.execute(text(sql))

    db.session.commit()


@phonetic_v3_bp.route("/progress-v3", methods=["GET"])
def get_progress_v3():
    """Get current progress status with GP info"""
    return jsonify(progress_tracker)


@phonetic_v3_bp.route("/statistics-v3", methods=["GET"])
def get_statistics_v3():
    """Enhanced statistics with GP breakdown"""
    table_name = request.args.get("table", "gram_panchayat_voters")
    gp_column = request.args.get("gp_column", "gram_panchayat")

    try:
        # Overall statistics
        sql = f"""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN status = 'INACTIVE' THEN 1 ELSE 0 END) as inactive,
                SUM(CASE WHEN status IS NULL OR status != 'INACTIVE' THEN 1 ELSE 0 END) as active,
                COUNT(DISTINCT {gp_column}) as total_gps
            FROM {DB_NAME}.{table_name}
        """

        result = db.session.execute(text(sql))
        row = dict(result.fetchone()._mapping)

        total = row.get('total', 0)
        inactive = row.get('inactive', 0)
        active = row.get('active', 0)
        total_gps = row.get('total_gps', 0)

        duplicate_percentage = round((inactive / total * 100), 2) if total > 0 else 0

        # GP-wise statistics (top 10)
        gp_stats_sql = f"""
            SELECT 
                {gp_column} as gp_name,
                COUNT(*) as total_voters,
                SUM(CASE WHEN status = 'INACTIVE' THEN 1 ELSE 0 END) as duplicates
            FROM {DB_NAME}.{table_name}
            GROUP BY {gp_column}
            ORDER BY duplicates DESC
            LIMIT 10
        """

        result = db.session.execute(text(gp_stats_sql))
        gp_stats = [dict(row._mapping) for row in result]

        return jsonify({
            "table_name": table_name,
            "total_records": total,
            "active_records": active,
            "inactive_records": inactive,
            "duplicate_percentage": duplicate_percentage,
            "total_gram_panchayats": total_gps,
            "top_gps_by_duplicates": gp_stats
        })

    except Exception as e:
        return jsonify({
            "error": str(e),
            "success": False
        }), 500


@phonetic_v3_bp.route("/reset-to-active-v3", methods=["POST"])
def reset_to_active_v3():
    """Reset all records to active status"""
    table_name = request.json.get("table_name", "gram_panchayat_voters")

    try:
        count_sql = f"""
            SELECT COUNT(*) as count
            FROM {DB_NAME}.{table_name}
            WHERE status = 'INACTIVE' OR check_status IS NOT NULL OR similar_too IS NOT NULL
        """
        result = db.session.execute(text(count_sql))
        records_to_reset = result.fetchone()[0]

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


@phonetic_v3_bp.route("/test-gender-normalization-v3", methods=["GET"])
def test_gender_normalization_v3():
    """Test endpoint for gender normalization"""
    test_values = [
        'पु', 'म', None, '५', '0', 'पुरुष', 'महिला', 'तृतीय लिंग',
        '42 लिंग:पु', '75 लिंग:पु', '69 लिंग:मह', 'MALE', 'FEMALE'
    ]

    results = []
    for val in test_values:
        results.append({
            "input": val,
            "normalized": normalize_gender(val)
        })

    return jsonify({
        "success": True,
        "test_results": results
    })