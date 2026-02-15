"""
Enhanced Voter Deduplication System v2.0
- Sorted + Adaptive Window algorithm
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

# Assuming these are imported from your main app
from config import db, Config
DB_NAME = Config.DB_NAME  # Use the database name from config

phonetic_v2_bp = Blueprint('phonetic_v2', __name__)

# Global progress tracker
progress_tracker = {
    'status': 'idle',
    'current_step': '',
    'records_processed': 0,
    'total_records': 0,
    'duplicates_found': 0,
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
    """
    if not name:
        return ""
    
    name = str(name).strip().lower()
    
    # Remove common titles and surnames (will be checked separately)
    titles = ['कुमार', 'कुमारी', 'देवी', 'सिंह', 'प्रसाद', 'यादव', 'पाल', 
              'शर्मा', 'वर्मा', 'गुप्ता', 'राजपूत', 'खान', 'अली', 'बेगम',
              'श्री', 'श्रीमती', 'कुँवर', 'बाबू', 'लाल']
    
    for title in titles:
        name = name.replace(title, '')
    
    # Aggressive vowel normalization (आ→अ, ई→इ, ऊ→उ, etc.)
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
        # Anusvara variations
        'ं': 'n', 'ँ': 'n', 'ः': 'h',
        
        # Consonant normalizations
        'व': 'ब', 'श': 'स', 'ष': 'स', 'ण': 'न',
        'ढ': 'ड', 'ढ़': 'ड', 'ऱ': 'र',
        
        # Nuqta normalization
        'क़': 'क', 'ख़': 'ख', 'ग़': 'ग', 'ज़': 'ज', 'फ़': 'फ',
        
        # Latin normalizations
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
    
    # Consonant skeleton (remove vowels)
    skel = re.sub(r'[aeiouy]', '', lat)
    
    # Double Metaphone
    meta_primary, meta_secondary = doublemetaphone(lat)
    
    # Normalized version for quick comparison
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
    
    # 1. Exact normalized match (highest weight)
    if norm1 and norm2 and norm1 == norm2:
        return 100
    
    # 2. Phonetic match (metaphone)
    phonetic_score = 100 if (meta1 == meta2 and meta1 != "") else 0
    
    # 3. Skeleton match (consonants only)
    skel_score = 0
    if skel1 and skel2:
        if skel1 == skel2:
            skel_score = 100
        elif skel1 in skel2 or skel2 in skel1:
            skel_score = 80
        else:
            # Character overlap
            common = len(set(skel1) & set(skel2))
            total = max(len(skel1), len(skel2))
            if total > 0:
                overlap_ratio = common / total
                if overlap_ratio > 0.7:
                    skel_score = 60
    
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


def update_progress(status, step, processed, total, duplicates):
    """Update global progress tracker"""
    global progress_tracker
    
    progress_tracker['status'] = status
    progress_tracker['current_step'] = step
    progress_tracker['records_processed'] = processed
    progress_tracker['total_records'] = total
    progress_tracker['duplicates_found'] = duplicates
    
    if total > 0:
        progress_tracker['percentage'] = round((processed / total) * 100, 2)
    
    # Estimate time remaining
    if processed > 0 and progress_tracker['start_time'] > 0:
        elapsed = time.time() - progress_tracker['start_time']
        rate = processed / elapsed
        remaining = total - processed
        if rate > 0:
            progress_tracker['estimated_seconds_remaining'] = int(remaining / rate)


def find_duplicates_sorted_adaptive(records, voter_threshold=85, father_threshold=80, 
                                    use_gender=True, max_window=200):
    """
    Find duplicates using Sorted + Adaptive Window algorithm
    
    Args:
        records: List of voter records
        voter_threshold: Minimum voter name match score (0-100)
        father_threshold: Minimum father name match score (0-100)
        use_gender: Whether to validate gender compatibility
        max_window: Maximum lookahead window size
    
    Returns: List of duplicate groups
    """
    global progress_tracker
    
    total = len(records)
    
    # Phase 1: Preprocess - Generate phonetic signatures
    update_progress('processing', 'Generating phonetic signatures...', 0, total, 0)
    
    for i, record in enumerate(records):
        voter_name = (record.get('voter_name') or "").strip()
        father_name = (record.get('father_husband_mother_name') or "").strip()
        
        # Generate signatures
        v_sig = get_enhanced_phonetic_signature(voter_name)
        f_sig = get_enhanced_phonetic_signature(father_name)
        
        record['_v_sig'] = v_sig
        record['_f_sig'] = f_sig
        record['_sort_key'] = v_sig[3]  # normalized version
        
        # Normalize gender
        record['_gender'] = normalize_gender(record.get('gender'))
        
        if i % 1000 == 0:
            update_progress('processing', 'Generating phonetic signatures...', i, total, 0)
    
    # Phase 2: Sort by normalized voter name
    update_progress('processing', 'Sorting records...', total, total, 0)
    records_sorted = sorted(records, key=lambda x: x.get('_sort_key', ''))
    
    # Phase 3: Adaptive window comparison
    update_progress('processing', 'Finding duplicates (adaptive window)...', 0, total, 0)
    
    duplicate_groups = []
    processed_ids = set()
    duplicates_found = 0
    
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
            
            # Quick pre-filter: check if normalized names are too different
            norm_i = records_sorted[i].get('_sort_key', '')
            norm_j = records_sorted[j].get('_sort_key', '')
            
            # Early termination if names diverge (first 3 chars different)
            if norm_i and norm_j and len(norm_i) >= 3 and len(norm_j) >= 3:
                if norm_i[:3] != norm_j[:3]:
                    # Names too different, stop looking
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
                        # Calculate combined score
                        combined_score = round((voter_score + father_score) / 2, 2)
                        
                        records_sorted[j]['voter_score'] = voter_score
                        records_sorted[j]['father_score'] = father_score
                        records_sorted[j]['combined_score'] = combined_score
                        
                        current_group.append(records_sorted[j])
                        processed_ids.add(records_sorted[j].get('id'))
                        duplicates_found += 1
            
            j += 1
            checked += 1
        
        if len(current_group) > 1:
            duplicate_groups.append(current_group)
        
        # Update progress every 100 records
        if i % 100 == 0:
            update_progress('processing', 'Finding duplicates (adaptive window)...', 
                          i, total, duplicates_found)
    
    update_progress('completed', 'Duplicate detection completed', total, total, duplicates_found)
    
    return duplicate_groups


@phonetic_v2_bp.route("/progress", methods=["GET"])
def get_progress():
    """Get current progress status"""
    return jsonify(progress_tracker)


@phonetic_v2_bp.route("/progress-stream", methods=["GET"])
def progress_stream():
    """Server-Sent Events stream for real-time progress"""
    def generate():
        while True:
            data = json.dumps(progress_tracker)
            yield f"data: {data}\n\n"
            time.sleep(0.5)  # Update every 500ms
            
            if progress_tracker['status'] in ['completed', 'error', 'idle']:
                break
    
    return Response(generate(), mimetype='text/event-stream')


@phonetic_v2_bp.route("/preview-duplicates-v2", methods=["GET"])
def preview_duplicates_v2():
    """
    Preview duplicate records using enhanced algorithm
    """
    table_name = request.args.get("table", "gram_panchayat_voters")
    voter_threshold = int(request.args.get("voter_threshold", 85))
    father_threshold = int(request.args.get("father_threshold", 80))
    use_gender = request.args.get("use_gender", "true").lower() == "true"
    limit = int(request.args.get("limit", 50))
    preview_size = int(request.args.get("preview_size", 5000))
    
    global progress_tracker
    progress_tracker['start_time'] = time.time()
    progress_tracker['status'] = 'processing'
    
    try:
        # Fetch active records
        sql = f"""
            SELECT id, voter_name, father_husband_mother_name, gender, status
            FROM {DB_NAME}.{table_name}
            WHERE status IS NULL OR status != 'INACTIVE'
            ORDER BY id ASC
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
        
        # Find duplicates
        duplicate_groups = find_duplicates_sorted_adaptive(
            rows, voter_threshold, father_threshold, use_gender
        )
        
        # Format for preview
        preview_data = []
        for group in duplicate_groups[:limit]:
            if len(group) > 1:
                preview_data.append({
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
        
        return jsonify({
            "success": True,
            "table_name": table_name,
            "voter_threshold": voter_threshold,
            "father_threshold": father_threshold,
            "use_gender_validation": use_gender,
            "records_analyzed": len(rows),
            "total_duplicate_groups": len(duplicate_groups),
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


@phonetic_v2_bp.route("/deduplicate-voters-v2", methods=["POST"])
def deduplicate_voters_v2():
    """
    Full deduplication with progress tracking
    """
    table_name = request.json.get("table_name", "gram_panchayat_voters")
    voter_threshold = request.json.get("voter_threshold", 85)
    father_threshold = request.json.get("father_threshold", 80)
    use_gender = request.json.get("use_gender", True)
    dry_run = request.json.get("dry_run", True)
    batch_size = request.json.get("batch_size", 50000)  # Process in batches
    
    global progress_tracker
    progress_tracker['start_time'] = time.time()
    progress_tracker['status'] = 'processing'
    
    try:
        pk_column = "id"
        
        # Get total count
        count_sql = f"""
            SELECT COUNT(*) as total
            FROM {DB_NAME}.{table_name}
            WHERE status IS NULL OR status != 'INACTIVE'
        """
        result = db.session.execute(text(count_sql))
        total_records = result.fetchone()[0]
        
        update_progress('processing', f'Starting deduplication of {total_records} records...', 
                       0, total_records, 0)
        
        # Fetch all active records (consider pagination for very large datasets)
        sql = f"""
            SELECT {pk_column} as id, voter_name, father_husband_mother_name, gender, status
            FROM {DB_NAME}.{table_name}
            WHERE status IS NULL OR status != 'INACTIVE'
            ORDER BY {pk_column} ASC
        """
        
        result = db.session.execute(text(sql))
        rows = [dict(row._mapping) for row in result]
        
        if not rows:
            return jsonify({
                "success": True,
                "message": "No active records found",
                "total_processed": 0,
                "duplicates_found": 0
            })
        
        # Find duplicates using enhanced algorithm
        duplicate_groups = find_duplicates_sorted_adaptive(
            rows, voter_threshold, father_threshold, use_gender
        )
        
        # Prepare deactivation list
        records_to_deactivate = []
        
        for group in duplicate_groups:
            if len(group) > 1:
                primary_record = group[0]
                duplicates = group[1:]
                
                for dup in duplicates:
                    records_to_deactivate.append({
                        "id": dup["id"],
                        "voter_name": dup.get("voter_name") or "(empty)",
                        "father_name": dup.get("father_husband_mother_name") or "(empty)",
                        "gender": dup.get("_gender", "UNKNOWN"),
                        "duplicate_of": primary_record["id"],
                        "voter_score": dup.get("voter_score", 0),
                        "father_score": dup.get("father_score", 0),
                        "combined_score": dup.get("combined_score", 0)
                    })
        
        # Execute updates (if not dry run)
        if not dry_run and records_to_deactivate:
            update_progress('processing', 'Marking duplicates as INACTIVE...', 
                          total_records, total_records, len(records_to_deactivate))
            
            deactivate_records_batch(table_name, pk_column, records_to_deactivate)
        
        update_progress('completed', 'Deduplication completed', 
                       total_records, total_records, len(records_to_deactivate))
        
        return jsonify({
            "success": True,
            "dry_run": dry_run,
            "voter_threshold": voter_threshold,
            "father_threshold": father_threshold,
            "use_gender_validation": use_gender,
            "total_records_processed": len(rows),
            "duplicate_groups_found": len(duplicate_groups),
            "records_to_deactivate": len(records_to_deactivate),
            "details": records_to_deactivate[:100],  # First 100 for preview
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
    """
    Mark duplicate records as INACTIVE in batches
    """
    if not records_to_deactivate:
        return
    
    ids_to_deactivate = [rec["id"] for rec in records_to_deactivate]
    similar_mapping = {rec["id"]: rec["duplicate_of"] for rec in records_to_deactivate}
    
    batch_size = 1000
    for i in range(0, len(ids_to_deactivate), batch_size):
        batch = ids_to_deactivate[i:i + batch_size]
        
        # Build CASE statement for similar_too
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


@phonetic_v2_bp.route("/statistics-v2", methods=["GET"])
def get_statistics_v2():
    """Enhanced statistics with gender breakdown"""
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


@phonetic_v2_bp.route("/reset-to-active-v2", methods=["POST"])
def reset_to_active_v2():
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


@phonetic_v2_bp.route("/test-gender-normalization", methods=["GET"])
def test_gender_normalization():
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
