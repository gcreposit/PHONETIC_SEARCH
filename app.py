import os
from dotenv import load_dotenv
from flask_cors import CORS
from config import Config
from flask import Flask, render_template, request, jsonify
import pymysql
# from config import Config

from config import create_app, db
from Controller.PageController import page_bp
from Controller.ApiController import api_bp
from Controller.PhoneticPythonController import phonetic_py_bp
# Add at the top with other imports
from phonetic_dedup_v2 import phonetic_v2_bp

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
app.register_blueprint(phonetic_v2_bp, url_prefix='/api/pysearch/v2')

from phonetic_dedup_v3 import phonetic_v3_bp
app.register_blueprint(phonetic_v3_bp, url_prefix='/api/pysearch/v3')

def get_conn():
    return pymysql.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        database=os.getenv("DB_NAME"),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )


def normalize_gender_filter(g: str) -> str:
    g = (g or "all").strip().lower()
    if g in ("male", "पुरुष", "m"): return "Male"
    if g in ("female", "महिला", "f"): return "Female"
    if g in ("third", "तृतीय", "third gender"): return "Third"
    if g in ("unknown", "other"): return "Unknown"
    return "all"


def normalize_caste_filter(c: str) -> str:
    c = (c or "all").strip()
    return c if c else "all"


def mapping_filter_sql(mapping_filter: str) -> str:
    """
    mapping_filter: 'all' | 'mapped' | 'unmapped'
    Rule: NULL/empty = mapped
    """
    if mapping_filter == "unmapped":
        return " AND LOWER(TRIM(mapping_status)) = 'unmapped' "
    if mapping_filter == "mapped":
        return " AND (mapping_status IS NULL OR TRIM(mapping_status) = '' OR LOWER(TRIM(mapping_status)) <> 'unmapped') "
    return ""


def build_where_clause(args):
    """
    Build WHERE clause and params for filtering
    Returns: (where_clause, params_list)
    """
    where = " WHERE 1=1 "
    params = []

    # Mapping filter
    mapping_filter = args.get("mapping", "all").lower()
    where += mapping_filter_sql(mapping_filter)

    # Gender filter
    gender_filter = normalize_gender_filter(args.get("gender", "all"))
    if gender_filter != "all":
        if gender_filter == "Male":
            where += " AND (TRIM(sex) IN ('पुरुष','Male','M','m')) "
        elif gender_filter == "Female":
            where += " AND (TRIM(sex) IN ('महिला','Female','F','f')) "
        elif gender_filter == "Third":
            where += " AND (TRIM(sex) IN ('तृतीय लिंग','Third')) "
        else:
            where += " AND (sex IS NULL OR TRIM(sex)='' OR TRIM(sex) NOT IN ('पुरुष','महिला','तृतीय लिंग','Male','Female','Third','M','F','m','f')) "

    # Caste filter
    caste_filter = normalize_caste_filter(args.get("caste", "all"))
    if caste_filter != "all":
        where += " AND (CASE WHEN caste IS NULL OR TRIM(caste)='' THEN 'Unknown' "
        where += " WHEN UPPER(TRIM(caste)) IN ('GEN','GENERAL') THEN 'General' "
        where += " WHEN UPPER(TRIM(caste)) IN ('OBC','O.B.C.') THEN 'OBC' "
        where += " WHEN UPPER(TRIM(caste)) IN ('SC','S.C.','SCHEDULED CASTE','ST','S.T.','SCHEDULED TRIBE','SC / ST') THEN 'SC / ST' "
        where += " WHEN LOWER(TRIM(caste))='muslim' THEN 'Muslim' "
        where += " WHEN UPPER(TRIM(caste))='YADAV' THEN 'Yadav' "
        where += " ELSE 'Others' END) = %s "
        params.append(caste_filter)

    # Age range filter
    age_min = args.get("age_min", "").strip()
    age_max = args.get("age_max", "").strip()
    if age_min.isdigit():
        where += " AND (age REGEXP '^[0-9]+$' AND CAST(age AS UNSIGNED) >= %s) "
        params.append(int(age_min))
    if age_max.isdigit():
        where += " AND (age REGEXP '^[0-9]+$' AND CAST(age AS UNSIGNED) <= %s) "
        params.append(int(age_max))

    # Age bucket filter from chart drilldown
    age_bucket = args.get("age_bucket", "").strip()
    if age_bucket:
        bucket_sql = ""
        if age_bucket == '18-25':
            bucket_sql = "CAST(age AS UNSIGNED) BETWEEN 18 AND 25"
        elif age_bucket == '26-35':
            bucket_sql = "CAST(age AS UNSIGNED) BETWEEN 26 AND 35"
        elif age_bucket == '36-45':
            bucket_sql = "CAST(age AS UNSIGNED) BETWEEN 36 AND 45"
        elif age_bucket == '46-60':
            bucket_sql = "CAST(age AS UNSIGNED) BETWEEN 46 AND 60"
        elif age_bucket == '60+':
            bucket_sql = "CAST(age AS UNSIGNED) > 60"
        elif age_bucket == 'Unknown':
            bucket_sql = "NOT (age REGEXP '^[0-9]+$')"
        else:
            bucket_sql = "1=1"

        if age_bucket == 'Unknown':
            where += f" AND ({bucket_sql}) "
        else:
            where += f" AND (age REGEXP '^[0-9]+$' AND {bucket_sql}) "

    # Booth/Section filter (for modal drilldown)
    booth_filter = args.get("booth", "").strip()
    if booth_filter:
        if booth_filter.lower() == 'unknown':
            where += " AND (section IS NULL OR TRIM(section) = '') "
        else:
            where += " AND TRIM(section) = %s "
            params.append(booth_filter)

    # Search filter - Enhanced
    search_val = args.get("search", "").strip()
    search_col = args.get("col", "wildcard")

    if search_val:
        if search_col == "wildcard":
            # Search across multiple key name/ID columns
            where += """ AND (e_name LIKE %s OR e_name_eng LIKE %s 
                             OR voter_i_card_no LIKE %s OR rel_name_eng LIKE %s) """
            term = f"%{search_val}%"
            params.extend([term, term, term, term])
        else:
            # Search in one specific column chosen by the user
            allowed_cols = ["e_name", "e_name_eng", "voter_i_card_no", "rel_name_eng", "house_no", "mobile_no",
                            "section", "caste"]
            if search_col in allowed_cols:
                where += f" AND {search_col} LIKE %s "
                params.append(f"%{search_val}%")

    return where, params


@app.route("/")
def dashboard():
    return render_template("dashboard.html")


@app.route("/api/summary")
def api_summary():
    where, params = build_where_clause(request.args)

    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) AS total FROM {Config.DB_TABLE} {where}", params)
        total = cur.fetchone()["total"]

        # Gender distribution
        cur.execute(f"""
            SELECT
              CASE
                WHEN sex IS NULL OR TRIM(sex)='' THEN 'Unknown'
                WHEN TRIM(sex) IN ('पुरुष','Male','M','m') THEN 'Male'
                WHEN TRIM(sex) IN ('महिला','Female','F','f') THEN 'Female'
                WHEN TRIM(sex) IN ('तृतीय लिंग','Third','Third Gender') THEN 'Other'
                ELSE 'Unknown'
              END AS gender,
              COUNT(*) AS cnt
            FROM {Config.DB_TABLE}
            {where}
            GROUP BY gender
            ORDER BY cnt DESC;
        """, params)
        gender = cur.fetchall()

        # Avg age (age is text; safe cast)
        age_where = where + " AND age REGEXP '^[0-9]+$'"
        cur.execute(f"""
            SELECT AVG(NULLIF(CAST(age AS UNSIGNED),0)) AS avg_age
            FROM {Config.DB_TABLE}
            {age_where}
        """, params)
        avg_age = cur.fetchone()["avg_age"]

        # Caste distribution (for summary cards)
        cur.execute(f"""
            SELECT
              caste_bucket,
              COUNT(*) AS cnt
            FROM (
              SELECT
                CASE
                  WHEN caste IS NULL OR TRIM(caste)='' THEN 'Unknown'
                  WHEN UPPER(TRIM(caste)) IN ('GEN','GENERAL') THEN 'General'
                  WHEN UPPER(TRIM(caste)) IN ('OBC','O.B.C.') THEN 'OBC'
                  WHEN UPPER(TRIM(caste)) IN ('SC','S.C.','SCHEDULED CASTE','ST','S.T.','SCHEDULED TRIBE','SC / ST') THEN 'SC / ST'
                  WHEN LOWER(TRIM(caste)) = 'muslim' THEN 'Muslim'
                  WHEN UPPER(TRIM(caste)) = 'YADAV' THEN 'Yadav'
                  ELSE 'Others'
                END AS caste_bucket
              FROM {Config.DB_TABLE}
              {where}
            ) x
            GROUP BY caste_bucket
            ORDER BY FIELD(caste_bucket, 'General', 'OBC', 'SC / ST', 'Muslim', 'Yadav', 'Others', 'Unknown');
        """, params)
        caste = cur.fetchall()

    conn.close()
    return jsonify({"total": total, "gender": gender, "avg_age": avg_age, "caste": caste})


@app.route("/api/age_gender")
def api_age_gender():
    where, params = build_where_clause(request.args)

    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT
              age_bucket,
              SUM(CASE WHEN gender='Male' THEN cnt ELSE 0 END) AS male,
              SUM(CASE WHEN gender='Female' THEN cnt ELSE 0 END) AS female,
              SUM(CASE WHEN gender NOT IN ('Male','Female') THEN cnt ELSE 0 END) AS other
            FROM (
              SELECT
                CASE
                  WHEN age REGEXP '^[0-9]+$' THEN
                    CASE
                      WHEN CAST(age AS UNSIGNED) BETWEEN 18 AND 25 THEN '18-25'
                      WHEN CAST(age AS UNSIGNED) BETWEEN 26 AND 35 THEN '26-35'
                      WHEN CAST(age AS UNSIGNED) BETWEEN 36 AND 45 THEN '36-45'
                      WHEN CAST(age AS UNSIGNED) BETWEEN 46 AND 60 THEN '46-60'
                      WHEN CAST(age AS UNSIGNED) > 60 THEN '60+'
                      ELSE 'Unknown'
                    END
                  ELSE 'Unknown'
                END AS age_bucket,
                CASE
                  WHEN TRIM(sex) IN ('पुरुष','Male','M','m') THEN 'Male'
                  WHEN TRIM(sex) IN ('महिला','Female','F','f') THEN 'Female'
                  ELSE 'Other'
                END AS gender,
                COUNT(*) AS cnt
              FROM {Config.DB_TABLE}
              {where}
              GROUP BY age_bucket, gender
            ) x
            GROUP BY age_bucket
            ORDER BY FIELD(age_bucket, '18-25', '26-35', '36-45', '46-60', '60+', 'Unknown');
        """, params)
        rows = cur.fetchall()

    conn.close()
    return jsonify(rows)


@app.route("/api/caste")
def api_caste():
    where, params = build_where_clause(request.args)

    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT
              caste_bucket,
              COUNT(*) AS cnt
            FROM (
              SELECT
                CASE
                  WHEN caste IS NULL OR TRIM(caste)='' THEN 'Unknown'
                  WHEN UPPER(TRIM(caste)) IN ('GEN','GENERAL') THEN 'General'
                  WHEN UPPER(TRIM(caste)) IN ('OBC','O.B.C.') THEN 'OBC'
                  WHEN UPPER(TRIM(caste)) IN ('SC','S.C.','SCHEDULED CASTE','ST','S.T.','SCHEDULED TRIBE','SC / ST') THEN 'SC / ST'
                  WHEN LOWER(TRIM(caste)) = 'muslim' THEN 'Muslim'
                  WHEN UPPER(TRIM(caste)) = 'YADAV' THEN 'Yadav'
                  ELSE 'Others'
                END AS caste_bucket
              FROM {Config.DB_TABLE}
              {where}
            ) x
            GROUP BY caste_bucket
            ORDER BY FIELD(caste_bucket, 'General', 'OBC', 'SC / ST', 'Muslim', 'Yadav', 'Others', 'Unknown');
        """, params)
        rows = cur.fetchall()

    conn.close()
    return jsonify(rows)


@app.route("/api/sections_top")
def api_sections_top():
    where, params = build_where_clause(request.args)

    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT
              COALESCE(NULLIF(TRIM(section),''),'Unknown') AS section,
              COUNT(*) AS cnt
            FROM {Config.DB_TABLE}
            {where}
            GROUP BY section
            ORDER BY cnt DESC
            LIMIT 10;
        """, params)
        rows = cur.fetchall()

    conn.close()
    return jsonify(rows)


@app.route("/api/mapping_distribution")
def api_mapping_distribution():
    """Get mapping status distribution"""
    where, params = build_where_clause(request.args)

    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT
              CASE
                WHEN mapping_status IS NULL OR TRIM(mapping_status) = '' THEN 'mapped'
                WHEN LOWER(TRIM(mapping_status)) = 'unmapped' THEN 'unmapped'
                ELSE 'mapped'
              END AS mapping_status,
              COUNT(*) AS cnt
            FROM {Config.DB_TABLE}
            {where}
            GROUP BY mapping_status
        """, params)
        rows = cur.fetchall()

    conn.close()
    return jsonify({"data": rows})


@app.route("/api/sections_all")
def api_sections_all():
    """Get all sections/booths sorted by count"""
    where, params = build_where_clause(request.args)

    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT
              COALESCE(NULLIF(TRIM(section),''),'Unknown') AS section,
              COUNT(*) AS cnt
            FROM {Config.DB_TABLE}
            {where}
            GROUP BY section
            ORDER BY cnt DESC;
        """, params)
        rows = cur.fetchall()

    conn.close()
    return jsonify(rows)


@app.route("/api/table")
def api_table():
    # 1. Get raw page_size from request
    raw_page_size = request.args.get("page_size", "10")

    # 2. Handle "all" case safely
    if str(raw_page_size).lower() == "all":
        page_size = 999999  # Set a very high number to fetch everything
        page = 1
    else:
        try:
            page_size = int(raw_page_size)
            page = int(request.args.get("page", "1"))
        except ValueError:
            page_size = 10  # Fallback if conversion fails
            page = 1

    offset = (page - 1) * page_size

    # 2. Sorting
    sort_column = request.args.get("sort_column", "id")
    sort_direction = request.args.get("sort_direction", "desc").upper()

    # Validate sort column to prevent SQL injection
    allowed_columns = ["id", "ac_no", "part_no", "sl_no_in_part", "voter_i_card_no",
                       "e_name", "rel_name", "e_name_eng", "rel_name_eng",
                       "house_no", "age", "sex", "section", "mapping_status", "caste", "mobile_no"]
    if sort_column not in allowed_columns:
        sort_column = "id"
    if sort_direction not in ["ASC", "DESC"]:
        sort_direction = "DESC"

    # 3. Build WHERE
    where, params = build_where_clause(request.args)

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # Get filtered count (for pagination info)
            cur.execute(f"SELECT COUNT(*) AS cnt FROM {Config.DB_TABLE} {where}", params)
            records_filtered = cur.fetchone()["cnt"]

            # Get paginated data
            sql = f"""
                SELECT id, ac_no, part_no, sl_no_in_part, voter_i_card_no,
                       e_name, rel_name, e_name_eng, rel_name_eng,
                       house_no, age, sex, section, mapping_status, caste, mobile_no
                FROM {Config.DB_TABLE}
                {where}
                ORDER BY {sort_column} {sort_direction}
                LIMIT %s OFFSET %s
            """
            cur.execute(sql, params + [page_size, offset])
            data = cur.fetchall()

            # Calculate total pages properly
            total_pages = max(1, (records_filtered + page_size - 1) // page_size) if page_size < 999999 else 1

            return jsonify({
                "data": data,
                "filtered": records_filtered,
                "page": page,
                "page_size": raw_page_size,
                "total_pages": total_pages
            })
    finally:
        conn.close()


@app.route("/api/export_data")
def api_export_data():
    """
    Export all filtered data for Excel/PDF
    """
    where, params = build_where_clause(request.args)

    columns = [
        "id", "ac_no", "part_no", "sl_no_in_part", "voter_i_card_no",
        "e_name", "rel_name", "e_name_eng", "rel_name_eng",
        "house_no", "age", "sex", "section", "mapping_status", "caste", "mobile_no"
    ]

    # Sorting for export
    sort_column = request.args.get("sort_column", "id")
    sort_direction = request.args.get("sort_direction", "desc").upper()

    if sort_column not in columns:
        sort_column = "id"
    if sort_direction not in ["ASC", "DESC"]:
        sort_direction = "DESC"

    conn = get_conn()
    with conn.cursor() as cur:
        sql = f"""
            SELECT {",".join(columns)}
            FROM {Config.DB_TABLE}
            {where}
            ORDER BY {sort_column} {sort_direction}
        """
        cur.execute(sql, params)
        data = cur.fetchall()

    conn.close()

    return jsonify({
        "data": data,
        "count": len(data)
    })


@app.route("/api/update_row", methods=["POST"])
def api_update_row():
    payload = request.get_json(force=True, silent=True) or {}
    row_id = payload.get("id")
    if not row_id:
        return jsonify({"ok": False, "error": "Missing id"}), 400

    # Allowlist fields for update
    allowed = ["e_name", "rel_name", "e_name_eng", "rel_name_eng", "house_no", "age", "sex", "section",
               "mapping_status", "caste", "mobile_no"]
    updates = []
    params = []
    for k in allowed:
        if k in payload:
            updates.append(f"{k}=%s")
            params.append(payload.get(k))

    if not updates:
        return jsonify({"ok": False, "error": "No fields to update"}), 400

    params.append(row_id)

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            sql = f"UPDATE {Config.DB_TABLE} SET " + ", ".join(updates) + " WHERE id=%s"
            cur.execute(sql, params)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        conn.close()


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
