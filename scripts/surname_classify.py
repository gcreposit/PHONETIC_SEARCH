import os
import re
import pymysql
from typing import Optional, Tuple, Dict, List

# ---------------------------
# CONFIG (edit or use env vars)
# ---------------------------
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "root3306")
DB_NAME = os.getenv("DB_NAME", "voting_db_2")
DB_TABLE = os.getenv("DB_TABLE", "voter_data")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))
DRY_RUN = os.getenv("DRY_RUN", "0") == "1"   # DRY_RUN=1 -> no DB updates, only prints summary


# ---------------------------
# CLEANING + TOKENIZATION
# ---------------------------
PUNCT_RE = re.compile(r"[^A-Za-z\u0900-\u097F\s]+")  # keep English + Devanagari + spaces

COMMON_NOISE = {
    # Hindi
    "श्री", "श्रीमती", "कु", "कु.", "सुश्री", "डॉ", "डॉ.", "प्रो", "प्रो.", "मो", "मो.", "मोहम्मद", "मोहम्मद.",
    "स्व", "स्व.", "श्रीमान", "श्रीमतीजी",
    # English
    "MR", "MRS", "MS", "DR", "SHRI", "SMT", "KUMARI", "MD", "MOHD", "MOHAMMAD"
}

def normalize_text(s: Optional[str]) -> str:
    if not s:
        return ""
    s = s.strip()
    s = PUNCT_RE.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def split_tokens(name: str) -> List[str]:
    name = normalize_text(name)
    if not name:
        return []
    toks = name.split(" ")
    toks = [t for t in toks if t and t.upper() not in COMMON_NOISE and t not in COMMON_NOISE]
    return toks

def guess_surname(hindi_name: str, eng_name: str) -> Optional[str]:
    """
    Choose surname/last token from English if available else Hindi.
    """
    et = split_tokens(eng_name)
    ht = split_tokens(hindi_name)

    if et:
        return et[-1].upper()
    if ht:
        return ht[-1]  # keep Hindi token
    return None


# ---------------------------
# SURNAME DICTIONARIES (EXTEND THESE!)
# ---------------------------
# MUSLIM: very common last names / identifiers (best-effort)
MUSLIM_HINTS = {
    "KHAN","ANSARI","SIDDIQUI","QURESHI","SAIFI","NAWAB","MIRZA","SHEIKH","SHAIKH","PATHAN",
    "HASHMI","FAROOQI","RAZA","JAFRI","RIZVI","ALI","HUSAIN","HUSSAIN","AHMAD","AHMED",
    "ABBASI","BILGRAMI","CHISHTI","FIRDAUSI","GHAURI","JAMAL","KAZMI","MADANI","NAQVI",
    "OSMANI","PIRZADA","SABRI","TURABI","USMANI","WARSI","ZAFAR","ZAHID","ZAMAN",
    "SULTAN","FAIZI","SALMAN","IMAM","MAQBOOL","NIZAMI","ASHRAF","KHATEEB","MALIK",
    "REHMAN","RAHMAN","NOORI","BARKATI","QADRI","HABIB","ISLAM","UDDIN", "KHAN","KHANAM","AHMAD","AHMED","ALI","ANSARI","SIDDIKI","SIDDIQI",
    "SHEKH","SHEIKH","MIRZA","RIZVI","QURESHI","USMANI","HASHMI",
    "FAROOQI","FARUKI","JAFRI","ABBASI","PATHAN","NAQVI",
    "KAZMI","ZAIDI","MANSURI","CHISHTI","SAIFI","QADRI", "IMRAN","IRFAN","SHAMIM","SHAFIQ","SHARIF","AZHAR","FAIZ","SALMANI",
    "ABDULLA","HUSAIN","MASUD","RAFIQ","SHABAZ","TABREZ","TAUFIQ",
    "MURTUJA","JUNAID","NIJAMI","SULEMAN","IMTIYAJ","ISHTIYAK",
    "FARHAN","AKBAR"

}

MUSLIM_HINTS_HI = {
    "खान","अंसारी","सिद्दीकी","कुरैशी","सैफी","नवाब","मिर्ज़ा","शेख","पठान","हाशमी",
    "फारूकी","रज़ा","जाफ़री","रिज़वी","अली","हुसैन","अहमद","अब्बासी","चिश्ती",
    "गौरी","काज़मी","मदनी","नक़वी","उस्मानी","पीरज़ादा","सबरी","उस्मान","वारसी",
    "ज़फर","ज़ाहिद","ज़मान","सुल्तान","फ़ैज़ी","इमाम","मक़बूल","निज़ामी",
    "अशरफ़","ख़तीब","मलिक","रहमान","नूरी","बरकाती","क़ादरी","हबीब","इस्लाम","उद्दीन", "खान","खानम","अहमद","अली","अंसारी","सिद्दीकी","शेख",
    "मिर्ज़ा","रिज़वी","क़ुरैशी","उस्मानी","हाशमी",
    "फारूकी","जाफ़री","अब्बासी","पठान","नक़वी",
    "काज़मी","ज़ैदी","मंसूरी","चिश्ती","सैफी","क़ादरी", "इरफ़ान","इमरान","शमीम","शफीक","शरीफ","अज़हर","फ़ैज़","सलमानी",
    "अब्दुल्ला","हुसैन","मसूद","रफ़ीक","शहबाज़","तबरेज़","तौफ़ीक",
    "मुर्तुजा","जुनैद","निज़ामी","सुलेमान","इम्तियाज़","इश्तियाक",
    "फ़रहान","अकबर"

}


# YADAV (special bucket)
YADAV_HINTS = {"YADAV","YADUVANSHI","AHIR", "YADAV","YADUVANSHI","YADUVAMSH","YADAVAM","YADAVAMSHI","CHANDRAVANSHI","RAGHUVANSHI","GOPALVANSHI"}
YADAV_HINTS_HI = {"यादव","यदुवंशी","अहीर","यादव","यादव","यदुवंशी","यदुवंश","यादववंशी","चंद्रवंशी","रघुवंशी","गोपालवंशी"}


# SC/ST: common Dalit/tribal surnames used in UP/North (NOT exhaustive)
SCST_HINTS = {
    "CHAMAR","JATAV","PASI","KORI","DHOBI","BIND","DOM","VALMIKI","BANSFOR","RAVIDAS",
    "MEHTAR","BHANGI","KOL","BHIL","GOND","SAHARIA","THARU","KANJAR","NAT","BASFOR",
    "HALALKHOR","MAJHI","MUSAHAR","SENTHIL","PARIYAR","MAHLI","SAVAR","LOHAR",
    "KARMAKAR","BAGHEL","RAJGOND","TANTI","TURI","GHASI","KHAIRA","PANIKA", "CHAMAR","PASI","KORI","KORIL","RAVIDAS","VALMIKI","DHANUK",
    "RAJBHAR","BAUDDH","MAJHI","KURIL","DOM","DHOBI",
    "HEMBROM","TIGGA","THARU", "AHIRVAR","KORAM","TIRKI","URAV","KHES","GAJBHIYE","BHUI"

}

SCST_HINTS_HI = {
    "चमार","जाटव","पासी","कोरी","धोबी","बिंद","डोम","वाल्मीकि","बांसफोर","रविदास",
    "मेहतर","भंगी","कोल","भील","गोंड","सहरिया","थारू","कंजर","नट","बसफोर",
    "हलालखोर","मझी","मुसहर","पारियार","महली","सावर","लोहार","कर्मकार","बघेल",
    "राजगोंड","तांती","तुरी","घासी","खैरा","पनिका", "चमार","पासी","कोरी","कोरिल","रविदास","वाल्मीकि","धानुक",
    "राजभर","बौद्ध","माझी","कुरिल","डोम","धोबी",
    "हेम्ब्रम","टिग्गा","थारू", "अहिरवार","कोरम","टिर्की","उरांव","खेस","गजभिये","भुई"
}


OBC_HINTS = {
    "KURMI","KUSHWAHA","MAURYA","SAINI","LODHI","LONIYA","NISHAD","KEWAT","MALLAH",
    "RAJBHAR","BHAR","PRAJAPATI","KUMHAR","VERMA","PAL","SAHU","SONI","TELI","KAHAR",
    "KACHHI","KALAR","KASERA","BARI","DARZI","MANSOORI","RANGREZ","ANSARI? (Muslim)",
    "KAMBOJ","KORI? (SC sometimes)","LODH","GAUR","GOALA","YOGI","GADERIYA","GWALA",
    "NAYAK","GIRI","PANDIT? (Gen sometimes)","KUSHWAHA","PATEL","CHAUHAN","TOMAR", "KANAUJIYA","KANNAUJIYA","MAURYA","LODHI","LODI","SAHU","SONI","KASERA",
    "KUMHAR","PRAJAPATI","KUSHWAHA","KURMI","KAHAR","MALI","GAUR",
    "RATHAUR","KESHARVANI","PAL","NISHAD","BATHAM","RAJBHAR", "LOHAR","JANGID","VISHVAKARMA","HALAVAI","BISWAKARMA",
    "SHIVHARE","GAUD"

}

OBC_HINTS_HI = {
    "कुर्मी","कुशवाहा","मौर्य","सैनी","लोधी","निषाद","केवट","मल्लाह","राजभर","भर",
    "प्रजापति","कुम्हार","वर्मा","पाल","साहू","सोनी","तेली","कहार","काछी","कलार",
    "कसेरा","बारी","दरजी","मंसूरी","रंगरेज़","कंबोज","गौला","योगी","गडेरिया",
    "ग्वाला","नायक","गिरि","पटेल","चौहान","तोमर", "कनौजिया","कन्नौजिया","मौर्य","लोधी","लोधी","साहू","सोनी","कसेरा",
    "कुम्हार","प्रजापति","कुशवाहा","कुर्मी","कहार","माली","गौर",
    "राठौर","केसरवानी","पाल","निषाद","बाथम","राजभर", "लोहार","जांगिड़","विश्वकर्मा","हलवाई","विश्वकर्मा",
    "शिवहरे","गौड़"
}


# GENERAL: common upper-caste/general identifiers (best-effort)
GENERAL_HINTS = {
    "SHARMA","MISHRA","TIWARI","TRIPATHI","PANDEY","PANDIT","JOSHI","SAXENA","AGARWAL",
    "GARG","SINGH","THAKUR","RAJPUT","BRAHMIN","KAYASTHA","BHATT","BANSAL","GOEL",
    "MITTAL","GUPTA","MAHESHWARI","JAIN","JOSHI","UPADHYAY","DWIVEDI","CHATURVEDI",
    "VEDI","SHUKLA","PATHAK","DIXIT","NIGAM","SRIVASTAVA","LAL","KAPOOR","MALHOTRA",
    "KHANNA","MEHRA","CHOPRA","SODHI","BHATIA","ARORA","SETH","AGRAWAL","TANDON", "SHARMA","TIWARI","TIVARI","PANDEY","PANDIT","MISHRA","SHUKLA","TRIPATHI",
"DUBE","DUBEY","JHA","BHATT","CHATURVEDI","ASTHANA","SAXENA",
"SRIVASTAVA","SHRIVASTAV","AGARWAL","AGRAWAL","GUPTA","BANSAL",
"GOYAL","JAIN","MEHTA","KAPUR","KHANDELWAL","MAHESHWARI",
"RASTOGI","KULASHRESHTHA","VARMA","VARMAN","NIGAM","BHARADVAJ","SINHA","BASIN","BISEN","PANT","JALOTA","KASHYAP",
    "SAHGAL","KHATTAR","KUKREJA","AHUJA","SURI",
    "CHANDANI","CHAWLA","BHALLA","SALUJA","BAHL","ARORA"

}

GENERAL_HINTS_HI = {
    "शर्मा","मिश्रा","तिवारी","त्रिपाठी","पाण्डेय","जोशी","सक्सेना","अग्रवाल",
    "गर्ग","सिंह","ठाकुर","राजपूत","भट्ट","बंसल","गोयल","मित्तल","महेश्वरी",
    "जैन","उपाध्याय","द्विवेदी","चतुर्वेदी","शुक्ल","पाठक","दीक्षित","निगम",
    "श्रीवास्तव","कपूर","मल्होत्रा","खन्ना","मेहरा","चोपड़ा","सोढ़ी",
    "भाटिया","अरोड़ा","सेठ","टंडन","शर्मा","तिवारी","त्रिवेदी","पाण्डेय","पंडित","मिश्रा","शुक्ल","त्रिपाठी",
"दुबे","दुबेय","झा","भट्ट","चतुर्वेदी","अस्थाना","सक्सेना",
"श्रीवास्तव","अग्रवाल","गुप्ता","बंसल","गोयल","जैन","मेहता",
"कपूर","खंडेलवाल","महेश्वरी","रस्तोगी","कुलश्रेष्ठ","वर्मा",
"निगम","भरद्वाज",  "सिन्हा","बसिन","बिसेन","पंत","जलोटा","कश्यप",
    "सहगल","खट्टर","कुकरेजा","आहूजा","सूरी",
    "चंदानी","चावला","भल्ला","सलूजा","बहल","अरोड़ा"

}


# Ambiguous tokens that we *avoid* using alone (can be in many communities)
AMBIGUOUS = {
    "KUMAR","LAL","DEVI","PRASAD","RAM","CHAND","DAS","NATH","NARAYAN"
}
AMBIGUOUS_HI = {
    "कुमार","लाल","देवी","प्रसाद","राम","चंद","दास","नाथ","नारायण"
}



# ---------------------------
# CLASSIFICATION LOGIC
# ---------------------------
def classify(hindi_name: str, eng_name: str) -> Tuple[str, str]:
    """
    Returns (bucket, reason)
    Buckets: GENERAL, OBC, SC / ST, MUSLIM, YADAV, OTHER
    """
    surname = guess_surname(hindi_name, eng_name)
    if not surname:
        return ("OTHER", "no_name")

    # Normalize surname forms
    s_en = surname.upper() if re.search(r"[A-Za-z]", surname) else ""
    s_hi = surname if re.search(r"[\u0900-\u097F]", surname) else ""

    # Skip ambiguous alone
    if (s_en in AMBIGUOUS) or (s_hi in AMBIGUOUS_HI):
        return ("OTHER", f"ambiguous_surname:{surname}")

    # Priority order: MUSLIM / YADAV / SCST / OBC / GENERAL / OTHER
    if (s_en in MUSLIM_HINTS) or (s_hi in MUSLIM_HINTS_HI):
        return ("MUSLIM", f"surname:{surname}")

    if (s_en in YADAV_HINTS) or (s_hi in YADAV_HINTS_HI):
        return ("YADAV", f"surname:{surname}")

    if (s_en in SCST_HINTS) or (s_hi in SCST_HINTS_HI):
        return ("SC / ST", f"surname:{surname}")

    if (s_en in OBC_HINTS) or (s_hi in OBC_HINTS_HI):
        return ("OBC", f"surname:{surname}")

    if (s_en in GENERAL_HINTS) or (s_hi in GENERAL_HINTS_HI):
        return ("GENERAL", f"surname:{surname}")

    return ("OTHER", f"unknown_surname:{surname}")


# ---------------------------
# DB WORK
# ---------------------------
def get_conn():
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )

def main():
    print(f"DB: {DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}  TABLE: {DB_TABLE}")
    print(f"DRY_RUN={DRY_RUN}  BATCH_SIZE={BATCH_SIZE}")

    stats: Dict[str, int] = {"GENERAL": 0, "OBC": 0, "SC / ST": 0, "MUSLIM": 0, "YADAV": 0, "OTHER": 0}
    updated = 0
    scanned = 0

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # Only rows where caste is NULL/blank
            cur.execute(f"""
                SELECT id, e_name, e_name_eng, caste
                FROM {DB_TABLE}
                WHERE caste IS NULL OR TRIM(caste) = ''
            """)
            rows = cur.fetchall()

        print(f"Rows needing caste fill: {len(rows)}")

        # Process in batches
        updates: List[Tuple[str, int]] = []
        for r in rows:
            scanned += 1
            bucket, reason = classify(r.get("e_name") or "", r.get("e_name_eng") or "")
            stats[bucket] += 1

            # Only update if bucket isn't OTHER (you can change this if you want)
            if bucket != "OTHER":
                updates.append((bucket, r["id"]))

            # Batch flush
            if len(updates) >= BATCH_SIZE:
                updated += flush_updates(conn, updates)
                updates = []

        # flush remainder
        if updates:
            updated += flush_updates(conn, updates)

        print("\n--- SUMMARY ---")
        print(f"Scanned: {scanned}")
        for k, v in stats.items():
            print(f"{k}: {v}")
        print(f"DB Updated rows: {updated}" if not DRY_RUN else f"Would update rows: {updated}")

    finally:
        conn.close()

def flush_updates(conn, updates: List[Tuple[str, int]]) -> int:
    if not updates:
        return 0

    if DRY_RUN:
        # Just count intended updates
        return len(updates)

    with conn.cursor() as cur:
        cur.executemany(f"UPDATE {DB_TABLE} SET caste=%s WHERE id=%s AND (caste IS NULL OR TRIM(caste)='')", updates)
        return cur.rowcount

if __name__ == "__main__":
    main()
