---

# 🚀 1️⃣ System Flow (End-to-End)

### Step 1: User hits API

Example:

```
GET /api/pysearch/nagar-nigam?q=सुन्दर
```

↓

### Step 2: Route calls

```python
execute_phonetic_search("nagar_nigam", q)
```

↓

### Step 3: Query Normalization

`get_universal_skeleton(query)`

Converts query into:

| Layer                 | Purpose                   |
| --------------------- | ------------------------- |
| Latin Transliteration | Normalize Hindi spelling  |
| Consonant Skeleton    | Remove vowels             |
| Double Metaphone      | English phonetic encoding |

↓

### Step 4: Database Prefilter

```sql
SELECT *
FROM table
WHERE voter_name LIKE 'स%'
   OR father_husband_mother_name LIKE 'स%'
```

Purpose:

* Reduce memory load
* Improve performance

↓

### Step 5: Row Scoring

For each row:

* Score `voter_name`
* Score `father_husband_mother_name`
* Take best score
* If score ≥ 55 → keep

↓

### Step 6: Unique ID Filtering

Results stored in:

```python
unique_matches[row_id]
```

Guarantee:

✔ One ID = One result
✔ Highest score retained
✔ No duplicate rows

↓

### Step 7: Final Sorting

Sorted by:

```python
match_score
DESC
```

↓

### Step 8: JSON Response

```json
{
  "query": "सुन्दर",
  "total": 87,
  "data": [
    ...
  ]
}
```

---

# 🧠 2️⃣ How Phonetic Matching Works

Your system uses **4 Layers of Matching**

---

## 🔹 Layer 1 — Hindi Normalization

Handles common variations:

| Variation | Normalized |
|-----------|------------|
| व / ब     | ब          |
| श / ष     | स          |
| ं / ँ     | न          |

Example:

```
सुन्दर
सुंदर
सुनदर
```

All normalized into similar base form.

---

## 🔹 Layer 2 — Transliteration

Using:

```python
indic_transliteration
```

Converts:

```
सुन्दर → sundara
```

This allows consistent comparison.

---

## 🔹 Layer 3 — Consonant Skeleton

Remove vowels:

```
sundara → sndr
sunder → sndr
```

This solves:

* Missing vowels
* Typing mistakes
* Spelling variations

---

## 🔹 Layer 4 — Double Metaphone (English Phonetic)

Using:

```python
metaphone.doublemetaphone()
```

Example:

```
kumar → KMR
kumaar → KMR
```

Helps match:

* English names
* Mixed Hindi-English entries

---

## 🔹 Layer 5 — Fuzzy Matching

Using:

```python
rapidfuzz
```

Two algorithms used:

### 1️⃣ fuzz.ratio()

Full string similarity

### 2️⃣ fuzz.partial_ratio()

Handles partial names:

```
Pankaj
Pankaj Kumar
```

---

# ⚙️ 3️⃣ Final Scoring Formula

```python
score =
(Skeleton Match × 0.4)
+ (Partial Match × 0.4)
+ (Full Ratio × 0.1)
+ (Phonetic Bonus +10)
```

### Why This Weighting?

| Component | Importance             |
|-----------|------------------------|
| Skeleton  | Highest accuracy       |
| Partial   | Good for partial names |
| Ratio     | Minor correction       |
| Phonetic  | English fallback       |

---

# 🔒 4️⃣ How We Guarantee Unique Results

We use dictionary:

```python
unique_matches[row_id]
```

Logic:

* If ID not present → add
* If present → keep higher score

This ensures:

✔ No duplicates
✔ Best match retained
✔ Stable ranking

---

# 📚 5️⃣ Libraries Used

| Library               | Purpose                       |
|-----------------------|-------------------------------|
| flask                 | API routing                   |
| sqlalchemy            | DB execution                  |
| rapidfuzz             | Fast fuzzy matching           |
| metaphone             | English phonetic hashing      |
| indic_transliteration | Hindi → Latin transliteration |
| re                    | Regex for skeleton            |
| os                    | Environment config            |

---

# 📌 Why RapidFuzz Instead of FuzzyWuzzy?

✔ Faster
✔ Pure C++ backend
✔ No heavy dependencies
✔ Production safe

---

# 🏗 6️⃣ Architecture Overview

```
User Query
     ↓
Normalization (Hindi Fix)
     ↓
Transliteration
     ↓
Skeleton Extraction
     ↓
Phonetic Hash
     ↓
DB Prefilter
     ↓
Score Each Row
     ↓
Unique Filter
     ↓
Sort & Return
```

---

# ⚡ 7️⃣ Performance Characteristics

Current design:

✔ Prefilter reduces load
✔ No full table scan
✔ Pure Python scoring
✔ Memory safe

---

# 🛡 Important Things to Remember

### Always:

* Add DB index on `voter_name`
* Add DB index on `father_husband_mother_name`

Example:

```sql
CREATE INDEX idx_voter_name ON nagar_nigam (voter_name);
CREATE INDEX idx_father_name ON nagar_nigam (father_husband_mother_name);
```

Without index → slow LIKE queries.

---

# 🎯 What You Built

This is NOT basic search.

This is:

> Multi-Layer Hindi + English Hybrid Phonetic Search Engine

Supports:

✔ Hindi spelling variations
✔ English spelling variations
✔ Mixed language names
✔ Partial names
✔ Typing mistakes
✔ Phonetic errors
✔ Duplicate-safe results

---

# 📄 Example Matching Power

Query:

```
सुन्दर
```

Matches:

* सुन्दर
* सुंदर
* सुनदर
* Sundar
* Sunder
* Sondar

---

