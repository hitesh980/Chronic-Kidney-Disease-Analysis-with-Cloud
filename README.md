# Chronic Kidney Disease (CKD) Data Pipeline
## End-to-End Medallion Architecture on Databricks

![Architecture](https://img.shields.io/badge/Architecture-Medallion-blue)
![Platform](https://img.shields.io/badge/Platform-Databricks-orange)
![Language](https://img.shields.io/badge/Language-PySpark-yellow)
![Storage](https://img.shields.io/badge/Storage-AWS%20S3-green)

## 📋 Project Overview

An end-to-end data engineering pipeline for analyzing the UCI Chronic Kidney Disease dataset, implementing a **Bronze-Silver-Gold** (Medallion) architecture on Databricks. The project demonstrates robust data quality handling, clinical feature engineering, and streaming ingestion from AWS S3.

**Dataset**: 200 patient records with 29 clinical features  
**Goal**: Transform raw, messy healthcare data into analysis-ready format

---

## 🏗️ Architecture

```
AWS S3 (Raw Data)
    ↓
Bronze Layer (Auto Loader - Streaming Ingestion)
    ↓
Silver Layer (Data Cleaning & Type Casting)
    ↓
Gold Layer (Feature Engineering & Analytics)
```

---

## 🚧 Technical Challenges & Solutions

### **Challenge 1: Delimiter Misidentification**
**Problem**: Raw file was comma-delimited but initially assumed to be tab-delimited, causing all columns to merge into a single field.

**Solution**:
- Inspected raw file using `spark.read.text()` to identify actual delimiter
- Changed split delimiter from `\t` to `,` in Auto Loader configuration
- Used text-based reading (`cloudFiles.format = "text"`) with manual column splitting instead of CSV parser

```python
df_raw = spark.readStream.format("cloudFiles").option("cloudFiles.format", "text").load(s3_path)
df_split = df_raw.withColumn("cols", split(col("value"), ","))
```

---

### **Challenge 2: Malformed Clinical Values**
**Problem**: Numeric columns contained non-standard formats that couldn't be directly cast:
- Range values: `"1.019 - 1.021"`, `"138 - 143"`
- Comparison operators: `"< 112"`, `"≥ 227.944"`
- Missing indicators: `"?"`

**Solution**: Developed regex-based parsing logic to handle each case:

```python
# Extract midpoint from ranges
when(col(col_name).rlike("\\d+\\.?\\d*\\s*-\\s*\\d+\\.?\\d*"),
    spark_round((split(col(col_name), "\\s*-\\s*")[0].cast("double") + 
                 split(col(col_name), "\\s*-\\s*")[1].cast("double")) / 2, 3))

# Remove comparison operators and extract number
.otherwise(regexp_replace(regexp_replace(col(col_name), "^[<≥>=]+\\s*", ""), "\\s*-\\s*.*$", ""))
```

**Result**: Successfully converted 17 numeric columns with 0% data loss

---

### **Challenge 3: Floating-Point Precision Errors**
**Problem**: Midpoint calculations produced values like `1.0099999999999998` instead of `1.01`

**Solution**: Added rounding to 3 decimal places (clinically appropriate precision)

```python
spark_round(calculated_value, 3)
```

---

### **Challenge 4: PySpark Version Incompatibility**
**Problem**: `try_cast()` function not available in Spark 3.3 (requires Spark 3.4+), causing casting errors

**Solution**: Implemented safe casting with regex validation:

```python
when(col(col_name).rlike("^-?\\d+\\.?\\d*$"), col(col_name).cast("double"))
.otherwise(None)  # Non-numeric → NULL
```

---

### **Challenge 5: String-to-Integer Casting**
**Problem**: Strings like `"1.0"` cannot be directly cast to INT in PySpark

**Solution**: Two-step casting process:

```python
col(col_name).cast("double").cast("int")  # string → double → int
```

---

### **Challenge 6: Serverless Compute S3 Access**
**Problem**: `spark.conf.set("fs.s3a.access.key", ...)` not supported on serverless compute

**Solution**: Used Hadoop configuration approach:

```python
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
```

*(Note: For production, use Unity Catalog External Locations instead)*

---

### **Challenge 7: Missing Columns in Cleaning Logic**
**Problem**: Columns `su`, `bp_limit`, `affected` not included in cleaning logic, causing casting errors with values like `"< 0"`

**Solution**: Extended `numeric_cols` list to include all columns requiring numeric transformation

---

## 📂 Project Structure

```
ckd-data-pipeline/
│
├── notebooks/
│   ├── S3_to_Bronze_Ingestion.ipynb        # Auto Loader streaming ingestion
│   ├── Bronze_to_Silver.ipynb              # Data cleaning & type casting
│   └── Silver_to_Gold.ipynb                # Feature engineering & analytics
│
├── data/
│   └── ckd_dataset.csv                     # UCI CKD dataset
│
├── README.md
└── requirements.txt
```

---

## 🔧 Technologies Used

| Component | Technology |
|-----------|------------|
| Cloud Storage | AWS S3 |
| Data Platform | Databricks (Serverless Compute) |
| Processing Engine | PySpark 3.3+ |
| Storage Format | Delta Lake |
| Ingestion | Auto Loader (cloudFiles) |
| Architecture | Medallion (Bronze-Silver-Gold) |

---

## 📊 Data Schema

### Clinical Features (29 total)

**High-Impact Kidney Markers (4)**
- `sc` - Serum Creatinine (mg/dL)
- `grf` - Glomerular Filtration Rate (mL/min)
- `bu` - Blood Urea (mg/dL)
- `al` - Albumin (0-5 scale)

**Continuous Lab Results (11)** - DOUBLE type  
`sg`, `bgr`, `bu`, `sod`, `sc`, `pot`, `hemo`, `pcv`, `rbcc`, `wbcc`, `grf`

**Discrete/Counts (6)** - INTEGER type  
`age`, `al`, `su`, `bp_diastolic`, `bp_limit`, `affected`

**Categorical (12)** - STRING type  
`class`, `rbc`, `pc`, `pcc`, `ba`, `htn`, `dm`, `cad`, `appet`, `pe`, `ane`, `stage`

---

## 🚀 Setup Instructions

### Prerequisites
- Databricks workspace
- AWS S3 bucket with access credentials
- UCI CKD dataset

### Step 1: Configure S3 Access

```python
dbutils.widgets.text("aws_access_key", "YOUR_ACCESS_KEY")
dbutils.widgets.text("aws_secret_key", "YOUR_SECRET_KEY")
dbutils.widgets.text("s3_bucket_name", "YOUR_BUCKET_NAME")

access_key = dbutils.widgets.get("aws_access_key")
secret_key = dbutils.widgets.get("aws_secret_key")

# For serverless compute
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
```

### Step 2: Create Schema

```sql
CREATE SCHEMA IF NOT EXISTS workspace.ckd_project;
```

### Step 3: Run Notebooks in Order

1. **Bronze Ingestion**: `S3_to_Bronze_Ingestion.ipynb`
2. **Silver Transformation**: `Bronze_to_Silver.ipynb`
3. **Gold Analytics**: `Silver_to_Gold.ipynb`

---

## 📈 Pipeline Stages

### Bronze Layer
- **Purpose**: Raw data ingestion
- **Format**: All columns as STRING
- **Processing**: Auto Loader streaming from S3
- **Output**: `workspace.ckd_project.ckd_bronze`

### Silver Layer
- **Purpose**: Data cleaning & standardization
- **Transformations**:
  - Replace `"?"` with NULL
  - Parse range values → midpoints
  - Remove comparison operators
  - Cast to proper types (double/int/string)
  - Lowercase categorical values
- **Output**: `workspace.ckd_project.ckd_silver`

### Gold Layer
- **Purpose**: Feature engineering & analytics-ready data
- **Features**: (To be implemented)
  - Null value analysis
  - Feature imputation
  - Derived metrics
- **Output**: `workspace.ckd_project.ckd_gold`

---

## 📊 Results

- ✅ **200 patient records** successfully ingested
- ✅ **29 clinical features** properly typed
- ✅ **0% data loss** through Bronze → Silver transformation
- ✅ **100% range value** parsing success
- ✅ **17 numeric columns** safely cast from malformed strings

---

## 🔍 Key Learnings

1. **Never assume file format** - Always inspect raw data first
2. **Regex is powerful** for unstructured text parsing in healthcare data
3. **Type casting requires validation** - Check data patterns before casting
4. **PySpark version matters** - API availability varies by version
5. **Precision matters in healthcare** - Round appropriately for clinical context
6. **Serverless has constraints** - Configuration options differ from cluster mode

---

## 🛠️ Future Enhancements

- [ ] Implement ML model for CKD prediction
- [ ] Add data quality monitoring dashboard
- [ ] Implement missing value imputation strategies
- [ ] Add unit tests for transformation logic
- [ ] Create data lineage visualization
- [ ] Deploy as production pipeline with orchestration

---

## 📝 Data Quality

### Transformation Pipeline
- **Malformed Value Handling**: 100% coverage
- **Type Safety**: Regex validation before casting
- **Missing Value Strategy**: Convert "?" to NULL
- **Range Value Processing**: Midpoint calculation with 3-decimal precision
- **Comparison Operators**: Extract numeric values (< 112 → 112)

---

## 🎓 Educational Value

This project demonstrates:
- **Real-world data quality challenges** in healthcare
- **Medallion architecture** implementation
- **PySpark streaming** with Auto Loader
- **Regex pattern matching** for complex string parsing
- **Type system** handling and casting strategies
- **Version compatibility** problem-solving

---

## 📝 License

This project is for educational purposes using the UCI Machine Learning Repository dataset.

**Dataset Citation:**  
Dua, D. and Graff, C. (2019). UCI Machine Learning Repository. Irvine, CA: University of California, School of Information and Computer Science.

---

## 👤 Author

Hitesh Allakki

---

## 🙏 Acknowledgments

- UCI Machine Learning Repository for the CKD dataset
- Databricks documentation and community
- Apache Spark community

---

## 📞 Contact

For questions or collaboration opportunities, please reach out via:
- LinkedIn: [Your Profile]
- Email: [Your Email]
- GitHub Issues: [Repository Issues]
