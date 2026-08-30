# الشرح التفصيلي للأكواد سطر بسطر (Line-by-Line Code Breakdown)
## مشروع خط البيانات الهجين (Hybrid ELT Data Pipeline)

تم إعداد هذا المستند ليقدم شرحاً دقيقاً وتفصيلياً لكل ملف من ملفات الكود المصدري للمشروع، موضّحاً وظيفة كل سطر وكتلة برمجية، ليكون مرجعاً سهلاً أثناء المراجعة والمناقشة العملية.

---

## 📑 فهرس الملفات المشروحة

1. [`config/settings.py` (ملف الإعدادات)](#1-ملف-الإعدادات-configsettingspy)
2. [`src/main.py` (نقطة الدخول والتشغيل)](#2-نقطة-الدخول-srcmainpy)
3. [`src/file_router.py` (موجّه الملفات الذكي)](#3-موجّه-الملفات-srcfile_routerpy)
4. [`src/mongo_setup.py` (إعداد قاعدة البيانات والفهارس)](#4-إعداد-قاعدة-البيانات-srcmongo_setuppy)
5. [`src/batch_loader.py` (محرك الملفات الصغيرة)](#5-محرك-الباتش-srcbatch_loaderpy)
6. [`src/spark_loader.py` (محرك الملفات الكبيرة)](#6-محرك-سبارك-srcspark_loaderpy)
7. [`src/elt_pipeline.py` (المنسق المركزي لخط البيانات)](#7-المنسق-المركزي-srcelt_pipelinepy)
8. [`src/upsert_writer.py` (محرك الكتابة بالـ Upsert)](#8-محرك-الـ-upsert-srcupsert_writerpy)
9. [`src/incremental_loader.py` (محرك التحميل التزايدي)](#9-التحميل-التزايدي-srcincremental_loaderpy)
10. [`src/metrics.py` (حساب المقاييس والاتساق)](#10-حاسب-المقاييس-srcmetricspy)
11. [`src/create_small_sample.py` (توليد العينات)](#11-توليد-العينات-srccreate_small_samplepy)
12. [مجلد قواعد التنظيف `src/cleaning/`](#12-مجلد-قواعد-التنظيف-srccleaning)
13. [مجلد التحقق والعزل `src/validation/`](#13-مجلد-التحقق-والعزل-srcvalidation)
14. [مجلد الاختبارات `tests/`](#14-مجلد-الاختبارات-tests)

---

## 1. ملف الإعدادات: `config/settings.py`

```python
import os
from pathlib import Path
```
* **الشرح**: استيراد مكتبتي `os` (للتعامل مع متغيرات البيئة) و `Path` (للتعامل مع مسارات الملفات بشكل احترافي ومتوافق مع جميع أنظمة التشغيل).

```python
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
REPORTS_DIR = BASE_DIR / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)
```
* **الشرح**:
  - `BASE_DIR`: تحديد المسار الجذري للمشروع تلقائياً.
  - `DATA_DIR` & `REPORTS_DIR`: تحديد مسارات مجلد البيانات ومجلد التقارير.
  - `mkdir(...)`: إنشاء مجلد التقارير `reports/` تلقائياً إن لم يكن موجوداً.

```python
SMALL_FILE_THRESHOLD_MB = float(os.getenv("SMALL_FILE_THRESHOLD_MB", "200.0"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))
```
* **الشرح**: 
  - `SMALL_FILE_THRESHOLD_MB`: الحد الفاصل (200 ميجابايت) لاختيار المحرك، مع إمكانية تغييره من متغيرات البيئة.
  - `BATCH_SIZE`: تحديد حجم الدفعة (5000 سجل) عند القراءة والإدخال لتفادي امتلاء الذاكرة.

```python
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "midterm_db")

COLLECTION_RAW = "orders_raw"
COLLECTION_VALIDATED = "orders_validated"
COLLECTION_QUARANTINE = "orders_quarantine"
```
* **الشرح**: إعدادات الاتصال بقاعدة بيانات MongoDB وتسمية المجموعات الثلاث (`orders_raw` للبيانات الخام، `orders_validated` للنظيفة، `orders_quarantine` للمعزولة).

```python
SPARK_APP_NAME = "MidtermDataPipeline"
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
```
* **الشرح**: تسمية تطبيق سبارك وضبط وضع التشغيل الافتراضي محلياً `local[*]` (باستخدام كافة أنوية المعالج)، مع إمكانية تحويله لعنقود خارجي عبر متغير `SPARK_MASTER`.

```python
VALID_STATUSES = {
    "مؤكد": "مؤكد", "قيد الانتظار": "قيد الانتظار", "ملغي": "ملغي",
    "مرتجع": "مرتجع", "مكتمل": "مكتمل", "تم الدفع": "تم الدفع", ...
}
```
* **الشرح**: قاموس الحالات القياسية لتوحيد المرادفات (مثل تحويل "بانتظار" إلى "قيد الانتظار").

```python
DATE_FORMATS = [
    "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y", ...
]
```
* **الشرح**: مصفوفة تحتوي على كافة صيغ التواريخ المحتملة في الملفات لتحويلها لاحقاً إلى صيغة ISO القياسية.

---

## 2. نقطة الدخول: `src/main.py`

```python
def process_single_file(file_path: str, incremental: bool = False):
```
* **الشرح**: دالة لمعالجة ملف CSV مفرد عبر خط البيانات.

```python
    run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_id = f"run_{run_timestamp}_{uuid.uuid4().hex[:6]}"
```
* **الشرح**: توليد معرف فريد للتشغيل `run_id` يجمع بين الوقت والتاريخ وكود عشوائي فريد لتتبع السجلات.

```python
    file_size_bytes = os.path.getsize(file_path)
    file_size_mb = file_size_bytes / (1024 * 1024)
    engine = route_file(file_path)
```
* **الشرح**: حساب حجم الملف بالميجابايت، ثم استدعاء دالة `route_file` لفحص الحجم وتحديد المحرك المناسب تلقائياً.

```python
    metrics = run_elt_pipeline(
        file_path=file_path, run_id=run_id, engine=engine,
        file_size_mb=file_size_mb, incremental=incremental
    )
    if metrics:
        save_run_metrics(metrics)
```
* **الشرح**: تشغيل الـ Pipeline بالكامل، وعند اكتمال المعالجة يتم حفظ الإحصائيات في `reports/results.json`.

```python
def main():
    parser = argparse.ArgumentParser(description="Hybrid ELT Data Pipeline (Python Batch & PySpark)")
    parser.add_argument("--input", default="data", help="Path to CSV or directory")
    parser.add_argument("--incremental", action="store_true", help="Incremental mode")
```
* **الشرح**: تهيئة مستلم الأوامر من الـ Terminal مع جعل مجلد `data/` هو الخيار الافتراضي.

```python
    if os.path.isdir(target_path):
        csv_files = sorted([os.path.join(target_path, f) for f in os.listdir(target_path) if f.endswith(".csv")])
        for csv_file in csv_files:
            process_single_file(csv_file, args.incremental)
    else:
        process_single_file(target_path, args.incremental)
```
* **الشرح**: فحص المسار المدخل؛ إذا كان مجلداً يبحث عن كافة ملفات الـ CSV ويعالجها بالترتيب، وإذا كان ملفاً مفرداً يعالجه مباشرة.

---

## 3. موجّه الملفات: `src/file_router.py`

```python
def route_file(file_path: str) -> str:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Source file not found at path: {file_path}")
        
    file_size_bytes = os.path.getsize(file_path)
    file_size_mb = file_size_bytes / (1024 * 1024)
```
* **الشرح**: التأكد من وجود الملف ثم حساب حجمه بالميجابايت.

```python
    if file_size_mb <= settings.SMALL_FILE_THRESHOLD_MB:
        engine = "python_batch"
        reason = "File size is below or equal to the threshold. Choosing Python Batch engine..."
    else:
        engine = "pyspark"
        reason = "File size exceeds the threshold. Choosing PySpark engine..."
```
* **الشرح**: المقارنة الشرطية: إذا كان الحجم $\le 200\text{MB}$ يختار `python_batch` مع تبرير عدم الحاجة لـ Spark، وإذا كان $> 200\text{MB}$ يختار `pyspark` لتوزيع المعالجة على الذاكرة.

---

## 4. إعداد قاعدة البيانات: `src/mongo_setup.py`

```python
def get_mongo_client():
    return pymongo.MongoClient(settings.MONGO_URI)

def get_database(client=None):
    if client is None:
        client = get_mongo_client()
    return client[settings.MONGO_DB_NAME]
```
* **الشرح**: دوال مساعدة لإنشاء وإرجاع اتصال مع خادم وقاعدة بيانات MongoDB.

```python
    # 1. orders_raw
    if settings.COLLECTION_RAW not in db.list_collection_names():
        db.create_collection(settings.COLLECTION_RAW)
    db[settings.COLLECTION_RAW].create_index("run_id")
```
* **الشرح**: إنشاء جدول `orders_raw` بدون أي شروط أو فهارس فريدة لضمان قبول كل السجلات الخام كما هي، مع عمل فهرس على `run_id` لسرعة الاستعلام.

```python
    # 2. orders_validated (Unique Index)
    db[settings.COLLECTION_VALIDATED].create_index([("order_id", pymongo.ASCENDING)], unique=True)
    db[settings.COLLECTION_VALIDATED].create_index("run_id")
```
* **الشرح**: إنشاء **فهرس فريد (`unique=True`) على حقل `order_id`** لمنع أي تكرار تجاري للسجلات وضمان الـ Idempotency.

```python
    # 3. orders_quarantine
    db[settings.COLLECTION_QUARANTINE].create_index("order_id")
    db[settings.COLLECTION_QUARANTINE].create_index("run_id")
```
* **الشرح**: إنشاء جدول العزل `orders_quarantine` بفهارس عادية تقبل السجلات المكررة والتالفة.

---

## 5. محرك الباتش: `src/batch_loader.py`

```python
def load_csv_in_batches(file_path: str, run_id: str) -> int:
```
* **الشرح**: دالة قراءة وتدفق الملفات الصغيرة إلى MongoDB بنظام الـ Streaming.

```python
    with open(file_path, 'r', encoding='utf-8-sig', errors='replace') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=1):
```
* **الشرح**: فتح الملف بتشفير `utf-8-sig` (لمعالجة الـ BOM العربي)، وقراءة الملف سطراً بسطر بنظام Streaming دون تحميله كاملاً في الذاكرة.

```python
            raw_document = {
                "run_id": run_id,
                "source_file": file_path,
                "source_row_number": i,
                "ingested_at": datetime.utcnow(),
                "engine_used": "python_batch",
                "raw_record": cleaned_row
            }
            current_batch.append(raw_document)
```
* **الشرح**: تغليف السجل الخام وإضافة بيانات التتبع (رقم الصف، المصدر، وقت التحميل، واسم المحرك).

```python
            if len(current_batch) >= settings.BATCH_SIZE:
                raw_collection.insert_many(current_batch, ordered=False)
                # حساب السرعة وطباعة التقدم
                current_batch = []
```
* **الشرح**: عند اكتمال الدفعة (5000 سجل) يتم إدخالها إلى `orders_raw` دفعة واحدة عبر `insert_many`، وتفريغ الدفعة لمتابعة القراءة.

---

## 6. محرك سبارك: `src/spark_loader.py`

```python
venv_python = sys.executable
os.environ["PYSPARK_PYTHON"] = venv_python
os.environ["PYSPARK_DRIVER_PYTHON"] = venv_python
```
* **الشرح**: توجيه عمال سبارك (Workers) لاستخدام نفس بايثون البيئة الافتراضية حتى تتمكن من استيراد مكتبة `pymongo`.

```python
def build_spark_session() -> SparkSession:
    builder = SparkSession.builder \
        .appName(settings.SPARK_APP_NAME) \
        .master(settings.SPARK_MASTER) \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g")
    return builder.getOrCreate()
```
* **الشرح**: بناء جلسة سبارك وتخصيص 4 جيجابايت من الذاكرة لضمان سرعة المعالجة.

```python
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("order_date", StringType(), True),
        ...
        StructField("items_json", StringType(), True)
    ])
```
* **الشرح**: تعريف Schema ثابتة مع قراءة الحقول كنصوص `StringType` لمنع سبارك من تشويه أو حذف القيم غير النظيفة تلقائياً.

```python
    df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .schema(schema) \
        .load(file_path)
```
* **الشرح**: قراءة الـ CSV مع ضبط `escape` و `quote` لضمان قراءة حقل الـ JSON الذي يحتوي على فواصل ونصوص مقتبسة دون مشاكل.

```python
    def write_partition_to_mongo(partition_iterator):
        # كتابة كل تقسيم من تقسيمات سبارك مباشرة إلى MongoDB
        ...
    df_raw.foreachPartition(write_partition_to_mongo)
```
* **الشرح**: تنفيذ عملية الكتابة بالتوازي عبر تقسيمات الـ DataFrame (`foreachPartition`) مباشرة إلى `orders_raw`.

---

## 7. المنسق المركزي: `src/elt_pipeline.py`

```python
def run_elt_pipeline(file_path: str, run_id: str, engine: str, file_size_mb: float, incremental: bool = False) -> Dict[str, Any]:
```
* **الشرح**: الدالة المسؤولة عن تنسيق كامل تدفق الـ ELT.

```python
    # Phase 1: Ingestion
    if engine == "python_batch":
        raw_loaded = load_csv_in_batches(file_path, run_id)
    elif engine == "pyspark":
        spark_results = load_csv_with_spark(file_path, run_id)
```
* **الشرح**: المرحلة الأولى (التحميل الخام): استدعاء المحرك المختار لإدخال السجلات إلى `orders_raw`.

```python
    # Phase 2 & 3: Processing & Classification
    cursor = raw_collection.find({"run_id": run_id}, no_cursor_timeout=True)
    for doc in cursor:
        cleaned_rec, corrections = registry.clean_record(raw_record)
        final_rec, status = classify_and_tag_record(processed_rec, raw_record, corrections, seen_order_ids)
```
* **الشرح**: فتح مؤشر (`Cursor`) لقراءة السجلات الخام على شكل تدفق، وتطبيق قواعد التنظيف وتصنيف كل سجل.

```python
        if status == "quarantined":
            quarantine_chunk.append(final_rec)
        else:
            validated_chunk.append(final_rec)
```
* **الشرح**: فرز السجلات: التالف يذهب لدفعة العزل، والسليم/المصحح يذهب لدفعة التحقق.

```python
        if len(validated_chunk) >= chunk_size:
            ins, upd, unc = validated_writer(validated_chunk)
            validated_chunk = []
```
* **الشرح**: كتابة الدفعات في MongoDB عند بلوغ كل 5000 سجل، مما يضمن ثبات استهلاك الذاكرة في حدود $O(1)$.

---

## 8. محرك الـ Upsert: `src/upsert_writer.py`

```python
def write_validated_records_bulk(records: List[Dict[str, Any]]) -> Tuple[int, int, int]:
    operations = []
    for rec in records:
        match_query = {"order_id": str(order_id).strip()}
        update_doc = {
            "$setOnInsert": { "ingested_at": rec.get("ingested_at"), ... },
            "$set": { "run_id": rec.get("run_id"), "customer_name": rec.get("customer_name"), ... }
        }
        operations.append(UpdateOne(match_query, update_doc, upsert=True))
```
* **الشرح**: بناء عمليات `UpdateOne` مع تفعيل `upsert=True` بالاعتماد على `order_id`؛ حيث تُحفظ بيانات الإدخال الأولى بـ `$setOnInsert` وتُحدّث البيانات بـ `$set`.

```python
    result = collection.bulk_write(operations, ordered=False)
    inserted = result.upserted_count
    updated = result.modified_count
    unchanged = max(0, result.matched_count - result.modified_count)
```
* **الشرح**: تنفيذ العمليات دفعة واحدة وحساب عدد السجلات الجديدة المضافة، والمحدثة، والتي لم تتغير بدقة.

---

## 9. التحميل التزايدي: `src/incremental_loader.py`

```python
def write_incremental_records_bulk(records: List[Dict[str, Any]]) -> Tuple[int, int, int]:
    order_ids = [str(r["order_id"]).strip() for r in records if r.get("order_id")]
    existing_dates = fetch_existing_dates_and_versions(order_ids)
```
* **الشرح**: استرجاع التواريخ السابقة للسجلات الموجودة في قاعدة البيانات لمقارنتها.

```python
    for rec in records:
        if order_id in existing_dates:
            old_date_str = existing_dates[order_id]
            if new_date_str < old_date_str:
                unchanged_count += 1
                continue # تخطي التحديث لأن السجل القادم أقدم من المخزن
```
* **الشرح**: تطبيق **Version Handling**؛ إذا كان تاريخ السجل الجديد أقدم من الموجود يتم تخطيه لمنع الكتابة فوق البيانات الأحدث.

---

## 10. حاسب المقاييس: `src/metrics.py`

```python
def save_run_metrics(metrics: Dict[str, Any]) -> str:
    raw_loaded = metrics.get("raw_loaded", 0)
    valid_count = metrics.get("valid_count", 0)
    corrected_count = metrics.get("corrected_count", 0)
    quarantine_count = metrics.get("quarantine_count", 0)
    
    computed_sum = valid_count + corrected_count + quarantine_count
```
* **الشرح**: استخراج عدادات المعالجة وحساب مجموع السجلات المفروزة.

```python
    if raw_loaded == computed_sum:
        print("  [SUCCESS] Consistency Equation holds: raw_loaded == valid + corrected + quarantine.")
        metrics["consistency_valid"] = True
```
* **الشرح**: التحقق من المعادلة الرياضية للاتساق ومطابقتها قبل حفظ النتائج في `reports/results.json`.

---

## 11. توليد العينات: `src/create_small_sample.py`

```python
def create_sample(input_path, output_path, target_rows):
    with open(input_path, 'r', encoding='utf-8-sig') as infile:
        reader = csv.reader(infile)
        header = next(reader)
        with open(output_path, 'w', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(header)
            for count, row in enumerate(reader, 1):
                writer.writerow(row)
                if count >= target_rows: break
```
* **الشرح**: قراءة تيار الـ CSV من الملف الكبير وكتابة عدد محدد من الصفوف `target_rows` إلى ملف جديد بطريقة برمجية آمنة.

---

## 12. مجلد قواعد التنظيف: `src/cleaning/`

### أ) القاعدة الأساسية: `base_rule.py`
```python
class CleaningRule(ABC):
    @property
    @abstractmethod
    def rule_code(self) -> str: pass
    
    @abstractmethod
    def apply(self, record: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]: pass
```
* **الشرح**: صنف أساسي يحدد العقد البرمجي الذي يجب أن تلتزم به جميع القواعد (إرجاع السجل المعدل + قائمة التصحيحات للـ Audit Trail).

---

### ب) القاعدة 1 - الأرقام الشرقية: `arabic_numbers.py`
```python
    def _convert(self, val_str: str) -> str:
        arabic_digits = "٠١٢٣٤٥٦٧٨٩٫"
        latin_digits = "0123456789."
        trans_table = str.maketrans(arabic_digits, latin_digits)
        return val_str.translate(trans_table)
```
* **الشرح**: استخدام جدول تحويل الأحرف `maketrans` لتحويل الأرقام الشرقية `٠-٩` والفواصل `٫` إلى أرقام لاتينية قياسية `0-9` و `.`.

---

### ج) القاعدة 2 - نصوص العملات: `currency_text.py`
```python
    currency_patterns = [r"\s*ريال\s*يمني", r"\s*ريال", r"\s*لاير", r"\s*[yY][eE][rR]"]
    cleaned_val = combined_pattern.sub("", val).strip()
```
* **الشرح**: البحث بتعابير Regex عن نصوص العملات في حقول المبالغ وإزالتها، وتعيين حقل `currency` إلى `YER`.

---

### د) القاعدة 3 - فواصل الآلاف: `thousands_sep.py`
```python
    comma_pattern = re.compile(r"(\d),(\d{3})")
    # استبدال الفواصل بين الأرقام: 125,000.00 -> 125000.00
```
* **الشرح**: إزالة فواصل الآلاف التي تفصل بين كل 3 خانات رقمية لتصبح قابلة للتحويل إلى أرقام عشرية صحيحة.

---

### هـ) القاعدة 4 - الأسعار بالكلمات: `word_price.py`
```python
    self.words_map = {
        "ألف": "1000", "ألفان": "2000", "خمسة آلاف": "5000", "عشرة آلاف": "10000", ...
    }
```
* **الشرح**: مطابقة الكلمات الشائعة لأسعار الطلبات المكتوبة باللغة العربية وتحويلها إلى قيم رقمية مكافئة.

---

### و) القاعدة 5 - أرقام الهواتف: `phone_number.py`
```python
    # معالجة التنسيق المقلوب: "4567 123 77 967+"
    reversed_match = re.match(r"^(\d{4})\s+(\d{3})\s+(\d{2})\s+(\d{3})\+$", phone_str)
    if reversed_match:
        num_clean = f"+{reversed_match.group(4)}{reversed_match.group(3)}{reversed_match.group(2)}{reversed_match.group(1)}"
```
* **الشرح**: فحص الأرقام المقلوبة وإعادة ترتيب خاناتها بالشكل الصحيح وتوحيد الصيغة اليمنية الدولية `+967XXXXXXXXX`.

---

### ز) القاعدة 6 - تصحيح البريد: `email_fix.py`
```python
    email_fixed_at = re.sub(r"@+", "@", email) # تحويل @@ إلى @
    email_fixed_dots = re.sub(r"\.+", ".", email_fixed_at) # تحويل .. إلى .
```
* **الشرح**: إصلاح الأخطاء المطبعية الشائعة بتكرار الرموز في عناوين البريد الإلكتروني.

---

### ح) القاعدة 7 - توحيد التاريخ: `date_normalize.py`
```python
    for fmt in settings.DATE_FORMATS:
        try:
            parsed_date = datetime.strptime(date_str, fmt)
            standardized = parsed_date.strftime("%Y-%m-%dT%H:%M:%S")
```
* **الشرح**: محاولة قراءة التاريخ وفق الصيغ المدعومة وتحويله إلى صيغة ISO-8601 القياسية الموحدة.

---

### ط) القاعدة 8 - توحيد الحالات والمسافات: `status_normalize.py`
```python
    cleaned[key] = val.strip() # إزالة المسافات الزائدة
    if val in settings.VALID_STATUSES:
        cleaned[field] = settings.VALID_STATUSES[val] # توحيد المرادفات
```
* **الشرح**: عمل `trim` لكافة النصوص وتوحيد مرادفات الحالات وفق القاموس المعتمد.

---

### ي) القاعدة 9 - إعادة حساب الإجمالي: `total_recalc.py`
```python
    # إصلاح الكميات السالبة إذا كان الإجمالي موجباً
    if qty < 0 and total > 0:
        qty = abs(qty)
    # إعادة حساب إجمالي السلة + سعر التوصيل
    expected_total = items_total_sum + delivery_cost
    if abs(total_amount - expected_total) > 0.01:
        cleaned["total_amount"] = str(expected_total)
```
* **الشرح**: تفكيك JSON العناصر، إصلاح أخطاء الإدخال في الكميات السالبة، وإعادة حساب إجمالي الفاتورة ومطابقتها.

---

### ك) سجل القواعد: `rule_registry.py`
```python
class RuleRegistry:
    def __init__(self):
        self.rules = [
            ArabicNumbersRule(), ThousandsSeparatorRule(), CurrencyTextRule(),
            WordPriceRule(), PhoneNormalizationRule(), EmailFixRule(),
            DateNormalizeRule(), StatusNormalizeRule(), TotalRecalculationRule()
        ]
```
* **الشرح**: تجميع القواعد وتطبيقها بتسلسل منطقي سليم وتجميع كل عناصر الـ `Audit Trail` في قائمة واحدة.

---

## 13. مجلد التحقق والعزل: `src/validation/`

### أ) مصنف أسباب العزل: `quarantine_classifier.py`
```python
def classify_quarantine_errors(record: Dict[str, Any], seen_order_ids: Set[str]):
    # فحص 1: هل order_id مفقود؟ -> MISSING_ORDER_ID
    # فحص 2: هل order_id مكرر؟ -> DUPLICATE_ORDER_ID
    # فحص 3: هل customer_id مفقود؟ -> MISSING_CUSTOMER_ID
    # فحص 4: هل التاريخ مستحيل؟ -> INVALID_IMPOSSIBLE_DATE
    # فحص 5: هل JSON العناصر تالف؟ -> CORRUPTED_ITEMS_JSON
    # فحص 6: هل قائمة العناصر فارغة؟ -> EMPTY_ITEMS
    # فحص 7: هل السعر مجهول؟ -> UNKNOWN_PRICE
    # فحص 8: هل توجد مبالغ سالبة غير قابلة للحل؟ -> AMBIGUOUS_NEGATIVE_VALUE
    # فحص 9: هل توجد 3 أخطاء أو أكثر؟ -> MULTIPLE_CONFLICTING_ERRORS
```
* **الشرح**: التحقق الصارم من سلامة السجل وتحديد رموز الأخطاء الـ 9 بدقة.

### ب) مصنف السجلات: `record_classifier.py`
```python
def classify_and_tag_record(...):
    if error_codes:
        record["quality_status"] = "quarantined"
        record["error_codes"] = error_codes
    elif corrections:
        record["quality_status"] = "corrected"
        record["corrections"] = corrections
    else:
        record["quality_status"] = "valid"
```
* **الشرح**: إسناد الحالة النهائية للسجل (`valid` أو `corrected` أو `quarantined`) ودمج أثر التعديل أو تفاصيل العزل.

---

## 14. مجلد الاختبارات: `tests/`

* **`test_cleaning_rules.py`**: يحتوي على دوال اختبارية مستقلة لكل قاعدة من القواعد الـ 9 للتأكد من أنها ترجع القيمة المصححة والأثر التوثيقي المتوقع.
* **`test_classification.py`**: يختبر تصنيف الحالات السليمة، والحالات المفقودة المعرّف، والحالات المكررة، وحالات الأخطاء المتعددة.
* **`test_upsert_idempotency.py`**: يختبر الاتصال بقاعدة البيانات وإثبات أن تكرار الإدخال يرجع `Inserts=0` وتعديل السجل يرجع `Updates=1`، واختبار حماية الإصدارات في التحميل التزايدي.
