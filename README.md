# منصة معالجة وتحليل البيانات الضخمة (Big Data Platform - Final Project)
## Hybrid ELT Data Pipeline, Fast Analytics, Indexes, Materialized Views & Unified FastAPI
### مشروع مقرر البيانات الضخمة - المرحلة الثانية والنهائية (Phase 2 / Final Project)
**إشراف المهندس: عمر أبوسند**  
**تنفيذ المهندس: عتيق غنام**

---

## 📑 جدول المحتويات (Table of Contents)
1. [نظرة عامة على المشروع (Project Overview)](#1-نظرة-عامة-على-المشروع-project-overview)
2. [المتطلبات الأساسية (Prerequisites & Requirements)](#2-المتطلبات-الأساسية-prerequisites--requirements)
3. [خطوات التثبيت والإعداد (Installation & Setup)](#3-خطوات-التثبيت-والإعداد-installation--setup)
4. [متغيرات البيئة (Environment Variables)](#4-متغيرات-البيئة-environment-variables)
5. [إعداد قاعدة البيانات (Database Setup & Indexes)](#5-إعداد-قاعدة-البيانات-database-setup--indexes)
6. [تشغيل خط بيانات المرحلة الأولى (Running ELT Pipeline)](#6-تشغيل-خط-بيانات-المرحلة-الأولى-running-elt-pipeline)
7. [تشغيل واجهة التطبيقات الموحدة (Running FastAPI Server)](#7-تشغيل-واجهة-التطبيقات-الموحدة-running-fastapi-server)
8. [التوثيق التفاعلي وسواجر (Swagger UI - /docs)](#8-التوثيق-التفاعلي-وسواجر-swagger-ui---docs)
9. [الاستعلامات الخمسة (Queries Implementation)](#9-الاستعلامات-الخمسة-queries-implementation)
10. [استراتيجية الفهارس (Indexes Strategy)](#10-استراتيجية-الفهارس-indexes-strategy)
11. [إحصائيات التنفيذ الحقيقية (Explain & ExecutionStats Benchmark)](#11-إحصائيات-التنفيذ-الحقيقية-explain--executionstats-benchmark)
12. [تقارير التجميع الخمسة (Aggregation Reports)](#12-تقارير-التجميع-الخمسة-aggregation-reports)
13. [الجداول المجمعة مسبقاً (Materialized Views)](#13-الجداول-المجمعة-مسبقاً-materialized-views)
14. [آلية التحديث التزايدي (Incremental Refresh Mechanism)](#14-آلية-التحديث-التزايدي-incremental-refresh-mechanism)
15. [المهام المجدولة (Scheduled Jobs)](#15-المهام-المجدولة-scheduled-jobs)
16. [التشغيل اليدوي للمهام (Manual Job Execution & Logging)](#16-التشغيل-اليدوي-للمهام-manual-job-execution--logging)
17. [مرجع مسارات واجهة البرمجة (API Endpoints Reference)](#17-مرجع-مسارات-واجهة-البرمجة-api-endpoints-reference)
18. [دليل الاختبارات وضمان الجودة (Automated Testing & QA)](#18-دليل-الاختبارات-وضمان-الجودة-automated-testing--qa)
19. [الهيكل المعماري للمشروع (Project Structure)](#19-الهيكل-المعماري-للمشروع-project-structure)
20. [قابلية التشغيل ببيانات مختلفة (Different Dataset Support)](#20-قابلية-التشغيل-ببيانات-مختلفة-different-dataset-support)
21. [قائمة التحقق المكتملة (Requirements Verification Checklist)](#21-قائمة-التحقق-المكتملة-requirements-verification-checklist)

---

## 1. نظرة عامة على المشروع (Project Overview)

يمثل هذا المشروع نظاماً هندسياً متكاملاً لمعالجة وتحليل واستعلام البيانات الضخمة (Big Data) لمتجر إلكتروني يمني. يقوم النظام بالربط المحكم بين:
- **المشروع النصفي (Phase 1)**: خط بيانات هجين بنمط ELT يستقبل البيانات غير النظيفة، يوجهها تلقائياً بحسب الحجم (Python Batch Streaming للملفات $\le 200\text{ MB}$، وApache Spark للملفات الأكبر)، يطبق 9 قواعد تنظيف مع Audit Trail وحفظ السجلات السليمة بالـ Idempotent Upsert وعزل السجلات التالفة في Quarantine دون أي فقدان للبيانات.
- **المشروع النهائي (Phase 2)**: منصة استعلام متقدمة فوق MongoDB مدعومة بفهارس مركبة ومصممة بدقة، تقارير Aggregations لحظية، جداول مجسدة (Materialized Views) تعتمد على آلية التحديث التزايدي (Incremental Refresh) لتجنب إعادة حساب ملايين السجلات في كل دورة، مهام مجدولة آلية (Scheduled Jobs) تسجل تاريخ التشغيل في قاعدة البيانات، وواجهة برمجية موحدة عبر FastAPI مدعومة بتوثيق Swagger تفاعلي واختبارات آلية شاملة.

قاعدة البيانات تحتوي بالفعل على أكثر من **27 مليون سجل خام**، و**7.78 مليون سجل مفحوص وسليم**، و**960 ألف سجل معزول**، مما أتاح إجراء قياسات أداء حقيقية وغير نظرية.

---

## 2. المتطلبات الأساسية (Prerequisites & Requirements)

- **نظام التشغيل**: Linux / Ubuntu (أو أي توزيعة تدعم Python 3.10+)
- **Python**: الإصدار `3.12+` (أو `3.10+`)
- **MongoDB**: الإصدار `7.x` أو `8.x` يعمل على المنفذ `27017`
- **Java**: OpenJDK 11 أو 17 (لتشغيل محرك Apache Spark)

---

## 3. خطوات التثبيت والإعداد (Installation & Setup)

1. **استنساخ المستودع والدخول للمجلد**:
   ```bash
   cd /home/ateeq/Desktop/proggime/programs_python/Big_Data/midetarm
   ```

2. **إنشاء وتفعيل البيئة الافتراضية**:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

3. **تثبيت المتطلبات البرمجية**:
   ```bash
   pip install -r requirements.txt
   ```

4. **تشغيل خدمة MongoDB**:
   ```bash
   # تشغيل خادم مونجو باستخدام مجلد البيانات المحلي
   TCMALLOC_PER_CPU_CACHES=0 mongod --dbpath data/db_test --bind_ip 127.0.0.1 --logpath logs/mongod.log --fork
   ```

### ⚡ التشغيل والاختبار الكامل بنقرة واحدة (1-Click Master Runner)
لتشغيل واختبار المشروع بالكامل وفحص كافة المتطلبات وطباعة مصفوفة التحقق الأكاديمي الشاملة بأمر واحد:
```bash
./run_all.sh
```
أو عبر بايثون مباشرة:
```bash
./venv/bin/python run_and_verify.py
```
يقوم هذا السكربت آلياً بـ:
1. التحقق من البيئة والاتصال بقاعدة بيانات MongoDB.
2. تشغيل خادم FastAPI في الخلفية والتأكد من استجابته على المنفذ `8000`.
3. تشغيل كافة الاختبارات الآلية (54 اختباراً ناجحاً في pytest).
4. اختبار جميع مسارات API المباشرة (الاستعلامات، الفهارس، التجميعات، المهام المجدولة، وخط الإدخال).
5. إجراء قياسات Explain الحقيقية ومقارنة COLLSCAN بـ IXSCAN.
6. إجراء اختبار التحديث التزايدي الحي (Incremental Refresh) وحقن سجل تجريبي والتحقق من تحديث الأجزاء المتأثرة فقط.
7. طباعة مصفوفة التحقق والتقييم النهائي الملونة.

---

## 4. متغيرات البيئة (Environment Variables)

تم توفير ملف نموذجي `env.example`. لتهيئة البيئة:
```bash
cp env.example .env
```
محتوى المتغيرات في `env.example`:
```ini
# MongoDB Connection Configuration
MONGO_URI=mongodb://localhost:27017
MONGO_DB_NAME=midterm_db

# Engine Routing Configuration
SMALL_FILE_THRESHOLD_MB=200.0
BATCH_SIZE=5000

# Apache Spark Configuration
SPARK_MASTER=local[*]
SPARK_APP_NAME=MidtermDataPipeline

# FastAPI Unified Server Configuration
API_HOST=0.0.0.0
API_PORT=8000
```
> [!NOTE]
> لا يحتوي الملف على أي كلمات مرور أو مفاتيح سرية، ويعتمد على المنافذ القياسية.

---

## 5. إعداد قاعدة البيانات (Database Setup & Indexes)

لإعداد المجموعات الأساسية، الفهارس الفريدة، وفهارس المرحلة الثانية برمجياً:
```bash
# إعداد مجموعات وقواعد المرحلة الأولى
./venv/bin/python src/mongo_setup.py

# إنشاء وفحص فهارس المرحلة الثانية
./venv/bin/python -c "from src.indexes.index_manager import create_phase2_indexes; print(create_phase2_indexes())"
```

---

## 6. تشغيل خط بيانات المرحلة الأولى (Running ELT Pipeline)

خط البيانات النصفي لم يتغير ومحافظ عليه بنسبة 100%:

- **تشغيل مجلد البيانات بالكامل**:
  ```bash
  ./venv/bin/python src/main.py --input data/
  ```
- **تشغيل عينة صغيرة (Python Batch Streaming)**:
  ```bash
  ./venv/bin/python src/main.py --input data/sample_orders.csv
  ```
- **تشغيل الوضع التزايدي (Incremental Path B with Version Handling)**:
  ```bash
  ./venv/bin/python src/main.py --input data/sample_orders.csv --incremental
  ```

---

## 7. تشغيل واجهة التطبيقات الموحدة (Running FastAPI Server)

لتشغيل خادم API الموحد:
```bash
./venv/bin/python -m uvicorn src.api.app:app --host 0.0.0.0 --port 8000 --reload
```
عند بدء التشغيل، يقوم الخادم تلقائياً بتهيئة المجدول الخلفي للمهام (`APScheduler`) ليعمل بالتوازي وبصورة آمنة.

---

## 8. التوثيق التفاعلي وسواجر (Swagger UI - /docs)

بمجرد تشغيل الخادم، يتوفر التوثيق التفاعلي الكامل عبر المتصفح:
- **Swagger UI**: [http://localhost:8000/docs](http://localhost:8000/docs)
- **ReDoc UI**: [http://localhost:8000/redoc](http://localhost:8000/redoc)

جميع الـ Endpoints ظاهرة، موثقة بالأنواع، وقابلة للاختبار المباشر (Try it out).

---

## 9. الاستعلامات الخمسة (Queries Implementation)

تم تصميم وبناء 5 استعلامات عملية تلبي المتطلبات التشغيلية والتجارية للمتجر الإلكتروني:

| رقم الاستعلام | الاسم البرمجي | الوصف التجاري والتقني | المعاملات (Parameters) | الفهرس الخادم |
|---|---|---|---|---|
| **Query 1** | `orders_by_customer` | استعلام سجل طلبات عميل محدد مرتباً زمنياً من الأحدث إلى الأقدم | `customer_id`, `limit` | `idx_customer_id` |
| **Query 2** | `orders_by_city_and_status` | استعلام لوجستي لفرز وتوزيع طلبات التوصيل حسب المدينة والحالة التشغيلية | `city`, `status`, `limit` | `idx_city_status` (Compound) |
| **Query 3** | `quarantine_records_by_error` | استعلام مهندسي البيانات لفحص السجلات المعزولة حسب كود الخطأ | `error_code`, `limit` | `idx_quarantine_error_codes` (Multikey) |
| **Query 4** | `recent_orders_by_date_range` | استعلام تدقيق زمني لجلب الطلبات المحصورة بين تاريخين محددين | `start_date`, `end_date`, `limit` | `idx_version_date` |
| **Query 5** | `high_value_orders_by_delivery` | استعلام إدارة المخاطر والمالية للطلبات ذات القيمة العالية حسب نوع التوصيل | `delivery_type`, `min_amount`, `limit` | `idx_delivery_type` |

**تشغيل الاستعلامات برمجياً**:
```bash
./venv/bin/python -c "from src.queries.order_queries import execute_query; print(execute_query('orders_by_customer', {'customer_id': 'عميل-0', 'limit': 3}))"
```

---

## 10. استراتيجية الفهارس (Indexes Strategy)

تم إنشاء 3 فهارس رئيسية تخدم الاستعلامات السابقة بشكل مباشر، مع مراعاة الحقول وترتيبها:

### الفهرس الأول: Single Field Index على `customer_id`
- **المجموعة**: `orders_validated`
- **المفتاح**: `{"customer_id": 1}`
- **الاسم**: `idx_customer_id`
- **السبب**: استعلام سجل العميل من أكثر الاستعلامات تكراراً في التجارة الإلكترونية. دونه، يضطر النظام لمسح 7.78 مليون مستند للبحث عن سجلات عميل واحد.

### الفهرس الثاني: Multikey Index على مصفوفة `error_codes`
- **المجموعة**: `orders_quarantine`
- **المفتاح**: `{"error_codes": 1}`
- **الاسم**: `idx_quarantine_error_codes`
- **السبب**: حقل `error_codes` عبارة عن مصفوفة (Array). يتيح الفهرس متعدد المفاتيح استهداف الأخطاء المحددة (مثل `CORRUPTED_ITEMS_JSON`) مباشرة دون فحص عناصر المصفوفة لكل مستند معزول.

### الفهرس الثالث: Compound Index مركب على `(city, status)`
- **المجموعة**: `orders_validated`
- **المفتاح**: `{"city": 1, "status": 1}`
- **الاسم**: `idx_city_status`
- **السبب وترتيب الحقول**: طبقنا قاعدة Equality-Sort-Range (ESR). حقل `city` يمتلك تباينية أعلى (Cardinality) ومطابقة تساوي، وحقل `status` يضيق النطاق للحالة المطلوبة (مثل الطلبات المؤكدة في صنعاء)، مما يتيح لمحرك MongoDB القفز مباشرة لمطابقة الشرطين معاً.

---

## 11. إحصائيات التنفيذ الحقيقية (Explain & ExecutionStats Benchmark)

تم قياس الأداء الحقيقي بالأرقام الدقيقة عبر `explain("executionStats")` قبل وبعد إنشاء الفهارس على قاعدة البيانات الفعلية (7.78 مليون سجل):

```
+-----------------------------------------------------------------------------------------+
|                  EXPLAIN EXECUTIONSTATS BENCHMARK (REAL DATA METRICS)                   |
+----------------------+--------------------+--------------------+------------------------+
| Query Name           | Metric             | Before Index       | After Index            |
+----------------------+--------------------+--------------------+------------------------+
| Query 1              | Scan Type          | COLLSCAN           | IXSCAN                 |
| (Orders by Customer) | executionTime      | 3,873 ms           | 1 ms (99.97% drop!)    |
|                      | totalDocsExamined  | 7,780,451 docs     | 1 doc                  |
|                      | totalKeysExamined  | 0 keys             | 1 key                  |
+----------------------+--------------------+--------------------+------------------------+
| Query 2              | Scan Type          | COLLSCAN           | IXSCAN                 |
| (City + Status)      | executionTime      | 15 ms              | 0 ms                   |
|                      | totalDocsExamined  | 2,260 docs         | 100 docs               |
|                      | totalKeysExamined  | 0 keys             | 100 keys               |
+----------------------+--------------------+--------------------+------------------------+
| Query 3              | Scan Type          | COLLSCAN           | IXSCAN                 |
| (Quarantine Errors)  | executionTime      | 35 ms              | 2 ms                   |
|                      | totalDocsExamined  | 1,850 docs         | 50 docs                |
|                      | totalKeysExamined  | 0 keys             | 50 keys                |
+----------------------+--------------------+--------------------+------------------------+
```

لإعادة تشغيل المقارنة وتوليد التقرير المحدث:
```bash
./venv/bin/python -c "from src.explain.explain_runner import run_single_explain, BENCHMARK_QUERIES; print([run_single_explain(q['collection'], q['filter'], limit=q['limit']) for q in BENCHMARK_QUERIES])"
```
الملف الموثق الكامل متاح في: [`reports/index_strategy.md`](file:///home/ateeq/Desktop/proggime/programs_python/Big_Data/midetarm/reports/index_strategy.md).

---

## 12. تقارير التجميع الخمسة (Aggregation Reports)

تم بناء 5 دوال تجميع مستقلة ومبنية على مسارات (Pipelines) تقرأ البيانات الحقيقية من MongoDB مع دعم التحويل الديناميكي للأنواع (`$toDouble`) وتفعيل `allowDiskUse=True`:

1. **`sales_by_city`**: تقرير الإيرادات، عدد الطلبات، ومتوسط قيمة الطلب مجمعة حسب المدينة ومرتبة تنازلياً بالإيراد.
2. **`top_customers`**: تقرير كبار العملاء مرتبين بإجمالي المبالغ المنفقة وعدد طلباتهم ومتوسط قيمة الشراء.
3. **`orders_by_status`**: التوزيع المالي والكمي للطلبات حسب حالتها (مؤكد، قيد الانتظار، ملغي، مرتجع، توصيل).
4. **`sales_by_period`**: تحليل الاتجاه الشهري للمبيعات وحجم الطلبات مشتقاً من `order_date` (صيغة `YYYY-MM`).
5. **`payment_and_delivery_breakdown`**: تقرير تقاطع وسائل الدفع مع أنواع التوصيل وإجمالي تكاليف الشحن.

**تشغيل تقرير تجميعي برمجياً**:
```bash
./venv/bin/python -c "from src.aggregations.reports import execute_aggregation; print(execute_aggregation('sales_by_city', {'limit': 3}))"
```

---

## 13. الجداول المجمعة مسبقاً (Materialized Views)

نظراً لأن تنفيذ Aggregation Pipeline على 7.78 مليون سجل يستغرق بين 13 إلى 45 ثانية، تم إنشاء جدولين مجمعين مسبقاً (Materialized Views):

1. **`daily_sales_summary`** (مجموعة `mv_daily_sales_summary`):
   - **المفتاح الفريد**: `(date, city)`
   - **المقاييس**: إجمالي الإيراد اليومي للمدينة، عدد الطلبات، متوسط قيمة الطلب، وتكاليف الشحن.
   - **زمن الاستعلام**: **~3.5 مللي ثانية** بدلاً من 13 ثانية!
2. **`city_performance_summary`** (مجموعة `mv_city_performance_summary`):
   - **المفتاح الفريد**: `city`
   - **المقاييس**: مؤشرات الأداء التراكمي الشامل للمدن.
   - **زمن الاستعلام**: **~3.3 مللي ثانية**!

---

## 14. آلية التحديث التزايدي (Incremental Refresh Mechanism)

تعتمد آلية التحديث التزايدي على تكامل مباشر مع خط البيانات النصفي دون إعادة بناء الجدول بالكامل:
1. **نقطة القياس المائية (Watermark Tracking)**: يتم حفظ تاريخ آخر مزامنة ناجحة في مجموعة خاصة `mv_metadata`.
2. **اكتشاف السجلات الجديدة**: عند استدعاء `refresh_materialized_views(mode="incremental")`، يتم الاستعلام فقط عن المستندات في `orders_validated` التي تمتلك `ingested_at > last_synced_at`.
3. **الأمان عند عدم وجود بيانات جديدة**: إذا لم يُكتشف أي سجل جديد، تعود الدالة فوراً بحالة `up_to_date` بزمن قدره بضع أجزاء من الثانية دون أي معالجة إضافية.
4. **التحديث الدقيق للأجزاء المتأثرة فقط (Selective Partition Update)**: عند وجود بيانات جديدة، يتم استخراج المدن والتواريخ المتأثرة فقط (`affected_partitions`)، وإعادة حساب التجميع لتلك التواريخ والمدن المحددة فقط، ثم تحديثها بـ `bulk upsert` داخل المجموعات المادية وتحديث الـ Watermark.

---

## 15. المهام المجدولة (Scheduled Jobs)

تم تطبيق نظام جدولة خلفي مبني على `APScheduler` مع تسجيل دوري لحالة التنفيذ:

| اسم المهمة (Job Name) | الجدول الزمني (Schedule) | الوظيفة الفعلية |
|---|---|---|
| `refresh_materialized_views_job` | كل 15 دقيقة (`*/15 * * * *`) | استدعاء التحديث التزايدي للـ Materialized Views بناءً على الـ Watermark |
| `daily_data_health_and_quarantine_audit_job` | يومياً الساعة 02:00 صباحاً (`0 2 * * *`) | تدقيق صحة النظام، مقارنة أحجام المجموعات، فرز أسباب العزل وحفظها في `reports/daily_health_report.json` |

---

## 16. التشغيل اليدوي للمهام (Manual Job Execution & Logging)

يمكن تشغيل أي مهمة مجدولة يدوياً وفورياً برمجياً أو عبر الـ API:

```bash
# تشغيل يدوي لمهمة التدقيق الصحي
./venv/bin/python -c "from src.jobs.job_runner import run_job; print(run_job('daily_data_health_and_quarantine_audit_job'))"

# تشغيل يدوي لمهمة تحديث الجداول المادية
./venv/bin/python -c "from src.jobs.job_runner import run_job; print(run_job('refresh_materialized_views_job'))"
```

كل عملية تشغيل (آلية أو يدوية) تسجل تلقائياً في مجموعة `job_execution_logs` الحقول التالية:
- `job_name`: اسم المهمة.
- `started_at`: وقت البدء بدقة الميكروثانية.
- `finished_at`: وقت الانتهاء.
- `status`: حالة التنفيذ (`success` أو `failed`).
- `duration_seconds`: الزمن المستغرق بالثواني.
- `records_processed`: عدد السجلات المعالجة.
- `error`: رسالة الخطأ (إن وجدت) أو `None`.
- `details`: ملخص المخرجات التفصيلية.

---

## 17. مرجع مسارات واجهة البرمجة (API Endpoints Reference)

| الطريقة | المسار (Endpoint) | الوصف | مثال الاستجابة |
|---|---|---|---|
| `GET` | `/health` | فحص صحة النظام والاتصال بقاعدة البيانات | `{"status": "ok", "database": "connected", ...}` |
| `POST` | `/ingest` | تشغيل خط إدخال البيانات النصفي الأصلي دون تكرار | `{"status": "success", "metrics": {...}}` |
| `GET` | `/indexes` | استعراض قائمة الفهارس ومواصفاتها | `{"indexes": [...]}` |
| `POST` | `/indexes` | التحقق من الفهارس وإنشائها بصورة Idempotent | `{"status": "success", "created_count": 0, "existing_count": 3}` |
| `GET` | `/queries` | استرجاع قائمة الاستعلامات المتاحة ومعاملاتها | `{"queries": ["orders_by_customer", ...]}` |
| `GET` | `/queries/{name}` | تشغيل استعلام بالاسم وتمرير معاملات اختيارية | `{"status": "success", "count": 50, "results": [...]}` |
| `GET` | `/aggregations` | استرجاع قائمة تقارير التجميع المتاحة | `{"aggregations": ["sales_by_city", ...]}` |
| `GET` | `/aggregations/{name}` | تشغيل تقرير تجميعي محدد بالاسم | `{"status": "success", "count": 20, "results": [...]}` |
| `POST` | `/refresh-mv` | تحديث الـ Materialized Views تزايدياً | `{"status": "up_to_date", "new_records_detected": 0}` |
| `GET` | `/materialized-views` | استعراض قائمة الجداول المجمعة مسبقاً | `{"materialized_views": [...]}` |
| `GET` | `/materialized-views/{name}` | استعلام سريع فوري من الجداول المادية | `{"view_name": "daily_sales_summary", "data": [...]}` |
| `GET` | `/jobs` | استرجاع المهام المجدولة وحالة آخر تشغيل | `{"jobs": [{"job_name": "...", "schedule": "..."}]}` |
| `POST` | `/jobs/{name}/run` | تشغيل مهمة مجدولة يدوياً وتوثيق سجلها | `{"status": "success", "execution": {...}}` |

---

## 18. دليل الاختبارات وضمان الجودة (Automated Testing & QA)

تم بناء حزمة اختبارات شاملة تضم **54 اختباراً آلياً** باستخدام `pytest`:
- **25 اختباراً للمشروع النصفي**: تغطي قواعد التنظيف التسع، العزل، وتأكيد الـ Idempotency وVersion Protection.
- **29 اختباراً للمشروع النهائي**: تغطي الاستعلامات الخمسة، الفهارس، إحصائيات IXSCAN، الجداول المادية والتحديث التزايدي، المهام المجدولة وفحص معالجة الفشل الآمنة والتوثيق (Failure Handling & Logging)، وكافة مسارات FastAPI مع معالجة الأخطاء وحالات الـ 404 والـ 400.

**تشغيل كافة الاختبارات**:
```bash
./venv/bin/pytest tests/ -v
```
**النتيجة**:
```
======================= 54 passed, 5 warnings in 13.92s ========================
```

---

## 19. الهيكل المعماري للمشروع (Project Structure)

```
midterm-data-pipeline/
├── README.md                      # دليل التوثيق والتشغيل الشامل
├── requirements.txt               # المكتبات البرمجية للمشروع كاملاً
├── env.example                    # نموذج متغيرات البيئة الخالي من الأسرار
├── pytest.ini                     # إعدادات pytest ومسارات الاستيراد
├── config/
│   └── settings.py                # إعدادات المنظومة وحدود المحركات ومونجو
├── data/
│   ├── sample_orders.csv          # عينة بيانات صغيرة للاختبار السريع
│   ├── orders_1m.csv              # ملف مليون سجل
│   └── db_test/                   # مجلد بيانات MongoDB الفعلي (7.78M مستند)
├── src/
│   ├── main.py                    # نقطة تشغيل خط بيانات المرحلة الأولى
│   ├── file_router.py             # توجيه الملفات للمحرك المناسب
│   ├── batch_loader.py            # محرك التحميل الدفعي Python Batch Streaming
│   ├── spark_loader.py            # محرك التحميل الموزع PySpark
│   ├── elt_pipeline.py            # منسق مسار ELT والتنظيف والتصنيف
│   ├── mongo_setup.py             # تهيئة مجموعات وفهارس مونجو الأساسية
│   ├── upsert_writer.py           # كتابة السجالت بالـ Idempotent Upsert
│   ├── incremental_loader.py      # إدارة التحميل التزايدي للطلبات (Path B)
│   ├── metrics.py                 # جمع وحفظ مؤشرات الأداء في results.json
│   ├── cleaning/                  # قواعد التنظيف التسع وسجل القواعد
│   ├── validation/                # مصنفات السجلات وحالات العزل
│   ├── utils/
│   │   └── json_encoder.py        # محول BSON types إلى JSON قياسي
│   ├── queries/                   # الاستعلامات الخمسة
│   │   ├── __init__.py
│   │   └── order_queries.py
│   ├── indexes/                   # إدارة وإنشاء الفهارس
│   │   ├── __init__.py
│   │   └── index_manager.py
│   ├── explain/                   # مقارنة executionStats وتوثيق الأثر
│   │   ├── __init__.py
│   │   └── explain_runner.py
│   ├── aggregations/              # تقارير التجميع الخمسة
│   │   ├── __init__.py
│   │   └── reports.py
│   ├── materialized_views/        # الجداول المادية والتحديث التزايدي
│   │   ├── __init__.py
│   │   └── mv_manager.py
│   ├── jobs/                      # المهام المجدولة وسجلات التنفيذ
│   │   ├── __init__.py
│   │   ├── job_definitions.py
│   │   └── job_runner.py
│   └── api/                       # خادم FastAPI الموحد والمسارات
│       ├── __init__.py
│       ├── app.py
│       └── routes/
│           ├── health.py
│           ├── ingest.py
│           ├── indexes.py
│           ├── queries.py
│           ├── aggregations.py
│           ├── materialized_views.py
│           └── jobs.py
├── tests/                         # الاختبارات الآلية (54 اختباراً)
│   ├── test_cleaning_rules.py
│   ├── test_classification.py
│   ├── test_upsert_idempotency.py
│   ├── test_phase2_queries.py
│   ├── test_phase2_indexes.py
│   ├── test_phase2_materialized_views.py
│   ├── test_phase2_jobs.py
│   └── test_phase2_api.py
└── reports/
    ├── results.json               # مقاييس تشغيل خط البيانات
    ├── index_benchmarks.json      # بيانات مقارنة الفهارس الفعلية
    ├── index_strategy.md          # تقرير استراتيجية الفهارس ومقارنة Explain
    └── daily_health_report.json   # تقرير فحص صحة البيانات اليومي
```

---

## 20. قابلية التشغيل ببيانات مختلفة (Different Dataset Support)

النظام مصمم هندسياً ليعمل على أي ملف بيانات طلبات يحمل المخطط العام (`Schema`)، ولا يعتمد إطلاقاً على أي افتراضات خاصة ببيانات التدريب:
- **لا توجد أسماء ملفات ثابتة**: يقبل المسار `--input` أي ملف أو مجلد جديد ديناميكياً.
- **لا توجد أرقام أو كميات ثابتة**: كافة العمليات تعتمد على دوال التجميع (`$sum`, `$avg`, `$count`) والحساب الفعلي من محتويات قاعدة البيانات.
- **لا يوجد عميل أو مدينة ثابتة في منطق المعالجة**: جميع الفلاتر تمرر كمعاملات اختيارية وتتكيف تلقائياً مع القيم الواردة في السجلات.
- **معالجة تزايدية مرنة**: تعتمد آلية التحديث على طوابع زمنية حقيقية (`ingested_at`) وتتعامل مع أي مجموعة بيانات واردة بدقة تامة.

---

## 21. قائمة التحقق المكتملة (Requirements Verification Checklist)

| البند المطلوب (Requirement) | الحالة | آلية التحقق والدليل الفعلي |
|---|:---:|---|
| **5 Practical Queries** | ✅ | منفذة في `src/queries/order_queries.py` وتعمل بـ 4ms |
| **3 Useful Indexes** | ✅ | منفذة ومسجلة في MongoDB (`idx_customer_id`, `idx_quarantine_error_codes`, `idx_city_status`) |
| **Compound Index** | ✅ | منفذ على `(city: 1, status: 1)` ويخدم Query 2 |
| **3 executionStats (Before/After)** | ✅ | موثقة في `reports/index_strategy.md` ومثبت انخفاض المسح من 7.78M إلى 1 وثيقة |
| **5 Aggregations Reports** | ✅ | منفذة في `src/aggregations/reports.py` ومجربة على 7.78M مستند |
| **2 Materialized Views** | ✅ | منشأة ومفهرسة في `mv_daily_sales_summary` و `mv_city_performance_summary` |
| **Incremental Refresh** | ✅ | آلية تعتمد على Watermark وتحدث فقط الأجزاء المتأثرة دون إعادة حساب الكل |
| **Safe Refresh (No New Data)** | ✅ | تم اختباره ويعود بـ `up_to_date` في 3 ثوانٍ بدون لمس البيانات |
| **2 Scheduled Jobs** | ✅ | مهمة التحديث التزايدي ومهمة التدقيق اليومي تعمل عبر `APScheduler` |
| **Manual Job Run** | ✅ | دالة ومسار `POST /jobs/{name}/run` ينفذ المهمة فورياً |
| **Execution Logging** | ✅ | تسجل الحقول في `job_execution_logs` بنجاح |
| **FastAPI Unified API** | ✅ | خادم متكامل يضم كافة المسارات المطلوبة في `src/api/` |
| **Swagger UI (/docs)** | ✅ | يعمل على المنفذ `8000` ويعرض كافة المسارات باختبار تفاعلي |
| **requirements.txt** | ✅ | محدث وشامل لكافة التبعيات دون حزم غير مستخدمة |
| **env.example** | ✅ | نموذج نظيف تماماً وخالٍ من كلمات المرور أو الأسرار |
| **README.md** | ✅ | توثيق هندسي شامل لجميع النقاط الـ 21 المطلوبة |
| **Different Dataset Support** | ✅ | كود ديناميكي مبني على المخطط والبيانات الحقيقية دون أي Hardcoding |
| **Midterm Integrity Preserved** | ✅ | كافة ملفات واختبارات المرحلة الأولى تعمل بنسبة 100% (54/54 tests passed) |

---
**تم بحمد الله وتوفيقه إنجاز متطلبات المشروع النهائي بالكامل وفق أعلى معايير الجودة وهندسة البيانات الضخمة.**
