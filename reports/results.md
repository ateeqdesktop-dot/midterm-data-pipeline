# تقرير نتائج Midterm Data Pipeline

## ملخص التنفيذ

تم تنفيذ خط بيانات هجين لطلبات متجر إلكتروني غير نظيفة. يقرأ المسار الأول CSV بطريقة Streaming/Batch في Python ويستخدم `insert_many` للطبقة الخام، بينما يقرأ المسار الثاني بالـPySpark عبر DataFrame وFixed String Schema وPartitions، ثم يطبق Transform/Quality ويكتب إلى MongoDB Spark Connector عند استخدام MongoDB الحقيقي. لا يحذف النظام السجلات المرفوضة؛ يحفظها في Quarantine مع الأكواد والتفاصيل و`raw_record`.

## بيئة التشغيل المثبتة والمتحقق منها

| المكوّن | الحالة |
|---|---|
| Python | 3.12.3 |
| Java | 21 |
| PySpark | 4.0.1 |
| MongoDB Server | 8.0.31 محليًا على `127.0.0.1:27017` |
| mongosh | 2.10.0 |
| MongoDB Spark Connector | 10.5.0، تم تنزيله وتشغيله عبر Ivy أثناء اختبار Spark |
| pytest/pytest-cov | مثبتان ومستخدمان |
| Ruff | مثبت ومستخدم في CI والمحلي |
| Docker | غير متوفر في بيئة التنفيذ؛ Compose موفر ولم يُدّعَ تشغيله |

## Python Batch + MongoDB الحقيقي

الأمر المستخدم كان:

```bash
PYTHONPATH=src python3 src/main.py \
  --input data/sample_orders.csv --backend mongo --engine python_batch \
  --batch-size 2 --mongo-uri mongodb://127.0.0.1:27017 \
  --mongo-database midterm_orders --reports reports/results.json \
  --check-idempotency
```

النتيجة الفعلية للعينة ذات السبعة سجلات كانت:

| المقياس | القيمة |
|---|---:|
| `rows_read` | 7 |
| `raw_loaded` | 7 |
| `valid_count` | 0 |
| `corrected_count` | 3 |
| `quarantine_count` | 4 |
| `orders_raw` بعد أول run | 7 |
| `orders_validated` | 3 |
| `orders_quarantine` | 4 |
| `batch_size` | 2 |
| `batches` | 4 |
| `throughput` | 412.770 rows/sec في ذلك التشغيل |
| `idempotency.passed` | true |
| `after_validated` في replay | 3 |
| `unchanged` في replay | 3 |

بعد إعادة نفس الملف، أصبح عدد Raw هو 14 لأن Raw طبقة تاريخية تحتفظ بمحاولة كل `run_id`، بينما بقيت الحالة التجارية النهائية بلا تكرار: 3 سجلات Validated و4 سجلات Quarantine. هذا يطابق سياسة PDF التي تجعل Idempotency مرتبطة بالحالة التجارية النهائية لا بحذف تاريخ Raw.

## PySpark + MongoDB Spark Connector الحقيقي

الأمر المستخدم كان:

```bash
PYTHONPATH=src python3 src/main.py \
  --input data/sample_orders.csv --backend mongo --engine pyspark \
  --spark-master 'local[2]' --partitions 2 \
  --mongo-uri mongodb://127.0.0.1:27017 \
  --mongo-database midterm_orders --reports reports/spark-mongo-results.json
```

نجح المسار في إنشاء SparkSession، قراءة CSV بـString Schema، تشغيل DataFrame على partitionين، تطبيق quality transformation، ثم الكتابة الفعلية إلى MongoDB عبر Connector. النتائج كانت Raw 7 وValidated 3 وQuarantine 4، مع اكتشاف `MISSING_CUSTOMER_ID`, `DUPLICATE_ORDER_ID`, `EMPTY_ITEMS`, و`MULTIPLE_CONFLICTING_ERRORS`. الفهارس التي ظهرت في MongoDB شملت `uq_order_id` و`uq_quarantine_source_row`.

وأُعيد تشغيل Spark + Mongo على نفس الملف مع `--check-idempotency`. بقيت `orders_validated=3` و`orders_quarantine=4`، وارتفع Raw إلى 14 فقط باعتباره سجلًا تاريخيًا لمحاولتي التشغيل.

## مقارنة Python Batch وPySpark

| جانب المقارنة | Python Batch | PySpark |
|---|---|---|
| القرار | مناسب للملفات تحت threshold | مناسب للملفات الأكبر أو عند فرض المحرك |
| قراءة CSV | `csv.DictReader` Streaming | DataFrame مع Fixed String Schema |
| الذاكرة | Raw/Valid/Quarantine بحجم batch فقط | المعالجة عبر partitions و`mapPartitions` |
| الكتابة | `insert_many` للـRaw وUpsert للمخرجات | MongoDB Spark Connector بكتابة partitioned وupsert/replace |
| إعداد التشغيل المثبت | `batch_size=2` في الاختبار | `local[2]`, `partitions=2` |
| زمن العينة | نحو 0.017 ثانية في تشغيل Mongo Python | نحو 19.3 ثانية في تشغيل Spark/Mongo؛ يتضمن بدء Spark |
| النتيجة التجارية | 3 Validated و4 Quarantine | 3 Validated و4 Quarantine |

زمن PySpark أعلى على العينة الصغيرة بسبب كلفة بدء Spark؛ وهذا هو سبب استخدام Router لمسار Python عندما يكون الملف دون threshold. المقارنة ليست benchmark لمليون سجل أو Cluster، لأن ذلك يتطلب بيانات/عقدًا منفصلة ولم يُدّعَ هنا.

## الاختبارات

```text
PYTHONPATH=src python3 -m pytest -q       -> 7 passed
ruff check src tests scripts              -> All checks passed
ruff format --check src tests scripts      -> All files formatted
python3 -m compileall -q src tests scripts -> passed
```

تغطي الاختبارات قواعد التنظيف وAudit Trail، تصنيف Quarantine، الاتساق، Idempotency، Router threshold، bounded batch behavior، الفهارس وQuarantine upsert، ومسار Incremental B. كما تم تشغيل Python وPySpark على MongoDB Server الحقيقي خارج اختبارات mongomock.

## مسار B الاختياري

ينفذ `src/incremental_loader.py` آلية `version` مع Initial Load ثم Delta تحتوي Insert وUpdate ثم Replay وStale version. أثبت `tests/test_incremental.py` أن Replay لا يكرر الأثر وأن الإصدار الأقدم لا يكتب فوق الإصدار الأحدث. مسار A، أي Spark Standalone على جهازين ومعالجة مليون سجل، غير مدعى لأن البيئة الحالية لا توفر جهازين منفصلين.

## القيود المعروفة

لم يتم تشغيل Docker/Compose لأن Docker daemon غير متوفر في بيئة التنفيذ؛ ملفات `Dockerfile` و`docker-compose.yml` موجودة للتشغيل على جهاز الدكتور. لم تُنشأ لقطات Compass أو Spark UI وهمية؛ يمكن التقاطها عند تشغيل MongoDB وSpark محليًا أو على Cluster. كما أن `local[2]` ليس Spark Cluster مستقلًا، لذلك لا يمثل Path A.
