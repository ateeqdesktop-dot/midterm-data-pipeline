# مصفوفة مطابقة متطلبات PDF

هذه المصفوفة تربط متطلبات التكليف الرسمية بالتنفيذ القابل للفحص داخل المستودع. نتائج التشغيل المشار إليها مأخوذة من تشغيل فعلي محلي على MongoDB Server وPySpark local mode، وليست ادعاءً بتشغيل Cluster على جهازين.

| بند PDF | المتطلب | التنفيذ والدليل | حالة التحقق |
|---|---|---|---|
| 6.1 | خط هجين Python Batch للملف الصغير وPySpark للملف الكبير | `src/file_router.py`, `src/batch_loader.py`, `src/spark_loader.py`, `src/main.py`؛ اختبارات `test_router_uses_threshold` وتشغيل Python وSpark موثق في `reports/results.md` | مكتمل محليًا |
| 6.2 | إدخال كل سجل إلى Raw قبل التنظيف | `batch_loader.py` يجمع Raw في دفعات ويستعمل `insert_many`؛ Spark يبني `raw_frame` ويكتبه بالConnector قبل المخرجات النهائية | مكتمل ومجرب |
| 6.3 | معالجة Streaming/Batch وعدم تحميل الملف كاملًا في Python | `batch_loader.py` يحتفظ بـ`raw_batch`, `valid_batch`, `quarantine_batch` فقط بحجم `batch_size`؛ `test_batch_loader_uses_bounded_insert_many_batches` يثبت `[2, 1]` | مكتمل |
| 6.4 | PySpark DataFrame وFixed String Schema وPartitions | `spark_loader.py` يستخدم `StructType(StringType)` للـraw، `repartition(partitions)` و`mapPartitions`؛ `scripts/inspect_spark_csv.py` يثبت قراءة JSON المقتبس | مكتمل ومجرب في `local[2]` |
| 6.5 | فصل Raw عن Transform/Quality ثم Validated/Quarantine | `quality_rules.py` منفصل عن loaders، و`mongo_setup.py` يدير collections؛ المساران يكتبان الطبقات الثلاث بالترتيب | مكتمل |
| 6.6 | قواعد التصحيح والتطبيع، ثماني قواعد أو أكثر | `quality_rules.py`: trim، أرقام عربية، عملة، كلمات سعر، أرقام، هاتف، بريد، تاريخ، مرادفات، وحساب total من items/shipping | مكتمل |
| 6.7 | Audit Trail للتصحيحات | `corrections` يحفظ `field`, `original_value`, `corrected_value`, `rule_code`؛ اختبارات `test_cleaning_rules.py` تتحقق من ذلك | مكتمل |
| 6.8 | Quarantine مع الأكواد الرسمية وraw record | الأكواد تشمل `MISSING_ORDER_ID`, `MISSING_CUSTOMER_ID`, `INVALID_IMPOSSIBLE_DATE`, `CORRUPTED_ITEMS_JSON`, `EMPTY_ITEMS`, `UNKNOWN_PRICE`, `AMBIGUOUS_NEGATIVE_VALUE`, `DUPLICATE_ORDER_ID`, `MULTIPLE_CONFLICTING_ERRORS`؛ `raw_record` محفوظ في المخرج المعزول | مكتمل على fixtures العينة |
| 6.9 | Collections وSchema Validation وUnique Index | `MongoRepository.setup()` ينشئ `orders_raw`, `orders_validated`, `orders_quarantine`، ويطبق validators في MongoDB الحقيقي وUnique Index باسم `uq_order_id`، وفهرس Quarantine idempotent | مجرب على MongoDB 8.0.31 |
| 6.10 | Stable Business Key وUpsert وIdempotency | `order_id` مفتاح validated؛ Python يستخدم upsert وSpark Connector يستخدم `operationType=replace`, `upsertDocument=true`; إعادة Python وSpark لنفس العينة حافظت على 3 validated و4 quarantine | مكتمل ومجرب |
| 6.11 | اتساق كل raw record إلى نتيجة واحدة | `reports/results.json` يحسب `consistency_check` من `raw_loaded = valid + corrected + quarantine`؛ التشغيل الفعلي أعطى `7 = 0 + 3 + 4` | مكتمل |
| 6.12 | المقاييس المطلوبة | `RunMetrics` و`reports/results.json` يحتويان run/file/engine/rows/raw/valid/corrected/quarantine/time/throughput/batch-or-partitions/errors/upsert counters | مكتمل |
| 9 | متطلبات الكود والتشغيل | أمر واحد موثق في README؛ الإعدادات قابلة للتمرير؛ `try/finally` لإغلاق Spark/Mongo؛ فصل loader/quality/repository؛ اختبارات وprogress output | مكتمل |
| 10 | سيناريو العرض | أوامر Python وSpark وMongo وIdempotency في README؛ أمثلة records وmetrics في التقارير؛ Spark UI/Compass يمكن عرضهما عند تشغيل المستخدم للخدمات | مكتمل، مع توضيح اللقطات |
| 11 | ملفات التسليم | README، requirements، architecture، results.md/json، tests، CI، Docker Compose، LICENSE، traceability، وملفات المصدر موجودة | مكتمل |
| 12 | معايير التقييم الأساسية | تغطية router، ELT، quality، Mongo، metrics، idempotency، upsert، والاختبارات موجودة في المشروع والتقرير | مكتمل |
| 13 | تشغيل/عرض المسار المتقدم | Path B الاختياري منفذ في `incremental_loader.py` مع Initial/Delta/Replay وversion handling؛ Path A موثق كقيد لأنه يحتاج جهازين منفصلين ومليون سجل | Path B مكتمل؛ Path A غير مدعى |
| 14 | النزاهة وقابلية إعادة الإنتاج | العينة مولدة/ثابتة، لا تعديل يدوي للبيانات، الأوامر قابلة للتكرار، الأسرار مستبعدة، وCI يشغل الاختبارات | مكتمل |

## أدلة التشغيل الأساسية

| الدليل | الأمر/الملف | النتيجة |
|---|---|---|
| Unit/integration tests | `PYTHONPATH=src python3 -m pytest -q` | 7 اختبارات ناجحة |
| Lint/format | `ruff check src tests scripts` و`ruff format --check src tests scripts` | بلا أخطاء |
| Python + MongoDB | `src/main.py --engine python_batch --backend mongo --check-idempotency` | Raw 7، Validated 3، Quarantine 4، Idempotent |
| PySpark + MongoDB | `src/main.py --engine pyspark --backend mongo --spark-master local[2]` | Raw 7، Validated 3، Quarantine 4، Connector write ناجح |
| Spark replay | نفس الأمر مع `--check-idempotency` | Validated بقي 3 وQuarantine بقي 4؛ Raw يحتفظ بتاريخ كل run كما ينص PDF |
| Path B | `tests/test_incremental.py` | Initial ثم Insert+Update ثم Replay وStale version بلا أثر زائد |

## القيود المعلنة

لم يُدّعَ تشغيل Spark Standalone Cluster على جهازين أو معالجة مليون سجل، لأن بيئة التنفيذ الحالية جهاز/حاوية واحدة؛ هذا المسار مطلوب للمجموعات فقط وليس شرطًا للطالب الفردي. كما أن MongoDB Compass واجهة رسومية خارج البيئة، لذلك يقدم المشروع collections والفهارس ونتائج mongosh القابلة للتصوير بدل اختلاق screenshot. مسار Docker وCompose موفر للتسليم ولكنه يحتاج Docker daemon خارجيًا للتشغيل.

## References

[1]: https://www.mongodb.com/docs/spark-connector/current/batch-mode/batch-write-config/ MongoDB, “Batch Write Configuration Options — Spark Connector.”

[2]: https://www.mongodb.com/docs/spark-connector/current/batch-mode/batch-write/ MongoDB, “Write to MongoDB in Batch Mode — Spark Connector.”
