# Hybrid Orders Data Pipeline

مشروع Midterm لمقرر **البيانات الضخمة العملي**: خط بيانات هجين يعالج CSV غير نظيف للطلبات باستخدام Python Batch للملفات الصغيرة وPySpark للملفات الكبيرة، ثم يطبق ELT داخل MongoDB مع طبقات `orders_raw` و`orders_validated` و`orders_quarantine`.

## القيمة والالتزام بالتكليف

لا يحذف النظام أي سجل سيئ أثناء التحميل الأولي. كل صف يدخل Raw مع `run_id` واسم المصدر ورقم الصف والوقت والمحرك، ثم يصنف إلى `valid` أو `corrected` أو `quarantined`. التصحيحات تحفظ Audit Trail كاملًا، والأخطاء غير القابلة للتصحيح تحفظ مع `error_codes` و`error_details`.

يختار `src/main.py` المحرك تلقائيًا وفق `SMALL_FILE_THRESHOLD_MB=200` القابلة للتغيير. الحد ليس ادعاءً عالميًا؛ هو قرار تعليمي/تشغيلي يوازن بين كلفة بدء Spark وفائدة التقسيم المتوازي للملفات الأكبر. يمكن فرض المحرك للاختبار بواسطة `--engine`، لكن نقطة التشغيل واحدة.

## التثبيت

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

للتشغيل الكامل مع MongoDB محليًا:

```bash
# شغل MongoDB على mongodb://localhost:27017
export MONGO_URI=mongodb://localhost:27017
```

إذا لم يتوفر MongoDB، يمكن تشغيل الاختبار التجريبي بـ`--backend memory` باستخدام mongomock؛ هذا بديل اختبار فقط، بينما مسار التسليم الأساسي هو MongoDB الحقيقي.

## التشغيل

أنشئ عينة قابلة للتغيير من الملف المقدم دون استخدام Excel:

```bash
PYTHONPATH=src python src/create_small_sample.py \
  --input data/orders_huge_mixed_quality.csv \
  --output data/orders_sample.csv --rows 100000
```

شغل العينة المحلية الحتمية لإثبات قواعد الجودة:

```bash
PYTHONPATH=src python src/main.py \
  --input data/sample_orders.csv \
  --backend memory --reports reports/results.json \
  --check-idempotency
```

سيطبع النظام حجم الملف، المحرك المختار، سبب الاختيار، `run_id`، العدادات، ونتيجة Idempotency. في التشغيل الحقيقي استخدم `--backend mongo` واتصال MongoDB:

```bash
PYTHONPATH=src python src/main.py \
  --input data/orders_sample.csv \
  --backend mongo --mongo-uri "$MONGO_URI" \
  --mongo-database midterm_orders \
  --reports reports/results.json
```

## PySpark والملفات الكبيرة

المسار الكبير يستخدم `SparkSession` وDataFrame API وSchema ثابتة من String للحفاظ على القيم غير النظيفة في Raw، ويستخدم عدد partitions قابلًا للضبط. التشغيل المحلي:

```bash
PYTHONPATH=src python src/main.py \
  --input data/orders_huge_mixed_quality.csv \
  --engine pyspark --spark-master 'local[*]' \
  --partitions 4 --backend mongo
```

في بيئة Spark Connector شغّل Spark مع الحزمة `org.mongodb.spark:mongo-spark-connector_2.13:10.5.0`، وهي مضبوطة افتراضيًا في إعدادات loader. المسار A الخاص بالمجموعة يتطلب Spark Standalone خارجيًا بعنوان `spark://MASTER_IP:7077` ولقطات Spark UI؛ لا يمكن إثبات جهازين منفصلين داخل sandbox، لذلك لا يُدّعى تنفيذه محليًا.

## قواعد الجودة

ينفذ المشروع أكثر من ثماني قواعد تصحيح واضحة: الأرقام العربية، العملة، فواصل الآلاف، السعر بالكلمات المعروفة، الهاتف، البريد ذي الرموز المتكررة، التاريخ، المسافات والمرادفات، وإعادة حساب الإجمالي من العناصر والتوصيل. أما `order_id` أو `customer_id` المفقودان وJSON التالف والتاريخ المستحيل والسعر المجهول والعناصر الفارغة والقيم السالبة الملتبسة والتكرار فتذهب إلى Quarantine مع سبب صريح.

## النتائج والمقاييس

ينشئ التشغيل `reports/results.json` بالحقول المطلوبة: `run_id`, `file_name`, `file_size_mb`, `engine_used`, `rows_read`, `raw_loaded`, `valid_count`, `corrected_count`, `quarantine_count`, `elapsed_seconds`, `throughput`, `batch_size/partitions`, `error_case_counts`, `inserted_count`, `updated_count`, `unchanged_count`, و`consistency_check`. شرط الاتساق هو:

```text
raw_loaded = valid_count + corrected_count + quarantine_count
```

## الاختبارات

```bash
PYTHONPATH=src pytest -q
ruff check src tests
python -m compileall -q src tests
```

تغطي الاختبارات قواعد التنظيف وAudit Trail، التصنيف، Raw/Quarantine، العدادات، Upsert، وإعادة التشغيل دون زيادة Business Records. الملفات المولدة غير مرفوعة إلى Git، والبيانات الجامعية الأصلية يجب وضعها محليًا فقط داخل `data/`.

## البنية

```text
config/settings.py              # جميع الإعدادات وEnvironment Variables
src/main.py                     # نقطة التشغيل الوحيدة
src/file_router.py              # اختيار Python Batch أو PySpark
src/create_small_sample.py      # عينة streaming قابلة للتغيير
src/batch_loader.py             # CSV streaming وinsert_many وELT
src/spark_loader.py             # SparkSession وDataFrame وschema/partitions
src/quality_rules.py            # التنظيف والتصنيف وAudit Trail
src/elt_pipeline.py             # facade للتدفق
src/incremental_loader.py       # Path B اختياري: Delta/version
src/mongo_setup.py              # collections وindexes وUpsert
src/metrics.py                  # reports/results.json
tests/                          # اختبارات التنظيف والتصنيف والتكامل
reports/                        # نتائج التشغيل واللقطات
```

## ملاحظات التسليم

المشروع يحقق المسار الفردي الأساسي. Path B موجود كمسار تمييز اختياري، أما Cluster path A فيحتاج جهازين/VMs وSpark UI خارج هذه البيئة. لا توجد مفاتيح أو كلمات مرور داخل المستودع. يجب تشغيل MongoDB الحقيقي عند العرض أمام الدكتور، وإرفاق لقطة Compass وSpark UI إذا تم تشغيل المسار الكبير على Cluster.
