# دليل استخدام واجهة سطر الأوامر (CLI Guide)
## خط البيانات الهجين لمعالجة الطلبات (Hybrid ELT Data Pipeline)

يوفر هذا الدليل مرجعاً شاملاً وسريعاً لجميع الأوامر والخيارات المتاحة لتشغيل واختبار وإدارة المشروع عبر سطر الأوامر (Terminal / CLI).

---

## 📋 جدول المحتويات
1. [المتطلبات الأساسية والتهيئة (Prerequisites & Setup)](#1-المتطلبات-الأساسية-والتهيئة)
2. [الأمر الرئيسي للتشغيل (`src/main.py`)](#2-الأمر-الرئيسي-للتشغيل)
3. [متغيرات البيئة والتحكم في المحركات (Environment Variables)](#3-متغيرات-البيئة-والتحكم-في-المحركات)
4. [سيناريوهات التشغيل الشائعة (Common Use Cases)](#4-سيناريوهات-التشغيل-الشائعة)
5. [إدارة عنقود سبارك (Spark Cluster Management)](#5-إدارة-عنقود-سبارك)
6. [أدوات التوليد والاختبار والفحص (Testing & Utilities)](#6-أدوات-التوليد-والاختبار-والفحص)
7. [الاستعلام عن النتائج والتقارير (Reports & Metrics)](#7-الاستعلام-عن-النتائج-والتقارير)

---

## 1. المتطلبات الأساسية والتهيئة

### أ. تفعيل البيئة وتثبيت الحزم
```bash
# إنشاء البيئة الافتراضية وتفعيلها
python3 -m venv venv
source venv/bin/activate

# تثبيت متطلبات المشروع
pip install -r requirements.txt
```

### ب. تشغيل خادم MongoDB
```bash
# إنشاء مجلد البيانات والسجلات
mkdir -p data/db_test logs

# تشغيل MongoDB كخدمة في الخلفية
mongod --dbpath ./data/db_test --logpath ./logs/mongod.log --fork
```

---

## 2. الأمر الرئيسي للتشغيل

نقطة الدخول الموحدة للمشروع هي السكربت [`src/main.py`](file:///home/ateeq/Desktop/proggime/programs_python/Big_Data/midetarm/src/main.py):

```bash
PYTHONPATH=. ./venv/bin/python src/main.py [OPTIONS]
```

### الخيارات والمعاملات المتاحة (CLI Flags):

| المعامل (Flag) | النوع | الافتراضي | الوصف |
|---|---|---|---|
| `--input <path>` | مسار ملف CSV | `data/orders_huge_mixed_quality.csv` | مسار ملف البيانات المطلوب معالجته. يقوم النظام بقياس حجم الملف تلقائياً وتوجيهه للمحرك المناسب. |
| `--incremental` | Flag اختياري | `False` | تفعيل نمط التحميل التراكمي (Path B) مع حماية الإصدارات (Version Protection) والـ Upsert الآمن. |
| `-h`, `--help` | مساعدة | - | عرض رسالة المساعدة وشرح المعاملات. |

---

## 3. متغيرات البيئة والتحكم في المحركات

يمكن ضبط سلوك خط البيانات عبر تمرير متغيرات البيئة قبل أمر التشغيل:

| متغير البيئة (Env Var) | القيمة الافتراضية | الوصف والغرض |
|---|---|---|
| `SPARK_MASTER` | `local[*]` | عنوان خادم سبارك ماستر (`local[*]`, `spark://127.0.0.1:7077`, `spark://IP:7077`). |
| `SMALL_FILE_THRESHOLD_MB` | `200.0` | الحد الفاصل بالميجابايت للتوجيه التلقائي (أقل من الحد = Python Batch، أكبر = PySpark). |
| `BATCH_SIZE` | `5000` | حجم حزمة القراءة والكتابة الدفعية لمحرك بايثون. |
| `MONGO_URI` | `mongodb://localhost:27017` | رابط الاتصال بخادم MongoDB. |
| `MONGO_DB_NAME` | `midterm_db` | اسم قاعدة البيانات المستخدمة. |
| `SPARK_DRIVER_HOST` | `127.0.0.1` | عنوان IP لجهاز الـ Driver للتواصل عبر الشبكة مع العقد. |

---

## 4. سيناريوهات التشغيل الشائعة

### السيناريو 1: معالجة ملف صغير عبر محرك بايثون المتدفق (Python Batch)
الملفات الأصغر من 200MB تذهب تلقائياً لمحرك البايثون المعتمد على Streaming لحماية الذاكرة:
```bash
PYTHONPATH=. ./venv/bin/python src/main.py --input data/sample_orders.csv
```

### السيناريو 2: معالجة ملف ضخم (1,000,000 سجل) عبر عنقود سبارك المستقل
```bash
# 1. بدء العنقود
bash scripts/start_spark_cluster.sh

# 2. تشغيل المعالجة الموزعة
PYTHONPATH=. SPARK_MASTER="spark://127.0.0.1:7077" \
  ./venv/bin/python src/main.py --input data/orders_1m.csv
```

### السيناريو 3: تشغيل التحديث التراكمي وحماية الإصدارات (Incremental Mode)
```bash
# تحديث السجلات الأحدث فقط وتجاهل القديم دون تكرار أي سجل تجاري
PYTHONPATH=. ./venv/bin/python src/main.py --input data/orders_delta.csv --incremental
```

### السيناريو 4: إجبار استخدام محرك PySpark حتى للملفات الصغيرة (للاختبار)
```bash
SMALL_FILE_THRESHOLD_MB=0.001 PYTHONPATH=. SPARK_MASTER="spark://127.0.0.1:7077" \
  ./venv/bin/python src/main.py --input data/spark_test_orders.csv
```

---

## 5. إدارة عنقود سبارك (Spark Cluster Management)

### التشغيل المحلي المستقل (Local Cluster on Same Host):
```bash
# بدء الماستر والعامل معاً في الخلفية
bash scripts/start_spark_cluster.sh

# إيقاف العنقود
bash scripts/stop_spark_cluster.sh
```

### التشغيل الموزع على جهازين منفصلين (Two Physical/Virtual Machines):
```bash
# في الجهاز الأول (Master Server - IP: 192.168.1.100):
MASTER_HOST=192.168.1.100 ./scripts/start_spark_master.sh

# في الجهاز الثاني (Worker Node - IP: 192.168.1.101):
WORKER_HOST=192.168.1.101 ./scripts/start_spark_worker.sh spark://192.168.1.100:7077
```

### التشغيل عبر Docker Compose:
```bash
# تشغيل العنقود بحاويات معزولة
docker compose -f docker-compose.cluster.yml up -d

# إيقاف الحاويات
docker compose -f docker-compose.cluster.yml down
```

---

## 6. أدوات التوليد والاختبار والفحص

### أ. توليد عينات بيانات بالحجم المطلوب
```bash
# توليد ملف يحتوي على 50,000 سجل متسخ
python src/create_small_sample.py --rows 50000 --output data/sample_50k.csv

# توليد ملف بـ 1,000,000 سجل
python src/create_small_sample.py --rows 1000000 --output data/orders_1m.csv
```

### ب. تشغيل حزمة الاختبارات الآلية (Pytest)
```bash
# تشغيل جميع الاختبارات الـ 18
PYTHONPATH=. ./venv/bin/pytest tests/ -v

# تشغيل اختبارات الـ Upsert و Idempotency فقط
PYTHONPATH=. ./venv/bin/pytest tests/test_upsert_idempotency.py -v
```

### ج. التحقق الشامل وفحص جودة الكود
```bash
# تشغيل المدقق الشامل للمشروع
PYTHONPATH=. python scripts/verify_project.py

# فحص المعايير والتنسيق عبر Ruff
ruff check src tests

# فحص ترجمة بايثون (Syntax & Compile)
python -m compileall -q src tests
```

---

## 7. الاستعلام عن النتائج والتقارير

يتم حفظ مؤشرات ومقاييس كل عملية تشغيل في ملف JSON وملف Markdown:
- **ملف المقاييس الرقمية**: [`reports/results.json`](file:///home/ateeq/Desktop/proggime/programs_python/Big_Data/midetarm/reports/results.json)
- **ملف التقرير التحليلي**: [`reports/results.md`](file:///home/ateeq/Desktop/proggime/programs_python/Big_Data/midetarm/reports/results.md)
- **مجلد لقطات الشاشة**: [`reports/screenshots/`](file:///home/ateeq/Desktop/proggime/programs_python/Big_Data/midetarm/reports/screenshots/)

### قراءة آخر نتيجة تشغيل عبر سطر الأوامر:
```bash
# عرض آخر سجل مقاييس من results.json
python3 -c "import json; data=json.load(open('reports/results.json')); print(json.dumps(data[-1], indent=2, ensure_ascii=False))"
```
