# خط البيانات الهجين لمعالجة بيانات الطلبات
## Hybrid Data Pipeline for Order Processing (ELT)
### مشروع مقرر البيانات الضخمة (العملي) - جامعة الرازي
**إشراف المهندس: عمر أبوسند**

---

## 📌 فكرة المشروع

يستقبل هذا المشروع ملفات بيانات الطلبات غير النظيفة لمتجر إلكتروني، ويقوم بتوجيهها تلقائياً حسب حجم الملف:
1. **الملفات الصغيرة (≤ 200 ميجابايت)**: يتم تحميلها باستخدام محرك **Python Batch Loader** بطريقة Ingestion Streaming للحفاظ على كفاءة الذاكرة.
2. **الملفات الكبيرة (> 200 ميجابايت)**: يتم تحميلها ومعالجتها بالتوازي باستخدام محرك **PySpark Loader** (DataFrame API) وتوزيعها على الذاكرة.

يطبق المشروع نمط **ELT (Extract, Load, Transform)** حيث يتم تحميل البيانات الخام بالكامل أولاً إلى `orders_raw` للحفاظ على مصدر البيانات التاريخي، ثم يتم تطبيق **9 قواعد تنظيف** ومصادقة لتصنيف البيانات إلى سليمة/مصححة (`orders_validated`) أو معزولة (`orders_quarantine`) بالكامل دون فقدان أي سجل وبشكل يضمن الـ **Idempotency** الكامل عبر عمليات الـ **Upsert**.

---

## 🛠️ المعمارية والتدفق المنطقي للمشروع

```
                  [Dirty CSV Data]
                         │
                         ▼
             [File Router: Size Check]
            /                         \
    (≤ 200 MB)                       (> 200 MB)
      /                                 \
  [Python Batch Loader]             [PySpark Loader]
  (Streaming batches)               (Parallel Partitioning)
      \                                 /
       ▼                               ▼
    [MongoDB Collection: orders_raw (Raw Ingestion)]
                         │
                         ▼
        [Cleaning & Validation Engine (9 Rules)]
        /                │                     \
    (Valid)         (Corrected)            (Failed Validation)
      │                  │                      │
      │           [Audit Trail Log]             │
      \                  /                      ▼
       ▼                ▼            [orders_quarantine]
      [Idempotent Upsert]            (With Error Codes & Details)
               │
               ▼
      [orders_validated]
  (Stable Business Key Index)
               │
               ▼
    [reports/results.json]
```

---

## 📁 بنية المجلدات (Project Structure)

```
midterm-data-pipeline/
├── README.md                  # دليل التشغيل والتوضيح
├── requirements.txt           # مكتبات المشروع المطلوبة
├── config/
│   └── settings.py            # إعدادات MongoDB والـ Spark وحدود الملفات
├── data/                      # المجلد الذي يحتوي ملفات البيانات
│   ├── sample_orders.csv      # عينة الطلبات الصغيرة (~4.2 MB)
│   └── orders_huge_mixed_quality.csv  # ملف البيانات الكبير (~9.5 GB)
├── src/
│   ├── main.py                # نقطة التشغيل الرئيسية للمشروع
│   ├── file_router.py         # فحص حجم الملف وتوجيهه للمحرك المناسب
│   ├── create_small_sample.py # توليد عينات اختبار صغيرة برمجياً
│   ├── batch_loader.py        # محرك تحميل Python Batch (Streaming)
│   ├── spark_loader.py        # محرك تحميل PySpark (DataFrame API)
│   ├── mongo_setup.py         # إعداد الجداول (Collections) والفهارس الفريدة
│   ├── upsert_writer.py       # محرك الكتابة بالـ Upsert الموثوق
│   ├── incremental_loader.py  # معالجة التحميل التزايدي (Incremental)
│   ├── metrics.py             # جمع وحساب مؤشرات الأداء والاتساق
│   ├── cleaning/              # مجلد قواعد التنظيف التسع
│   │   ├── base_rule.py
│   │   ├── arabic_numbers.py  # القاعدة 1: تحويل الأرقام الشرقية
│   │   ├── currency_text.py   # القاعدة 2: إزالة نص العملة وتوحيد YER
│   │   ├── thousands_sep.py   # القاعدة 3: إزالة فواصل الآلاف
│   │   ├── word_price.py      # القاعدة 4: تحويل السعر بالكلمات
│   │   ├── phone_number.py    # القاعدة 5: توحيد أرقام الهواتف
│   │   ├── email_fix.py       # القاعدة 6: معالجة تكرار رموز البريد
│   │   ├── date_normalize.py  # القاعدة 7: توحيد صيغ التواريخ إلى ISO
│   │   ├── status_normalize.py# القاعدة 8: trim وتوحيد الحالات
│   │   ├── total_recalc.py    # القاعدة 9: إعادة حساب الإجمالي من العناصر
│   │   └── rule_registry.py   # إدارة تسلسل تشغيل القواعد
│   └── validation/            # تصنيف السجلات والعزل
│       ├── quarantine_classifier.py
│       └── record_classifier.py
├── tests/                     # الاختبارات الآلية للمشروع
│   ├── test_cleaning_rules.py
│   ├── test_classification.py
│   └── test_upsert_idempotency.py
└── reports/
    └── results.json           # ملف حفظ مقاييس ونتائج كل عملية تشغيل
```

---

## 🚀 إعداد وتشغيل المشروع

### 1. المتطلبات الأساسية
- تثبيت **Python 3.10+**
- تثبيت وتفعيل **MongoDB** محلياً على المنفذ `27017`
- تثبيت **Java OpenJDK 11 أو 17** (لتشغيل Spark)

### 2. تثبيت المكتبات البرمجية
قم بإنشاء بيئة افتراضية وتثبيت المتطلبات:
```bash
# إنشاء البيئة الافتراضية
python3 -m venv venv

# تفعيل البيئة
source venv/bin/activate

# تثبيت المكتبات
pip install -r requirements.txt
```

### 3. إعداد قواعد البيانات والـ Indexes
سيقوم الكود بإنشاء المجموعات والفهارس الفريدة تلقائياً عند التشغيل الأول، ولكن يمكنك إعدادها مسبقاً عبر:
```bash
PYTHONPATH=. ./venv/bin/python src/mongo_setup.py
```

### 4. تشغيل الاختبارات الآلية
يحتوي المشروع على 16 اختباراً آلياً تغطي كافة القواعد وحالات Idempotency والـ Quarantine:
```bash
PYTHONPATH=. ./venv/bin/pytest tests/ -v
```

---

## 🏃 تشغيل خط البيانات (E2E Run)

### أولاً: تشغيل مجلد البيانات بالكامل (All files in data directory)
يقوم البرنامج تلقائياً بالبحث عن جميع ملفات الـ CSV داخل مجلد `data/` ومعالجتها بالترتيب وتوجيه كل ملف للمحرك المناسب:
```bash
PYTHONPATH=. ./venv/bin/python src/main.py
# أو بتحديد مسار المجلد صراحة:
PYTHONPATH=. ./venv/bin/python src/main.py --input data/
```

### ثانياً: تشغيل ملف محدد (العينة الصغيرة - Python Batch)
يتم تمرير مسار الملف مباشرة:
```bash
PYTHONPATH=. ./venv/bin/python src/main.py --input data/sample_orders.csv
```

### ثانياً: تشغيل الملف الكبير (PySpark Engine)
يتم تمرير ملف يتعدى الـ 200 ميجابايت؛ سيقوم الموجّه تلقائياً باختيار **PySpark**:
```bash
PYTHONPATH=. ./venv/bin/python src/main.py --input data/spark_test_orders.csv
```

### ثالثاً: تشغيل التحميل التزايدي (Incremental Path B)
لتشغيل Pipeline في الوضع التراكمي وتطبيق منطق **Version Handling** لمنع التكرار ومعالجة النسخ الأحدث بالـ `order_date`:
```bash
PYTHONPATH=. ./venv/bin/python src/main.py --input data/sample_orders.csv --incremental
```

---

## 🧪 قواعد التنظيف الآلي المطبقة (9 قواعد)

| رقم القاعدة | الكود | الوصف والمعالجة المتوقعة |
|-------------|-------|--------------------------|
| **1** | `ARABIC_NUMERALS` | تحويل الأرقام الشرقية (مثل `٥٠٠٠` أو `٢٠٠٠٫٠`) إلى أرقام لاتينية (`5000` أو `2000.0`) |
| **2** | `CURRENCY_TEXT_REMOVAL` | إزالة نصوص العملات (مثل `ريال` أو `ريال يمني`) من حقول المبالغ وتوحيد العملة إلى `YER` |
| **3** | `THOUSANDS_SEPARATOR` | إزالة فواصل الآلاف من الأرقام (مثل `125,000.00` -> `125000.00`) |
| **4** | `WORD_PRICE_CONVERSION` | تحويل الأسعار المكتوبة بالكلمات (مثل `ألفان` -> `2000`, `خمسة آلاف` -> `5000`) |
| **5** | `PHONE_NORMALIZATION` | إزالة الرموز والمسافات وتوحيد صيغة الهاتف محلياً ودولياً للمظهر اليمني (+967XXXXXXXXX) بما في ذلك معالجة التنسيق المقلوب |
| **6** | `EMAIL_REPEATED_SYMBOLS` | إصلاح التكرار الواضح للرموز في البريد الإلكتروني (مثل `user@@mail..com` -> `user@mail.com`) |
| **7** | `DATE_FORMAT_NORMALIZATION` | توحيد صيغ التاريخ والوقت المتعددة إلى شكل قياسي ISO-8601 (`YYYY-MM-DDTHH:MM:SS`) |
| **8** | `STATUS_WHITESPACE_SYNONYM` | إزالة المسافات الزائدة (trim) ومطابقة وتوحيد مرادفات حالات الطلب والدفع إلى القاموس القياسي |
| **9** | `TOTAL_RECALCULATION` | فحص محتويات الـ JSON للعناصر وإصلاح الكميات السالبة وإعادة حساب الإجماليات ومطابقتها مع المبالغ الكلية |

---

## 🛡️ آلية العزل والـ Quarantine (9 رموز عزل)

لا يتم حذف أي سجل سيء! بل يُعزل في مجموعة `orders_quarantine` مع حفظ سجل الخطأ والسبب والبيانات الخام الأصلية للرجوع إليها:
1. `MISSING_ORDER_ID`: معرف الطلب غير متوفر أو فارغ.
2. `MISSING_CUSTOMER_ID`: معرف العميل غير متوفر.
3. `INVALID_IMPOSSIBLE_DATE`: تاريخ الطلب غير صالح أو لا يطابق الصيغ المدعومة.
4. `CORRUPTED_ITEMS_JSON`: الـ JSON الخاص بالعناصر تالف أو غير قابل للتحليل.
5. `EMPTY_ITEMS`: الطلب لا يحتوي على أي عنصر في القائمة.
6. `UNKNOWN_PRICE`: إجمالي المبلغ غير متوفر ولا يمكن استنتاجه.
7. `AMBIGUOUS_NEGATIVE_VALUE`: قيم سالبة غير قابلة للحل (مثل أسعار أو مبالغ سالبة).
8. `DUPLICATE_ORDER_ID`: تكرار معرف الطلب في نفس عملية التشغيل.
9. `MULTIPLE_CONFLICTING_ERRORS`: وجود 3 أخطاء جوهرية أو أكثر على نفس السجل.

---

## 📊 قياسات الأداء ومؤشر الاتساق (Metrics & Consistency)

في نهاية كل تشغيل، يتم حفظ تقرير المقاييس في `reports/results.json` ويحتوي على:
- معرف التشغيل الفريد `run_id` وزمن التنفيذ الكلي.
- معدل المعالجة (Throughput) بالسجلات في الثانية.
- عدادات السجلات: `Ingested`, `Valid`, `Corrected`, `Quarantine`.
- عدادات الـ Database: `Inserted`, `Updated`, `Unchanged`.
- تصنيف أعداد حالات الخطأ في الـ Quarantine.
- **تطبيق قانون الاتساق الأساسي (Rule 6.11)**:
  `raw_loaded == valid_count + corrected_count + quarantine_count`

---

## 🌟 مميزات الكود النظيف وجودة التصميم

- **Single Responsibility Principle (SRP)**: فصل تام بين عمليات التحميل، التنظيف، التصنيف، والاتصال بقاعدة البيانات.
- **Open/Closed Principle (OCP)**: يمكن إضافة أي قاعدة تنظيف جديدة بإنشاء ملف مستقل داخل مجلد `cleaning` وتسجيلها في الـ `RuleRegistry` دون تعديل منطق الكود الرئيسي.
- **Memory Safety (O(1) Space)**: تتم معالجة وتدفق البيانات برمجياً عبر MongoDB Cursors ودفعات (chunks) حجمها `5000` للحماية من استهلاك ذاكرة الخادم (Out of Memory) على الملفات المليونية.
- **Robust Database Fallback**: تم دعم كتابة PySpark عبر `MongoDB Spark Connector` مع تصميم معمارية fallback برمجية تقوم بالـ bulk write الموازي مباشرة من Spark Executors في حال عدم توفر مكتبات الـ JAR محلياً.
- **Idempotency & Version Handling**: كتابة بالـ `bulk upsert` بناءً على الـ Business Key الفريد للطلب (`order_id`) لمنع حدوث duplicate وحماية تسلسل التحديثات بالـ `order_date`.
