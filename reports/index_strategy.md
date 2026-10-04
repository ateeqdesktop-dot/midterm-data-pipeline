# استراتيجية الفهارس ومقارنة الأداء (Index Strategy & Explain Benchmarks)
## Big Data Phase 2 - Final Project

توثيق علمي وتطبيقي دقيق لأثر الفهارس المنشأة ومقارنة إحصائيات التنفيذ الحقيقية (`executionStats`) قبل وبعد إنشاء الفهارس.

---

### Orders by Customer (Single Field Index)
- **الاستعلام (Query)**: `db.orders_validated.find({'customer_id': 'عميل-0'}).limit(10)`
- **المجموعة (Collection)**: `orders_validated`
- **الفهرس المستهدف (Target Index)**: `idx_customer_id` (Single Field Index {customer_id: 1})

#### 1. ما المشكلة قبل الفهرس؟ (Problem Before Index)
عند البحث عن طلبات عميل محدد، تقوم قاعدة البيانات بمسح كامل الجدول (COLLSCAN) وفحص ملايين السجلات مما يؤدي لاستهلاك عالي للذاكرة وبطء شديد.

#### 2. ما الفهرس ولماذا تم اختياره؟ (Index Choice & Reason)
إنشاء فهرس أحادي على customer_id للوصول المباشر لسجلات العميل عبر B-Tree دون مسح الجدول.

#### 3. مقارنة مؤشرات الأداء الحقيقية (ExecutionStats Comparison)

| المقياس (Metric) | قبل الفهرس (Before) | بعد الفهرس (After) | التحسن (Improvement) |
|---|---|---|---|
| نوع المسح (Scan Type) | **COLLSCAN** (COLLSCAN) | **IXSCAN** (IXSCAN) | انتقال جذري إلى مسح الفهرس |
| زمن التنفيذ (executionTimeMillis) | `3873 ms` | `0 ms` | وفر `3873 ms` |
| المستندات المفحوصة (totalDocsExamined) | `7,780,451` | `1` | انخفاض بنسبة `100.00%` |
| مفاتيح الفهرس المفحوصة (totalKeysExamined) | `0` | `1` | استهداف مباشر للمفاتيح |
| السجلات المسترجعة (nReturned) | `1` | `1` | استرجاع مطابق بدقة تامة |

#### 4. النتيجة والخلاصة (Result & Analysis)
أثبتت التجربة العملية تفوق الفهرس المطبق `idx_customer_id`؛ حيث تم القضاء على المسح الشامل للجدول (COLLSCAN) الذي كان يفحص مستندات لا حصر لها، والاعتماد كلياً على الـ B-Tree Index (IXSCAN) مما وفّر أزمنة المعالجة وموارد وحدة المعالجة المركزية (CPU) والذاكرة العشوائية (RAM).

---

### Orders by City and Status (Compound Index)
- **الاستعلام (Query)**: `db.orders_validated.find({'city': 'صنعاء', 'status': 'مؤكد'}).limit(100)`
- **المجموعة (Collection)**: `orders_validated`
- **الفهرس المستهدف (Target Index)**: `idx_city_status` (Compound Index {city: 1, status: 1})

#### 1. ما المشكلة قبل الفهرس؟ (Problem Before Index)
عمليات الفلترة المزدوجة لشركات الخدمات اللوجستية تتطلب تصفية كل من المدينة والحالة، وبدون فهرس مركب يتم فحص كل المستندات أو استخدام فهرس جزئي ثم فلترة الباقي في الذاكرة.

#### 2. ما الفهرس ولماذا تم اختياره؟ (Index Choice & Reason)
إنشاء فهرس مركب بترتيب (city, status) لمطابقة شرط التساوي لكلا الحقلين مباشرة باستخدام IXSCAN.

#### 3. مقارنة مؤشرات الأداء الحقيقية (ExecutionStats Comparison)

| المقياس (Metric) | قبل الفهرس (Before) | بعد الفهرس (After) | التحسن (Improvement) |
|---|---|---|---|
| نوع المسح (Scan Type) | **COLLSCAN** (COLLSCAN) | **IXSCAN** (IXSCAN) | انتقال جذري إلى مسح الفهرس |
| زمن التنفيذ (executionTimeMillis) | `15 ms` | `0 ms` | وفر `15 ms` |
| المستندات المفحوصة (totalDocsExamined) | `2,260` | `100` | انخفاض بنسبة `95.58%` |
| مفاتيح الفهرس المفحوصة (totalKeysExamined) | `0` | `100` | استهداف مباشر للمفاتيح |
| السجلات المسترجعة (nReturned) | `100` | `100` | استرجاع مطابق بدقة تامة |

#### 4. النتيجة والخلاصة (Result & Analysis)
أثبتت التجربة العملية تفوق الفهرس المطبق `idx_city_status`؛ حيث تم القضاء على المسح الشامل للجدول (COLLSCAN) الذي كان يفحص مستندات لا حصر لها، والاعتماد كلياً على الـ B-Tree Index (IXSCAN) مما وفّر أزمنة المعالجة وموارد وحدة المعالجة المركزية (CPU) والذاكرة العشوائية (RAM).

---

### Quarantine by Error Code (Multikey Array Index)
- **الاستعلام (Query)**: `db.orders_quarantine.find({'error_codes': 'CORRUPTED_ITEMS_JSON'}).limit(50)`
- **المجموعة (Collection)**: `orders_quarantine`
- **الفهرس المستهدف (Target Index)**: `idx_quarantine_error_codes` (Multikey Index {error_codes: 1})

#### 1. ما المشكلة قبل الفهرس؟ (Problem Before Index)
مراقبة وفرز سجلات العزل حسب كود الخطأ تتطلب قراءة كل سجلات العزل وفحص مصفوفة الأخطاء يدوياً عند كل استعلام.

#### 2. ما الفهرس ولماذا تم اختياره؟ (Index Choice & Reason)
إنشاء فهرس متعدد المفاتيح (Multikey) على مصفوفة error_codes للقفز الفوري للسجلات التي تحتوي الخطأ المحدد.

#### 3. مقارنة مؤشرات الأداء الحقيقية (ExecutionStats Comparison)

| المقياس (Metric) | قبل الفهرس (Before) | بعد الفهرس (After) | التحسن (Improvement) |
|---|---|---|---|
| نوع المسح (Scan Type) | **COLLSCAN** (COLLSCAN) | **IXSCAN** (IXSCAN) | انتقال جذري إلى مسح الفهرس |
| زمن التنفيذ (executionTimeMillis) | `35 ms` | `0 ms` | وفر `35 ms` |
| المستندات المفحوصة (totalDocsExamined) | `1,850` | `50` | انخفاض بنسبة `97.30%` |
| مفاتيح الفهرس المفحوصة (totalKeysExamined) | `0` | `50` | استهداف مباشر للمفاتيح |
| السجلات المسترجعة (nReturned) | `50` | `50` | استرجاع مطابق بدقة تامة |

#### 4. النتيجة والخلاصة (Result & Analysis)
أثبتت التجربة العملية تفوق الفهرس المطبق `idx_quarantine_error_codes`؛ حيث تم القضاء على المسح الشامل للجدول (COLLSCAN) الذي كان يفحص مستندات لا حصر لها، والاعتماد كلياً على الـ B-Tree Index (IXSCAN) مما وفّر أزمنة المعالجة وموارد وحدة المعالجة المركزية (CPU) والذاكرة العشوائية (RAM).

---
