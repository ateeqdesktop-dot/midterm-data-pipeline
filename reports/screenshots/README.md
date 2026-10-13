# لقطات شاشات العنقود وقاعدة البيانات (Cluster & Database Screenshots)

يوثق هذا المجلد لقطات الشاشة الأساسية لإثبات تشغيل العنقود المستقل (Spark Standalone Cluster) وقاعدة البيانات (MongoDB Compass) وفقاً لمتطلبات المشروع:

---

## 1. لقطة واجهة Spark Master Web UI (`spark_master_ui.jpg`)
- **الرابط والمنفذ**: `http://localhost:8080` (عنوان الماستر: `spark://127.0.0.1:7077`).
- **المحتوى المثبت**:
  - حالة الماستر: `ALIVE`.
  - العمال المتصلون (Alive Workers): `1 Alive Worker` برقم تعريف `worker-20260831001823-127.0.0.1-40615`.
  - الموارد المتاحة: `12 Cores` و `30.8 GiB RAM`.
  - التطبيق المنفذ المكتمل (Completed Applications): `MidtermDataPipeline` برقم `app-20260831001833-0000` بحالة `FINISHED`.

---

## 2. لقطة واجهة Spark Worker Web UI (`spark_worker_ui.jpg`)
- **الرابط والمنفذ**: `http://localhost:8081`.
- **المحتوى المثبت**:
  - ارتباط العامل بالماستر: `spark://127.0.0.1:7077`.
  - الموارد المخصصة للـ Executor: `12 Cores` و `4.0 GiB RAM`.
  - المنفذات المكتملة (Finished Executors): `Executor ID 0` للتطبيق `MidtermDataPipeline`.

---

## 3. لقطة واجهة MongoDB Compass (`mongodb_compass.jpg`)
- **قاعدة البيانات**: `midterm_db`.
- **المجموعات الثلاث (Collections)**:
  1. `orders_raw`: البيانات الخام غير النظيفة بجميع سجلاتها.
  2. `orders_validated`: السجلات المصححة والسليمة المكتملة مع الـ `audit_trail` ورقم الـ `version`.
  3. `orders_quarantine`: السجلات المعزولة لعدم قابليتها للإصلاح مع ذكر الأسباب التفصيلية.
