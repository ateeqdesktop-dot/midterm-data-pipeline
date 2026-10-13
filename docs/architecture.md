# معمارية خط البيانات الهجين (Architecture Design)

## 🏗️ نظرة عامة على المعمارية

تم بناء هذا المشروع وفق نمط **ELT (Extract, Load, Transform)** لمعالجة تدفقات البيانات غير النظيفة، بحيث يتم أولاً تحميل البيانات بنسختها الأصلية الكاملة إلى طبقة التخزين الخام (`orders_raw`)، ثم تُجرى عليها عمليات التحويل والتنظيف والتصنيف وتُنقل إما إلى طبقة البيانات الصالحة (`orders_validated`) أو طبقة العزل (`orders_quarantine`).

```mermaid
flowchart TD
    A[Dirty CSV Input] --> B[File Router]
    B -->|Size <= 200 MB| C[Python Batch Loader]
    B -->|Size > 200 MB| D[PySpark Loader]
    
    C -->|Streaming Batches| E[(orders_raw)]
    D -->|Parallel Partitions| E
    
    E --> F[Chunked Ingestion Engine]
    F --> G[Cleaning Rules Engine - 9 Rules]
    G --> H[Quarantine & Quality Classifier]
    
    H -->|Valid / Corrected| I[Idempotent Upsert Writer]
    H -->|Quarantined| J[(orders_quarantine)]
    
    I --> K[(orders_validated)]
    
    K --> L[Metrics Engine]
    J --> L
    L --> M[reports/results.json]
```

---

## 🎯 المبادئ المعمارية المطبقة (Design Principles)

1. **Memory-Safe Streaming**:
   - لا يتم تحميل أي ملف بالكامل داخل الذاكرة كـ DataFrame أو List.
   - يتم تدفق البيانات وقراءتها من MongoDB عبر مؤشرات (`Cursors`) ومعالجتها في دفعات بحجم `5000` سجل ثابتة في الذاكرة $O(1)$.

2. **Decoupled Architecture & Single Responsibility**:
   - كل ملف وكل صنف مسؤول عن وظيفة وحيدة (Routing، Ingestion، Cleaning، Upserting، Metrics).

3. **Open / Closed Principle (OCP)**:
   - إضافة أي قاعدة تنظيف جديدة تتم بإنشاء صنف يرث من `CleaningRule` وإضافتها في الـ `RuleRegistry` دون تعديل كود المعالجة الرئيسي.

4. **Idempotency & Version Handling**:
   - الفهرس الفريد على `order_id` يمنع تكرار البيانات.
   - التحميل التزايدي يفحص `order_date` لضمان عدم الكتابة فوق التحديثات الأحدث.

5. **Auditing & Traceability**:
   - السجلات المصححة تحتفظ بمصفوفة `corrections` توضح الحقول المعدلة والقيم القديمة والجديدة ورمز القاعدة.
   - السجلات المعزولة تحتفظ بنسخة من السجل الخام ومصفوفة `error_codes` و `error_details`.
