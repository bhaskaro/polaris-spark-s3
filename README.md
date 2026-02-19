---

# 🚀 Spark + Iceberg REST + Polaris + RustFS Lakehouse

A fully containerized **distributed Spark 3.5.1 cluster (Master + Workers)** integrated with:

* 🧊 **Apache Iceberg** (REST Catalog mode)
* 🏛 **Apache Polaris** (Catalog + Governance)
* 🗄 **RustFS** (S3-compatible object storage)
* 🔐 OAuth-secured REST APIs
* 🧠 Multi-catalog architecture

GitHub Repository:
👉 [https://github.com/bhaskaro/polaris-spark-s3](https://github.com/bhaskaro/polaris-spark-s3)

---

# 📥 Clone & Setup

## 1️⃣ Clone Repository

```bash
git clone https://github.com/bhaskaro/polaris-spark-s3.git
cd polaris-spark-s3
```

## 2️⃣ Start Environment

```bash
docker compose up -d
```

Verify:

```bash
docker ps
```

You should see:

* spark-master
* spark-worker-1
* spark-worker-2
* polaris
* rustfs

---

## 🛑 Stop Environment

```bash
docker compose down
```

## ⚠ Full Reset (Deletes All Data)

```bash
docker compose down -v
```

---

# 🏗 Architecture Overview

```
Spark Master + Workers
        ↓
Iceberg REST Client
        ↓
Polaris (REST Catalog + Governance)
        ↓
RustFS (S3 Object Storage)
```

---

# 🗄 S3 Bucket Management (RustFS)

Before creating catalogs or tables, create a bucket.

---

## ✅ Create Bucket

```bash
docker run --rm -it \
  --network lakehouse_net \
  -e AWS_ACCESS_KEY_ID=polaris_root \
  -e AWS_SECRET_ACCESS_KEY=polaris_pass \
  -e AWS_DEFAULT_REGION=us-west-2 \
  amazon/aws-cli:latest \
  --endpoint-url http://rustfs:9000 \
  s3 mb s3://bucket123
```

---

## ✅ List Buckets

```bash
docker run --rm -it \
  --network lakehouse_net \
  -e AWS_ACCESS_KEY_ID=polaris_root \
  -e AWS_SECRET_ACCESS_KEY=polaris_pass \
  -e AWS_DEFAULT_REGION=us-west-2 \
  amazon/aws-cli:latest \
  --endpoint-url http://rustfs:9000 \
  s3 ls
```

---

## ✅ Remove Bucket (Cleanup)

```bash
docker run --rm -it \
  --network lakehouse_net \
  -e AWS_ACCESS_KEY_ID=polaris_root \
  -e AWS_SECRET_ACCESS_KEY=polaris_pass \
  -e AWS_DEFAULT_REGION=us-west-2 \
  amazon/aws-cli:latest \
  --endpoint-url http://rustfs:9000 \
  s3 rb s3://bucket123 --force
```

---

# 🔐 Polaris OAuth Authentication

Base URL:

```
http://localhost:8181
```

## Get Access Token

```bash
curl -X POST http://localhost:8181/api/catalog/v1/oauth/tokens \
  -d 'grant_type=client_credentials' \
  -d 'client_id=root' \
  -d 'client_secret=s3cr3t' \
  -d 'scope=PRINCIPAL_ROLE:ALL'
```

Export token:

```bash
export TOKEN="<access_token>"
```

---

# 📚 Catalog Lifecycle (Management API)

---

## 🔎 List Catalogs

```bash
curl -X GET \
  http://localhost:8181/api/management/v1/catalogs \
  -H "Authorization: Bearer $TOKEN" \
  -H "Polaris-Realm: POLARIS"
```

---

## ➕ Create Catalog

```bash
curl -X POST http://localhost:8181/api/management/v1/catalogs \
  -H "Authorization: Bearer $TOKEN" \
  -H "Polaris-Realm: POLARIS" \
  -H "Content-Type: application/json" \
  -d '{
        "catalog": {
          "name": "analytics_catalog",
          "type": "INTERNAL",
          "properties": {
            "default-base-location": "s3://bucket123/analytics"
          },
          "storageConfigInfo": {
            "storageType": "S3",
            "allowedLocations": ["s3://bucket123/analytics"],
            "endpoint": "http://rustfs:9000",
            "endpointInternal": "http://rustfs:9000",
            "pathStyleAccess": true
          }
        }
      }'
```

---

## ❌ Delete Catalog

```bash
curl -X DELETE \
  http://localhost:8181/api/management/v1/catalogs/analytics_catalog \
  -H "Authorization: Bearer $TOKEN" \
  -H "Polaris-Realm: POLARIS"
```

---

# 📂 Namespace Lifecycle

---

## ➕ Create Namespace

```bash
curl -X POST \
  http://localhost:8181/api/catalog/v1/analytics_catalog/namespaces \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
        "namespace": ["finance"],
        "properties": {}
      }'
```

---

## 🔎 List Namespaces

```bash
curl -X GET \
  http://localhost:8181/api/catalog/v1/analytics_catalog/namespaces \
  -H "Authorization: Bearer $TOKEN"
```

---

## ❌ Delete Namespace

```bash
curl -X DELETE \
  http://localhost:8181/api/catalog/v1/analytics_catalog/namespaces/finance \
  -H "Authorization: Bearer $TOKEN"
```

---

# 📊 Table Lifecycle

---

## ➕ Create Table

```bash
curl -X POST \
  http://localhost:8181/api/catalog/v1/analytics_catalog/namespaces/finance/tables \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
        "name": "transactions",
        "schema": {
          "type": "struct",
          "schema-id": 0,
          "fields": [
            { "id": 1, "name": "id", "required": true, "type": "long" },
            { "id": 2, "name": "amount", "required": false, "type": "double" }
          ]
        }
      }'
```

---

## 🔎 List Tables

```bash
curl -X GET \
  http://localhost:8181/api/catalog/v1/analytics_catalog/namespaces/finance/tables \
  -H "Authorization: Bearer $TOKEN"
```

---

## 📄 Get Table Metadata

```bash
curl -X GET \
  http://localhost:8181/api/catalog/v1/analytics_catalog/namespaces/finance/tables/transactions \
  -H "Authorization: Bearer $TOKEN"
```

---

## ❌ Delete Table

```bash
curl -X DELETE \
  http://localhost:8181/api/catalog/v1/analytics_catalog/namespaces/finance/tables/transactions \
  -H "Authorization: Bearer $TOKEN"
```

---

# ⚠ Spark SQL Package Download Fix (Important)

When using:

```
--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2
```

Spark downloads dependencies to:

```
/home/spark/.ivy2
```

This directory does **not exist by default**, causing failures.

---

## ✅ One-Time Fix

Run once after containers start:

```bash
docker exec -u root -it spark-master bash -c "
mkdir -p /home/spark/.ivy2/jars &&
chown -R spark:spark /home/spark
"
```

This ensures Spark can download Iceberg dependencies properly.

---

# 🧊 Spark SQL Integration

Enter Spark container:

```bash
docker exec -it spark-master bash
```

Start Spark SQL:

```bash
/opt/spark/bin/spark-sql \
  --master spark://spark-master:7077 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.iceberg:iceberg-aws-bundle:1.4.2 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.quick=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.quick.type=rest \
  --conf spark.sql.catalog.quick.uri=http://polaris:8181/api/catalog \
  --conf spark.sql.catalog.quick.credential=root:s3cr3t \
  --conf spark.sql.catalog.quick.scope=PRINCIPAL_ROLE:ALL \
  --conf spark.sql.catalog.quick.warehouse=analytics_catalog \
  --conf spark.sql.catalog.quick.s3.endpoint=http://rustfs:9000 \
  --conf spark.sql.catalog.quick.s3.path-style-access=true \
  --conf spark.sql.catalog.quick.s3.access-key-id=polaris_root \
  --conf spark.sql.catalog.quick.s3.secret-access-key=polaris_pass \
  --conf spark.sql.catalog.quick.client.region=us-west-2
```

---

## Spark Operations

```sql
SHOW CATALOGS;
SHOW NAMESPACES IN quick;
CREATE NAMESPACE quick.finance;
CREATE TABLE quick.finance.orders (id INT, name STRING);
INSERT INTO quick.finance.orders VALUES (1, 'Vijay');
SELECT * FROM quick.finance.orders;
```

---

# 🕒 Iceberg Time Travel

```sql
SELECT * FROM quick.finance.orders.snapshots;
```

```sql
SELECT * FROM quick.finance.orders VERSION AS OF <snapshot_id>;
```

---

# 🧹 Cleanup Order (Recommended)

1️⃣ Delete tables
2️⃣ Delete namespaces
3️⃣ Delete catalog
4️⃣ Delete bucket

---

# 🎯 What This Demonstrates

✔ Distributed Spark cluster
✔ Iceberg REST catalog
✔ OAuth-secured governance
✔ Multi-catalog architecture
✔ S3-compatible storage
✔ Full lifecycle management
✔ Snapshot-based time travel

---

# 🚀 Future Enhancements

* Add Trino engine
* Enable Polaris RBAC
* Iceberg branching
* Terraform provisioning
* CI/CD integration

---
