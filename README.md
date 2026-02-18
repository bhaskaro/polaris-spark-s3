# 🚀 Spark + Iceberg REST + Polaris + RustFS Lakehouse

A fully containerized **distributed Spark 3.5.1 cluster (Master + Workers)** integrated with:

* 🧊 **Apache Iceberg (REST Catalog mode)**
* 🏛 **Apache Polaris (Catalog + Governance)**
* 🗄 **RustFS (S3-compatible object storage)**
* 🔐 OAuth-based REST authentication
* 🧠 Multi-catalog architecture

This project demonstrates a **modern lakehouse architecture** similar to:

* Databricks Unity Catalog
* Snowflake multi-catalog governance
* Enterprise Iceberg REST deployments

---

# 🏗 Architecture Overview

```
                +-------------------+
                |     Spark SQL     |
                |  Spark Master     |
                +---------+---------+
                          |
                +---------+----------+
                |  Spark Workers     |
                | (Distributed Exec) |
                +---------+----------+
                          |
                +---------+----------+
                | Iceberg REST Client|
                +---------+----------+
                          |
                +---------+----------+
                |     Polaris        |
                | Iceberg REST API   |
                +---------+----------+
                          |
                +---------+----------+
                |      RustFS        |
                |  S3 Object Store   |
                +--------------------+
```

---

# 📦 Components

| Component           | Purpose                      |
| ------------------- | ---------------------------- |
| Spark 3.5.1         | Distributed SQL Engine       |
| Iceberg             | Table format                 |
| Polaris             | REST Catalog + Governance    |
| RustFS              | S3 compatible object storage |
| AWS CLI (container) | Bucket management            |

---

# 🐳 Docker Compose Setup

## Start Everything

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

## Stop Everything

```bash
docker compose down
```

---

## Full Reset (⚠ Deletes Data)

```bash
docker compose down -v
```

---

# 🗄 Create S3 Bucket (RustFS)

Polaris requires a bucket before creating tables.

```bash
docker run --rm -it \
  --network polaris_lakehouse_net \
  -e AWS_ACCESS_KEY_ID=polaris_root \
  -e AWS_SECRET_ACCESS_KEY=polaris_pass \
  -e AWS_DEFAULT_REGION=us-west-2 \
  amazon/aws-cli:latest \
  --endpoint-url http://rustfs:9000 \
  s3 mb s3://bucket123
```

---

# 🧊 Spark SQL – Connecting to Polaris (Single Catalog)

Enter Spark container:

```bash
docker exec -it spark-master bash
```

Start Spark SQL:

```bash
/opt/spark/bin/spark-sql \
  --master spark://spark-master:7077 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.iceberg:iceberg-aws-bundle:1.4.2 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.quick=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.quick.type=rest \
  --conf spark.sql.catalog.quick.warehouse=quickstart_catalog \
  --conf spark.sql.catalog.quick.uri=http://polaris:8181/api/catalog \
  --conf spark.sql.catalog.quick.credential=root:s3cr3t \
  --conf spark.sql.catalog.quick.scope=PRINCIPAL_ROLE:ALL \
  --conf spark.sql.catalog.quick.s3.endpoint=http://rustfs:9000 \
  --conf spark.sql.catalog.quick.s3.path-style-access=true \
  --conf spark.sql.catalog.quick.s3.access-key-id=polaris_root \
  --conf spark.sql.catalog.quick.s3.secret-access-key=polaris_pass \
  --conf spark.sql.catalog.quick.client.region=us-west-2
```

---

# 📂 Spark SQL Operations

## List Catalogs

```sql
SHOW CATALOGS;
```

## List Namespaces

```sql
SHOW NAMESPACES IN quick;
```

## Create Namespace

```sql
CREATE NAMESPACE quick.sales;
```

## Create Table

```sql
CREATE TABLE quick.sales.orders (
  id INT,
  name STRING
);
```

## Insert Data

```sql
INSERT INTO quick.sales.orders VALUES (1, 'Vijay');
```

## Query Table

```sql
SELECT * FROM quick.sales.orders;
```

---

# 🕒 Time Travel

List snapshots:

```sql
SELECT * FROM quick.sales.orders.snapshots;
```

Query previous version:

```sql
SELECT * FROM quick.sales.orders VERSION AS OF <snapshot_id>;
```

---

# 🌐 Polaris REST API Usage

Base URL:

```
http://localhost:8181
```

---

## 🔐 Get OAuth Token

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

# 📚 Catalog Management (Management API)

## List Catalogs

```bash
curl -X GET \
  http://localhost:8181/api/management/v1/catalogs \
  -H "Authorization: Bearer $TOKEN" \
  -H "Polaris-Realm: POLARIS"
```

---

## Create Catalog

⚠ Must use non-overlapping S3 path

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

## Delete Catalog

```bash
curl -X DELETE \
  http://localhost:8181/api/management/v1/catalogs/analytics_catalog \
  -H "Authorization: Bearer $TOKEN" \
  -H "Polaris-Realm: POLARIS"
```

---

# 📂 Namespace Operations (REST)

## Create Namespace

```bash
curl -X POST \
  http://localhost:8181/api/catalog/v1/quickstart_catalog/namespaces \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
        "namespace": ["finance"],
        "properties": {}
      }'
```

---

## List Namespaces

```bash
curl -X GET \
  http://localhost:8181/api/catalog/v1/quickstart_catalog/namespaces \
  -H "Authorization: Bearer $TOKEN"
```

---

## Delete Namespace

```bash
curl -X DELETE \
  http://localhost:8181/api/catalog/v1/quickstart_catalog/namespaces/finance \
  -H "Authorization: Bearer $TOKEN"
```

---

# 📊 Table Operations (REST)

## Create Table

```bash
curl -X POST \
  http://localhost:8181/api/catalog/v1/quickstart_catalog/namespaces/finance/tables \
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

## List Tables

```bash
curl -X GET \
  http://localhost:8181/api/catalog/v1/quickstart_catalog/namespaces/finance/tables \
  -H "Authorization: Bearer $TOKEN"
```

---

## Get Table Metadata

```bash
curl -X GET \
  http://localhost:8181/api/catalog/v1/quickstart_catalog/namespaces/finance/tables/transactions \
  -H "Authorization: Bearer $TOKEN"
```

---

## Delete Table

```bash
curl -X DELETE \
  http://localhost:8181/api/catalog/v1/quickstart_catalog/namespaces/finance/tables/transactions \
  -H "Authorization: Bearer $TOKEN"
```

---

# 🧹 Cleanup Data in RustFS

Delete objects:

```bash
aws --endpoint-url http://localhost:9000 s3 rm s3://bucket123 --recursive
```

Delete bucket:

```bash
aws --endpoint-url http://localhost:9000 s3 rb s3://bucket123
```

---

# 🛠 Troubleshooting

### ❌ Connection Refused to localhost:9000

Use container hostname:

```
http://rustfs:9000
```

Not `localhost`.

---

### ❌ Bucket Does Not Exist

Create bucket before creating tables.

---

### ❌ Catalog Overlapping Location

Each catalog must use a unique base S3 prefix.

---

# 🎯 What This Project Demonstrates

✔ Distributed Spark Cluster
✔ Iceberg REST Catalog
✔ OAuth-secured REST operations
✔ Multi-catalog governance
✔ S3-compatible object storage
✔ Snapshot-based time travel
✔ Full API-driven lakehouse control

---

# 🚀 Next Enhancements

* Add Trino engine
* Enable RBAC in Polaris
* Iceberg branching
* CI/CD deployment
* Terraform-based infra provisioning

---

# 🏁 Conclusion

You now have a fully working enterprise-style Iceberg REST Lakehouse using:

* Spark
* Polaris
* RustFS
* Docker

This architecture is production-aligned and suitable for experimentation, demos, and advanced lakehouse learning.

---
