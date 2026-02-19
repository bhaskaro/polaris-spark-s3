package com.vijay.lakehouse;

import org.apache.spark.sql.SparkSession;


/**
 *
 * Author : bhask
 * Created : 02-19-2026
 */
public class PolarisIcebergDemo {

    public static void main(String[] args) {

        System.setProperty("aws.region","us-west-2");
        String DOCKER_HOST_IP = "192.168.1.222";


        SparkSession spark = SparkSession.builder()
                .appName("Polaris-Remote-Client")
                .master("local[*]")

                // --- 1. Polaris REST Catalog Configuration ---
                .config("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.polaris.type", "rest")
                .config("spark.sql.catalog.polaris.uri", "http://192.168.1.222:8181/api/catalog")

                // IMPORTANT: Check this token format.
                // If "root:s3cr3t" fails, Polaris may require an OAuth2 token exchange.
                // For bootstrap/testing, ensure this matches your POLARIS_BOOTSTRAP_CREDENTIALS
                .config("spark.sql.catalog.polaris.token", "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJwb2xhcmlzIiwic3ViIjoicm9vdCIsImlhdCI6MTc3MTU0MTE0NywiZXhwIjoxNzcxNTQ0NzQ3LCJqdGkiOiJlOTU3ZDk0MC00NmNlLTRkNjktOTZhOS01NGU2MmZmNGVkNmIiLCJhY3RpdmUiOnRydWUsImNsaWVudF9pZCI6InJvb3QiLCJwcmluY2lwYWxJZCI6MSwic2NvcGUiOiJQUklOQ0lQQUxfUk9MRTpBTEwifQ.VJvuFGrdzItUbSYzlqn9GSTQQaUPBmqQ621c2c8IyLV8QOjnFsRZCpLNb0DVLIuUKennFOQ4i6_mcnVz_8xDYXNJglXqoIjUpiwVH-NII0mzm1d9QATeOCez7Ql9bozkAbbepvytQGe0fux4HOAqV7rYcNQqvkyOe_-rZSzhWlxCMVBOtvmUQxo6U6Ih_dQo2C3WeS1Ez3-VokhneTha8yHaRD7lSLWzKOBfpswMaMsefuiDGgu5AsU3M7MLzWymmL6sj4wUPKBRABc_1sZ7knZxg4mxIvCwVxsX7K7b_yBTTuZ2rb7JVp3n6M040QFd8YFJ_CtKJKLdnHDxV9mymA")

                .config("spark.sql.catalog.polaris.warehouse", "analytics_catalog")
                .config("spark.sql.catalog.polaris.header.Polaris-Realm", "POLARIS")

                // --- 2. RustFS (S3) Connection Details ---
                .config("spark.sql.catalog.polaris.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                .config("spark.sql.catalog.polaris.s3.endpoint", "http://192.168.1.222:9000")
                .config("spark.sql.catalog.polaris.s3.path-style-access", "true")
                .config("spark.sql.catalog.polaris.s3.access-key-id", "polaris_root")
                .config("spark.sql.catalog.polaris.s3.secret-access-key", "polaris_pass")
                .config("spark.sql.catalog.polaris.s3.region", "us-west-2")

                // Add or update these specific lines in your SparkSession builder
                .config("spark.sql.catalog.polaris.client.region", "us-west-2")
                .config("spark.sql.catalog.polaris.s3.region", "us-west-2")

                // --- 3. Authorization Header Fix ---
                // If you are using the 'root' principal directly, sometimes Polaris
                // requires the Bearer prefix if the client doesn't handle OAuth2 automatically.
                // Try adding this if the error persists:
                .config("spark.sql.catalog.polaris.header.Authorization", "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJwb2xhcmlzIiwic3ViIjoicm9vdCIsImlhdCI6MTc3MTU0MTE0NywiZXhwIjoxNzcxNTQ0NzQ3LCJqdGkiOiJlOTU3ZDk0MC00NmNlLTRkNjktOTZhOS01NGU2MmZmNGVkNmIiLCJhY3RpdmUiOnRydWUsImNsaWVudF9pZCI6InJvb3QiLCJwcmluY2lwYWxJZCI6MSwic2NvcGUiOiJQUklOQ0lQQUxfUk9MRTpBTEwifQ.VJvuFGrdzItUbSYzlqn9GSTQQaUPBmqQ621c2c8IyLV8QOjnFsRZCpLNb0DVLIuUKennFOQ4i6_mcnVz_8xDYXNJglXqoIjUpiwVH-NII0mzm1d9QATeOCez7Ql9bozkAbbepvytQGe0fux4HOAqV7rYcNQqvkyOe_-rZSzhWlxCMVBOtvmUQxo6U6Ih_dQo2C3WeS1Ez3-VokhneTha8yHaRD7lSLWzKOBfpswMaMsefuiDGgu5AsU3M7MLzWymmL6sj4wUPKBRABc_1sZ7knZxg4mxIvCwVxsX7K7b_yBTTuZ2rb7JVp3n6M040QFd8YFJ_CtKJKLdnHDxV9mymA")

                .getOrCreate();


        // Create Namespace
        spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.demo");

        // Create Table
//        spark.sql("CREATE TABLE polaris.demo.users (id INT, name STRING, age INT ) USING iceberg ");

        // Insert Data
        spark.sql("INSERT INTO polaris.demo.users VALUES (1, 'Alice', 30), (2, 'Bob', 25) ");

        // Query Data
        spark.sql("SELECT * FROM polaris.demo.users").show();


        spark.sql("show catalogs").show();
        spark.sql("SHOW NAMESPACES IN polaris").show();

        spark.sql("USE polaris").show();
        spark.sql("SHOW NAMESPACES").show();

        spark.sql("SHOW TABLES IN polaris.demo").show();


        spark.stop();
    }
}
