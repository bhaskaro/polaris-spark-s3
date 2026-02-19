package com.vijay.lakehouse;

import org.apache.spark.sql.SparkSession;
import java.util.HashMap;
import java.util.Map;

public class PolarisIcebergDemo2 {

    public static void main(String[] args) {
        // 1. Load configuration from environment variables (or use defaults)
        String hostIp = getEnv("POLARIS_HOST", "192.168.1.222");
        String clientId = getEnv("POLARIS_CLIENT_ID", "root");
        String clientSecret = getEnv("POLARIS_CLIENT_SECRET", "s3cr3t");
        String s3AccessKey = getEnv("S3_ACCESS_KEY", "polaris_root");
        String s3SecretKey = getEnv("S3_SECRET_KEY", "polaris_pass");
        String region = getEnv("AWS_REGION", "us-west-2");

        // 2. Build the Catalog Configuration Map
        Map<String, String> polarisConfigs = new HashMap<>();

        // --- Polaris Connection & OAuth ---
        polarisConfigs.put("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog");
        polarisConfigs.put("spark.sql.catalog.polaris.type", "rest");
        polarisConfigs.put("spark.sql.catalog.polaris.uri", String.format("http://%s:8181/api/catalog", hostIp));
        polarisConfigs.put("spark.sql.catalog.polaris.oauth2-server-uri", String.format("http://%s:8181/api/catalog/v1/oauth/tokens", hostIp));
        polarisConfigs.put("spark.sql.catalog.polaris.credential", String.format("%s:%s", clientId, clientSecret));
        polarisConfigs.put("spark.sql.catalog.polaris.warehouse", "analytics_catalog");
        polarisConfigs.put("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL");
        polarisConfigs.put("spark.sql.catalog.polaris.header.Polaris-Realm", "POLARIS");

        // --- S3 / RustFS Storage Layer ---
        polarisConfigs.put("spark.sql.catalog.polaris.io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        polarisConfigs.put("spark.sql.catalog.polaris.s3.endpoint", String.format("http://%s:9000", hostIp));
        polarisConfigs.put("spark.sql.catalog.polaris.s3.path-style-access", "true");
        polarisConfigs.put("spark.sql.catalog.polaris.s3.access-key-id", s3AccessKey);
        polarisConfigs.put("spark.sql.catalog.polaris.s3.secret-access-key", s3SecretKey);
        polarisConfigs.put("spark.sql.catalog.polaris.client.region", region);
        polarisConfigs.put("spark.sql.catalog.polaris.s3.region", region);

        // 3. Initialize Spark with the Config Map
        SparkSession.Builder builder = SparkSession.builder()
                .appName("Polaris-Production-Client")
                .master("local[*]");

        polarisConfigs.forEach(builder::config);
        SparkSession spark = builder.getOrCreate();

        try {
            runDemoQueries(spark);
        } finally {
            spark.stop();
        }
    }

    private static void runDemoQueries(SparkSession spark) {
        spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.demo");
        spark.sql("CREATE TABLE IF NOT EXISTS polaris.demo.users (id INT, name STRING, age INT) USING iceberg");
        spark.sql("INSERT INTO polaris.demo.users VALUES (1, 'Alice', 30), (2, 'Bob', 25)");
        spark.sql("SELECT * FROM polaris.demo.users").show();
    }

    private static String getEnv(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value != null) ? value : defaultValue;
    }
}