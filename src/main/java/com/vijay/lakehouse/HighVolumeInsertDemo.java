package com.vijay.lakehouse;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 *
 * Author : bhask
 * Created : 02-19-2026
 */
public class HighVolumeInsertDemo {

    public static void main(String[] args) throws InterruptedException {

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

        // ... (Insert your existing SparkSession builder here) ...
        SparkSession spark = builder.getOrCreate();

        // --- FIX 1: Pre-create the table schema ---
        // This ensures 'heavy_table' exists before any thread tries to write or count.
        spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.demo");
        spark.sql("CREATE TABLE IF NOT EXISTS polaris.demo.heavy_table (" +
                "id LONG, name STRING, payload_1 STRING, payload_2 STRING, created_at TIMESTAMP) " +
                "USING iceberg");

        StructType schema = new StructType()
                .add("id", "long")
                .add("name", "string")
                .add("payload_1", "string")
                .add("payload_2", "string")
                .add("created_at", "timestamp");

        int threadCount = 5;
        int recordsPerThread = 20000;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int batchId = i;
            executor.submit(() -> {
                try {
                    List<Row> data = new ArrayList<>();
                    for (int j = 0; j < recordsPerThread; j++) {
                        data.add(RowFactory.create(
                                (long) (batchId * recordsPerThread + j),
                                "User_" + j,
                                "Large blob data 1...",
                                "Large blob data 2...",
                                new java.sql.Timestamp(System.currentTimeMillis())
                        ));
                    }

                    System.out.println("Thread " + batchId + " starting write...");
                    // Note: use .save("polaris.demo.heavy_table") or .insertInto(...)
                    spark.createDataFrame(data, schema)
                            .write()
                            .format("iceberg")
                            .mode("append")
                            .save("polaris.demo.heavy_table");

                    System.out.println("Thread " + batchId + " finished.");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        // --- FIX 2: Wait for workers to finish ---
        executor.shutdown();
        boolean finished = executor.awaitTermination(15, TimeUnit.MINUTES);

        if (finished) {
            System.out.println("All threads complete. Querying count...");
            spark.sql("SELECT count(*) FROM polaris.demo.heavy_table").show();
        } else {
            System.err.println("Timeout reached before threads finished!");
        }

        spark.stop();
    }

    private static String getEnv(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value != null) ? value : defaultValue;
    }
}
