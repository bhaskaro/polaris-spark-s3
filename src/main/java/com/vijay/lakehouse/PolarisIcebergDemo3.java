package com.vijay.lakehouse;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * Author : bhask
 * Created : 02-19-2026
 */
public class PolarisIcebergDemo3 {

    public static void main(String[] args) {

        System.setProperty("aws.region", "us-west-2");
        String DOCKER_HOST_IP = "192.168.1.222";


        // 1. Load configuration from environment variables (or use defaults)
        String hostIp = getEnv("POLARIS_HOST", DOCKER_HOST_IP);
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
        polarisConfigs.put("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");

        // 3. Initialize Spark with the Config Map
        SparkSession.Builder builder = SparkSession.builder()
                .appName("Polaris-Production-Client")
                .master("local[*]");

        polarisConfigs.forEach(builder::config);
        SparkSession spark = builder.getOrCreate();


        // Create Namespace
        spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.demo");

        // 1. Drop the table if it exists
        spark.sql("DROP TABLE IF EXISTS polaris.demo.users");

        // 2. Create the table
        spark.sql("CREATE TABLE polaris.demo.users (" +
                "id INT, " +
                "name STRING, " +
                "age INT" +
                ") USING iceberg");

        // Create Table
        // spark.sql("CREATE TABLE polaris.demo.users (id INT, name STRING, age INT ) USING iceberg ");

        // Delete a specific user by ID
        spark.sql("DELETE FROM polaris.demo.users WHERE id = 1");

        // Delete all users older than 30
        spark.sql("DELETE FROM polaris.demo.users WHERE age >= 30");

        // truncate table
        spark.sql("TRUNCATE TABLE polaris.demo.users");

        // Insert Data
        spark.sql("INSERT INTO polaris.demo.users VALUES (1, 'Alice', 30), (2, 'Bob', 25) ");

        // Query Data
        spark.sql("SELECT * FROM polaris.demo.users").show();


        spark.sql("show catalogs").show();
        spark.sql("SHOW NAMESPACES IN polaris").show();

        spark.sql("USE polaris").show();
        spark.sql("SHOW NAMESPACES").show();

        spark.sql("SHOW TABLES IN polaris.demo").show();

        // 1. Get the DataFrame
        Dataset<Row> df = spark.sql("SELECT id, name, age FROM polaris.demo.users");

        // 2. Collect the data into a Java List (This brings data to the Driver memory)
        List<Row> rows = df.collectAsList();

        // 3. Iterate
        for (Row row : rows) {
            int id = row.getInt(0);               // Access by index
            String name = row.getAs("name");      // Access by column name
            int age = row.getInt(row.fieldIndex("age"));

            System.out.println("User: " + name + " (ID: " + id + ", age : " + age + ")");
        }


        // 1. Prepare a List to hold your data in memory
        List<Row> userRows = new ArrayList<>();

        // 2. Loop to generate data
        for (int i = 1; i <= 100; i++) {
            // Adding (id, name, age)
            userRows.add(RowFactory.create(i, "User_" + i, 20 + (i % 50)));
        }

        // 3. Define the structure to match your table
        // Note: Ensure the column names and types match your 'CREATE TABLE' exactly
        StructType userSchema = new StructType()
                .add("id", "int")
                .add("name", "string")
                .add("age", "int");

        // 4. Convert to DataFrame and Append to the Iceberg table in ONE transaction
        spark.createDataFrame(userRows, userSchema)
                .write()
                .format("iceberg")
                .mode("append")
                .save("polaris.demo.users");

        System.out.println("Successfully bulk inserted 100 users.");

        spark.sql("SELECT min(id), max(id), count(*) FROM polaris.demo.users").show();

        // This is an Iceberg-specific administrative command
        spark.sql("CALL polaris.system.expire_snapshots('demo.users')");

        spark.stop();

    }

    private static String getEnv(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value != null) ? value : defaultValue;
    }
}
