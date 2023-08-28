package org.example;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class App2Test {

    private static SparkSession sparkSession;

    @BeforeAll
    public static void setUp() {
        sparkSession = SparkSession.builder()
                .appName("TestApp")
                .master("local[2]")
                .getOrCreate();
    }

    @AfterAll
    public static void tearDown() {
        if(sparkSession != null) {
            sparkSession.stop();
        }
    }

    @Test
    @DisplayName("Test Schema Comparison")
    public void testApp2() {
        System.out.println("Running schema comparison test...");

        String excpeted_Outputpath = "/home/naveen/part-00000-c41b8635-cc49-47a5-b120-73934087f549-c000.snappy.parquet";

        StructField[] fields = {
                DataTypes.createStructField("ID", DataTypes.StringType, true),
                DataTypes.createStructField("Area", DataTypes.StringType, true),
                DataTypes.createStructField("City", DataTypes.StringType, true),
                DataTypes.createStructField("Restaurant", DataTypes.StringType, true),
                DataTypes.createStructField("Price", DataTypes.FloatType, true),
                DataTypes.createStructField("Avg_ratings", DataTypes.FloatType, true),
                DataTypes.createStructField("Total_ratings", DataTypes.FloatType, true),
                DataTypes.createStructField("Food_type", DataTypes.StringType, true),
                DataTypes.createStructField("Address", DataTypes.StringType, true),
                DataTypes.createStructField("Delivery_time", DataTypes.IntegerType, true)
        };

        StructType expected_schema = new StructType(fields);
        Dataset<Row> paquet_Data = sparkSession.read().parquet(excpeted_Outputpath);

        //.long expected_count = 8680;

        StructType actualSchema = paquet_Data.schema();

        assertEquals(expected_schema, actualSchema, "test");
        System.out.println("Expected Schema: " + new StructType(fields));


    }

    @Test
    public void testCount() {

        System.out.println("Running count comparison test...");

        String testCsvPath = "/home/naveen/swiggy.csv";

        String excpeted_Outputpath = "/home/naveen/part-00000-c41b8635-cc49-47a5-b120-73934087f549-c000.snappy.parquet";

        Dataset<Row> csvData = sparkSession.read().option("header", "true").csv(testCsvPath);


        Dataset<Row> parquetData = sparkSession.read().parquet(excpeted_Outputpath);



        long actualCount = parquetData.count();

        long csvCount = csvData.count();

        assertEquals(actualCount,csvCount,"test failed");

        System.out.println("test Case successfully executed");
    }

}
