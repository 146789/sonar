package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.*;

public class AveragePrice {

    public static void main(String[] args) {

        if(args.length < 1) {

            System.out.println("uage of swiggy csv file");
            System.exit(1);
        }

        String filePath =  args[0];
        SparkSession session =  SparkSession.builder().appName("AveragePrice").master("local[*]").getOrCreate();

        Dataset<Row> restaurants = session.read().option("header", "true").csv(filePath);

        restaurants = restaurants.withColumn("Price", restaurants.col("Price").cast("double"));

        Dataset<Row> averagePrice  = restaurants.groupBy("City").agg(round(avg("Price")).alias("AveragePriceinCity")).orderBy(desc("AveragePriceinCity"));

        averagePrice.show();

        averagePrice.write().mode("overwrite").parquet(args[1] + "/averagePrice/");

        System.out.println("File Saved successfully to the path : "  + args[1] + "/averagePrice/");

    }
}
