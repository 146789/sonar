package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class App3 {

    public static void main(String[] args) {

        if(args.length < 1) {

            System.out.println("uage of swiggy csv file");
            System.exit(1);
        }

        String filePath =  args[0];
        SparkSession session =  SparkSession.builder().appName("App3").master("yarn").getOrCreate();

        Dataset<Row> df = session.read().parquet("file:///"+filePath);


        df.printSchema();
        df.show();

        Dataset<Row> filterData = df.filter(df.col("Price").geq(50.0));

        Dataset<Row> selectedData = filterData.select("Area", "City", "Restaurant", "Price");

        Dataset<Row> aggregatedData =df.groupBy("City").agg(avg("Avg_ratings").alias("AVG_ratings"), sum("Total_ratings").alias("Total_ratings")).orderBy(desc("Total_ratings"));

        filterData.show();

        selectedData.show();

        aggregatedData.show();

//        Row max = (Row) df.agg(max("Price").alias("Max_Price"));
//
//        System.out.println("max ---->   " + max);

        filterData.write().mode("overwrite").parquet(args[1]);
        System.out.println("file write successfull");
        session.stop();
    }
}
