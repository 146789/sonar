package org.example;

import BasePackage.BaseApp;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


import static org.apache.spark.sql.functions.*;
public class App2 extends BaseApp {
    public String csvPath=null;
    public String  outputpath= null;

    public void Start() {
        init();

        println("---------------------------reading csv file ----------------------------");
        Dataset<Row> df = sparkSession.read().option("header", "true").csv(csvPath);

        println("---------------------------Manipulation columns-------------------------");
        df =df.withColumn("Price", col("Price").cast("float"))
                        .withColumnRenamed("Avg ratings","Avg_ratings")
                                .withColumnRenamed("Total ratings", "Total_ratings")
                                        .withColumnRenamed("Delivery time", "Delivery_time")
                                                .withColumn("Avg_ratings" ,col("Avg_ratings").cast("float"))
                                                        .withColumn("Total_ratings", col("Total_ratings").cast("float"))
                                                                .withColumn("Delivery_time", col("Delivery_time").cast("int"))
                                                                        .withColumnRenamed("Food type", "Food_type");


        println("Total number of records in the file is " + df.count());

        println("-------------------------------printing schema-----------------------");
        df.printSchema();
        println("------------------------------printing data--------------------------");
        df.show();

        println("-----------------------------writing to a file--------------------------");
        df.write().mode("overwrite").parquet(outputpath);
        println("file successfully written to the given path : " + outputpath);
    }

    public static void main(String[] args) {

        if(args.length < 1) {

            System.out.println("uage of swiggy csv file");
           System.exit(1);
        }

        App2 app2 = new App2();
        app2.csvPath = args[0];
        app2.outputpath = args[1];
        app2.Start();
        app2.close();

    }



}


