package org.example;

import BasePackage.BaseApp;
import org.apache.spark.sql.*;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;



public class DataValidation extends BaseApp {

    public String paquetPath =  null;

    public String outputPath = null;

    public void start() {
        init();
        println("----------------------------reading_parquet_file-------------------------");
        Dataset<Row> df = sparkSession.read().parquet(outputPath);

        println("----------------------------printing parquet data--------------------------");
        df.show();

        println("-------------------------------checking missing values--------------------------");
//        Dataset<Row> missing_value = df.select(
//                Arrays.stream(df.columns()).map(column -> col(column).isNull().alias(column) ).toArray(Column[]::new)
//        );

        long missingCount = Arrays.stream(df.columns()).map(colName -> {
                Column colExpr = col(colName);
                long colMissingCount = df.filter(colExpr.isNull()).count();
                if(colMissingCount > 0) {
                    println("Missing column name " + colName + ": " + colMissingCount);
                }
                return colMissingCount;

        }).reduce(0L,Long::sum);
        /*for(String colName: df.columns()) {

            long colmissingCount = df.filter(col(colName).isNull()).count();
            missingCount += colmissingCount;

            if(colmissingCount > 0) {

                println("Missing colum name " + colName + ": " + colmissingCount);
            }
        }*/



        if(missingCount > 0) {

            println("the count of missing values are : " +missingCount);


//            println("------------------------------found missing values----------------------------");
//            println("--------------count of missing values-----------------");
//            println(missing_value.count());
//            println("\n");
//            println("\n");
//            missing_value.show();
        } else {
            println("not found any missing value");
        }


            println("-------invalid values---------------------------");
            Dataset<Row> invalid_values = df.filter((col("Price").lt(0)).or((col("Avg_ratings").lt(0))).or((col("Total_ratings").lt(0))));
            if (invalid_values.count() > 0) {
                invalid_values.show();
            } else {
                println("No value invalid values");
            }

            println("--------------------check distinct values in food types-------------------");
            Dataset<Row> distinctFoodtypes = df.select("Food_type").distinct();
            distinctFoodtypes.show();

            println("----------------------checkunique----------------------------------");
            Dataset<Row> uniqueValuees = df.groupBy("ID").agg(count("*").alias("count")).filter(col("count").gt(1));
            uniqueValuees.show();

            println("--------------------------checking-------------------------------------");
            Dataset<Row> checkDeliveryTime = df.filter(col("Delivery_time").lt(0));
            checkDeliveryTime.show();
        }

    public static void main(String[] args) {

        if(args.length < 1){

            println("usage of parquet file");
            System.exit(0);
        }

        DataValidation dv = new DataValidation();
        dv.paquetPath = args[0];
        dv.outputPath = args[1];
        dv.start();
        dv.close();
    }
}
