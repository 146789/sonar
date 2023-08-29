package org.example;

import BasePackage.BaseApp;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.*;


import java.util.Arrays;

public class DataValidation extends BaseApp {

    public String paquetPath =  null;

    public String outputPath = null;

    public void start() {

        init();
        println("----------------------------reading_parquet_file-------------------------");
        Dataset<Row> df = sparkSession.read().parquet(outputPath);

        println("-------------------------------checking missing values--------------------------");
        Dataset<Row> missing_value = df.select(
                Arrays.stream(df.columns()).map(column -> when(col(column).isNull(), column).otherwise(lit(null)) ).toArray(Column[]::new)
        );

        if(missing_value.count() > 0) {

            println("------------------------------found missing values----------------------------");
            missing_value.show();
        } else {
            println("not found any missing value");
        }

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
