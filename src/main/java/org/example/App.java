package org.example;

import org.apache.spark.sql.SparkSession;

import java.util.Scanner;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {

        SparkSession session = SparkSession.builder().master("local[*]").appName("mySpark").getOrCreate();
        System.out.println();
        System.out.println();
        System.out.println("----------------------------------------------");
        System.out.println("Spark Session object " + session);
        System.out.println("----------------------------------------------");
        System.out.println();
        session.close();
    }
}
