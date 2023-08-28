package BasePackage;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class BaseApp {

    public SparkConf conf;
    public SparkSession sparkSession;

    public JavaSparkContext sc;

    long sessionStartTime;

    public static void println(Object obj){
        System.out.println(obj);
    }

    public void init() {
        println("Initializing SparkSession");
        conf = new SparkConf().setAppName(getClass().getName()).setAppName("local[*]");
        sparkSession = SparkSession.builder().config(conf).getOrCreate();
        final SparkContext sparkContext = sparkSession.sparkContext();
        sc = new JavaSparkContext(sparkContext);
        sessionStartTime = System.currentTimeMillis();

    }

    public void close() {
        sparkSession.close();
        println(String.format("Session is closed. Total duration: %.2s seconds", (System.currentTimeMillis()-sessionStartTime)/1000));
    }








}
