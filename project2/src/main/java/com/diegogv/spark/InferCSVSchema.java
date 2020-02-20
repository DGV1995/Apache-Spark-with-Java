package com.diegogv.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class InferCSVSchema {
    public void printSchema() {
        SparkSession spark = SparkSession.builder()
                .appName("Complex CSV to Dataframe")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .option("sep", ";")
                .option("multiline", true)
                .option("quote", "^")
                .option("dateFormat", "M/d/y")
                .csv("src/main/resources/amazonProducts.txt");

        System.out.println("Excerpt of the dataframe content:");
        df.show();
        //df.show(10, 90); // truncate after 93 chars
        System.out.println("Dataframe Schema:");
        df.printSchema();
    }
}
