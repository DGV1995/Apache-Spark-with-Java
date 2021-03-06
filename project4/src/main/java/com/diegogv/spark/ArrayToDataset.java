package com.diegogv.spark;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class ArrayToDataset {
    public void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Array to Dataset<String>")
                .master("local")
                .getOrCreate();

        String[] stringList = new String[] {"Banana", "Car", "Glass", "Banana", "Computer", "Car"};
        List<String> data = Arrays.asList(stringList);

        Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
        // Dataset to Dataframe
        /*Dataset<Row> df = ds.toDF();
        // Dataframe to Dataset
        ds = df.as(Encoders.STRING());

        ds.show();
        ds.printSchema();

        Dataset<Row> df2 = df.groupBy("value").count();
        df2.show();*/

        // Map function
        Dataset<String> ds2 = ds.map((MapFunction<String, String>) row -> "word: " + row, Encoders.STRING());
        ds2.show();

        // Reduce function
        String finalValue = ds.reduce((ReduceFunction<String>) (row1, row2) -> row1 + row2);
        System.out.println(finalValue);
    }
}
