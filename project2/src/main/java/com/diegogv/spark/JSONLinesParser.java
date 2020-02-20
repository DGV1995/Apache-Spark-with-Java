package com.diegogv.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JSONLinesParser {
    public void parseJsonLines() {
        SparkSession spark = SparkSession.builder()
                .appName("JSON Lines to Dataframe")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .json("src/main/resources/simple.json");

        df.show();
        df.printSchema();

        Dataset<Row> df2 = spark.read()
                .option("multiline", true)
                .json("src/main/resources/multiline.json");

        df2.show();
        df2.printSchema();
    }
}
