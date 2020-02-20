package com.diegogv.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

import static org.apache.spark.sql.functions.*; // concat, lit, etc

public class Application {
    public static void main(String args[]) {
        // Create session
        SparkSession spark = new SparkSession.Builder().appName("CSV to DB").master("local").getOrCreate();
        // Get data
        Dataset<Row> df = spark.read()
                .option("header", true)
                .csv("src/main/resources/name_and_comments.txt");

        df.show();

        // Transformation
        //df = df.withColumn("full_name", concat("first_name", lit(", "), "last_name"));
        df = df.filter(df.col("comment").rlike("\\d+"))
                .orderBy(df.col("last_name").asc()); // comments that have numbers within them

        // Save the DataFrame into a database
        String urlConnection = "jdbc:postgresql://localhost/course_data";
        Properties props = new Properties();
        props.setProperty("user", "postgres");
        props.setProperty("password", "postgres");
        props.setProperty("driver", "org.postgresql.Driver");

        df.write()
                .mode(SaveMode.Overwrite)
                .jdbc(urlConnection, "project1", props);
    }
}
