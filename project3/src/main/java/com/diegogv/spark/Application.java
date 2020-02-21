package com.diegogv.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class Application {
    public static void main(String args[]) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkSession spark = SparkSession.builder()
                .appName("Combine 2 Datasets")
                .master("local")
                .getOrCreate();

        Dataset<Row> durhamDF = buildDurhamDataFrame(spark);
        Dataset<Row> philDF = buildPhilParksDataFrame(spark);

        // durhamDF.show(10);
        // philDF.show(10);

        combineDataframes(durhamDF, philDF);
    }

    public static Dataset<Row> buildDurhamDataFrame(SparkSession spark) {
        Dataset<Row> df = spark.read()
                .option("multiline", true)
                .json("src/main/resources/durham-parks.json");

        df = df.withColumn("park_id", functions.concat(df.col("datasetid"), functions.lit("_"), df.col("fields.objectid"), functions.lit("_Durham")))
                .withColumn("park_name", df.col("fields.park_name"))
                .withColumn("city", functions.lit("Durham"))
                .withColumn("address", df.col("fields.address"))
                .withColumn("has_playground", df.col("fields.playground"))
                .withColumn("zipcode", df.col("fields.zip"))
                .withColumn("land_in_acres", df.col("fields.acres"))
                .withColumn("geoX", df.col("geometry.coordinates").getItem(0))
                .withColumn("geoY", df.col("geometry.coordinates").getItem(1))
                .drop("datasetid", "fields", "geometry", "record_timestamp", "recordid");

        return df;
    }

    public static Dataset<Row> buildPhilParksDataFrame(SparkSession spark) {
        Dataset<Row> df =  spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src/main/resources/philadelphia_recreations.csv");

        //df = df.filter(functions.lower(df.col("USE_")).like("%park%"));
        df = df.filter("lower(USE_) like '%park%'")
                .withColumn("park_id", functions.concat(functions.lit("phil_"), df.col("OBJECTID")))
                .withColumnRenamed("ASSET_NAME", "park_name") // get an existing column and rename it
                .withColumn("city", functions.lit("Philadelphia"))
                .withColumnRenamed("ADDRESS", "address")
                .withColumn("has_playground", functions.lit("UNKNOWN"))
                .withColumnRenamed("ZIPCODE", "zipcode")
                .withColumnRenamed("ACREAGE", "land_in_acres")
                .withColumn("geoX", functions.lit("UNKNOWN"))
                .withColumn("geoY", functions.lit("UNKNOWN"))
                .drop("OBJECTID", "SITE_NAME", "CHILD_OF", "TYPE", "USE_", "DESCRIPTION", "SQ_FEET", "ALLIAS",
                        "CHRONOLOGY", "NOTES", "DATE_EDITED", "EDITED_BY", "OCCUPANT", "TENANT", "LABEL");

        return df;
    }

    public static void combineDataframes(Dataset<Row> df1, Dataset<Row> df2) {
        // Match by column names using the unionByName() method
        // If we use just the union() method, it matches the columns based on order.
        Dataset<Row> df = df1.unionByName(df2);
        df.show(500);
        df.printSchema();
        System.out.println("We have " + df.count() + " records.");

        Partition[] partitions = df.rdd().partitions();
        System.out.println("Total number of partitions: " + partitions.length);

        // Repartition the dataframe
        df = df.repartition(5);
        System.out.println("New total number of partitions: " + df.rdd().partitions().length);
    }
}
