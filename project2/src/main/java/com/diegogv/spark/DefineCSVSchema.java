package com.diegogv.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class DefineCSVSchema {
    public void printDefinedSchema() {
        SparkSession spark = SparkSession.builder().
                appName("Complex CVS with a schema to DataFrame")
                .master("local")
                .getOrCreate();

        // Define the schema for the columns
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, true),
                DataTypes.createStructField("product_id", DataTypes.IntegerType, false),
                DataTypes.createStructField("item_name", DataTypes.StringType, false),
                DataTypes.createStructField("publish_date", DataTypes.DateType, true),
                DataTypes.createStructField("url", DataTypes.StringType, false)
        });

        Dataset<Row> df = spark.read()
                .option("header", true)
                .option("multiline", true)
                .option("quote", "^")
                .option("sep", ";")
                .option("dateFormat", "M/d/y")
                .schema(schema)
                .csv("src/main/resources/amazonProducts.txt");

        df.show();
        df.printSchema();
    }
}
