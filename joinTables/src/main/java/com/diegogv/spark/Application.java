package com.diegogv.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

import java.util.Properties;

public class Application {
    public static void main(String args[]) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkSession spark = SparkSession.builder()
                .master("local")
                .getOrCreate();

        String dbUrl = "jdbc:postgresql://localhost:5432/data";

        Properties props = new Properties();
        props.setProperty("driver", "org.postgresql.Driver");
        props.setProperty("user", "diego");
        props.setProperty("password", "postgres");

        Dataset<Row> users = spark.read()
                .jdbc(dbUrl, "users", props);
        Dataset<Row> shoppings = spark.read()
                .jdbc(dbUrl, "shoppings", props);

        users.show();
        shoppings.show();

        Dataset<Row> joinedTable = users.join(shoppings, "user_id");
        joinedTable.printSchema();
        joinedTable.show();

        joinedTable.write()
                .mode(SaveMode.Overwrite)
                .jdbc(dbUrl, "user_shoppings", props);
    }
}
