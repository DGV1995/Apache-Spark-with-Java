package com.diegogv.spark;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class Application {
    public static void main(String args[]) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        InferCSVSchema csvParser = new InferCSVSchema();
        csvParser.printSchema();

        DefineCSVSchema definedSchemaParser = new DefineCSVSchema();
        definedSchemaParser.printDefinedSchema();

        JSONLinesParser jsonParser = new JSONLinesParser();
        jsonParser.parseJsonLines();
    }
}
