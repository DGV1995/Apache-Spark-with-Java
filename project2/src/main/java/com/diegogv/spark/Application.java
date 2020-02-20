package com.diegogv.spark;

public class Application {
    public static void main(String args[]) {
        // InferCSVSchema csvParser = new InferCSVSchema();
        // csvParser.printSchema();

        // DefineCSVSchema definedSchemaParser = new DefineCSVSchema();
        // definedSchemaParser.printDefinedSchema();

        JSONLinesParser jsonParser = new JSONLinesParser();
        jsonParser.parseJsonLines();
    }
}
