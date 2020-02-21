package com.diegogv.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Application {
    public static void main(String args[]) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        ArrayToDataset app = new ArrayToDataset();
        app.start();
    }
}
