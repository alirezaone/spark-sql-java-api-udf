package com.alireza.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class SparkRunner {

    static {
        Logger.getRootLogger().setLevel(Level.WARN);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
    }

}
