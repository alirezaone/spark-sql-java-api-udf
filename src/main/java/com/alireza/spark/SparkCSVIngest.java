package com.alireza.spark;

import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper;
import org.apache.commons.math3.stat.descriptive.summary.Product;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import scala.Product$class;
import scala.Product1;

import static org.apache.spark.sql.types.DataTypes.StringType;

public class SparkCSVIngest extends SparkRunner {

    private static final SparkSession spark = SparkSession.builder()
            .master("local[*]")
            .getOrCreate();

    /* 1. raw data csv = spark.read():
       ----------------------------*/
    public static void main(String[] args) {
        Dataset<Row> csv = spark
                .read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src/main/java/com/alireza/spark/dataex.csv");

        csv.show();
        csv.printSchema();

        /* 2. filtered data agg = csv.filter() then grouped by size & group id - averaged by price:
           --------------------------------------------------------------------------------------*/
        Dataset<Row> agg = csv.filter((FilterFunction<Row>) row -> row
                .getAs("product")
                .equals("Coca Cola"))
                .groupBy("size","group id")
                .agg(functions.avg("price").as("averagePrice"));

        agg.show();
        agg.printSchema();

        /* A User-defined function to tack readableID
           ---------------------------------------*/
        spark.udf().register("readableId", new UDF1<Integer, String>() {
            @Override
            public String call(Integer integer) throws Exception {
                return Integer.toString(integer);
            }
        }, StringType);

        /* 3. final data ordered = agg.withcolumn() for readableId - and sort by the averagePrice:
           ------------------------------------------------------------------------------------*/
        Dataset<Row> ordered = agg
                .withColumn("readableId", functions.callUDF("readableId", functions.col("group id")))
                .orderBy(functions.col("averagePrice").desc());

         ordered.show();
         ordered.printSchema();

         ordered.explain(true);
    }

}
