package io.github.xpakx.psephopl;

import org.apache.spark.ml.stat.Correlation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class App 
{
    public static void main( String[] args )
    {
        App app = new App();
        app.start();
    }

    private void start() {
        SparkSession session = SparkSession.builder()
                .master("local")
                .getOrCreate();

        Dataset<Row> elections2015ByGminasDataSet = session.read()
                .format("csv")
                .option("header", "true")
                .load("src/main/resources/2015-gl-lis-gm.csv");

        Dataset<Row> elections2015ByElectoralDistrictDataSet = session.read()
                .format("csv")
                .option("header", "true")
                .load("src/main/resources/2015-gl-lis-okr.csv");

        Dataset<Row> degurbaDataSet = session.read()
                .format("csv")
                .option("header", "true")
                .load("src/main/resources/DGURBA_PT_2014.csv");
        degurbaDataSet = degurbaDataSet
                .filter(degurbaDataSet.col("CNTR_CODE").equalTo("PL"));
        degurbaDataSet = degurbaDataSet
                .withColumn("TERC",
                        concat(
                                substring(degurbaDataSet.col("NSI"),2,2),
                                substring(degurbaDataSet.col("NSI"),6,4)
                        ))
                .withColumnRenamed("DGURBA_CLA", "DGURBA")
                .drop("CNTR_CODE")
                .drop("NSI")
                .drop("LAU_CODE");


        System.out.println(degurbaDataSet.count());
        Dataset<Row> gminasWithDegurba = elections2015ByGminasDataSet
                .join(
                        degurbaDataSet,
                        elections2015ByGminasDataSet.col("TERYT").equalTo(degurbaDataSet.col("TERC")),
                        "inner"
                );
        gminasWithDegurba.show(5);
        System.out.println(gminasWithDegurba.count());
    }
}
