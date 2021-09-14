package io.github.xpakx.psephopl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

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

        Dataset<Row> elections2005DataSet = session.read()
                .format("csv")
                .option("header", "true")
                .load("src/main/resources/2015-gl-lis-okr.csv");

        elections2005DataSet.show(5);
    }
}
