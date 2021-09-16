package io.github.xpakx.psephopl.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataLoader
{
    private final SparkSession session;
    private final String path = "src/main/resources/";

    public DataLoader(SparkSession session) {
        this.session = session;
    }

    public Dataset<Row>  loadFromCsv(String name) {
        return session.read()
                .format("csv")
                .option("header", "true")
                .load(path.concat(name));
    }


}
