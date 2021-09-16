package io.github.xpakx.psephopl.utils;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public class DegurbaTransformer {



    public Dataset<Row> transformToTERCtoDEGURBATable(Dataset<Row> dataset) {
        return dataset
                .withColumn("TERC", transformLAU2ToTERC("NSI"))
                .withColumnRenamed("DGURBA_CLA", "DGURBA")
                .drop("CNTR_CODE")
                .drop("NSI")
                .drop("LAU_CODE");
    }

    public Dataset<Row> filterByCountry(Dataset<Row> dataset, String countryCode) {
        return dataset
                .filter(col("CNTR_CODE").equalTo(countryCode));
    }


    private Column transformLAU2ToTERC(String columnName) {
        return concat(
                substring(col(columnName),2,2),
                substring(col(columnName),6,4)
        );
    }
}
