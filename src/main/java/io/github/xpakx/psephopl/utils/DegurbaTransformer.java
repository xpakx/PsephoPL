package io.github.xpakx.psephopl.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;
import static io.github.xpakx.psephopl.utils.ColumnFunctions.*;

public class DegurbaTransformer {
    Dataset<Row> dataset;

    public static DegurbaTransformer of(Dataset<Row> dataset) {
        return new DegurbaTransformer(dataset);
    }

    private DegurbaTransformer(Dataset<Row> dataset) {
        this.dataset = dataset;
    }

    public DegurbaTransformer transformToTERCtoDEGURBATable() {
        return new DegurbaTransformer(getTERCTransformation());
    }

    private Dataset<Row> getTERCTransformation() {
        return dataset
                .withColumn("TERC", transformLAU2ToTERC("NSI"))
                .withColumnRenamed("DGURBA_CLA", "DGURBA")
                .drop("CNTR_CODE")
                .drop("NSI")
                .drop("LAU_CODE");
    }

    public DegurbaTransformer filterByCountry(String countryCode) {
        return new DegurbaTransformer(getCountryFilter(countryCode));
    }

    private Dataset<Row> getCountryFilter(String countryCode) {
        return dataset
                .filter(col("CNTR_CODE").equalTo(countryCode));
    }

    public Dataset<Row> get() {
        return dataset;
    }



}
