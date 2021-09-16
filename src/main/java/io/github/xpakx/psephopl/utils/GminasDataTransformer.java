package io.github.xpakx.psephopl.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static io.github.xpakx.psephopl.utils.ColumnFunctions.colToInt;
import static org.apache.spark.sql.functions.col;

public class GminasDataTransformer {
    Dataset<Row> dataset;

    public static GminasDataTransformer of(Dataset<Row> dataset) {
        return new GminasDataTransformer(dataset);
    }

    private GminasDataTransformer(Dataset<Row> dataset) {
        this.dataset = dataset;
    }

    public GminasDataTransformer transformVotesColumn(String from, String to) {
        return  new GminasDataTransformer(getVotesColumnTransformation(from, to));
    }

    private Dataset<Row> getVotesColumnTransformation(String from, String to) {
        String[] singletonOfColumnName = new String[1];
        singletonOfColumnName[0] = to;
        return dataset
                .withColumn(to, colToInt(from))
                .drop(from)
                .na().fill(0, singletonOfColumnName);
    }

    public GminasDataTransformer calculateVoteShareForParty(String party, String total) {
        return  new GminasDataTransformer(getVoteShareTransformation(party, total));
    }

    private Dataset<Row> getVoteShareTransformation(String party, String total) {
        return dataset
                .withColumn(party.concat("%"), col(party).divide(col(total)));
    }

    public Dataset<Row> get() {
        return dataset;
    }
}
