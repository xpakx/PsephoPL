package io.github.xpakx.psephopl;

import io.github.xpakx.psephopl.utils.GminasDataTransformer;
import org.apache.spark.ml.linalg.DenseMatrix;
import io.github.xpakx.psephopl.utils.DataLoader;
import io.github.xpakx.psephopl.utils.DegurbaTransformer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.stat.Correlation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;
import static io.github.xpakx.psephopl.utils.ColumnFunctions.*;

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
        DataLoader loader = new DataLoader(session);

        Dataset<Row> elections2015ByGminasDataSet = loader.loadFromCsv("2015-gl-lis-gm.csv");
        Dataset<Row> elections2015ByElectoralDistrictDataSet = loader.loadFromCsv("2015-gl-lis-okr.csv");
        Dataset<Row> degurbaDataSet = loader.loadFromCsv("DGURBA_PT_2014.csv");

        degurbaDataSet = prepareDegurbaDataSet(degurbaDataSet);
        elections2015ByGminasDataSet = prepareElectionsByGminasDataSet(elections2015ByGminasDataSet);

        Dataset<Row> gminasWithDegurba = elections2015ByGminasDataSet
                .join(
                        degurbaDataSet,
                        elections2015ByGminasDataSet.col("TERYT").equalTo(degurbaDataSet.col("TERC")),
                        "inner"
                );
        gminasWithDegurba = gminasWithDegurba.withColumn("DGURBA_INT", col("DGURBA").cast("integer"));

        Dataset<Row> df = gminasWithDegurba
                .select(col("DGURBA_INT"), col("PiS%"), col("PO%"), col("Razem%"),
                        col("Korwin%"), col("PSL%"), col("SLD%"), col("Kukiz%"),
                        col("N%"));

        VectorAssembler degurbaVectorAssembler = new VectorAssembler();
        degurbaVectorAssembler.setInputCols(new String[]{"DGURBA_INT", "PiS%", "PO%", "Razem%", "Korwin%", "PSL%", "SLD%", "Kukiz%", "N%"});
        degurbaVectorAssembler.setOutputCol("features");
        Dataset<Row> newDataSet = degurbaVectorAssembler.transform(df);

        Row corr = Correlation.corr(newDataSet, "features", "pearson").head();

        String[] tst = new String[]{"DGUR", "PiS", "PO", "Raz", "Kor", "PSL", "SLD", "Kuk", "N"};
        DenseMatrix corrMatrix = (DenseMatrix) corr.get(0);
        System.out.print("      ");
        for(int i=0; i<9; i++ ){
            System.out.printf("%5s ", tst[i]);
        }
        System.out.println();
        for(int i=0; i<9; i++) {
            System.out.printf("%5s ", tst[i]);
            for(int j=0; j<9; j++ ){
                System.out.printf("%5.2f ", corrMatrix.apply(i, j));
            }
            System.out.println();
        }
    }

    private Dataset<Row> prepareDegurbaDataSet(Dataset<Row> degurbaDataSet) {
        return DegurbaTransformer.of(degurbaDataSet)
                .filterByCountry("PL")
                .transformToTERCtoDEGURBATable()
                .get();
    }

    private Dataset<Row> prepareElectionsByGminasDataSet(Dataset<Row> elections2015ByGminasDataSet) {
        return GminasDataTransformer.of(elections2015ByGminasDataSet)
                .transformVotesColumn("Głosy ważne", "TOTAL_VOTES")
                .transformVotesColumn("1 - Komitet Wyborczy Prawo i Sprawiedliwość", "PiS")
                .calculateVoteShareForParty("PiS", "TOTAL_VOTES")
                .transformVotesColumn("2 - Komitet Wyborczy Platforma Obywatelska RP", "PO")
                .calculateVoteShareForParty("PO", "TOTAL_VOTES")
                .transformVotesColumn("3 - Komitet Wyborczy Partia Razem", "Razem")
                .calculateVoteShareForParty("Razem", "TOTAL_VOTES")
                .transformVotesColumn("4 - Komitet Wyborczy KORWiN", "Korwin")
                .calculateVoteShareForParty("Korwin", "TOTAL_VOTES")
                .transformVotesColumn("5 - Komitet Wyborczy Polskie Stronnictwo Ludowe", "PSL")
                .calculateVoteShareForParty("PSL", "TOTAL_VOTES")
                .transformVotesColumn("6 - Koalicyjny Komitet Wyborczy Zjednoczona Lewica SLD+TR+PPS+UP+Zieloni", "SLD")
                .calculateVoteShareForParty("SLD", "TOTAL_VOTES")
                .transformVotesColumn("7 - Komitet Wyborczy Wyborców „Kukiz'15”", "Kukiz")
                .calculateVoteShareForParty("Kukiz", "TOTAL_VOTES")
                .transformVotesColumn("8 - Komitet Wyborczy Nowoczesna Ryszarda Petru", "N")
                .calculateVoteShareForParty("N", "TOTAL_VOTES")
                .get();
    }
}
