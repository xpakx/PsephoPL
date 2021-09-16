package io.github.xpakx.psephopl;

import org.apache.spark.ml.linalg.DenseMatrix;
import io.github.xpakx.psephopl.utils.DataLoader;
import io.github.xpakx.psephopl.utils.DegurbaTransformer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.stat.Correlation;
import org.apache.spark.sql.Column;
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

        degurbaDataSet = DegurbaTransformer.of(degurbaDataSet)
                .filterByCountry("PL")
                .transformToTERCtoDEGURBATable()
                .get();


        elections2015ByGminasDataSet = elections2015ByGminasDataSet
                .withColumn("PiS", colToInt("1 - Komitet Wyborczy Prawo i Sprawiedliwość"))
                .drop("1 - Komitet Wyborczy Prawo i Sprawiedliwość")
                .withColumn("PO", colToInt("2 - Komitet Wyborczy Platforma Obywatelska RP"))
                .drop("2 - Komitet Wyborczy Platforma Obywatelska RP")
                .withColumn("Razem", colToInt("3 - Komitet Wyborczy Partia Razem"))
                .drop("3 - Komitet Wyborczy Partia Razem")
                .withColumn("Korwin", colToInt("4 - Komitet Wyborczy KORWiN"))
                .drop("4 - Komitet Wyborczy KORWiN")
                .withColumn("PSL", colToInt("5 - Komitet Wyborczy Polskie Stronnictwo Ludowe"))
                .drop("5 - Komitet Wyborczy Polskie Stronnictwo Ludowe")
                .withColumn("SLD", colToInt("6 - Koalicyjny Komitet Wyborczy Zjednoczona Lewica SLD+TR+PPS+UP+Zieloni"))
                .drop("6 - Koalicyjny Komitet Wyborczy Zjednoczona Lewica SLD+TR+PPS+UP+Zieloni")
                .withColumn("Kukiz", colToInt("7 - Komitet Wyborczy Wyborców „Kukiz'15”"))
                .drop("7 - Komitet Wyborczy Wyborców „Kukiz'15”")
                .withColumn("N", colToInt("8 - Komitet Wyborczy Nowoczesna Ryszarda Petru"))
                .drop("8 - Komitet Wyborczy Nowoczesna Ryszarda Petru")
                .withColumn("TOTAL_VOTES", colToInt("Głosy ważne"))
                .drop("Głosy ważne")
                .na().fill(0, new String[]{"PiS", "PO", "Razem", "Korwin", "PSL", "SLD", "Kukiz", "N"});

        elections2015ByGminasDataSet = elections2015ByGminasDataSet
                .withColumn("PiS%", col("PiS").divide(col("TOTAL_VOTES")))
                .withColumn("PO%", col("PO").divide(col("TOTAL_VOTES")))
                .withColumn("Razem%", col("Razem").divide(col("TOTAL_VOTES")))
                .withColumn("Korwin%", col("Korwin").divide(col("TOTAL_VOTES")))
                .withColumn("PSL%", col("PSL").divide(col("TOTAL_VOTES")))
                .withColumn("SLD%", col("SLD").divide(col("TOTAL_VOTES")))
                .withColumn("Kukiz%", col("Kukiz").divide(col("TOTAL_VOTES")))
                .withColumn("N%", col("N").divide(col("TOTAL_VOTES")))
                .na().fill(0);

        Dataset<Row> gminasWithDegurba = elections2015ByGminasDataSet
                .join(
                        degurbaDataSet,
                        elections2015ByGminasDataSet.col("TERYT").equalTo(degurbaDataSet.col("TERC")),
                        "inner"
                );
        gminasWithDegurba = gminasWithDegurba.withColumn("DGURBA_INT", col("DGURBA").cast("integer"));

        Dataset<Row> df = gminasWithDegurba
                .na().fill(0, new String[]{"DGURBA_INT"})
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
}
