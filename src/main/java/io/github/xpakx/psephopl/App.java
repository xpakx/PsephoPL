package io.github.xpakx.psephopl;

import org.apache.spark.ml.feature.VectorAssembler;
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

        elections2015ByGminasDataSet = elections2015ByGminasDataSet
                .withColumn("PiS", col("1 - Komitet Wyborczy Prawo i Sprawiedliwość").cast("integer"))
                .withColumn("PO", col("2 - Komitet Wyborczy Platforma Obywatelska RP").cast("integer"))
                .withColumn("Razem", col("3 - Komitet Wyborczy Partia Razem").cast("integer"))
                .withColumn("Korwin", col("4 - Komitet Wyborczy KORWiN").cast("integer"))
                .withColumn("PSL", col("5 - Komitet Wyborczy Polskie Stronnictwo Ludowe").cast("integer"))
                .withColumn("SLD", col("6 - Koalicyjny Komitet Wyborczy Zjednoczona Lewica SLD+TR+PPS+UP+Zieloni").cast("integer"))
                .withColumn("Kukiz", col("7 - Komitet Wyborczy Wyborców „Kukiz'15”").cast("integer"))
                .na().fill(0, new String[]{"PiS", "PO", "Razem", "Korwin", "PSL", "SLD", "Kukiz"});

        System.out.println(degurbaDataSet.count());
        Dataset<Row> gminasWithDegurba = elections2015ByGminasDataSet
                .join(
                        degurbaDataSet,
                        elections2015ByGminasDataSet.col("TERYT").equalTo(degurbaDataSet.col("TERC")),
                        "inner"
                );
        gminasWithDegurba = gminasWithDegurba.withColumn("DGURBA_INT", col("DGURBA").cast("integer"));
        gminasWithDegurba.show(5);
        System.out.println(gminasWithDegurba.count());

        Dataset<Row> df = gminasWithDegurba
                .na().fill(0, new String[]{"DGURBA_INT"})
                .select(col("DGURBA_INT"), col("PiS"), col("PO"), col("Razem"),
                        col("Korwin"), col("PSL"), col("SLD"), col("Kukiz"));

        VectorAssembler degurbaVectorAssembler = new VectorAssembler();
        degurbaVectorAssembler.setInputCols(new String[]{"DGURBA_INT", "PiS", "PO", "Razem", "Korwin", "PSL", "SLD", "Kukiz"});
        degurbaVectorAssembler.setOutputCol("features");
        Dataset<Row> newDataSet = degurbaVectorAssembler.transform(df);

        Row corr = Correlation.corr(newDataSet, "features", "pearson").head();
        System.out.println(corr.get(0));

    }
}
