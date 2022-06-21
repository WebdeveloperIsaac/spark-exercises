package de.rondiplomatico.spark.candy;

import de.rondiplomatico.spark.candy.base.SparkBase;
import de.rondiplomatico.spark.candy.base.data.Crush;
import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RequiredArgsConstructor
public class SparkPersistence extends SparkBase {

    private static final Logger log = LoggerFactory.getLogger(SparkPersistence.class);

    private static final String TEAM_NAME = "TeamA";
    public static final String DATALAKE_PATH = "abfss://data@stsparktraining.dfs.core.windows.net/" + TEAM_NAME + "/";

    public static void main(String[] args) {
        int nData = 200;
        JavaRDD<Crush> exampleInput = new SparkBasics().generate(nData);
        SparkPersistence sp = new SparkPersistence();

        // Let's start with writing our data to local storage; Check you provided output folder if you can find the files
        // Goals: fileFormat: parquet; outputFolder: "localOut"
        // Tip: you can use SparkBase.toDataset() to transform our JavaRDD to a dataset
//        sp.e1_writeRDD(exampleInput, "localOut", Crush.class);

        // Let's try to minimize the number of files writen
        // Goals: fileFormat: parquet; outputFolder: "localOut"; Only 2 parquet files written
//        sp.e1_writeRDD(exampleInput, "localOut", Crush.class, 2);

        long x = System.currentTimeMillis();
//
//        // Next step: reading the data from local storage and check if the number of written data is correct
//        // Tip: you can use SparkBase.toJavaRDD() generate and JavaRDD from a Dataset
        JavaRDD<Crush> data;

        data = sp.e2_readRDD(Crush.class, "localOut").filter(e -> e.getUser().equals("Hans"));

        log.info("Expected: {}, Actual {}, Time {}", nData, data.count(), System.currentTimeMillis() - x);
//
//        data = sp.e2_readRDD(Crush.class, "localOut", "user=='Hans'");
//
//        log.info("Expected: {}, Actual {}, Time {}", nData, data.count(), System.currentTimeMillis() - x);

        // Operator PushDown

//
//        // To the cloud, upload the data on an azure datalake
//        // Careful pls use a unique folder (don't disrupt your teammates)
//        sp.e1_writeRDD(exampleInput, DATALAKE_PATH + "fromLocal", Crush.class);
//
//        // Now we read the data from the cloud, this one should be simple
//        log.info("Expected: {}, Actual {}", nData, sp.e2_readRDD(Crush.class, DATALAKE_PATH + "fromLocal").count());
    }

    public <T> void e1_writeRDD(JavaRDD<T> rdd, String folder, Class<T> clazz) {
        Dataset<T> ds = toDataset(rdd, clazz);
        ds.write()
//                TODO APPEND/OVERRIDE MODE
                .mode("overwrite")
                .parquet(folder);
    }

    public <T> void e1_writeRDD(JavaRDD<T> rdd, String folder, Class<T> clazz, int outputPartitionNum) {
        JavaRDD<T> repartitioned = rdd.repartition(outputPartitionNum);
        e1_writeRDD(repartitioned, folder, clazz);
    }

    public <T> JavaRDD<T> e2_readRDD(Class<T> clazz, String folder) {
        Dataset<Row> dataset = getSparkSession().read().parquet(folder);
        dataset.explain();
        return toJavaRDD(dataset, clazz);
    }

    public <T> JavaRDD<T> e2_readRDD(Class<T> clazz, String folder, String condition) {
        Dataset<Row> dataset = getSparkSession().read().parquet(folder).filter(condition);
        dataset.explain();
        return toJavaRDD(dataset, clazz);
    }
}
