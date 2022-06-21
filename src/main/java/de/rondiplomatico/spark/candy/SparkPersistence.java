package de.rondiplomatico.spark.candy;

import de.rondiplomatico.spark.candy.base.SparkBase;
import de.rondiplomatico.spark.candy.base.data.Crush;
import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RequiredArgsConstructor
public class SparkPersistence extends SparkBase {

    private static final Logger log = LoggerFactory.getLogger(SparkPersistence.class);

    private static final String TEAM_NAME = "TeamA";
    public static final String DATALAKE_PATH = "abfss://data@stsparktraining.dfs.core.windows.net/" + TEAM_NAME + "/";

    public static void main(String[] args) {
        int nData = 1000;
        JavaRDD<Crush> exampleInput = new SparkBasics().generate(1000);
        SparkPersistence sp = new SparkPersistence();

        // Let's start with writing our data to local storage; Check you provided output folder if you can find the files
        // Goals: fileFormat: parquet; outputFolder: "localOut"
        // Tip: you can use toDataset to transform our JavaRDD to a dataset
        sp.writeRDD(exampleInput, "localOut", Crush.class);

        // Next step: reading the data from local storage and check if the number of written data is correct
        // Tip: you can use toJavaRDD generate and JavaRDD from a Dataset
        log.info("Expected: {}, Actual {}", nData, sp.readRDD(Crush.class, "localOut").count());

        // Let's try to minimize the number of files writen
        // Goals: fileFormat: parquet; outputFolder: "localOut"; Only 2 parquet files written
        sp.writeRDD(exampleInput, "localOut", 2, Crush.class);

        // To the cloud, upload the data on an azure datalake
        // Careful pls use a unique folder (don't disrupt your teammates)
        sp.writeRDD(exampleInput, DATALAKE_PATH + "fromLocal", Crush.class);

        // Now we read the data from the cloud, this one should be simple
        log.info("Expected: {}, Actual {}", nData, sp.readRDD(Crush.class, DATALAKE_PATH + "fromLocal").count());
    }

    public <T> void writeRDD(JavaRDD<T> rdd, String folder, Class<T> clazz) {
        Dataset<T> ds = toDataset(rdd, clazz);
        ds.write().mode("append").parquet(folder);
    }

    public <T> void writeRDD(JavaRDD<T> rdd, String folder, int outputPartitionNum, Class<T> clazz) {
        writeRDD(rdd.repartition(outputPartitionNum), folder, clazz);
    }

    public <T> JavaRDD<T> readRDD(Class<T> clazz, String folder) {
        return toJavaRDD(getSparkSession()
                .read()
                .parquet(folder), clazz);
    }
}
