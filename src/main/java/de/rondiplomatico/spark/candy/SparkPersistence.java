package de.rondiplomatico.spark.candy;

import de.rondiplomatico.spark.candy.base.SparkBase;
import de.rondiplomatico.spark.candy.base.data.Crush;
import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RequiredArgsConstructor
public class SparkPersistence extends SparkBase {

    private static final Logger log = LoggerFactory.getLogger(SparkPersistence.class);

    private static final String USER_NAME = "qukoegl";

    public static void main(String[] args) {
        int nData = 20000000;
        JavaRDD<Crush> exampleInput = new SparkBasics().e1_crushRDD(nData);
        SparkPersistence sp = new SparkPersistence();

        JavaRDD<Crush> data;
        long x;

        sp.e1_writeRDD(exampleInput, getOutputDirectory() + "local", Crush.class);

        x = System.currentTimeMillis();
        data = sp.e4_readRDD(Crush.class, getOutputDirectory() + "local", "user=='H'");
        log.info("Expected: {}, Actual {}, Time {}", nData, data.count(), System.currentTimeMillis() - x);

        x = System.currentTimeMillis();
        data = sp.e2_readRDD(Crush.class, getOutputDirectory() + "local").filter(e -> e.getUser().equals("H"));
        log.info("Expected: {}, Actual {}, Time {}", nData, data.count(), System.currentTimeMillis() - x);

    }

    public static String getOutputDirectory() {
        // Without any scheme, the string will be interpreted relative to the current working directory using the default file system
        return "localOut";

        // <Schema>://<container_name>@<storage_account_name>.dfs.core.windows.net/<local_path>
        // return "abfss://data@stsparktraining.dfs.core.windows.net/" + USER_NAME + "/";
    }

    public <T> void e1_writeRDD(JavaRDD<T> rdd, String folder, Class<T> clazz) {
        Dataset<T> ds = toDataset(rdd, clazz);
        ds.write()
          // TODO Experiment with different APPEND/OVERRIDE MODE
          // TODO APPEND/OVERRIDE MODE
          .mode("overwrite")
          .parquet(folder);
    }

    // TODO add e1_writeRDD(JavaRDD<T> rdd, String folder, Class<T> clazz, int outputPartitionNum)
    public <T> void e1_writeRDD(JavaRDD<T> rdd, String folder, Class<T> clazz, int outputPartitionNum) {
        JavaRDD<T> repartitioned = rdd.repartition(outputPartitionNum);
        e1_writeRDD(repartitioned, folder, clazz);
    }

    // TODO Use DatasetRead from SparkSession and SparkBase.toJavaRDD()
    public <T> JavaRDD<T> e2_readRDD(Class<T> clazz, String folder) {
        Dataset<Row> dataset = getSparkSession().read().parquet(folder);
        dataset.explain();
        return toJavaRDD(dataset, clazz);
    }

    // TODO Filter the dataset
    public <T> JavaRDD<T> e4_readRDD(Class<T> clazz, String folder, String condition) {
        Dataset<Row> dataset = getSparkSession().read().parquet(folder).filter(condition);
        dataset.explain();
        return toJavaRDD(dataset, clazz);
    }

    public void showCrushBeanSchema() {
        Encoder<Crush> enc = getBeanEncoder(Crush.class);
        log.info("Bean schema for class Crush: {}", enc);
    }
}
