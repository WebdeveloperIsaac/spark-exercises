package de.rondiplomatico.spark.candy;

import de.rondiplomatico.spark.candy.base.SparkBase;
import de.rondiplomatico.spark.candy.base.Timer;
import de.rondiplomatico.spark.candy.base.data.Crush;
import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
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

        sp.e1_writeRDD(exampleInput, getOutputDirectory() + "local", Crush.class);

//
//        Timer timerPostFilter = Timer.start();
//        data = sp.e2_readRDD(Crush.class, getOutputDirectory() + "local").filter(e -> e.getUser().equals("Hans"));
//        log.info("Expected: {}, Actual {}, Time {}", nData, data.count(), timerPostFilter.stop());
//
//        Timer timerOperatorPushDown = Timer.start();
//        data = sp.e4_readRDD(Crush.class, getOutputDirectory() + "local", "user=='Hans'");
//        log.info("Expected: {}, Actual {}, Time {}", nData, data.count(), timerOperatorPushDown.stop());

    }

    /**
     * Used to switch the output directory betwenn local and cloud storage
     */
    public static String getOutputDirectory() {
        // Without any scheme, the string will be interpreted relative to the current working directory using the default file system
        return "output/";

        /*
         * TODO: Use the following shema to change the output directory to an azure storage account
         * <Schema>://<container_name>@<storage_account_name>.dfs.core.windows.net/<WindowsUserName>/<path>
         */
    }

    /**
     * Writes an RDD to a specified output folder;
     *
     * @param clazz
     *            clazz specifying the type of the rdd
     */
    public <T> void e1_writeRDD(JavaRDD<T> rdd, String folder, Class<T> clazz) {
        Dataset<T> ds = toDataset(rdd, clazz);

        /*
         * TODO A: Write the dataset as parquet files to the given folder
         * TODO B: After you wrote the files successfully experiment with different SaveModes
         */
    }

    /**
     * Repartitions the dataset to reduce the number of files written
     * {@link #e1_writeRDD(JavaRDD, String, Class)}
     */
    public <T> void e1_writeRDD(JavaRDD<T> rdd, String folder, Class<T> clazz, int outputPartitionNum) {
        /*
         * TODO: Reduce the number of files written by repartitioning the dataset
         */
    }

    /**
     * Reads data from a given folder and returns a JavaRDD
     *
     * @param clazz
     *            clazz specifying the data type
     * @param folder
     *            data location
     */
    public <T> JavaRDD<T> e2_readRDD(Class<T> clazz, String folder) {
        /*
         * TODO: Read the parquet files from the given folder
         * Use DatasetRead from SparkSession and SparkBase.toJavaRDD()
         */
        return null;
    }

    /**
     * Reads data from a given folder and returns a JavaRDD
     * Operator Pushdown is used to filter the data by the filesystem
     */
    public <T> JavaRDD<T> e4_readRDD(Class<T> clazz, String folder, String condition) {
        /*
         * TODO A: Use a sql filter directly on the read dataset to allow for operator pushdown
         * Everything else is similar to e2_readRDD
         * 
         * TODO B: use dataset.explain() to view the execution plan with and without operator pushdown
         */
        return null;
    }

    public void showCrushBeanSchema() {
        Encoder<Crush> enc = getBeanEncoder(Crush.class);
        log.info("Bean schema for class Crush: {}", enc);
    }
}
