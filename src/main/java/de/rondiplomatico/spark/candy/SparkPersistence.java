package de.rondiplomatico.spark.candy;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;

import de.rondiplomatico.spark.candy.base.SparkBase;
import de.rondiplomatico.spark.candy.base.data.Crush;
import lombok.RequiredArgsConstructor;
import org.codehaus.janino.Java;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RequiredArgsConstructor
public class SparkPersistence extends SparkBase {

    private static final Logger log = LoggerFactory.getLogger(SparkPersistence.class);
    private static final String DATALAKE_PATH = "abfss://data@stsparktraining.dfs.core.windows.net/teamA/";

    public static void main(String[] args) {
        JavaRDD<Crush> exampleInput = new SparkBasics().generate(1000);
        SparkPersistence sp = new SparkPersistence();

        sp.writeRDDToLocalDisk(exampleInput, Crush.class);
        log.info("Read {} candies from local file", sp.readRDDFromLocalDisk(Crush.class).count());

        sp.writeRDDToDatalake(exampleInput, Crush.class, "local");
        log.info("Read {} candies from datalake file", sp.readRDDFromDatalake(Crush.class, "local").count());
    }

    // TODO rdd to local
    public <T> void writeRDDToLocalDisk(JavaRDD<T> rdd, Class<T> clazz){
        Dataset<T> ds = toDataset(rdd, clazz);
        ds.write().mode("append").parquet("output");
    }

    
    // TODO rdd to local in 10 partitions.
    public <T> void writeRDDToLocalDiskWithPartitioning(JavaRDD<T> rdd, Class<T> clazz){
        writeRDDToLocalDisk(rdd.repartition(5), clazz);
    }
    
    // TODO rdd from local to local evaluation (a simple script)
    public <T> JavaRDD<T> readRDDFromLocalDisk(Class<T> clazz){
        return getSparkSession().read().parquet("output").as(getBeanEncoder(clazz)).toJavaRDD();
    }
    
    // TODO rdd to datalake
    public <T> void writeRDDToDatalake(JavaRDD<T> rdd, Class<T> clazz, String subFolder){
        Dataset<T> ds = toDataset(rdd, clazz);
        ds.write().mode("append").parquet(DATALAKE_PATH + subFolder);
    }
    
    // TODO rdd from datalake to local evaluation (a simple script)
    public <T> JavaRDD<T> readRDDFromDatalake(Class<T> clazz, String subFolder){
        return getSparkSession()
                .read()
                .parquet(DATALAKE_PATH + subFolder)
                .as(getBeanEncoder(clazz))
                .toJavaRDD();
    }

}
