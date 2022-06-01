package de.rondiplomatico.spark.candy;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;

import de.rondiplomatico.spark.candy.base.Utils;
import de.rondiplomatico.spark.candy.base.data.Crush;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SparkPersistence {

    private final JavaSparkContext jsc;

    public static void main(String[] args) {

        JavaSparkContext jsc = Utils.getJavaSparkContext();

        SparkBasics sb = new SparkBasics(jsc);

        JavaRDD<Crush> rdd = sb.generate(1000000);

        Dataset<Crush> ds = Utils.RDDToDataset(rdd, Crush.class);

        ds.printSchema();

//        ds.write()
//          .format("parquet")
//          .mode("append")
//          .option("path", "out")
//          .save();

        ds.write().parquet("out");

    }
    
    // TODO rdd to local
    
    // TODO rdd to datalake
    
    // TODO rdd to datalake in 10 partitions.

}
