package de.rondiplomatico.spark.candy;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;

import de.rondiplomatico.spark.candy.base.SparkBase;
import de.rondiplomatico.spark.candy.base.data.Crush;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SparkPersistence extends SparkBase {

    public static void main(String[] args) {

        SparkBasics sb = new SparkBasics();

        JavaRDD<Crush> rdd = sb.generate(1000000);
        
        SparkPersistence sp = new SparkPersistence();

        Dataset<Crush> ds = sp.toDataset(rdd, Crush.class);

        ds.printSchema();

//        ds.write()
//          .format("parquet")
//          .mode("append")
//          .option("path", "out")
//          .save();

        ds.write().parquet("out");

    }
    
    // TODO rdd to local
    
    // TODO rdd to local in 10 partitions.
    
    // TODO rdd from local to local evaluation (a simple script)
    
    // TODO rdd to datalake
    
    // TODO rdd from datalake to local evaluation (a simple script)

}
