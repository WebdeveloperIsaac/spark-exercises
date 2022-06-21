package de.rondiplomatico.spark.candy;

import de.rondiplomatico.spark.candy.base.data.Crush;
import org.apache.spark.api.java.JavaRDD;

public class ClusterDataGeneration {

    public static void main(String[] args){
        SparkAdvanced generator = new SparkAdvanced();
        JavaRDD<Crush> data = generator.generateInParallel(100, 10);

        SparkPersistence persistence = new SparkPersistence();
        persistence.e1_writeRDD(data, SparkPersistence.DATALAKE_PATH + "cluster",Crush.class);
    }
}
