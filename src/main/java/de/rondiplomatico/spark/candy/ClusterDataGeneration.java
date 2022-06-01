package de.rondiplomatico.spark.candy;

import de.rondiplomatico.spark.candy.base.data.Crush;
import org.apache.spark.api.java.JavaRDD;

public class ClusterDataGeneration {

    public static void main(String[] args){
        SparkBasics generator = new SparkBasics();
        JavaRDD<Crush> data = generator.generateInParallel(100, 10);

        SparkPersistence persistence = new SparkPersistence();
        persistence.writeRDDToDatalake(data, Crush.class, "cluster");
    }
}
