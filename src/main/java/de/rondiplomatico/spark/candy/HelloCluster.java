package de.rondiplomatico.spark.candy;

import de.rondiplomatico.spark.candy.base.data.Crush;
import org.apache.spark.api.java.JavaRDD;

public class HelloCluster {

    public static void main(String[] args){
        SparkBasics basics = new SparkBasics();
        SparkPersistence persistence = new SparkPersistence();

        JavaRDD<Crush> crushJavaRDD = persistence.e2_readRDD(Crush.class, SparkPersistence.getOutputDirectory() + "local");

        crushJavaRDD.cache();
        basics.e2_countCandiesRDD(crushJavaRDD);
        basics.e3_countByColorRDD(crushJavaRDD);
        basics.e4_cityLookupRDD(crushJavaRDD);
        crushJavaRDD.unpersist();
    }
}
