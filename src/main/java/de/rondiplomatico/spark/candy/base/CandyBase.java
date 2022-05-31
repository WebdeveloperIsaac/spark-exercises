package de.rondiplomatico.spark.candy.base;

import java.time.LocalTime;
import java.time.Month;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import de.rondiplomatico.spark.candy.Generator;
import de.rondiplomatico.spark.candy.base.data.Candy;
import de.rondiplomatico.spark.candy.base.data.Color;
import de.rondiplomatico.spark.candy.base.data.Crush;
import de.rondiplomatico.spark.candy.base.data.Deco;
import lombok.Getter;
import scala.Tuple2;

/**
 * @author wirtzd
 * @since 11.05.2021
 */
public class CandyBase {

    /*
     * Create one static instance of a spark context.
     * This is all that is required to start your local spark application!
     */
    @Getter
    private static JavaSparkContext sparkContext = new JavaSparkContext("local[*]", "CandyDemo");

    /**
     * 
     * Call this method to crush n candies
     *
     * @param n
     *            Number of candies to crush
     * @return A RDD of crushed candies
     */
    protected JavaRDD<Crush> crushCandies(final int n) {
        System.out.println("The world is crushing candies...");

        /*
         * Fill a local list with crushes (all in the memory of the spark driver process!)
         */
        JavaRDD<Crush> res = getSparkContext().parallelize(Generator.generate(n), 50) // Distribute into
                                              .cache(); // Dont re-compute the test data every time it is used

        System.out.println(res.count() + " Candies have been crushed!");
        return res;
    }

}