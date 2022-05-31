package de.rondiplomatico.spark.candy;

import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import com.google.common.collect.Iterables;

import de.rondiplomatico.spark.candy.base.CandyBase;
import de.rondiplomatico.spark.candy.base.data.Crush;
import de.rondiplomatico.spark.candy.base.data.Deco;
import scala.Tuple2;

/**
 *
 * Example implementations for the basic candy crush questions
 *
 * @author wirtzd
 * @since 11.05.2021
 */
public class Q4 extends CandyBase {

    public Q4(final boolean runB) {
        // Crush the crazy amount of 6 mio candies!
        JavaRDD<Crush> crushes = crushCandies(6000000);
        // Get the start time
        long start = System.currentTimeMillis();

        /*
         * Compute the amount of crushed horizontally striped candies per user
         */
        JavaPairRDD<String, Integer> stripedPerUser =
                        crushes.filter(c -> c.getCandy().getDeco() == Deco.HSTRIPES)
                               .mapToPair(c -> new Tuple2<>(c.getUser(), c.getCandy()))
                               .groupByKey()
                               .mapValues(v -> Iterables.size(v));
        /*
         * Compute the amount of crushed wrapped candies per user
         */
        JavaPairRDD<String, Integer> wrappedPerUser =
                        crushes.filter(c -> c.getCandy().getDeco() == Deco.WRAPPED)
                               .mapToPair(c -> new Tuple2<>(c.getUser(), c.getCandy()))
                               .groupByKey()
                               .mapValues(v -> Iterables.size(v));

        /*
         * Join the two datasets via the user and select the requested answer
         */
        List<Tuple2<String, Integer>> res =
                        // This performs an inner join on the "Person" String [Transformation]
                        stripedPerUser.join(wrappedPerUser)
                                      // Compute the difference for each person [Transformation]
                                      .mapValues(t -> t._1 - t._2)
                                      // Select those cases with more striped than wrapped [Transformation]
                                      .filter(t -> t._2 > 0)
                                      // Collect the result
                                      .collect();

        // Get elapsed time and produce some output
        long time = (System.currentTimeMillis() - start) / 1000;
        System.out.println("Users with more striped than wrapped crushes computed in " + time + "s");
        res.forEach(r -> System.out.println("User " + r._1 + ": " + r._2 + " more striped than wrapped"));

        // Call fast implementation with same data
        if (runB) {
            System.out.println("Starting efficient implementation..");
            Q4b(crushes);
        }
    }

    public void Q4b(final JavaRDD<Crush> crushes) {
        // Get the start time
        long start = System.currentTimeMillis();

        /*
         * This implementation computes the crushes per user per partition first and then
         * sums up the reduced results
         */
        List<Tuple2<String, Integer>> res =
                        // Key all data by the person doing the crushin'
                        crushes.keyBy(c -> c.getUser())
                               /*
                                * Count the candies matching the two criteria per person,
                                * but at first locally per partition and then sum up the reduced results
                                */
                               .aggregateByKey(new Tuple2<>(0, 0),
                                               (ex, c) -> {
                                                   int str = c.getCandy().getDeco() == Deco.HSTRIPES ? 1 : 0;
                                                   int wrap = c.getCandy().getDeco() == Deco.WRAPPED ? 1 : 0;
                                                   return str > 0 || wrap > 0 ? new Tuple2<>(ex._1 + str, ex._2 + wrap) : ex;
                                               },
                                               (a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2))
                               /*
                                * Same as other solution - compute the difference and select
                                * the requested cases
                                */
                               .mapValues(t -> t._1 - t._2)
                               .filter(t -> t._2 > 0)
                               .collect();

        // Get elapsed time and produce some output
        long time = (System.currentTimeMillis() - start) / 1000;
        System.out.println("Users with more striped than wrapped crushes computed in " + time + "s");
        res.forEach(r -> System.out.println("User " + r._1 + ": " + r._2 + " more striped than wrapped"));
    }

    public static void main(final String[] args) {
//        new Q4(false);
        new Q4(true);
    }

}
