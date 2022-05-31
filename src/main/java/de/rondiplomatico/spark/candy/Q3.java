package de.rondiplomatico.spark.candy;

import org.apache.spark.api.java.JavaRDD;
import com.google.common.collect.Iterables;

import de.rondiplomatico.spark.candy.base.CandyBase;
import de.rondiplomatico.spark.candy.base.data.Color;
import de.rondiplomatico.spark.candy.base.data.Crush;
import scala.Tuple2;

/**
 *
 * Example implementations for the basic candy crush questions
 *
 * @author wirtzd
 * @since 11.05.2021
 */
public class Q3 extends CandyBase {

    /**
     * This implements the logic that solves Q3.
     */
    public Q3(final boolean runB) {
        // Crush unbelievable 5Mio candies!
        JavaRDD<Crush> crushes = crushCandies(5000000);
        // Measure the starting point
        long start = System.currentTimeMillis();

        // Do the math!
        JavaRDD<Integer> countsPerTime =
                        // Get the blue candies
                        crushes.filter(c -> c.getCandy().getColor() == Color.BLUE)
                               // Key every crush entry by its time [Transformation]
                               .keyBy(c -> c.getTime())
                               // Group by time [Transformation]
                               .groupByKey()
                               // Just compute the number of crushes, the exact time is not needed anymore!
                               // [Transformation]
                               .map(d -> Iterables.size(d._2))
                               .cache(); // Tell spark to keep this rdd 
        /*
         * Compute sum [Action]
         *
         * The "reduce" function is an action that adds up all the values found!
         */
        int nCrushes = countsPerTime.reduce((a, b) -> a + b);
        /*
         * Count different times [Action]
         */
        long nTimes = countsPerTime.count();

        // Compute the average per minute
        double average = nCrushes / (double) nTimes;

        // Measure the elapsed time and produce some classy output
        long time = (System.currentTimeMillis() - start) / 1000;
        System.out.println("Average Candy crushes per Minute: " + average + ", computed in " + time + "s");

        // Run a more efficient implementation of this question using the same data!
        if (runB) {
            System.out.println("Computing same result with efficient variant b) ..");
            Q3b(crushes);
        }
    }

    /**
     * This method implements the logic that solves Q3, but in a way more efficient manner.
     */
    public void Q3b(final JavaRDD<Crush> crushes) {
        // Measure the start time
        long start = System.currentTimeMillis();

        Tuple2<Integer, Integer> both =
                        // Start again with filtering the crushes of blue candies
                        crushes.filter(c -> c.getCandy().getColor() == Color.BLUE)
                               // Key each crush by its time
                               .keyBy(c -> c.getTime())
                               /*
                                * Here starts part 1 of the magic:
                                * We locally (i.e. per partition on each worker) compute the number of
                                * crushes per each distinct time found in the respective partition,
                                * and then merge the counted crushes per global time instances.
                                */
                               .aggregateByKey(0, 
                                               (ex, n) -> ex + 1, 
                                               (a, b) -> a + b)
                               /*
                                * Part 2 of the magic:
                                * Iterate once over all the data, counting and summarizing at the same time.
                                * This is done by incrementing a first "counter" by one for each data,
                                * and summing up the counted values at the same time.
                                */
                               .aggregate(new Tuple2<>(0, 0), 
                                          (agg, n) -> new Tuple2<>(agg._1 + 1, agg._2 + n._2),
                                          (p1, p2) -> new Tuple2<>(p1._1 + p2._1, p1._2 + p2._2));

        // Compute average
        double average = both._2 / (double) both._1;
        // Get elapsed time and produce some output
        long time = (System.currentTimeMillis() - start) / 1000;
        System.out.println("Average Candy crushes per Minute: " + average + ", computed in " + time + "s");
    }

    public static void main(final String[] args) {
//        new Q3(false);
         new Q3(true);
    }

}
