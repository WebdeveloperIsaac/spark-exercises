package de.rondiplomatico.spark.candy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import de.rondiplomatico.spark.candy.base.SparkBase;
import de.rondiplomatico.spark.candy.base.Timer;
import de.rondiplomatico.spark.candy.base.Utils;
import de.rondiplomatico.spark.candy.base.data.Color;
import de.rondiplomatico.spark.candy.base.data.Crush;
import de.rondiplomatico.spark.candy.base.data.Deco;
import scala.Tuple2;

/**
 * Main class for the exercises of section 4: advanced spark
 * 
 * @since 2022-06-22
 * @author wirtzd
 *
 */
@SuppressWarnings("java:S100")
public class SparkAdvanced extends SparkBase {

    private static final Logger log = LoggerFactory.getLogger(SparkAdvanced.class);

    /**
     * Configure your environment to run this class for section 4.
     * 
     * @param args
     * @throws InterruptedException
     */
    public static void main(String[] args) throws InterruptedException {

        // Create a local instance
        SparkAdvanced sa = new SparkAdvanced();
        int total = 12000000;

        /**
         * E1: Creating distributed crushes
         */
        // JavaRDD<Crush> crushes = sa.e1_distributedCrushRDD(total / 150, 150);

        /**
         * E2: Averaging
         */
        // sa.e2_averageCrushesPerMinute(crushes);

        /*
         * TODO E2: Averaging
         * 
         * Play around with execution times for different number of partitions:
         * - Create 12 Million crushes (distributed)
         * - Use Partitions: 2, 10, 50, 100, 150, 500
         * - Investigate the job, stage and task structure locally (localhost:4040)
         * - Note the times and maybe create a excel plot for time against number of partitions
         * 
         * Hints:
         * - You may use a loop or repeat the code in the main method
         * - You may disable the log output from FunctionalJava#e1_crush for better readability
         * 
         * Bonus task: Also vary the data size on top of the number of partitions!
         */

        /**
         * E3: Efficient averaging
         */
        // sa.e3_averageCrushesPerMinuteEfficient(crushes);

        /**
         * E4: Joins
         */
        /*
         * TODO E4: Joins
         * 
         * Play around with execution times for different number of partitions:
         * - Create 12 Million crushes (distributed)
         * - Use Partitions: 2, 10, 50, 100, 150, 500
         * - Investigate the job, stage and task structure locally (localhost:4040)
         * - Note the times and maybe create a excel plot for time against number of partitions
         */

        /*
         * TODO E6:
         * 
         * - Complete the method e6_lookupWithJoin
         * - Run the method with 12M crushes
         * - Investigate the job, stage and task structure locally (localhost:4040)
         * - How many shuffles are happening with this implementation?
         * - What might be inefficient about this implementation?
         */
        // sa.e6_lookupWithJoin(crushes);

        /**
         * E7: Broadcasts
         */
        // sa.e7_lookupWithBroadcast(crushes);

        /*
         * In any case: sleep for 10 mins to enable exploration of the spark execution history
         * at http://localhost:4040 after the spark jobs are finished.
         * 
         * Simply stop execution with your IDE's stop buttons etc.
         */
        System.out.println("Sleeping");
        Thread.sleep(1000 * 60 * 10);
    }

    /**
     * Creates a RDD of crushes in parallel.
     * Results in parallelism x n crushes.
     * 
     * @param n
     *            the number of crushes
     * @param partitions
     *            the number of partitions to create crushes at
     * @return
     */
    public JavaRDD<Crush> e1_distributedCrushRDD(final int n, int partitions) {
        Timer t = Timer.start();

        /*
         * TODO E1: Distributed crushing
         * 
         * - Create a local list of "parallelism" integers of value "n"
         * - Parallelize that list
         * - Create a transformation that uses that integer to create n crushes.
         * 
         * Hint: The function "flapMap" allows to return a collection of elements that are automatically combined by spark.
         */
        JavaRDD<Crush> res = null;

        

        /*
         * Additional helper code to facilitate fair comparison of runtimes in later examples - explained later.
         * As spark is lazy as far as possible, the creation time of the crushes would count into
         * the first evaluation. If the same RDD is used multiple times, this creates a bias (extra time) for the first use
         */
        res = res.cache();
        // Counting is an action and triggers creation of the rdd and all its contents.
        long cnt = res.count();
        log.info("Created {} crushes over {} partitions in {}ms", cnt, res.getNumPartitions(), t.elapsedMS());
        return res;
    }

    /**
     * This implements the logic that solves Q3.
     */
    public long e2_averageCrushesPerMinute(final JavaRDD<Crush> crushes) {
        // Measure the starting point
        Timer t = Timer.start();

        // Do the math!
        JavaRDD<Integer> countsPerTime =
                        // Get the blue candies
                        crushes.filter(c -> c.getCandy().getColor() == Color.BLUE)
                               // Key every crush entry by its time [Transformation]
                               .keyBy(Crush::getTime)
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

        // Compute the average per minute locall on driver
        double average = nCrushes / (double) nTimes;

        // Measure the elapsed time and produce some classy output
        log.info("Average Candy crushes per Minute: {}, computed in {}ms. {} crushes in {} partitions", average,
                 t.elapsedMS(), crushes.count(), crushes.getNumPartitions());

        return t.elapsedMS();
    }

    /**
     * This method implements the logic that solves E2, but in a way more efficient manner.
     */
    public void e3_averageCrushesPerMinuteEfficient(final JavaRDD<Crush> crushes) {
        // Measure the start time
        Timer t = Timer.start();

        /*
         * TODO E3: Efficient averaging
         * 
         * Implement the scenario E2 "How many blue crushes per Minute on average?" efficiently
         * - Use the aggregateByKey and aggregate PairRDD functions
         * - Log the results as in e2_averageCrushesPerMinute
         * - Investigate the job, stage and task structure locally (localhost:4040)
         * 
         * Hints:
         * The aggregateByKey function takes three arguments:
         * 1. What is the initial value
         * 2. How to combine local aggregates (pre-shuffle)
         * 3. How to combine partition results (post-shuffle)
         * 
         * The same holds true for the aggregate function.
         * 
         * Bonus task: Add the efficient implementation to your partitioning experiment code from the last exercise and see the speedup!
         */
       

    }

    /**
     * Implements the question "Who�s crushing more horizontally striped candies than wrapped? Any how many more?"
     * using a split & join paradigm
     * 
     * @param crushes
     */
    public void e4_crushCompareWithJoin(final JavaRDD<Crush> crushes) {
        // Get the start time
        Timer tm = Timer.start();

        /*
         * Compute the amount of crushed horizontally striped candies per user
         */
        JavaPairRDD<String, Integer> stripedPerUser =
                        crushes.filter(c -> c.getCandy().getDeco() == Deco.HSTRIPES)
                               .mapToPair(c -> new Tuple2<>(c.getUser(), c.getCandy()))
                               .groupByKey()
                               .mapValues(Iterables::size);
        /*
         * Compute the amount of crushed wrapped candies per user
         */
        JavaPairRDD<String, Integer> wrappedPerUser =
                        crushes.filter(c -> c.getCandy().getDeco() == Deco.WRAPPED)
                               .mapToPair(c -> new Tuple2<>(c.getUser(), c.getCandy()))
                               .groupByKey()
                               .mapValues(Iterables::size);

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
        log.info("Users with more striped than wrapped crushes computed in {}ms", tm.elapsedMS());
        res.forEach(r -> log.info("User {}: {} more striped than wrapped", r._1, r._2));
    }

    /**
     * Implements the question "Who�s crushing more horizontally striped candies than wrapped? Any how many more?"
     * using a simultaneous aggregation across the input rdd.
     * 
     * @param crushes
     */
    public void e5_crushCompareWithAggregation(final JavaRDD<Crush> crushes) {
        // Get the start time
        Timer ti = Timer.start();

        /*
         * TODO E5: Joint aggregation
         * 
         * Implement the logic of E4 using aggregateByKey
         * Add the efficient implementation to your partitioning experiment code from the last exercise and compare the speedup!
         * You can investigate the job, stage and task structure locally (localhost:4040)
         */
      
    }

    /**
     * Implements the question "How many candies are crushed in each Person�s home town?"
     * using a join between the crush dataset and the cities list as pair rdd.
     * 
     * @param crushes
     */
    public void e6_lookupWithJoin(final JavaRDD<Crush> crushes) {
        Timer ti = Timer.start();

        /*
         * TODO E6: Full joins
         * 
         * Create a RDD from the homeCities map provided in Utils.
         */
        JavaPairRDD<String, String> homeRDD = null;
       

        /**
         * Implements the question "How many candies are crushed in each Person�s home town?"
         * using a join on the user and later counting the crushes within each city.
         */
        List<Tuple2<String, Integer>> res =
                        // Key the crush data by user [Transformation]
                        crushes.keyBy(Crush::getUser)
                               // Join with the living places [Transformation]
                               .join(homeRDD)
                               // Key the results by the found place [Transformation]
                               .mapToPair(d -> new Tuple2<>(d._2._2, d._2._1))
                               // Group by place [Transformation]
                               .groupByKey()
                               // Compute number of candies [Transformation]
                               .mapValues(Iterables::size)
                               // Collect to driver [Action]
                               .collect();

        // Get elapsed time and produce some output
        log.info("Crushes per place computed in {}ms", ti.elapsedMS());
        res.forEach(r -> log.info("Place {}: {} crushed candies!", r._1, r._2));
    }

    /**
     * Efficient implementation of the crushes per city question, using a broadcast of the cities map
     *
     * @param crushes
     * @param homeRDD
     */
    public void e7_lookupWithBroadcast(final JavaRDD<Crush> crushes) {
        Timer ti = Timer.start();

        /*
         * TODO E7: Broadcasts
         * 
         * - Implement "How many candies are crushed in each Person's home town?" using a spark broadcast
         * - Avoid using groupByKey
         * - Investigate the job, stage and task structure locally (localhost:4040)
         * - How is the DAG different from the previous exercise?
         *
         * Hints:
         * - The java spark context provides methods to create broadcasts.
         * - The Broadcast object can be used in transformations directly. Access it's payload with "value()"
         */
        
    }

}
