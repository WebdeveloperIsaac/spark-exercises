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

import de.rondiplomatico.spark.candy.base.SparkBase;
import de.rondiplomatico.spark.candy.base.Timer;
import de.rondiplomatico.spark.candy.base.Utils;
import de.rondiplomatico.spark.candy.base.data.Color;
import de.rondiplomatico.spark.candy.base.data.Crush;
import de.rondiplomatico.spark.candy.base.data.Deco;
import lombok.RequiredArgsConstructor;
import scala.Tuple2;

@RequiredArgsConstructor
public class SparkAdvanced extends SparkBase {

    private static final Logger log = LoggerFactory.getLogger(SparkAdvanced.class);

    public static void main(String[] args) {

        SparkAdvanced sa = new SparkAdvanced();

        // Crush unbelievable 5Mio candies!
        JavaRDD<Crush> crushes = sa.e1_distributedCrushRDD(80, 500000);

        // sa.e2_averageCrushesPerMinute(crushes);

        // sa.e2_averageCrushesPerMinuteEfficient(crushes);

        // sa.Q4(crushes);

        // sa.Q4b(crushes);

        // sa.Q5(crushes);

        // sa.Q5b(crushes);

    }

    // TODO
    public JavaRDD<Crush> e1_distributedCrushRDD(int parallelism, final int n) {
        List<Integer> helperList = new ArrayList<>(parallelism);
        for (int i = 0; i < parallelism; i++) {
            helperList.add(0);
        }
        return getJavaSparkContext().parallelize(helperList, parallelism)
                                    .flatMap(e -> FunctionalJava.e1_crush(n).iterator())
                                    .cache();
    }

    /**
     * This implements the logic that solves Q3.
     */
    public void e2_averageCrushesPerMinute(final JavaRDD<Crush> crushes) {

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

        // Compute the average per minute
        double average = nCrushes / (double) nTimes;

        // Measure the elapsed time and produce some classy output
        log.info("Average Candy crushes per Minute: {}, computed in {}s", average, t.elapsedSeconds());
    }

    /**
     * This method implements the logic that solves Q3, but in a way more efficient manner.
     */
    public void e3_averageCrushesPerMinuteEfficient(final JavaRDD<Crush> crushes) {
        // Measure the start time
        Timer t = Timer.start();

        Tuple2<Integer, Integer> both =
                        // Start again with filtering the crushes of blue candies
                        crushes.filter(c -> c.getCandy().getColor() == Color.BLUE)
                               // Key each crush by its time
                               .keyBy(Crush::getTime)
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

        log.info("Average Candy crushes per Minute: {}, computed in {}s", average, t.elapsedSeconds());

    }

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
        log.info("Users with more striped than wrapped crushes computed in {}s", tm.elapsedSeconds());
        res.forEach(r -> log.info("User {}: {} more striped than wrapped", r._1, r._2));
    }

    public void e5_crushCompareWithAggregation(final JavaRDD<Crush> crushes) {
        // Get the start time
        Timer ti = Timer.start();

        /*
         * This implementation computes the crushes per user per partition first and then
         * sums up the reduced results
         */
        List<Tuple2<String, Integer>> res =
                        // Key all data by the person doing the crushin'
                        crushes.keyBy(Crush::getUser)
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
        log.info("Users with more striped than wrapped crushes computed in {}s", ti.elapsedSeconds());
        res.forEach(r -> log.info("User {}: {} more striped than wrapped", r._1, r._2));
    }

    public void e6_lookupWithJoin(final JavaRDD<Crush> crushes) {
        Timer ti = Timer.start();

        /*
         * Create an RDD of all the homes
         */
        List<Tuple2<String, String>> homesAsList =
                        Utils.getHomeCities()
                             .entrySet()
                             .stream()
                             .map(e -> new Tuple2<>(e.getKey(), e.getValue()))
                             .collect(Collectors.toList());
        JavaPairRDD<String, String> homeRDD =
                        getJavaSparkContext().parallelizePairs(homesAsList);

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
        log.info("Crushes per place computed in {}s", ti.elapsedSeconds());
        res.forEach(r -> log.info("Place {}: {} crushed candies!", r._1, r._2));
    }

    /**
     * Efficient implementation of the Question 5.
     *
     * @param crushes
     * @param homeRDD
     */
    public void e7_lookupWithBroadcast(final JavaRDD<Crush> crushes) {
        Timer ti = Timer.start();

        // Collect the "small" data of person->home associations as local map
        Map<String, String> homeMap = Utils.getHomeCities();
        // "Broadcast" the complete map to every executor
        final Broadcast<Map<String, String>> homeBC = getJavaSparkContext().broadcast(new HashMap<>(homeMap));

        List<Tuple2<String, Integer>> res =
                        /*
                         * Directly map the crushes to the respective person's home and set a count of 1.
                         *
                         * The broadcast is used to obtain the home for the person - no shuffling but a direct O(1)
                         * HashMap lookup!
                         *
                         * [Transformation]
                         */
                        crushes.mapToPair(c -> new Tuple2<>(homeBC.value().get(c.getUser()), 1))
                               // Reduce the number of counts per Place [Transformation]
                               .reduceByKey((a, b) -> a + b)
                               // Collect the result [Action]
                               .collect();

        // Get elapsed time and produce some output
        log.info("Crushes per place computed in {}s", ti.elapsedSeconds());
        res.forEach(r -> log.info("Place {}: {} crushed candies!", r._1, r._2));
    }

}
