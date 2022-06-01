package de.rondiplomatico.spark.candy;

import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

import de.rondiplomatico.spark.candy.base.SparkBase;
import de.rondiplomatico.spark.candy.base.Timer;
import de.rondiplomatico.spark.candy.base.data.Color;
import de.rondiplomatico.spark.candy.base.data.Crush;
import lombok.RequiredArgsConstructor;
import scala.Tuple2;

@RequiredArgsConstructor
public class SparkAdvanced extends SparkBase {

    private static final Logger log = LoggerFactory.getLogger(SparkAdvanced.class);

    public static void main(String[] args) {

        // Crush unbelievable 5Mio candies!
        JavaRDD<Crush> crushes = new SparkBasics().generate(5000000);

        SparkAdvanced sa = new SparkAdvanced();

        sa.Q3(crushes);

        sa.Q3b(crushes);

    }

    /**
     * This implements the logic that solves Q3.
     */
    public void Q3(final JavaRDD<Crush> crushes) {

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
    public void Q3b(final JavaRDD<Crush> crushes) {
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

}
