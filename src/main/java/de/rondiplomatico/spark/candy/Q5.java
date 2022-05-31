package de.rondiplomatico.spark.candy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;

import com.google.common.collect.Iterables;

import de.rondiplomatico.spark.candy.base.CandyBase;
import de.rondiplomatico.spark.candy.base.Utils;
import de.rondiplomatico.spark.candy.base.data.Crush;
import scala.Tuple2;

/**
 *
 * Example implementations for the basic candy crush questions
 *
 * @author wirtzd
 * @since 11.05.2021
 */
public class Q5 extends CandyBase {

    public Q5(final boolean runB) {
        // Crush 3 mio candies!
        JavaRDD<Crush> crushes = crushCandies(3000000);
        JavaPairRDD<String, String> homeRDD = Utils.getHomeCities();
        // Get the start time
        long start = System.currentTimeMillis();

        List<Tuple2<String, Integer>> res =
                        // Key the crush data by user [Transformation]
                        crushes.keyBy(c -> c.getUser())
                               // Join with the living places [Transformation]
                               .join(homeRDD)
                               // Key the results by the found place [Transformation]
                               .mapToPair(d -> new Tuple2<>(d._2._2, d._2._1))
                               // Group by place [Transformation]
                               .groupByKey()
                               // Compute number of candies [Transformation]
                               .mapValues(v -> Iterables.size(v))
                               // Collect to driver [Action]
                               .collect();

        // Get elapsed time and produce some output
        long time = (System.currentTimeMillis() - start) / 1000;
        System.out.println("Crushes per place computed in " + time + "s");
        res.forEach(r -> System.out.println("Place " + r._1 + ": " + r._2 + " crushed candies!"));

        if (runB) {
            System.out.println("Computing Q5 with places broadcast.");
            Q5b(crushes, homeRDD);
        }
    }

    /**
     * Efficient implementation of the Question 5.
     *
     * @param crushes
     * @param homeRDD
     */
    public void Q5b(final JavaRDD<Crush> crushes, final JavaPairRDD<String, String> homeRDD) {
        // Get the start time
        long start = System.currentTimeMillis();

        // Collect the "small" data of person->home associations as local map
        Map<String, String> homeMap = homeRDD.collectAsMap();
        // "Broadcast" the complete map to every executor
        final Broadcast<Map<String, String>> homeBC = getSparkContext().broadcast(new HashMap<>(homeMap));

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
        long time = (System.currentTimeMillis() - start) / 1000;
        System.out.println("Crushes per place computed in " + time + "s");
        res.forEach(r -> System.out.println("Place " + r._1 + ": " + r._2 + " crushed candies!"));
    }

    public static void main(final String[] args) {
        // new Q5(false);
        new Q5(true);
    }

}
