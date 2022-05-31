package de.rondiplomatico.spark.candy;

import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
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
public class Q2 extends CandyBase {

    /**
     * This implements the logic that solves Q2.
     */
    public Q2() {
        // Crush a million candies!
        JavaRDD<Crush> crushes = crushCandies(1000000);

        /*
         * Key the set of crushes by their color [Transformation]
         */
        JavaPairRDD<Color, Iterable<Crush>> groupedCrushes =
                        crushes.groupBy(d -> d.getCandy().getColor());

        /*
         * Replace the list of crushes by their size! [Transformation]
         *
         * Alternative:
         * groupedCrushes.mapToPair(d -> new Tuple2<>(d._1, Iterables.size(d._2)));
         */
        JavaPairRDD<Color, Integer> countedCrushes =
                        groupedCrushes.mapValues(v -> Iterables.size(v));

        // Collect the distributed color counts [Action]
        List<Tuple2<Color, Integer>> countedCrushesLocal = countedCrushes.collect();

        // Some output
        countedCrushesLocal.forEach(c -> System.out.println("We have " + c._2 + " crushed Candies of Color " + c._1));
    }

    public static void main(final String[] args) {
        new Q2();
    }
}
