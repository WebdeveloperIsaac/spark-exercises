package de.rondiplomatico.spark.candy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

import de.rondiplomatico.spark.candy.base.SparkBase;
import de.rondiplomatico.spark.candy.base.Utils;
import de.rondiplomatico.spark.candy.base.data.Candy;
import de.rondiplomatico.spark.candy.base.data.Color;
import de.rondiplomatico.spark.candy.base.data.Crush;
import de.rondiplomatico.spark.candy.base.data.Deco;
import scala.Tuple2;

public class SparkBasics extends SparkBase {

    private static final Logger log = LoggerFactory.getLogger(SparkBasics.class);

    private Map<String, String> cities = Utils.getHomeCities();

    public static void main(String[] args) {

        SparkBasics s = new SparkBasics();

        JavaRDD<Crush> rdd = s.e1_crushRDD(1000);

        log.info("Number of rdd partitions: {}, number of elements: {}", rdd.getNumPartitions(), rdd.count());

        // s.e2_countCandiesSpark(rdd);

        // s.e3_countByColorRDD(rdd);

        s.e4_cityLookup(rdd);

    }

    public JavaRDD<Crush> e1_crushRDD(int n) {
        List<Crush> data = FunctionalJava.e1_crush(n);
        return getJavaSparkContext().parallelize(data)
                                    .cache();
    }

    public void e2_countCandiesRDD(JavaRDD<Crush> crushes) {
        // Crush some candies!

        long crushedRedStriped =
                        // Get the RED candies [Transformation]
                        crushes.filter(c -> c.getCandy().getColor() == Color.RED)
                               // Get the striped ones [Transformation]
                               .filter(c -> c.getCandy().getDeco() == Deco.HSTRIPES || c.getCandy().getDeco() == Deco.VSTRIPES)
                               // Count everything [Action]
                               .count();
        log.info("Crushed {} red striped candies!", crushedRedStriped);

        long crushedWrappedAtTime =
                        // Get the RED candies [Transformation]
                        crushes// Get the striped ones [Transformation]
                               .filter(c -> c.getCandy().getDeco().equals(Deco.WRAPPED))
                               // .filter(c -> c.getTime().getHour() >= 12 && c.getTime().getHour() <= 13)
                               .filter(c -> c.asLocalTime().getHour() >= 12 && c.asLocalTime().getHour() <= 13)
                               // Count everything [Action]
                               .count();

        log.info("The crush data contains {} wrapped striped candies that have been crushed between 12 and 13 o'clock!", crushedWrappedAtTime);
    }

    public void e3_countByColorRDD(JavaRDD<Crush> crushes) {
        /*
         * 1. Group the RDD by the candy color
         */
        JavaPairRDD<Color, Iterable<Crush>> grouped =
                        crushes.groupBy(d -> d.getCandy().getColor());

        /*
         * 2. Replace the list of crushes by their size! [Transformation]
         */
        JavaPairRDD<Color, Integer> counted =
                        grouped.mapValues(Iterables::size);

        /*
         * 3. Collect the distributed color counts [Action]
         */
        List<Tuple2<Color, Integer>> res = counted.collect();

        res.forEach(t -> log.info("The crush data contains {} {} candies", t._2, t._1));

        /*
         * Fast variant: Let spark do the counting!
         */
        Map<Color, Long> quickRes = crushes.keyBy(c -> c.getCandy().getColor()) // Transformation
                                           .countByKey(); // Action
        quickRes.forEach((c, i) -> log.info("The crush data contains {} {} candies", i, c));

        /*
         * TODO
         * Implement
         */
        Map<Deco, Long> quickRes2 = crushes.map(Crush::getCandy)
                                           .filter(c -> c.getColor().equals(Color.BLUE))
                                           .keyBy(Candy::getDeco)
                                           .countByKey();
        quickRes2.forEach((c, i) -> log.info("The crush data contains {} {} blue candies", i, c));
    }

    public void e4_cityLookup(JavaRDD<Crush> crushes) {

        /*
         * "Naive" implementation of counting cities using a count map
         */
        Map<String, Integer> counts = new HashMap<>();
        crushes.foreach(c -> {
            String city = Utils.getHomeCities().get(c.getUser());
            int newcnt = counts.getOrDefault(city, 0) + 1;
            counts.put(city, newcnt);
            log.info("Counting crush in {}, totalling {}", city, newcnt);
        });
        log.info("Counted crushes in {} cities", counts.size());
        counts.forEach((c, i) -> log.info("There are {} crushes in {}", i, c));
        
        // What's wrong here?

        // TODO: Implement functional similar to the FunctionalJava E4, using the "cities" field of this SparkBasics class!
//        Map<String, Long> res = crushes.map(c -> cities.get(c.getUser()))
//                                       .countByValue();
//        // What's still going wrong?
//
//        // Creating a local reference is key to avoid serialization of the whole surrounding class.
        final Map<String, String> local = cities;
        Map<String, Long> res2 = crushes.map(c -> local.get(c.getUser()))
                                        .countByValue();
//
        res2.forEach((c, i) -> log.info("There are {} crushes in {}", i, c));
//
//        /*
//         * TODO:
//         * How many candies in Ismaning between 14-15 o'clock, counted by color?
//         */
//        Map<Color, Long> res3 = crushes.filter(c -> "Ismaning".equals(local.get(c.getUser())))
//                                       // .filter(c -> c.getTime().getHour() >= 14 && c.getTime().getHour() <= 15)
//                                       .filter(c -> c.asLocalTime().getHour() >= 14 && c.asLocalTime().getHour() <= 15)
//                                       .map(c -> c.getCandy().getColor())
//                                       .countByValue();
//
//        res3.forEach((c, i) -> log.info("There are {} crushes in Ismaning with {} candies", i, c));
    }

}
