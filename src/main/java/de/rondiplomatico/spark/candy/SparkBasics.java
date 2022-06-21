package de.rondiplomatico.spark.candy;

import java.util.ArrayList;
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

        JavaRDD<Crush> rdd = s.generate(1000);

        // s.count(rdd);

        // s.Q1_Spark(rdd);

        // s.Q2_Spark(rdd);

        s.Q3_countByCity(rdd);

    }

    public JavaRDD<Crush> generate(int n) {
        List<Crush> data = Generator.generate(n);
        return getJavaSparkContext().parallelize(data)
                                    .cache();
    }

    // TODO
    public JavaRDD<Crush> generateInParallel(int parallelism, final int n) {
        List<Integer> helperList = new ArrayList<>(parallelism);
        for (int i = 0; i < parallelism; i++) {
            helperList.add(0);
        }
        return getJavaSparkContext().parallelize(helperList, parallelism)
                                    .flatMap(e -> Generator.generate(n).iterator())
                                    .cache();
    }

    public void count(JavaRDD<Crush> rdd) {
        List<Crush> data = rdd.collect();
        log.info("Hello World! Distributed and collected {} crushes", data.size());
    }

    public void Q1_Spark(JavaRDD<Crush> crushes) {
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

    public void Q2_Spark(JavaRDD<Crush> crushes) {
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

    public void Q3_countByCity(JavaRDD<Crush> crushes) {

        // Map<String, Long> res = crushes.map(c -> cities.get(c.getUser()))
        // .countByValue();

        // Creating a local reference is key to avoid serialization of the whole surrounding class.
        final Map<String, String> local = cities;
        Map<String, Long> res = crushes.map(c -> local.get(c.getUser()))
                                       .countByValue();

        res.forEach((c, i) -> log.info("There are {} crushes in {}", i, c));

        /*
         * TODO:
         * How many candies in Ismaning between 14-15 o'clock, counted by color?
         */
        Map<Color, Long> res2 = crushes.filter(c -> "Ismaning".equals(local.get(c.getUser())))
                                       // .filter(c -> c.getTime().getHour() >= 14 && c.getTime().getHour() <= 15)
                                       .filter(c -> c.asLocalTime().getHour() >= 14 && c.asLocalTime().getHour() <= 15)
                                       .map(c -> c.getCandy().getColor())
                                       .countByValue();

        res2.forEach((c, i) -> log.info("There are {} crushes in Ismaning with {} candies", i, c));
    }

}
