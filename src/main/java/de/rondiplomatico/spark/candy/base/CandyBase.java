package de.rondiplomatico.spark.candy.base;

import java.time.LocalTime;
import java.time.Month;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import lombok.Getter;
import scala.Tuple2;

/**
 * @author wirtzd
 * @since 11.05.2021
 */
public class CandyBase {

    @Getter
    private List<String> places = Arrays.asList("Ismaning", "Cluj", "Tirgu Mures", "Echterdingen");
    @Getter
    private List<String> users = Arrays.asList("JonnyCage", "Hans", "Zolti", "RonDiplo", "Rambo", "Tibiko", "Inge", "Tibor");
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
        List<Crush> orders = new ArrayList<>(n);
        for (int o = 0; o < n; o++) {
            orders.add(new Crush(new Candy(Color.random(), Deco.random()), randUser(), randTime()));
        }
        JavaRDD<Crush> res = getSparkContext().parallelize(orders, 50) // Distribute into
                                              .cache(); // Dont re-compute the test data every time it is used

        System.out.println(res.count() + " Candies have been crushed!");
        return res;
    }

    /**
     * 
     * Returns the living places of all candy city citizens
     *
     * @return
     */
    protected JavaPairRDD<String, String> getLivingPlaces() {
        List<Tuple2<String, String>> homes = new ArrayList<>();
        for (String user : getUsers()) {
            Tuple2<String, String> t = new Tuple2<>(user, randPlace());
            System.out.println(t._1 + " lives in " + t._2);
            homes.add(t);
        }
        JavaPairRDD<String, String> homeRDD =
                        getSparkContext().parallelizePairs(homes, getUsers().size() / 2)
                                         .cache();
        System.out.println(homeRDD.count() + " users live in " + getPlaces().size() + " places.");
        return homeRDD;
    }

    protected int rand(final int max) {
        return (int) Math.floor(Math.random() * max);
    }

    protected Month randMonth() {
        return Month.values()[rand(Month.values().length)];
    }

    protected String randUser() {
        return users.get(rand(users.size()));
    }

    protected String randPlace() {
        return places.get(rand(places.size()));
    }

    protected LocalTime randTime() {
        return LocalTime.of(rand(24), rand(60));
    }

}