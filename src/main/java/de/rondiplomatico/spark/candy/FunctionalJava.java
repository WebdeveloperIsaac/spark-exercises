package de.rondiplomatico.spark.candy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.rondiplomatico.spark.candy.base.Utils;
import de.rondiplomatico.spark.candy.base.data.Candy;
import de.rondiplomatico.spark.candy.base.data.Color;
import de.rondiplomatico.spark.candy.base.data.Crush;
import de.rondiplomatico.spark.candy.base.data.Deco;

public class FunctionalJava {

    private static final Logger log = LoggerFactory.getLogger(FunctionalJava.class);

    public static void main(String[] args) {
        // TODO CL comment out
        List<Crush> data = e1_crush(1000);

        // e2_countCandies(data);

//        e3_countByColor(data);

         e4_cityLookup(data);
    }

    /**
     * Task: Implement logic that generates a list of n random Crush events!
     * Use the {@link Crush} and {@link Candy} constructors along with the {@link Utils} randXY methods.
     * 
     * Also log how many events have been generated with log4j at the end, using the "log" logger.
     *
     * @param n
     *            the number of events required
     * @return the list of crush events
     */
    public static List<Crush> e1_crush(int n) {
        List<Crush> orders = new ArrayList<>(n);
        for (int o = 0; o < n; o++) {
            //orders.add(new Crush(new Candy(Utils.randColor(), Utils.randDeco()), Utils.randUser(), Utils.randTime().toNanoOfDay()));
            orders.add(new Crush(new Candy(Utils.randColor(), Utils.randDeco()), Utils.randUser(), Utils.randTime()));
        }
        log.info("{} candies have been crushed!", orders.size());
        return orders;
    }

    public static void e2_countCandies(List<Crush> data) {

        long res = data.stream()
                       .map(Crush::getCandy)
                       .filter(c -> c.getColor().equals(Color.RED))
                       .filter(c -> c.getDeco().equals(Deco.HSTRIPES) || c.getDeco().equals(Deco.VSTRIPES))
                       .count();

        log.info("The crush data contains {} red striped candies!", res);

        // Exercise E2:
        // TODO Count how many wrapped candies have been crushed between 12-13 o'clock and log it.
        long res2 = data.stream()
                        //.filter(c -> c.asLocalTime().getHour() >= 12 && c.asLocalTime().getHour() <= 13)
                        .filter(c -> c.getTime().getHour() >= 12 && c.getTime().getHour() <= 13)
                        .filter(c -> c.getCandy().getDeco().equals(Deco.WRAPPED))
                        .count();

        log.info("The crush data contains {} wrapped striped candies that have been crushed between 12 and 13 o'clock!", res2);

    }

    public static void e3_countByColor(List<Crush> data) {

        Map<Color, Integer> res = new HashMap<>();
        for (Crush c : data) {
            Color col = c.getCandy().getColor();
            Integer count = res.get(col);
            if (count == null) {
                res.put(col, 1);
            } else {
                res.put(col, count + 1);
            }
        }
        res.forEach((c, i) -> log.info("The crush data contains {} {} candies", i, c));

        /*
         * TODO: Implement the same logic using the streaming api
         * Hints: The function "collect" with the "groupingBy" and downstream "counting" Collectors come in handy.
         */
        Map<Color, Long> res2 = data.stream()
                                    .map(Crush::getCandy)
                                    .collect(Collectors.groupingBy(Candy::getColor, Collectors.counting()));

        res2.forEach((c, i) -> log.info("The crush data contains {} {} candies", i, c));

        /*
         * TODO: Implement the same logic using the streaming api
         * Hints: The function "collect" with the "groupingBy" and downstream "counting" Collectors come in handy.
         */
        Map<Deco, Long> res3 = data.stream()
                                   .map(Crush::getCandy)
                                   .filter(c -> c.getColor().equals(Color.BLUE))
                                   .collect(Collectors.groupingBy(Candy::getDeco, Collectors.counting()));

        res3.forEach((c, i) -> log.info("The crush data contains {} {} blue candies", i, c));
    }

    public static void e4_cityLookup(List<Crush> data) {

        // Get the map of cities
        Map<String, String> cities = Utils.getHomeCities();

        /*
         * Imperative implementation: How may crushes per city?
         */
        Map<String, Integer> counts = new HashMap<>();
        for (Crush c : data) {
            String city = cities.get(c.getUser());
            counts.put(city, counts.getOrDefault(city, 0) + 1);
        }
        counts.forEach((c, i) -> log.info("There are {} crushes in {}", i, c));

        /*
         * TODO:
         * Functional implementation: How may crushes per city?
         */
        Map<String, Long> res = data.stream()
                                    .map(c -> cities.get(c.getUser()))
                                    .collect(Collectors.groupingBy(s -> s, Collectors.counting()));

        res.forEach((c, i) -> log.info("There are {} crushes in {}", i, c));

        // Teach-In: Demonstrating effectively final fields
        // cities = null;

        /*
         * TODO:
         * How many candies in Ismaning between 14-15 o'clock, counted by color?
         */
        Map<Color, Long> res2 = data.stream()
                                    .filter(c -> "Ismaning".equals(cities.get(c.getUser())))
                                    .filter(c -> c.getTime().getHour() >= 14 && c.getTime().getHour() <= 15)
                                    .collect(Collectors.groupingBy(c -> c.getCandy().getColor(), Collectors.counting()));

        res2.forEach((c, i) -> log.info("There are {} crushes in Ismaning with {} candies", i, c));
    }

}
