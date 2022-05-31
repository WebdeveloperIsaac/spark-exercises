package de.rondiplomatico.spark.candy;

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

public class FunctionalProcessing {

    private static final Logger log = LoggerFactory.getLogger(FunctionalProcessing.class);

    public static void main(String[] args) {
        List<Crush> data = Generator.generate(1000);

        Q1_countCrushes(data);

        Q2_countByColor(data);

        Q3_countByCity(data);
    }

    public static void Q1_countCrushes(List<Crush> data) {

        long res = data.stream()
                       .map(cr -> cr.getCandy())
                       .filter(c -> c.getColor().equals(Color.RED))
                       .filter(c -> c.getDeco().equals(Deco.HSTRIPES) || c.getDeco().equals(Deco.VSTRIPES))
                       .count();

        log.info("The crush data contains {} red striped candies!", res);

        // TODO: Count how many wrapped candies have been crushed between 12-13 o'clock and log it.
        long res2 = data.stream()
                        .filter(c -> c.getTime().getHour() >= 12 && c.getTime().getHour() <= 13)
                        .filter(c -> c.getCandy().getDeco().equals(Deco.WRAPPED))
                        .count();

        log.info("The crush data contains {} wrapped striped candies that have been crushed between 12 and 13 o'clock!", res2);

    }

    public static void Q2_countByColor(List<Crush> data) {

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

    public static void Q3_countByCity(List<Crush> data) {

        Map<String, String> cities = Utils.getHomeCities();

        Map<String, Long> res = data.stream()
                                    .map(c -> cities.get(c.getUser()))
                                    .collect(Collectors.groupingBy(s -> s, Collectors.counting()));

        res.forEach((c, i) -> log.info("There are {} crushes in {}", i, c));

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
