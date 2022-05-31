package de.rondiplomatico.spark.candy;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.rondiplomatico.spark.candy.base.Utils;
import de.rondiplomatico.spark.candy.base.data.Candy;
import de.rondiplomatico.spark.candy.base.data.Color;
import de.rondiplomatico.spark.candy.base.data.Crush;
import de.rondiplomatico.spark.candy.base.data.Deco;

/**
 * The Class Generator.
 */
public class Generator {

    private static final Logger log = LoggerFactory.getLogger(Generator.class);

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
    public static List<Crush> generate(int n) {
        List<Crush> orders = new ArrayList<>(n);
        for (int o = 0; o < n; o++) {
            orders.add(new Crush(new Candy(Utils.randColor(), Utils.randDeco()), Utils.randUser(), Utils.randTime()));
        }
        log.info("{} candies have been crushed!", orders.size());
        return orders;
    }

}
