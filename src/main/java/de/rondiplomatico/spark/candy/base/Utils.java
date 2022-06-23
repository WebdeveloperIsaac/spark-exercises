package de.rondiplomatico.spark.candy.base;

import java.time.LocalTime;
import java.time.Month;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.rondiplomatico.spark.candy.base.data.Color;
import de.rondiplomatico.spark.candy.base.data.Deco;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Utility methods to help with implementation of the examples
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Utils {

    private static final Logger log = LoggerFactory.getLogger(Utils.class);

    /**
     * Names of Cities. Feel free to edit :-)
     */
    private static final List<String> CITIES = Arrays.asList("Ismaning", "Cluj", "Tirgu Mures", "Stuttgart", "Braunschweig", "Ingolstadt", "Passau");

    /**
     * Names of People. Feel free to edit :-)
     */
    private static final List<String> USERS = Arrays.asList("Marlene", "Hans", "Zolti", "Schorsch", "Rambo", "Tibiko", "Ahmad", "Johansson", "Elena");

    /**
     * The central number generator.
     * 
     * Initialized with a fixed random seed, so repeated executions of demo code are identical.
     */
    private static final Random RND = new Random(1L);

    /**
     * A randomly created map between users and their home cities.
     */
    @Getter
    private static final Map<String, String> homeCities;

    /**
     * Static initializer code for the final homeCities map.
     */
    static {
        homeCities = new HashMap<>();
        for (String user : USERS) {
            homeCities.put(user, randCity());
            log.info("{} lives in {}", user, homeCities.get(user));
        }
        log.warn("{} users live in {} places.", homeCities.size(), CITIES.size());
    }

    /**
     * Returns a random integer between 0 (inclusive) and max (exclusive).
     * Can be used to pick a random element of a list of size n with rand(n)
     * 
     * @param max
     * @return
     */
    private static int rand(final int max) {
        return RND.nextInt(max);
    }

    /**
     * Returns a random month
     * 
     * @return
     */
    public static Month randMonth() {
        return Month.values()[rand(Month.values().length)];
    }

    /**
     * @return a random user from the USERS list.
     */
    public static String randUser() {
        return USERS.get(rand(USERS.size()));
    }

    /**
     * @return a random city from the CITIES list.
     */
    public static String randCity() {
        return CITIES.get(rand(CITIES.size()));
    }

    /**
     * @return A random local time of the day
     */
    public static LocalTime randTime() {
        return LocalTime.of(rand(24), rand(60));
    }

    /**
     * @return A random color from the {@link Color} enum
     */
    public static Color randColor() {
        return Color.values()[rand(Color.values().length)];
    }

    /**
     * @return A random Decoration from the {@link Deco} enum
     */
    public static Deco randDeco() {
        return RND.nextDouble() < .7 ? Deco.PLAIN : Deco.values()[rand(Deco.values().length)];
    }

}
