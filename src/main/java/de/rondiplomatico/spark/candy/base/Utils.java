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
 * The Class Utils.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Utils {

    private static final Logger log = LoggerFactory.getLogger(Utils.class);

    /*
     * Names of Cities. Feel free to edit :-)
     */
    private static final List<String> CITIES = Arrays.asList("Ismaning", "Cluj", "Tirgu Mures", "Stuttgart", "Braunschweig", "Ingolstadt", "Passau");

    /*
     * Names of People. Feel free to edit :-)
     */
    private static final List<String> USERS = Arrays.asList("Marlene", "Hans", "Zolti", "Schorsch", "Rambo", "Tibiko", "Ahmad", "Johansson", "Elena");

    private static final Random RND = new Random(1L);

    @Getter
    private static final Map<String, String> homeCities;

    static {
        homeCities = new HashMap<>();
        for (String user : USERS) {
            homeCities.put(user, randCity());
            log.info("{} lives in {}", user, homeCities.get(user));
        }
        log.warn("{} users live in {} places.", homeCities.size(), CITIES.size());
    }

    private static int rand(final int max) {
        return RND.nextInt(max);
    }

    public static Month randMonth() {
        return Month.values()[rand(Month.values().length)];
    }

    public static String randUser() {
        return USERS.get(rand(USERS.size()));
    }

    public static String randCity() {
        return CITIES.get(rand(CITIES.size()));
    }

    public static LocalTime randTime() {
        return LocalTime.of(rand(24), rand(60));
    }

    public static Color randColor() {
        return Color.values()[rand(Color.values().length)];
    }

    public static Deco randDeco() {
        return RND.nextDouble() < .7 ? Deco.PLAIN : Deco.values()[rand(Deco.values().length)];
    }

    public static byte[] randBytes() {
        byte[] bytes = new byte[1000000];
        RND.nextBytes(bytes);
        return bytes;
    }

}
