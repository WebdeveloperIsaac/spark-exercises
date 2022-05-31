package de.rondiplomatico.spark.candy.base;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalTime;
import java.time.Month;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.rondiplomatico.spark.candy.FunctionalProcessing;
import de.rondiplomatico.spark.candy.base.data.Color;
import de.rondiplomatico.spark.candy.base.data.Deco;

/**
 * The Class Utils.
 */
public class Utils {

    private static final Logger log = LoggerFactory.getLogger(FunctionalProcessing.class);

    public static final List<String> CITIES = Arrays.asList("Ismaning", "Cluj", "Tirgu Mures", "Stuttgart", "Braunschweig", "Ingolstadt", "Passau");

    public static final List<String> USERS = Arrays.asList("Marlene", "Hans", "Zolti", "Schorsch", "Rambo", "Tibiko", "Ahmad", "Johansson", "Elena");

    private static final Random RND = new Random(1L);

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

    /**
     * 
     * Returns the living places of all candy city citizens
     *
     * @return
     */
    public static Map<String, String> getHomeCities() {
        Map<String, String> homes = new HashMap<>();
        for (String user : USERS) {
            homes.put(user, randCity());
            log.info("{} lives in {}", user, homes.get(user));
        }
        log.warn("{} users live in {} places.", homes.size(), CITIES.size());
        return homes;
    }

    public static JavaSparkContext getSpark() {
        Builder b = SparkSession.builder();
        if (!SparkSession.getActiveSession().isDefined()) {
            b.config(readFromFile("spark.conf"));
        }
        return JavaSparkContext.fromSparkContext(b.getOrCreate().sparkContext());
    }

    private static SparkConf readFromFile(String configFile) {
        Properties props = new Properties();
        File in = new File(configFile);
        if (!in.isAbsolute()) {
            try (InputStream is = ClassLoader.getSystemResourceAsStream(configFile)) {
                if (is == null) {
                    throw new RuntimeException("Resource file " + configFile + " not found on ClassPath");
                } else {
                    props.load(is);
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed loading config file " + configFile + " from resources", e);
            }
        }
        SparkConf conf = new SparkConf();
        props.forEach((k, v) -> conf.set((String) k, (String) v));
        return conf;
    }

}
