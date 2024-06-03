package de.rondiplomatico.spark.candy;

import com.google.common.collect.Iterables;
import de.rondiplomatico.spark.candy.base.SparkBase;
import de.rondiplomatico.spark.candy.base.Utils;
import de.rondiplomatico.spark.candy.base.data.Candy;
import de.rondiplomatico.spark.candy.base.data.Cities;
import de.rondiplomatico.spark.candy.base.data.Color;
import de.rondiplomatico.spark.candy.base.data.Crush;
import de.rondiplomatico.spark.candy.base.data.Deco;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Exercises for the basic spark section of the course.
 *
 * @since 2022-06-22
 * @author wirtzd
 *
 */
@SuppressWarnings("java:S100")
public class SparkBasics extends SparkBase {

    private static final Logger log = LoggerFactory.getLogger(SparkBasics.class);

    /**
     * Local field containing the cities map.
     * (Placed here for a specific demonstration around serializability and spark)
     */
    public static Map<String, String> cities = new HashMap<String, String>();
    

    /**
     * Configure your environment to run this class for section 2.
     *
     * @param args
     */
    public static void main(String[] args) {
    	cities.put("KA", "BNG");
    	
        // Create a new instance of the SparkBasics exercise set.
        SparkBasics s = new SparkBasics();

        /**
         * E1: Generate crushes as RDD
         */
         JavaRDD<Crush> rdd = s.e1_crushRDD(250000);

        /*
         * TODO E1: Log the number of partitions and elements in the created RDD.
         */
         log.info("The number of partitions are"+1234);

        /**
         * E2: Filtering
         */
         s.e2_countCandiesRDD(rdd);

        /**
         * E3: Grouping
         */
         s.e3_countByColorRDD(rdd);

        /**
         * E4: Lookup
         */
         s.e4_cityLookupRDD(rdd);
    }

    /**
     * Creates a RDD of n crushes
     *
     * @param n
     * @return the rdd
     */
    public JavaRDD<Crush> e1_crushRDD(int n) {
        /*
         * TODO E1: Create crush RDD
         *
         * Use the functions from FunctionalJava to create some crushes and parallelize them using the java spark context
         */
    	FunctionalJava fn = new FunctionalJava();
    	SparkBasics s = new SparkBasics();
    	JavaRDD<Crush> res = s.getJavaSparkContext().parallelize(fn.e1_crush(n));
        return res;
    }

    /**
     * Implements the various counting questions from {@link FunctionalJava} using spark
     *
     * @param crushes
     */
    public void e2_countCandiesRDD(JavaRDD<Crush> crushes) {
        /*
         * TODO E2: Filtering
         *
         * Implement "How many red striped candies have been crushed?"
         */
        Long result = crushes.map(Crush :: getCandy)
        		.filter((Candy c)-> {
        			if(c.getColor().equals(Color.RED) && 
        					(c.getDeco().equals(Deco.VSTRIPES) || c.getDeco().equals(Deco.HSTRIPES))){
        				return true;
        			}
        			return false;
        		}).count();
        
        log.info("The Total Number of Red Striped Candies Crushed are {}",result);

        /*
         * TODO E2: Filtering
         *
         * Count how many wrapped candies have been crushed between 12-13 o'clock and log the results
         */
        
        long wrappedDuring = crushes.filter((Crush cr) -> {
        			if(cr.getTime().isAfter(java.time.LocalTime.parse("00:00:00")) && cr.getTime().isBefore(java.time.LocalTime.parse("13:00:00"))) {
        				return true;
        			}
        			return false;
        		})
                .map(Crush :: getCandy)
        		.filter((Candy c) -> {
//        			System.out.println(c.getDeco());
        			if(c.getDeco().equals("WRAPPED")) {
        				return true;
        			}
        			return false;
        		}).count();
        
        	log.info("The NUmber of wrapped candies crushed between 12 & 13 is",wrappedDuring);
       
    }

    /**
     * Performs various counts using spark
     *
     * @param crushes
     */
    public void e3_countByColorRDD(JavaRDD<Crush> crushes) {
        /*
         * TODO E3: Grouping
         * Implement FunctionalJava-E3 using Spark!
         * - How many Candies are crushed per color?
         * - Stick with the functional flow "group, count, collect"
         * - Log your results.
         *
         * Hint: Iterables::size is convenient should you need to count the number of elements of an iterator.
         */
    	Map <Color, Integer> crushedByColor = new EnumMap<Color,Integer>(Color.class);
    	Map <Color, Integer> colors = crushes.map(Crush :: getCandy)
    			.map((Candy c ) -> {
    				if(crushedByColor.containsKey(c.getColor())) {
    					int val = crushedByColor.get(c.getColor());
    					val++;
    					crushedByColor.put(c.getColor(),val);
    				}
    				else {
    					crushedByColor.put(c.getColor(),0);
    				}
    			return crushedByColor;
    			}).reduce((x,y) -> y);
    	
    	colors.forEach((x,y) -> log.info( "Candy of color {} is crushed {} times",x,y));
    	
        /*
         * TODO E3: (Bonus question)
         *
         * Implement "How many blue candies have been crushed per decoration type?"
         * - Avoid the groupBy() transformation - explore what better functions are available on JavaPairRDD!
         * - Can you also simplify the implementation of the first question similarly?
         */
    	Map<Deco,Integer> decors = new EnumMap<Deco,Integer>(Deco.class);
    	Map<Deco,Integer> result = crushes.map(Crush :: getCandy)
        			.filter((Candy c) -> (c.getColor().equals(Color.BLUE)))
        			.map((Candy c) -> {
        				if(decors.containsKey(c.getDeco())) {
        					int val = decors.get(c.getDeco());
        					val++;
        					decors.put(c.getDeco(), val);
        				}
        				else {
        					decors.put(c.getDeco(),0);
        				}
        				return decors;
        			}).reduce((first,second) -> second);
//        System.out.println(result);
         result.forEach((x,y) -> log.info( "Blue Candies in {} are crushed {} times",x,y));

        /*
         * Fast variant: Let spark do the counting!
         */
    	
    	
        
    }

    /**
     * Performs some statistics on crushes using spark and lookups.
     *
     * @param crushes
     */
    public int crush =0;
    public void e4_cityLookupRDD(JavaRDD<Crush> crushes) {

        /*
         * TODO E4: Lookups
         *
         * - Understand this implementation of counting cities using a count map
         * - Run the method
         * - Investigate what might be going wrong here?
         */
//        Map<String, Integer> counts = new HashMap<>();
//        
//        crushes.foreach((c) -> {
//            String city = Utils.homeCities.get(c.getUser());
//            int newcnt = counts.getOrDefault(city, 0) + 1;
//            counts.put(city, counts.getOrDefault(city, newcnt) + 1);
//            log.info("Counting crush in {}, totalling {}", city, newcnt);
//        });
//        log.info("Counted crushes in {} cities", crush);
//        counts.forEach((c, i) -> log.info("There are {} crushes in {}", i, c));

        /*
         * TODO E4: Lookups
         *
         * Implement the counting as result of the transformation, using
         * - the class field "cities" as lookup (similar to FJ-E4)
         * - countByValue() as action
         * - Log your results
         * - Run the code
         */
        Map<String,Integer> res = new HashMap<String,Integer>();
        Map<String,Integer> result = crushes.map((Crush c) -> {
        	String city = Utils.homeCities.get(c.getUser());
        	if(res.containsKey(city)) {
        		int val = res.get(city);
        		val++;
        		res.put(city, val);
        	}
        	else {
        		res.put(city, 0);
        	}
//        	System.out.println(res);
        	return res;
        }).reduce((x,y) -> y);
        
        result.forEach((x,y) -> log.info( "The City {} has {} Candy Crushes",x,y));
        
        /*
         * TODO E4: Lookups
         * How many candies in Ismaning between 14-15 o'clock, counted by color?
         * Use all you've learned before.
         */
        List results = new ArrayList<>();
         crushes.map((Crush c) -> {
        	String city = Utils.homeCities.get(c.getUser());
        	if(city.equals("Ismaning")) {
        		if((c.getTime().isAfter(java.time.LocalTime.parse("14:00:00")) && c.getTime().isBefore(java.time.LocalTime.parse("15:00:00")))) {
        			return c.getCandy().getColor();
        		}}
        	return null;
        }).countByValue().forEach((x,y) -> {
        	if(x!=null) {
        	log.info("IN Ismaning During 2-3 the Color {} was crushed {} times",x,y);
        	}
        	});
        
        
    }

}
