package de.rondiplomatico.spark.candy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.function.Function;
//import java.util.random.RandomGenerator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.awt.MultipleGradientPaint.ColorSpaceType;
import java.time.*;
import org.joda.time.LocalTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.rondiplomatico.spark.candy.base.Utils;
import de.rondiplomatico.spark.candy.base.data.Candy;
import de.rondiplomatico.spark.candy.base.data.Cities;
import de.rondiplomatico.spark.candy.base.data.Color;
import de.rondiplomatico.spark.candy.base.data.Crush;
import de.rondiplomatico.spark.candy.base.data.Deco;
//import jdk.internal.org.jline.utils.Colors;
//import jdk.internal.org.jline.utils.Log;

/**
 * Exercises for the first section of the course.
 *
 * Comprises basic functional operations using the java streaming api.
 *
 * @since 2022-06-22
 * @author wirtzd
 *
 */
interface GenerateCandy {
	List<Candy> generate(int num);
}

interface GenerateRandomColor{
	Color generateColor();
}

interface GenerateRandomDeco{
	Deco generateDeco();
}
@SuppressWarnings("java:S100")
public class FunctionalJava {

    private static final Logger log = LoggerFactory.getLogger(FunctionalJava.class);
    
	static Random rand = new Random();

    /**
     * Configure your environment to run this class for section 1.
     *
     * @param args
     */
    public static void main(String[] args) {
        /**
         * E1: Generate crushes
         */
         List<Crush> data = e1_crush(25000);
         System.out.println(data.size());

        /**
         * E2: Filtering
         */
         e2_countCandies(data);

        /**
         * E3: Grouping
         */
         e3_countByColor(data);

        /**
         * E4: Lookups
         */
         e4_cityLookup(data);
    }

    /**
     * Creates a specified amount of candy crush events.
     *
     * @param n
     *            the number of desired crush events
     * @return the list of crush events
     */
    public static List<Crush> e1_crush(int n) {
        List<Crush> orders = new ArrayList<>(n);
        List<Candy> candies = createCandies(n);
        /*
         * TODO E1: Generate crushes
         *
         * Implement logic that generates a list of n random Crush events!
         * Use the {@link Crush} and {@link Candy} constructors along with the {@link Utils} randXY methods.
         *
         * Also log how many events have been generated with log4j at the end, using the "log" logger.
         */
        
        for(int i =0;i<candies.size()-2;i++) {
        	if(candies.get(i).getColor() == candies.get(i+1).getColor() && candies.get(i+2).getColor() == candies.get(i).getColor()) {
        		Candy c = candies.get(i);
        		orders.add(new Crush(c, Utils.randUser(), Utils.randTime()));
        	}
        }
       log.info("Total Number of Horizantal Crushes are :" + orders.size());
       return orders;
    }
    
    public static List<Candy> createCandies(int numOfCandies) {
     GenerateRandomColor clr = () -> {
     	List<Color> colors = Arrays.asList(Color.values());
     	int color = rand.nextInt(6);
     	return colors.get(color);
     };
    	
     GenerateRandomDeco deco = () -> {
     	List<Deco> decorations = Arrays.asList(Deco.values());
     	int decor = rand.nextInt(4);
     	return decorations.get(decor);
     };
    	
     GenerateCandy fn = (int num) -> {
    	 List <Candy> candiesList = new ArrayList<Candy>();
    	 for(int i =0;i<num;i++) {
    		 candiesList.add(new Candy(clr.generateColor(),deco.generateDeco()));
    	 }
    	 return candiesList;
     };  	 
     	log.info("Generated :" + numOfCandies + " Candies");
    	 return fn.generate(numOfCandies);
    }

    /**
     * Performs various counts on crushed candies
     * 
     * 
     * Useful links for further reference:
     * 
     * Java Streaming API:
     * 
     * @see https://docs.oracle.com/javase/8/docs/api/java/util/stream/Stream.html
     * @see https://www.baeldung.com/java-8-streams
     * @see https://stackify.com/streams-guide-java-8/
     * 
     *      Lambdas:
     * 
     * @see https://www.w3schools.com/java/java_lambda.asp
     * @see https://docs.oracle.com/javase/tutorial/java/javaOO/lambdaexpressions.html
     * 
     * @param data
     *            A list of crushes
     */
    public static void e2_countCandies(List<Crush> data) {
        /**
         * Answers the question "how many red striped candies have been crushed?"
         */
        long res = data.stream() // stream() converts a Java collection to a java stream
                       .map(Crush::getCandy) // transforms the stream elements, here selecting the candy object of the crush
                       .filter(c -> c.getColor().equals(Color.RED)) // Filters a stream by a specified predicate
                       .filter(c -> c.getDeco().equals(Deco.HSTRIPES) || c.getDeco().equals(Deco.VSTRIPES))
                       .count(); // Terminates the stream, processing all it's elements by counting them
        // Log the counted results.
        log.info("The crush data contains {} red striped candies!", res);

        /*
         * TODO E2: Filtering
         *
         * Count how many wrapped candies have been crushed between 12-13 o'clock and log the results like above.
         */
       long result =  data.stream()
    		   			.filter((Crush c) -> {
    		   				if((c.getTime().isAfter(java.time.LocalTime.parse("12:00:00"))) && (c.getTime().isBefore(java.time.LocalTime.parse("13:00:00")))) {
    		   					return true;
    		   				}
    		   				return false;
    		   			})
    		   			.map(Crush::getCandy)
    		   			.filter(c -> c.getDeco().equals(Deco.WRAPPED))
    		   			.count();
       
       log.info("The Wrapped Candies Crushed between 12-13 is : {}", result);
    }

    /**
     * Performs various counts on the provided crush data
     *
     * @param data
     */
    public static void e3_countByColor(List<Crush> data) {
        /**
         * Classical, imperative implementation by using a map to
         * store the counts for each color.
         */
        Map<Color, Integer> res = new EnumMap<>(Color.class);
//        for (Crush c : data) {
//            Color col = c.getCandy().getColor();
//            Integer count = res.get(col);
//            if (count == null) {
//                res.put(col, 1);
//            } else {
//                res.put(col, count + 1);
//            }
//        }
//        res.forEach((c, i) -> log.info("The crush data contains {} {} candies", i, c));

        /*
         * TODO E3: Grouping
         *
         * Implement the same logic as above using the java streaming api.
         * Log your results and compare!
         *
         * Hints: The function "collect" with the "groupingBy" and downstream "counting" Collectors come in handy.
         */
        Color[] colors = Color.values();
        for(int i =0;i<Color.values().length;i++) {
        	res.put(colors[i], 0);
        }
        data.stream()
        			.map(Crush :: getCandy)
        			.map((Candy c)->{
        				int val = res.get(c.getColor());
        				val++;
        				res.put(c.getColor(), val);
        				return res;
        			}).collect(Collectors.toList());
        res.forEach((c, i) -> log.info("The crush data contains {} {} candies", i, c));
        /*
         * TODO E3: Grouping (Bonus question)
         *
         * Answer the question: "How many blue candies have been crushed per decoration type?"
         * Log your results.
         */
        Map<Deco,Integer> bluePerDeco = new EnumMap<Deco, Integer>(Deco.class); 
        Deco[] decorations = Deco.values();
        for(int i =0;i<decorations.length;i++) {
        	bluePerDeco.put(decorations[i],0);
        }
        
        data.stream().map(Crush :: getCandy)
        									.map((Candy c) -> {
        										if(c.getColor().equals(Color.BLUE)) {
        											int val = bluePerDeco.get(c.getDeco());
        											val++;
        											bluePerDeco.put(c.getDeco(), val);
        										}
        										return bluePerDeco;
        									}).collect(Collectors.toList());
       
       bluePerDeco.forEach((k,v) -> log.info("Total Blue Crushed for Decoration type {} is {}",k,v));
    }

    /**
     * Computes some statistics for candy crushes considering the city the persons are living in
     *
     * @param data
     */
    public static void e4_cityLookup(List<Crush> data) {

        /**
         * Get the map of cities from Utils
         */
        Map<String, String> cities = Utils.homeCities;

        /**
         * Imperative implementation: How may crushes per city?
         */
//        Map<String, Integer> counts = new HashMap<>();
//        for (Crush c : data) {
//            // Look up the city using the user as key
//            String city = cities.get(c.getUser());
//            // The "getOrDefault" allows to formulate the counting code more compact - compare e3_countByColor :-)
//            counts.put(city, counts.getOrDefault(city, 0) + 1);
//        }
//        counts.forEach((c, i) -> log.info("There are {} crushes in {}", i, c));

        /*
         * TODO E4: Lookups
         *
         * Implement "How may crushes per city?" using streams, map lookup and collectors.
         * Log your results.
         */

        /**
         * Teach-In: Demonstrating the requirement of "effectively final" fields
         */
        // cities = null;
        Map<String,Integer> crushesPerCity = new HashMap<>();
        
        data.stream().map((Crush c) -> {
        	String city = cities.get(c.getUser());
        	crushesPerCity.put(city, crushesPerCity.getOrDefault(city, 0) + 1);
        	return crushesPerCity;
        }).collect(Collectors.toList());
        crushesPerCity.forEach((k,v) -> log.info("For the City {} totoal crushes recorded are {}",k,v));
//        System.out.println(perCity);
        /*
         * TODO E4: Lookups (Bonus question)
         *
         * Implement "How many candies in Ismaning between 14-15 o'clock, counted by color?" with java streaming. Use what you have learned before to succeed.
         * Log your results.
         */
        Map<Color, Integer> isamingCandies = new EnumMap<Color,Integer>(Color.class);
        data.stream().map((Crush c) -> {
        	String city = cities.get(c.getUser());
        	if(city.equals("Ismaning") && c.getTime().isAfter(java.time.LocalTime.parse("14:00:00")) && c.getTime().isBefore(java.time.LocalTime.parse("15:00:00"))) {
        		int val = isamingCandies.getOrDefault(c.getCandy().getColor(), 0+1);
        		isamingCandies.put(c.getCandy().getColor(), val++);
        	}
        	return isamingCandies;
        }).collect(Collectors.toList());
        
        isamingCandies.forEach((k,v) -> log.info("In ismaning during the time of 14 to 15 the color {} were crushed {} times",k,v));
        
        
    }

}
