package de.rondiplomatico.spark.candy;

import org.apache.spark.api.java.JavaRDD;

import de.rondiplomatico.spark.candy.base.CandyBase;
import de.rondiplomatico.spark.candy.base.Color;
import de.rondiplomatico.spark.candy.base.Crush;
import de.rondiplomatico.spark.candy.base.Deco;

/**
 *
 * Example implementations for the basic candy crush questions
 *
 * @author wirtzd
 * @since 11.05.2021
 */
public class Q1 extends CandyBase {

    /**
     * This implements the logic that solves Q1.
     */
    public Q1() {
        // Crush some candies!
        JavaRDD<Crush> crushes = crushCandies(10000);

        long crushedRedStriped =
                        // Get the RED candies [Transformation]
                        crushes.filter(c -> c.getCandy().getColor() == Color.RED)
                               // Get the striped ones [Transformation]
                               .filter(c -> c.getCandy().getDeco() == Deco.HSTRIPES || c.getCandy().getDeco() == Deco.VSTRIPES)
                               // Count everything [Action]
                               .count();
        System.out.println("Crushed red striped candies: " + crushedRedStriped);
    }

    public static void main(final String[] args) {
        new Q1();
    }
}
