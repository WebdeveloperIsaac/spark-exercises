package de.rondiplomatico.spark.candy.base;

/**
 * @author wirtzd
 * @since 11.05.2021
 */
public enum Deco {

    PLAIN,
    HSTRIPES,
    VSTRIPES,
    WRAPPED;

    public static Deco random() {
        return Math.random() < .7 ? Deco.PLAIN : values()[(int) Math.floor(Math.random() * values().length)];
    }
}
