package de.rondiplomatico.spark.candy.base;

public enum Color {

    RED,
    PURPLE,
    ORANGE,
    YELLOW,
    BLUE,
    GREEN;

    public static Color random() {
        return values()[(int) Math.floor(Math.random() * values().length)];
    }
}
