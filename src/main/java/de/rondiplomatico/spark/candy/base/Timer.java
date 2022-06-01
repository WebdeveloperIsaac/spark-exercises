package de.rondiplomatico.spark.candy.base;

public class Timer {

    private final long start;
    private long end = -1L;

    public static Timer start() {
        return new Timer();
    }

    public Timer() {
        start = System.currentTimeMillis();
    }

    public long stop() {
        end = System.currentTimeMillis();
        return end;
    }

    public long elapsedMS() {
        if (end < 0) {
            stop();
        }
        return end - start;
    }

    public long elapsedSeconds() {
        return elapsedMS() / 1000;
    }

}
