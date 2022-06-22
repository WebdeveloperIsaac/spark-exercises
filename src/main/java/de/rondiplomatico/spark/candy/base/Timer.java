package de.rondiplomatico.spark.candy.base;

/**
 * A little helper tool to time executions of operations.
 * 
 * @since 2022-06-22
 * @author wirtzd
 *
 */
public class Timer {

    private final long start;
    private long end = -1L;

    /**
     * Creates a new Timer object, starting to time when this method is called.
     * 
     * @return
     */
    public static Timer start() {
        return new Timer();
    }

    private Timer() {
        start = System.currentTimeMillis();
    }

    /**
     * Stops this Timer and returns the end time in milliseconds.
     * 
     * Calling stop again will take the current time as new end time.
     * 
     * @return
     */
    public long stop() {
        end = System.currentTimeMillis();
        return end;
    }

    /**
     * Measures the elapsed time.
     * 
     * @return the measured time in milliseconds.
     */
    public long elapsedMS() {
        if (end < 0) {
            stop();
        }
        return end - start;
    }

    /**
     * Measures the elapsed time.
     * 
     * @return the measured time in seconds
     */
    public long elapsedSeconds() {
        return elapsedMS() / 1000;
    }

}
