package de.rondiplomatico.spark.candy.base;

/**
 * The Class LearningException.
 */
public class LearningException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * Instantiates a new learning exception.
     *
     * @param message
     *            the message
     */
    public LearningException(String message) {
        super(message);
    }

    /**
     * Instantiates a new learning exception.
     *
     * @param message
     *            the message
     * @param t
     *            the t
     */
    public LearningException(String message, Throwable t) {
        super(message, t);
    }

}
