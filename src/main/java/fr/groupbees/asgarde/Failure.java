package fr.groupbees.asgarde;

import org.apache.beam.sdk.transforms.WithFailures;

import java.io.Serializable;

/**
 * Class used by default to handle error cases and failure outputs.
 * <p>
 * This object take an inputElement in a {@link java.lang.String} type and the exception that occurred.
 * <p>
 * Factory methods are proposed to build a {@link fr.groupbees.asgarde.Failure} object from inputs.
 *
 * @author mazlum
 */
public class Failure implements Serializable {
    private final String inputElement;
    private final Throwable exception;

    private Failure(String inputElement, Throwable exception) {
        this.inputElement = inputElement;
        this.exception = exception;
    }

    /**
     * Build a {@link fr.groupbees.asgarde.Failure} object from an exception element provided by Beam.
     *
     * @param exceptionElement a {@link org.apache.beam.sdk.transforms.WithFailures.ExceptionElement} object
     * @param <T> a T class
     * @return a {@link fr.groupbees.asgarde.Failure} object
     */
    public static <T> Failure from(final WithFailures.ExceptionElement<T> exceptionElement) {
        final T inputElement = exceptionElement.element();
        return new Failure(inputElement.toString(), exceptionElement.exception());
    }

    /**
     * Build a {@link fr.groupbees.asgarde.Failure} object from a generic input element and {@link java.lang.Throwable}.
     *
     * @param element a T object
     * @param exception a {@link java.lang.Throwable} object
     * @param <T> a T class
     * @return a {@link fr.groupbees.asgarde.Failure} object
     */
    public static <T> Failure from(final T element, final Throwable exception) {
        return new Failure(element.toString(), exception);
    }

    /**
     * <p>Getter for the field <code>inputElement</code>.</p>
     *
     * @return a {@link java.lang.String} object
     */
    public String getInputElement() {
        return inputElement;
    }

    /**
     * <p>Getter for the field <code>exception</code>.</p>
     *
     * @return a {@link java.lang.Throwable} object
     */
    public Throwable getException() {
        return exception;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return "Failure{" +
                "inputElement='" + inputElement + '\'' +
                ", exception=" + exception +
                '}';
    }
}
