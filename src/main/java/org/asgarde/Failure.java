package org.asgarde;

import org.apache.beam.sdk.transforms.WithFailures;

import java.io.Serializable;

/**
 * Class used by default to handle error cases and failure outputs.
 * <p>
 * This object take an inputElement in a {@link String} type and the exception that occurred.
 * <p>
 * Factory methods are proposed to build a {@link Failure} object from inputs.
 */
public class Failure implements Serializable {
    private final String inputElement;
    private final Throwable exception;

    private Failure(String inputElement, Throwable exception) {
        this.inputElement = inputElement;
        this.exception = exception;
    }

    /**
     * Build a {@link Failure} object from an exception element provided by Beam.
     */
    public static <T> Failure from(final WithFailures.ExceptionElement<T> exceptionElement) {
        final T inputElement = exceptionElement.element();
        return new Failure(inputElement.toString(), exceptionElement.exception());
    }

    /**
     * Build a {@link Failure} object from a generic input element and {@link Throwable}.
     */
    public static <T> Failure from(final T element, final Throwable exception) {
        return new Failure(element.toString(), exception);
    }

    public String getInputElement() {
        return inputElement;
    }

    public Throwable getException() {
        return exception;
    }

    @Override
    public String toString() {
        return "Failure{" +
                "inputElement='" + inputElement + '\'' +
                ", exception=" + exception +
                '}';
    }
}
