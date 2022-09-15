package fr.groupbees.asgarde;

import org.apache.beam.sdk.transforms.WithFailures;

import java.io.Serializable;

import static java.util.Objects.requireNonNull;

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
    private final String pipelineStep;
    private final String inputElement;
    private final Throwable exception;

    public Failure() {
        this.pipelineStep = null;
        this.inputElement = null;
        this.exception = null;
    }

    public Failure(String pipelineStep,
                   String inputElement,
                   Throwable exception) {
        this.pipelineStep = pipelineStep;
        this.inputElement = inputElement;
        this.exception = exception;
    }

    /**
     * Build a {@link fr.groupbees.asgarde.Failure} object from an exception element provided by Beam.
     *
     * @param pipelineStep     the current pipeline step
     * @param exceptionElement a {@link org.apache.beam.sdk.transforms.WithFailures.ExceptionElement} object
     * @param <T>              a T class
     * @return a {@link fr.groupbees.asgarde.Failure} object
     */
    public static <T> Failure from(final String pipelineStep,
                                   final WithFailures.ExceptionElement<T> exceptionElement) {
        requireNonNull(pipelineStep);
        requireNonNull(exceptionElement);

        final T inputElement = exceptionElement.element();
        return new Failure(pipelineStep, inputElement.toString(), exceptionElement.exception());
    }

    /**
     * Build a {@link fr.groupbees.asgarde.Failure} object from a generic input element and {@link java.lang.Throwable}.
     *
     * @param pipelineStep the current pipeline step
     * @param element      a T object
     * @param exception    a {@link java.lang.Throwable} object
     * @param <T>          a T class
     * @return a {@link fr.groupbees.asgarde.Failure} object
     */
    public static <T> Failure from(final String pipelineStep,
                                   final T element,
                                   final Throwable exception) {
        requireNonNull(element);
        requireNonNull(exception);

        return new Failure(pipelineStep, element.toString(), exception);
    }

    /**
     * <p>Getter for the field <code>pipelineStep</code>.</p>
     *
     * @return a {@link java.lang.String} object
     */
    public String getPipelineStep() {
        return pipelineStep;
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

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "Failure{" +
                "pipelineStep='" + pipelineStep + '\'' +
                ", inputElement='" + inputElement + '\'' +
                ", exception=" + exception +
                '}';
    }
}
