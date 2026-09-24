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

    // Fixed to the value computed for Asgarde 1.1.0: failures serialized by a previous version (e.g. in flight
    // during a Dataflow streaming update) stay readable, the added fields are null for them.
    private static final long serialVersionUID = 1281992175361165062L;

    private final String pipelineStep;
    private final String inputElement;
    private final Throwable exception;
    private final String originElement;

    private Failure(String pipelineStep,
                    String inputElement,
                    Throwable exception,
                    String originElement) {
        this.pipelineStep = pipelineStep;
        this.inputElement = inputElement;
        this.exception = exception;
        this.originElement = originElement;
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

        return new Failure(
                pipelineStep,
                elementAsString(exceptionElement.element()),
                SerializableThrowable.of(exceptionElement.exception()),
                null
        );
    }

    /**
     * Build a {@link fr.groupbees.asgarde.Failure} object from a generic input element and {@link java.lang.Throwable}.
     *
     * <p>
     * If the exception can't be serialized (e.g. it holds a non serializable field), it's replaced by a
     * {@link SerializableThrowable} keeping its class name, message, stack trace and causes, instead of making the
     * job fail when the failure is encoded.
     * </p>
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
        requireNonNull(exception);

        return new Failure(pipelineStep, elementAsString(element), SerializableThrowable.of(exception), null);
    }

    /**
     * Returns a copy of this failure with the given origin element: the element that entered the flow, when the
     * origin is tracked with {@link CollectionComposer#withOriginElement}.
     *
     * @param originElement the origin element as a string
     * @return a copy of this failure with the origin element
     */
    public Failure withOriginElement(final String originElement) {
        return new Failure(pipelineStep, inputElement, exception, originElement);
    }

    /**
     * Never fails: a null element or an element whose {@code toString} throws must not break the error handling.
     */
    private static String elementAsString(final Object element) {
        try {
            return String.valueOf(element);
        } catch (RuntimeException e) {
            return "<toString() of " + element.getClass().getName() + " failed: " + e + ">";
        }
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
     * <p>Getter for the field <code>originElement</code>: the element that entered the flow, as a string.</p>
     *
     * @return the origin element, {@code null} if the origin is not tracked
     *         (see {@link CollectionComposer#withOriginElement})
     */
    public String getOriginElement() {
        return originElement;
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
                (originElement == null ? "" : ", originElement='" + originElement + '\'') +
                '}';
    }
}
