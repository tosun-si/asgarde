package fr.groupbees.asgarde;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithFailures;
import org.apache.beam.sdk.util.CoderUtils;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.time.Instant;

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
    private final Instant timestamp;
    private final EncodedElement encodedInputElement;
    private final EncodedElement encodedOriginElement;

    private Failure(String pipelineStep,
                    String inputElement,
                    Throwable exception,
                    String originElement,
                    Instant timestamp,
                    EncodedElement encodedInputElement,
                    EncodedElement encodedOriginElement) {
        this.pipelineStep = pipelineStep;
        this.inputElement = inputElement;
        this.exception = exception;
        this.originElement = originElement;
        this.timestamp = timestamp;
        this.encodedInputElement = encodedInputElement;
        this.encodedOriginElement = encodedOriginElement;
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
                null,
                Instant.now(),
                null,
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
        return from(pipelineStep, element, exception, null);
    }

    /**
     * Build a {@link fr.groupbees.asgarde.Failure} object with the input element converted by the given function.
     *
     * <p>
     * The conversion never fails: if the function throws or returns {@code null}, the element is converted with
     * {@code toString()}.
     * </p>
     *
     * @param pipelineStep    the current pipeline step
     * @param element         a T object
     * @param exception       a {@link java.lang.Throwable} object
     * @param elementToString converts the input element to a string, {@code toString()} if {@code null}
     * @param <T>             a T class
     * @return a {@link fr.groupbees.asgarde.Failure} object
     */
    public static <T> Failure from(final String pipelineStep,
                                   final T element,
                                   final Throwable exception,
                                   final SerializableFunction<Object, String> elementToString) {
        requireNonNull(exception);

        return new Failure(
                pipelineStep,
                elementAsString(element, elementToString),
                SerializableThrowable.of(exception),
                null,
                Instant.now(),
                null,
                null
        );
    }

    /**
     * Returns a copy of this failure with the given origin element: the element that entered the flow, when the
     * origin is tracked with {@link CollectionComposer#withOriginElement}.
     *
     * @param originElement the origin element as a string
     * @return a copy of this failure with the origin element
     */
    public Failure withOriginElement(final String originElement) {
        return new Failure(pipelineStep, inputElement, exception, originElement, timestamp, encodedInputElement, encodedOriginElement);
    }

    /**
     * Returns a copy of this failure with the input element encoded with the given coder, e.g. the coder of the
     * PCollection consumed by the failing step, to replay the element exactly.
     *
     * <p>
     * The encoding never fails: if the element can't be encoded, this failure is returned unchanged.
     * </p>
     *
     * @param element the input element
     * @param coder   the coder of the input element
     * @param <T>     the input element type
     * @return a copy of this failure with the encoded input element
     */
    public <T> Failure withEncodedInputElement(final T element, final Coder<T> coder) {
        final EncodedElement encoded = EncodedElement.of(element, coder);

        return encoded == null ? this : new Failure(pipelineStep, inputElement, exception, originElement, timestamp, encoded, encodedOriginElement);
    }

    /**
     * Returns a copy of this failure with the origin element encoded with the given coder, see
     * {@link #withEncodedInputElement(Object, Coder)}.
     *
     * @param origin the origin element
     * @param coder  the coder of the origin element
     * @param <T>    the origin element type
     * @return a copy of this failure with the encoded origin element
     */
    public <T> Failure withEncodedOriginElement(final T origin, final Coder<T> coder) {
        final EncodedElement encoded = EncodedElement.of(origin, coder);

        return encoded == null ? this : new Failure(pipelineStep, inputElement, exception, originElement, timestamp, encodedInputElement, encoded);
    }

    /**
     * Never fails: a failing or {@code null} result of the user function falls back to {@code toString()}.
     */
    private static String elementAsString(final Object element, final SerializableFunction<Object, String> elementToString) {
        if (elementToString == null) {
            return elementAsString(element);
        }

        try {
            final String result = elementToString.apply(element);
            return result == null ? elementAsString(element) : result;
        } catch (RuntimeException e) {
            return elementAsString(element);
        }
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
     * Class name of the exception, the original class name for an exception replaced by a
     * {@link SerializableThrowable}.
     *
     * @return the exception type, computed from the exception
     */
    public String getExceptionType() {
        return exception instanceof SerializableThrowable
                ? ((SerializableThrowable) exception).getOriginalClassName()
                : exception.getClass().getName();
    }

    /**
     * @return the message of the exception, {@code null} if the exception has no message
     */
    public String getExceptionMessage() {
        return exception.getMessage();
    }

    /**
     * Stack trace of the exception as a string, causes included, e.g. to write it to a dead letter queue.
     *
     * @return the stack trace, computed from the exception
     */
    public String getStackTrace() {
        final StringWriter stackTrace = new StringWriter();
        exception.printStackTrace(new PrintWriter(stackTrace));

        return stackTrace.toString();
    }

    /**
     * @return when the failure was created, {@code null} for a failure deserialized from Asgarde 1.2.0 or older
     */
    public Instant getTimestamp() {
        return timestamp;
    }

    /**
     * @return the input element encoded with its coder, {@code null} if it's not encoded
     *         (see {@link CollectionComposer#withEncodedElements()})
     */
    public byte[] getInputElementBytes() {
        return encodedInputElement == null ? null : encodedInputElement.bytes();
    }

    /**
     * @return the coder of the encoded input element as a string, {@code null} if it's not encoded
     */
    public String getInputElementCoder() {
        return encodedInputElement == null ? null : encodedInputElement.getCoder();
    }

    /**
     * @return the origin element encoded with its coder, {@code null} if it's not encoded
     */
    public byte[] getOriginElementBytes() {
        return encodedOriginElement == null ? null : encodedOriginElement.bytes();
    }

    /**
     * @return the coder of the encoded origin element as a string, {@code null} if it's not encoded
     */
    public String getOriginElementCoder() {
        return encodedOriginElement == null ? null : encodedOriginElement.getCoder();
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

    /**
     * An element encoded with its coder: the bytes and the coder as a string.
     */
    private static final class EncodedElement implements Serializable {

        private static final long serialVersionUID = 1L;

        private final byte[] bytes;
        private final String coder;

        private EncodedElement(final byte[] bytes, final String coder) {
            this.bytes = bytes;
            this.coder = coder;
        }

        /**
         * Never fails: returns {@code null} if the element can't be encoded.
         */
        private static <T> EncodedElement of(final T element, final Coder<T> coder) {
            if (coder == null) {
                return null;
            }

            try {
                return new EncodedElement(CoderUtils.encodeToByteArray(coder, element), coder.toString());
            } catch (Exception e) {
                return null;
            }
        }

        private byte[] bytes() {
            return bytes.clone();
        }

        private String getCoder() {
            return coder;
        }
    }
}
