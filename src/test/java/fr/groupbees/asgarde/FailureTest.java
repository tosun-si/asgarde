package fr.groupbees.asgarde;

import avro.generated.AvroTest;
import fr.groupbees.asgarde.settings.JsonUtil;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.WithFailures.ExceptionElement;
import org.apache.beam.sdk.util.SerializableUtils;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Contains the test of {@link Failure} class.
 */
@RunWith(JUnitParamsRunner.class)
public class FailureTest {

    /**
     * Contains input objects (Avro and other) and the linked exceptions.
     * These params allows to test the creation of {@link Failure} object, with different inputs.
     */
    public Object[] inputObjectAndExceptionParams() {
        final AvroTest avroObject = AvroTest.newBuilder()
                .setId(45)
                .setName("Avro test")
                .build();
        final IllegalArgumentException avroException = new IllegalArgumentException("Simulate error Avro object");
        final String avroObjectAsString = avroObject.toString();

        final ObjectTest otherObject = new ObjectTest(45, "Object test");
        final IllegalStateException otherObjectException = new IllegalStateException("Simulate error other object");
        final String otherObjectAsString = otherObject.toString();

        return new Object[][]{
                {"Avro object error", avroObject, avroObjectAsString, avroException},
                {"Other object error", otherObject, otherObjectAsString, otherObjectException}
        };
    }

    @Test
    @Parameters(method = "inputObjectAndExceptionParams")
    public <T> void givenObjectAndException_whenCreateFailureFromThem_thenFailureWithExpectedInputElementAndException(
            final String pipelineStep,
            final T inputObject,
            final String inputObjectAsString,
            final Exception exception) {

        // When.
        final Failure resultFailure = Failure.from(pipelineStep, inputObject, exception);

        // Then.
        assertResultFailure(resultFailure, pipelineStep, inputObjectAsString, exception);
    }

    @Test
    @Parameters(method = "inputObjectAndExceptionParams")
    public <T> void givenObjectAndException_whenCreateFailureFromExceptionElement_thenFailureWithExpectedInputElementAndException(
            final String pipelineStep,
            final T inputObject,
            final String inputObjectAsString,
            final Exception exception) {

        // When.
        final ExceptionElement<T> exceptionElement = ExceptionElement.of(inputObject, exception);
        final Failure resultFailure = Failure.from(pipelineStep, exceptionElement);

        // Then.
        assertResultFailure(resultFailure, pipelineStep, inputObjectAsString, exception);
    }

    @Test
    public void givenNonSerializableExceptionWithCause_whenCreateFailure_thenSerializableCopyKeepingDetails() {
        // Given.
        final IllegalArgumentException cause = new IllegalArgumentException("Root cause");
        final NonSerializableException exception = new NonSerializableException(cause);

        // When.
        final Failure resultFailure = Failure.from("Step", "element", exception);

        // Then.
        assertThat(resultFailure.getException())
                .isInstanceOf(SerializableThrowable.class)
                .hasMessage(NonSerializableException.MESSAGE)
                .hasCause(cause);
        assertThat(resultFailure.getException().getStackTrace()).isEqualTo(exception.getStackTrace());
        assertThat(((SerializableThrowable) resultFailure.getException()).getOriginalClassName())
                .isEqualTo(NonSerializableException.class.getName());
        assertThat(SerializableUtils.clone(resultFailure).getException().toString())
                .isEqualTo(NonSerializableException.class.getName() + ": " + NonSerializableException.MESSAGE);
    }

    @Test
    public void givenNonSerializableExceptionWithNonSerializableCauseAndSuppressed_whenCreateFailure_thenAllConverted() {
        // Given.
        final NonSerializableException cause = new NonSerializableException(null);
        final NonSerializableException suppressed = new NonSerializableException(null);
        final IllegalStateException serializableSuppressed = new IllegalStateException("Serializable suppressed");

        final NonSerializableException exception = new NonSerializableException(cause);
        exception.addSuppressed(suppressed);
        exception.addSuppressed(serializableSuppressed);

        // When.
        final Throwable resultException = SerializableUtils.clone(Failure.from("Step", "element", exception)).getException();

        // Then.
        assertThat(resultException.getCause()).isInstanceOf(SerializableThrowable.class);
        assertThat(resultException.getSuppressed()).hasSize(2);
        assertThat(resultException.getSuppressed()[0]).isInstanceOf(SerializableThrowable.class);
        assertThat(resultException.getSuppressed()[1])
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Serializable suppressed");
    }

    @Test
    public void givenFailuresSerializedWithAsgarde110And120_whenDeserialize_thenNoEncodedElements() throws Exception {
        for (String version : new String[]{"1.1.0", "1.2.0"}) {
            // Given: failures serialized with the published jars, without the encoded elements.
            final Failure resultFailure;
            try (InputStream in = getClass().getResourceAsStream("/failures/failure-serialized-with-asgarde-" + version + ".ser");
                 ObjectInputStream objectIn = new ObjectInputStream(in)) {

                // When.
                resultFailure = (Failure) objectIn.readObject();
            }

            // Then.
            assertThat(resultFailure.getPipelineStep()).isEqualTo("Step " + version);
            assertThat(resultFailure.getInputElementBytes()).isNull();
            assertThat(resultFailure.getInputElementCoder()).isNull();
            assertThat(resultFailure.getOriginElementBytes()).isNull();
            assertThat(resultFailure.getOriginElementCoder()).isNull();
        }
    }

    @Test
    public void givenElementToString_whenCreateFailure_thenInputElementWithTheFunctionOrToStringFallback() {
        // When.
        final Failure withFunction = Failure.from("Step", 42, new IllegalStateException("Error"), element -> "number " + element);
        final Failure withFailingFunction = Failure.from("Step", 42, new IllegalStateException("Error"), element -> {
            throw new IllegalStateException("Format error");
        });
        final Failure withoutFunction = Failure.from("Step", 42, new IllegalStateException("Error"), null);

        // Then.
        assertThat(withFunction.getInputElement()).isEqualTo("number 42");
        assertThat(withFailingFunction.getInputElement()).isEqualTo("42");
        assertThat(withoutFunction.getInputElement()).isEqualTo("42");
    }

    @Test
    public void givenEncodedElements_whenCopyFailure_thenEncodedElementsKept() {
        // Given.
        final Failure failure = Failure.from("Step", "element", new IllegalStateException("Error"))
                .withEncodedInputElement("element", StringUtf8Coder.of())
                .withEncodedOriginElement("origin", StringUtf8Coder.of());

        // When.
        final Failure resultFailure = SerializableUtils.clone(failure.withOriginElement("origin"));

        // Then.
        assertThat(resultFailure.getInputElementBytes()).isEqualTo(failure.getInputElementBytes());
        assertThat(resultFailure.getOriginElementBytes()).isEqualTo(failure.getOriginElementBytes());
        assertThat(resultFailure.getOriginElementCoder()).isEqualTo(StringUtf8Coder.of().toString());
        assertThat(resultFailure.getTimestamp()).isEqualTo(failure.getTimestamp());
    }

    @Test
    public void givenFailureSerializedWithAsgarde110_whenDeserialize_thenSameFieldsAndNoOriginElement() throws Exception {
        // Given: a failure serialized with the published Asgarde 1.1.0 jar (e.g. in flight in a streaming job updated
        // to a new Asgarde version).
        final Failure resultFailure;
        try (InputStream in = getClass().getResourceAsStream("/failures/failure-serialized-with-asgarde-1.1.0.ser");
             ObjectInputStream objectIn = new ObjectInputStream(in)) {

            // When.
            resultFailure = (Failure) objectIn.readObject();
        }

        // Then.
        assertThat(resultFailure.getPipelineStep()).isEqualTo("Step 1.1.0");
        assertThat(resultFailure.getInputElement()).isEqualTo("element 1.1.0");
        assertThat(resultFailure.getException()).isInstanceOf(IllegalStateException.class).hasMessage("Error 1.1.0");
        assertThat(resultFailure.getOriginElement()).isNull();
        assertThat(ObjectStreamClass.lookup(Failure.class).getSerialVersionUID()).isEqualTo(1281992175361165062L);
    }

    @Test
    public void givenFailureSerializedWithAsgarde120_whenDeserialize_thenOriginElementAndNoTimestamp() throws Exception {
        // Given: a failure serialized with the published Asgarde 1.2.0 jar.
        final Failure resultFailure;
        try (InputStream in = getClass().getResourceAsStream("/failures/failure-serialized-with-asgarde-1.2.0.ser");
             ObjectInputStream objectIn = new ObjectInputStream(in)) {

            // When.
            resultFailure = (Failure) objectIn.readObject();
        }

        // Then.
        assertThat(resultFailure.getPipelineStep()).isEqualTo("Step 1.2.0");
        assertThat(resultFailure.getOriginElement()).isEqualTo("origin 1.2.0");
        assertThat(resultFailure.getExceptionType()).isEqualTo(IllegalStateException.class.getName());
        assertThat(resultFailure.getExceptionMessage()).isEqualTo("Error 1.2.0");
        assertThat(resultFailure.getTimestamp()).isNull();
    }

    @Test
    public void givenExceptionWithCause_whenCreateFailure_thenComputedExceptionTypeMessageStackTraceAndTimestamp() {
        // Given.
        final Instant before = Instant.now();
        final IllegalStateException exception = new IllegalStateException("Error", new IllegalArgumentException("Root cause"));

        // When.
        final Failure resultFailure = Failure.from("Step", "element", exception);

        // Then.
        assertThat(resultFailure.getExceptionType()).isEqualTo(IllegalStateException.class.getName());
        assertThat(resultFailure.getExceptionMessage()).isEqualTo("Error");
        assertThat(resultFailure.getStackTrace())
                .startsWith("java.lang.IllegalStateException: Error")
                .contains("at fr.groupbees.asgarde.FailureTest.")
                .contains("Caused by: java.lang.IllegalArgumentException: Root cause");
        assertThat(resultFailure.getTimestamp()).isBetween(before, Instant.now());
        assertThat(resultFailure.withOriginElement("origin").getTimestamp()).isEqualTo(resultFailure.getTimestamp());
    }

    @Test
    public void givenNonSerializableException_whenCreateFailure_thenOriginalExceptionTypeInComputedFields() {
        // When.
        final Failure resultFailure = Failure.from("Step", "element", new NonSerializableException(null));

        // Then.
        assertThat(resultFailure.getExceptionType()).isEqualTo(NonSerializableException.class.getName());
        assertThat(resultFailure.getExceptionMessage()).isEqualTo(NonSerializableException.MESSAGE);
        assertThat(resultFailure.getStackTrace()).startsWith(NonSerializableException.class.getName() + ": " + NonSerializableException.MESSAGE);
    }

    @Test
    public void givenExceptionWithoutMessage_whenCreateFailure_thenNullExceptionMessage() {
        // When.
        final Failure resultFailure = Failure.from("Step", "element", new IllegalStateException());

        // Then.
        assertThat(resultFailure.getExceptionMessage()).isNull();
        assertThat(resultFailure.getStackTrace()).startsWith("java.lang.IllegalStateException");
    }

    @Test
    public void givenFailure_whenWithOriginElement_thenCopyWithOriginAndSameFields() {
        // Given.
        final IllegalStateException exception = new IllegalStateException("Error");
        final Failure failure = Failure.from("Step", "element", exception);

        // When.
        final Failure resultFailure = failure.withOriginElement("origin");

        // Then.
        assertThat(resultFailure).isNotSameAs(failure);
        assertThat(failure.getOriginElement()).isNull();
        assertThat(resultFailure.getOriginElement()).isEqualTo("origin");
        assertResultFailure(resultFailure, "Step", "element", exception);
        assertThat(resultFailure.toString()).endsWith(", originElement='origin'}");
        assertThat(failure.toString()).doesNotContain("originElement");
    }

    @Test
    public void givenNullElement_whenCreateFailure_thenInputElementAsNullString() {
        // When.
        final Failure resultFailure = Failure.from("Step", null, new IllegalStateException("Error"));

        // Then.
        assertThat(resultFailure.getInputElement()).isEqualTo("null");
    }

    @Test
    public void givenElementWithFailingToString_whenCreateFailure_thenFallbackInputElement() {
        // When.
        final Failure resultFailure = Failure.from("Step", new FailingToString(), new IllegalStateException("Error"));

        // Then.
        assertThat(resultFailure.getInputElement())
                .startsWith("<toString() of " + FailingToString.class.getName() + " failed:");
    }

    /**
     * Assert the given {@link Failure} object with expected input element as string and expected exception.
     */
    private <T> void assertResultFailure(final Failure resultFailure,
                                         final String expectedPipelineStep,
                                         final String expectedInputElement,
                                         final Exception exceptedException) {
        assertThat(resultFailure).isNotNull();
        assertThat(resultFailure.getPipelineStep())
                .isNotNull()
                .isNotEmpty()
                .isEqualTo(expectedPipelineStep);
        assertThat(resultFailure.getInputElement())
                .isNotNull()
                .isNotEmpty()
                .isEqualTo(expectedInputElement);
        assertThat(resultFailure.getException())
                .isNotNull()
                .isEqualTo(exceptedException);
    }

    private static class NonSerializableException extends RuntimeException {
        private static final String MESSAGE = "Error with a non serializable field";

        @SuppressWarnings("unused")
        private final Thread notSerializable = Thread.currentThread();

        NonSerializableException(final Throwable cause) {
            super(MESSAGE, cause);
        }
    }

    private static class FailingToString {
        @Override
        public String toString() {
            throw new IllegalStateException("toString error");
        }
    }

    private static class ObjectTest {
        private final int id;
        private final String name;

        public ObjectTest(int id, String name) {
            this.id = id;
            this.name = name;
        }

        public int getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        @Override
        public String toString() {
            return JsonUtil.serialize(this);
        }
    }
}
