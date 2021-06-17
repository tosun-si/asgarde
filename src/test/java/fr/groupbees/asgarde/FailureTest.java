package fr.groupbees.asgarde;

import avro.generated.AvroTest;
import fr.groupbees.asgarde.settings.JsonUtil;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.beam.sdk.transforms.WithFailures.ExceptionElement;
import org.junit.Test;
import org.junit.runner.RunWith;

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
                {avroObject, avroObjectAsString, avroException},
                {otherObject, otherObjectAsString, otherObjectException}
        };
    }

    @Test
    @Parameters(method = "inputObjectAndExceptionParams")
    public <T> void givenObjectAndException_whenCreateFailureFromThem_thenFailureWithExpectedInputElementAndException(
            final T inputObject,
            final String inputObjectAsString,
            final Exception exception) {

        // When.
        final Failure resultFailure = Failure.from(inputObject, exception);

        // Then.
        assertResultFailure(resultFailure, inputObjectAsString, exception);
    }

    @Test
    @Parameters(method = "inputObjectAndExceptionParams")
    public <T> void givenObjectAndException_whenCreateFailureFromExceptionElement_thenFailureWithExpectedInputElementAndException(
            final T inputObject,
            final String inputObjectAsString,
            final Exception exception) {

        // When.
        final ExceptionElement<T> exceptionElement = ExceptionElement.of(inputObject, exception);
        final Failure resultFailure = Failure.from(exceptionElement);

        // Then.
        assertResultFailure(resultFailure, inputObjectAsString, exception);
    }

    /**
     * Assert the given {@link Failure} object with expected input element as string and expected exception.
     */
    private <T> void assertResultFailure(final Failure resultFailure,
                                         final String expectedInputElement,
                                         final Exception exceptedException) {
        assertThat(resultFailure).isNotNull();
        assertThat(resultFailure.getInputElement())
                .isNotNull()
                .isNotEmpty()
                .isEqualTo(expectedInputElement);
        assertThat(resultFailure.getException())
                .isNotNull()
                .isEqualTo(exceptedException);
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
