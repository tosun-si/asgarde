package fr.groupbees.asgarde;

import fr.groupbees.asgarde.transforms.MapElementFn;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler.BadRecordErrorHandler;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;

import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Contains the tests of the {@link FailureTransforms} class.
 */
public class FailureTransformsTest implements Serializable {

    private static final String PARSE = "Parse";

    @Rule
    public transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void givenFailureWithOrigin_whenToRow_thenRowWithTheSchemaAndTheFailureFields() {
        // Given.
        final Failure failure = Failure.from(PARSE, "not a number", new NumberFormatException("Bad number"))
                .withOriginElement("origin");

        // When.
        final Row row = FailureTransforms.toRow(failure);

        // Then.
        assertThat(row.getSchema()).isEqualTo(FailureTransforms.SCHEMA);
        assertThat(row.getString("pipelineStep")).isEqualTo(PARSE);
        assertThat(row.getString("inputElement")).isEqualTo("not a number");
        assertThat(row.getString("originElement")).isEqualTo("origin");
        assertThat(row.getString("exceptionType")).isEqualTo(NumberFormatException.class.getName());
        assertThat(row.getString("exceptionMessage")).isEqualTo("Bad number");
        assertThat(row.getString("stackTrace")).startsWith("java.lang.NumberFormatException: Bad number");
        assertThat(row.getDateTime("timestamp").getMillis()).isEqualTo(failure.getTimestamp().toEpochMilli());
    }

    @Test
    public void givenFailureOfAsgarde110_whenToRow_thenNullOriginAndTimestamp() throws Exception {
        // Given: a failure serialized with the published Asgarde 1.1.0 jar (no origin, no timestamp).
        final Failure failure;
        try (InputStream in = getClass().getResourceAsStream("/failures/failure-serialized-with-asgarde-1.1.0.ser");
             ObjectInputStream objectIn = new ObjectInputStream(in)) {
            failure = (Failure) objectIn.readObject();
        }

        // When.
        final Row row = FailureTransforms.toRow(failure);

        // Then.
        assertThat(row.getString("originElement")).isNull();
        assertThat(row.getDateTime("timestamp")).isNull();
        assertThat(row.getString("exceptionType")).isEqualTo(IllegalStateException.class.getName());
    }

    @Test
    public void givenComposerFailures_whenToRows_thenRowsWithTheSchemaAndUnchangedFailureCoder() {
        // Given.
        final PCollection<String> values = pipeline.apply("Create values", Create.of("1", "not a number"));

        final Result<PCollection<Integer>, Failure> result = CollectionComposer.of(values)
                .apply(PARSE, MapElementFn.into(TypeDescriptors.integers()).via((String value) -> Integer.parseInt(value)))
                .getResult();

        // When.
        final PCollection<Row> rows = result.failures().apply("To rows", FailureTransforms.toRows());

        // Then.
        assertThat(rows.getSchema()).isEqualTo(FailureTransforms.SCHEMA);
        assertThat(result.failures().getCoder()).isInstanceOf(SerializableCoder.class);

        PAssert.that(rows.apply("To step and input", MapElements
                        .into(TypeDescriptors.strings())
                        .via((Row row) -> row.getString("pipelineStep") + "|" + row.getString("inputElement") + "|" + row.getString("exceptionType"))))
                .containsInAnyOrder(PARSE + "|not a number|" + NumberFormatException.class.getName());

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void givenFailureWithAndWithoutOrigin_whenToBadRecord_thenOriginOrInputElementAsRecord() {
        // Given.
        final Failure failure = Failure.from(PARSE, "input", new IllegalStateException("Error"));

        // When.
        final BadRecord badRecordWithoutOrigin = FailureTransforms.toBadRecord(failure);
        final BadRecord badRecordWithOrigin = FailureTransforms.toBadRecord(failure.withOriginElement("origin"));

        // Then.
        assertThat(badRecordWithoutOrigin.getRecord().getHumanReadableJsonRecord()).isEqualTo("input");
        assertThat(badRecordWithOrigin.getRecord().getHumanReadableJsonRecord()).isEqualTo("origin");
        assertThat(badRecordWithOrigin.getRecord().getEncodedRecord()).isNull();
        assertThat(badRecordWithOrigin.getFailure().getException()).isEqualTo("java.lang.IllegalStateException: Error");
        assertThat(badRecordWithOrigin.getFailure().getExceptionStacktrace()).startsWith("java.lang.IllegalStateException: Error");
        assertThat(badRecordWithOrigin.getFailure().getDescription()).isEqualTo(PARSE);
    }

    @Test
    public void givenFailureWithEncodedElements_whenToRowWithEncodedElements_thenRowWithTheExtendedSchema() {
        // Given.
        final Failure failure = Failure.from(PARSE, "input", new IllegalStateException("Error"))
                .withOriginElement("origin")
                .withEncodedInputElement("input", StringUtf8Coder.of())
                .withEncodedOriginElement("origin", StringUtf8Coder.of());

        // When.
        final Row row = FailureTransforms.toRowWithEncodedElements(failure);

        // Then.
        assertThat(row.getSchema()).isEqualTo(FailureTransforms.SCHEMA_WITH_ENCODED_ELEMENTS);
        assertThat(row.getString("inputElement")).isEqualTo("input");
        assertThat(row.getBytes("inputElementBytes")).isEqualTo(failure.getInputElementBytes());
        assertThat(row.getString("inputElementCoder")).isEqualTo(StringUtf8Coder.of().toString());
        assertThat(row.getBytes("originElementBytes")).isEqualTo(failure.getOriginElementBytes());
        assertThat(row.getString("originElementCoder")).isEqualTo(StringUtf8Coder.of().toString());
    }

    @Test
    public void givenSchema_whenCompareWithTheExtendedSchema_thenSchemaUnchangedAndIncluded() {
        // Then: the 1.3.0 schema is unchanged, the tables created with it keep working.
        assertThat(FailureTransforms.SCHEMA.getFieldNames()).containsExactly(
                "pipelineStep", "inputElement", "originElement", "exceptionType", "exceptionMessage", "stackTrace", "timestamp");
        assertThat(FailureTransforms.SCHEMA_WITH_ENCODED_ELEMENTS.getFieldNames())
                .startsWith(FailureTransforms.SCHEMA.getFieldNames().toArray(new String[0]))
                .endsWith("inputElementBytes", "inputElementCoder", "originElementBytes", "originElementCoder");
    }

    @Test
    public void givenComposerFailuresWithEncodedElements_whenToRowsWithEncodedElements_thenRowsWithTheBytes() {
        // Given.
        final PCollection<String> values = pipeline.apply("Create values", Create.of("not a number"));

        final Result<PCollection<Integer>, Failure> result = CollectionComposer.of(values)
                .withEncodedElements()
                .apply(PARSE, MapElementFn.into(TypeDescriptors.integers()).via((String value) -> Integer.parseInt(value)))
                .getResult();

        // When.
        final PCollection<Row> rows = result.failures().apply("To rows", FailureTransforms.toRowsWithEncodedElements());

        // Then.
        assertThat(rows.getSchema()).isEqualTo(FailureTransforms.SCHEMA_WITH_ENCODED_ELEMENTS);

        PAssert.that(rows.apply("To coder", MapElements
                        .into(TypeDescriptors.strings())
                        .via((Row row) -> row.getString("inputElementCoder") + "|" + (row.getBytes("inputElementBytes") != null))))
                .containsInAnyOrder(StringUtf8Coder.of() + "|true");

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void givenFailureWithEncodedElements_whenToBadRecord_thenEncodedRecordAndCoderOfTheOriginOrInput() {
        // Given.
        final Failure failure = Failure.from(PARSE, "input", new IllegalStateException("Error"))
                .withEncodedInputElement("input", StringUtf8Coder.of());
        final Failure failureWithOrigin = failure
                .withOriginElement("origin")
                .withEncodedOriginElement("origin", StringUtf8Coder.of());

        // When.
        final BadRecord badRecord = FailureTransforms.toBadRecord(failure);
        final BadRecord badRecordWithOrigin = FailureTransforms.toBadRecord(failureWithOrigin);

        // Then.
        assertThat(badRecord.getRecord().getEncodedRecord()).isEqualTo(failure.getInputElementBytes());
        assertThat(badRecord.getRecord().getCoder()).isEqualTo(StringUtf8Coder.of().toString());
        assertThat(badRecordWithOrigin.getRecord().getEncodedRecord()).isEqualTo(failureWithOrigin.getOriginElementBytes());
        assertThat(badRecordWithOrigin.getRecord().getHumanReadableJsonRecord()).isEqualTo("origin");
    }

    @Test
    public void givenFailureWithoutPipelineStep_whenToBadRecord_thenDefaultDescription() {
        // When.
        final BadRecord badRecord = FailureTransforms.toBadRecord(Failure.from(null, "input", new IllegalStateException("Error")));

        // Then.
        assertThat(badRecord.getFailure().getDescription()).isEqualTo("Asgarde failure");
    }

    @Test
    public void givenAsgardeFailuresAndBeamBadRecords_whenAddedToTheBeamErrorHandler_thenSingleDeadLetterQueue() throws Exception {
        // Given.
        final PCollection<String> values = pipeline.apply("Create values", Create.of("1", "not a number", "bad"));

        final Result<PCollection<Integer>, Failure> result = CollectionComposer.of(values)
                .apply(PARSE, MapElementFn.into(TypeDescriptors.integers()).via((String value) -> Integer.parseInt(value)))
                .getResult();

        final PCollection<BadRecord> beamBadRecords = pipeline
                .apply("Create Beam bad record", Create.of("beam record"))
                .apply("To Beam bad record", MapElements
                        .into(TypeDescriptor.of(BadRecord.class))
                        .via(FailureTransformsTest::toBeamBadRecord))
                .setCoder(BadRecord.getCoder(pipeline));

        // When.
        final BadRecordErrorHandler<PCollection<Long>> errorHandler = pipeline.registerBadRecordErrorHandler(new CountBadRecords());
        errorHandler.addErrorCollection(result.failures().apply("To bad records", FailureTransforms.toBadRecords()));
        errorHandler.addErrorCollection(beamBadRecords);
        errorHandler.close();

        // Then.
        PAssert.that(errorHandler.getOutput()).containsInAnyOrder(3L);

        pipeline.run().waitUntilFinish();
    }

    private static BadRecord toBeamBadRecord(final String record) {
        try {
            return BadRecord.fromExceptionInformation(record, StringUtf8Coder.of(), new IllegalStateException("Beam error"), "Beam transform");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Sink of the error handler, counting the bad records.
     */
    private static class CountBadRecords extends PTransform<PCollection<BadRecord>, PCollection<Long>> {
        @Override
        public PCollection<Long> expand(final PCollection<BadRecord> badRecords) {
            return badRecords.apply("Count bad records", Count.globally());
        }
    }
}
