package fr.groupbees.asgarde;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.time.Instant;

/**
 * Transforms converting the {@link Failure} objects, to write them to a sink or to integrate them with the Beam
 * native error handling:
 *
 * <ul>
 *     <li>{@link #toRows()}: Beam {@link Row}s with the {@link #SCHEMA}, e.g. to write the failures to BigQuery
 *     with {@code BigQueryIO.<Row>write().useBeamSchema()}</li>
 *     <li>{@link #toBadRecords()}: Beam {@link BadRecord}s, to add the failures to a Beam
 *     {@link org.apache.beam.sdk.transforms.errorhandling.ErrorHandler} with the bad records of the Beam IOs</li>
 * </ul>
 *
 * <p>
 * These conversions are explicit: {@link Failure} has no default schema, its coder stays the same (a default schema
 * would change the encoding of the failures and lose the exception).
 * </p>
 */
public final class FailureTransforms {

    /**
     * Schema of the failures converted with {@link #toRows()}.
     */
    public static final Schema SCHEMA = Schema.builder()
            .addNullableField("pipelineStep", FieldType.STRING)
            .addStringField("inputElement")
            .addNullableField("originElement", FieldType.STRING)
            .addStringField("exceptionType")
            .addNullableField("exceptionMessage", FieldType.STRING)
            .addStringField("stackTrace")
            .addNullableField("timestamp", FieldType.DATETIME)
            .build();

    private static final String DEFAULT_DESCRIPTION = "Asgarde failure";

    private FailureTransforms() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    /**
     * @return a transform converting the failures to {@link Row}s with the {@link #SCHEMA}
     */
    public static ToRows toRows() {
        return new ToRows();
    }

    /**
     * @return a transform converting the failures to Beam {@link BadRecord}s
     */
    public static ToBadRecords toBadRecords() {
        return new ToBadRecords();
    }

    /**
     * Converts a failure to a {@link Row} with the {@link #SCHEMA}.
     *
     * @param failure the failure
     * @return the row of the failure
     */
    public static Row toRow(final Failure failure) {
        final Instant timestamp = failure.getTimestamp();

        return Row.withSchema(SCHEMA)
                .addValues(
                        failure.getPipelineStep(),
                        failure.getInputElement(),
                        failure.getOriginElement(),
                        failure.getExceptionType(),
                        failure.getExceptionMessage(),
                        failure.getStackTrace(),
                        timestamp == null ? null : new org.joda.time.Instant(timestamp.toEpochMilli())
                )
                .build();
    }

    /**
     * Converts a failure to a Beam {@link BadRecord}:
     *
     * <ul>
     *     <li>record: the origin element when it's tracked (to replay from the start), the input element otherwise,
     *     as a string (no encoded record: Asgarde keeps the elements as strings)</li>
     *     <li>failure: the exception, its stack trace, and the pipeline step as description</li>
     * </ul>
     *
     * @param failure the failure
     * @return the bad record of the failure
     */
    public static BadRecord toBadRecord(final Failure failure) {
        final String element = failure.getOriginElement() != null ? failure.getOriginElement() : failure.getInputElement();
        final String description = failure.getPipelineStep() != null ? failure.getPipelineStep() : DEFAULT_DESCRIPTION;

        return BadRecord.builder()
                .setRecord(BadRecord.Record.builder()
                        .setHumanReadableJsonRecord(element)
                        .build())
                .setFailure(BadRecord.Failure.builder()
                        .setException(failure.getException().toString())
                        .setExceptionStacktrace(failure.getStackTrace())
                        .setDescription(description)
                        .build())
                .build();
    }

    /**
     * Transform converting the failures to {@link Row}s with the {@link #SCHEMA}.
     */
    public static final class ToRows extends PTransform<PCollection<Failure>, PCollection<Row>> {

        private ToRows() {
        }

        @Override
        public PCollection<Row> expand(final PCollection<Failure> failures) {
            return failures
                    .apply("Failure to row", MapElements.into(TypeDescriptors.rows()).via(FailureTransforms::toRow))
                    .setRowSchema(SCHEMA);
        }
    }

    /**
     * Transform converting the failures to Beam {@link BadRecord}s.
     */
    public static final class ToBadRecords extends PTransform<PCollection<Failure>, PCollection<BadRecord>> {

        private ToBadRecords() {
        }

        @Override
        public PCollection<BadRecord> expand(final PCollection<Failure> failures) {
            return failures
                    .apply("Failure to bad record", MapElements
                            .into(TypeDescriptor.of(BadRecord.class))
                            .via(FailureTransforms::toBadRecord))
                    .setCoder(BadRecord.getCoder(failures.getPipeline()));
        }
    }
}
