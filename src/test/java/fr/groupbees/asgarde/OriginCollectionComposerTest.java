package fr.groupbees.asgarde;

import avro.generated.AvroTest;
import fr.groupbees.asgarde.transforms.FilterFn;
import fr.groupbees.asgarde.transforms.FlatMapElementFn;
import fr.groupbees.asgarde.transforms.MapElementFn;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Contains the tests of the {@link OriginCollectionComposer} class: the failures keep the origin element, the
 * element that entered the flow.
 */
public class OriginCollectionComposerTest implements Serializable {

    private static final String PARSE = "Parse";
    private static final String TO_WORDS = "To words";
    private static final String VALIDATE = "Validate";

    // The origin conversion is evaluated in the DirectRunner, in the same JVM.
    private static final AtomicInteger ORIGIN_CONVERSIONS = new AtomicInteger();

    @Rule
    public transient TestPipeline pipeline = TestPipeline.create();

    @Before
    public void resetOriginConversions() {
        ORIGIN_CONVERSIONS.set(0);
    }

    @Test
    public void givenFailureInThirdStep_whenTrackOriginElement_thenFailureWithTheElementThatEnteredTheFlow() {
        // Given.
        final PCollection<String> messages = pipeline.apply("Create messages", Create.of("psg,ol", "real,bad"));

        // When.
        final Result<PCollection<String>, Failure> result = CollectionComposer.of(messages)
                .withOriginElement(message -> "message: " + message)
                .apply(PARSE, MapElementFn.into(TypeDescriptors.strings()).via((String message) -> message.toUpperCase()))
                .apply(TO_WORDS, FlatMapElementFn.into(TypeDescriptors.strings()).via((String line) -> Arrays.asList(line.split(","))))
                .apply(VALIDATE, FilterFn.by(OriginCollectionComposerTest::isValidWord))
                .getResult();

        // Then.
        PAssert.that(result.output()).containsInAnyOrder("PSG", "OL", "REAL");
        PAssert.that(toStepInputAndOrigin(result.failures()))
                .containsInAnyOrder(VALIDATE + "|BAD|message: real,bad");

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void givenGoodAndBadElements_whenTrackOriginElement_thenOriginConvertedOnlyForTheFailures() {
        // Given.
        final PCollection<String> words = pipeline.apply("Create words", Create.of("a", "b", "bad", "c"));

        // When.
        CollectionComposer.of(words)
                .withOriginElement(OriginCollectionComposerTest::countedOriginConversion)
                .apply(PARSE, MapElementFn.into(TypeDescriptors.strings()).via((String word) -> word))
                .apply(VALIDATE, FilterFn.by(OriginCollectionComposerTest::isValidWord))
                .getResult();

        pipeline.run().waitUntilFinish();

        // Then.
        assertThat(ORIGIN_CONVERSIONS.get()).isEqualTo(1);
    }

    @Test
    public void givenOriginConversionFailing_whenFailureOccurs_thenFailureWithFallbackOriginElement() {
        // Given.
        final PCollection<String> words = pipeline.apply("Create words", Create.of("bad"));

        // When.
        final Result<PCollection<String>, Failure> result = CollectionComposer.of(words)
                .withOriginElement(OriginCollectionComposerTest::failingOriginConversion)
                .apply(VALIDATE, FilterFn.by(OriginCollectionComposerTest::isValidWord))
                .getResult();

        // Then.
        PAssert.that(result.failures().apply(MapElements.into(TypeDescriptors.strings()).via(Failure::getOriginElement)))
                .satisfies(origins -> {
                    assertThat(origins).singleElement().asString()
                            .startsWith("<conversion of the origin element failed: java.lang.IllegalStateException");
                    return null;
                });

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void givenStepsBeforeTrackingTheOrigin_whenFailures_thenOnlyTheNextStepsGiveTheOrigin() {
        // Given.
        final PCollection<String> words = pipeline.apply("Create words", Create.of("xa", "bad"));

        // When.
        final Result<PCollection<String>, Failure> result = CollectionComposer.of(words)
                .apply("Before origin", MapElementFn.into(TypeDescriptors.strings()).via(OriginCollectionComposerTest::failIfPrefixed))
                .withOriginElement(word -> "origin " + word)
                .apply(VALIDATE, FilterFn.by(OriginCollectionComposerTest::isValidWord))
                .getResult();

        // Then.
        PAssert.that(toStepInputAndOrigin(result.failures()))
                .containsInAnyOrder("Before origin|xa|null", VALIDATE + "|bad|origin bad");

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void givenOriginComposerWithoutStep_whenGetResult_thenInputAsOutputAndNoFailure() {
        // Given.
        final PCollection<String> words = pipeline.apply("Create words", Create.of("a", "b"));

        // When.
        final Result<PCollection<String>, Failure> result = CollectionComposer.of(words)
                .withOriginElement(word -> word)
                .getResult();

        // Then.
        PAssert.that(result.output()).containsInAnyOrder("a", "b");
        PAssert.that(result.failures()).empty();

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void givenOriginComposer_whenGetResult_thenDeterministicTransformNamesAndSameResultTwice() {
        // Given.
        final PCollection<String> words = pipeline.apply("Create words", Create.of("a"));

        final OriginCollectionComposer<String, String> composer = CollectionComposer.of(words)
                .withOriginElement(word -> word)
                .apply(PARSE, MapElementFn.into(TypeDescriptors.strings()).via((String word) -> word))
                .apply(VALIDATE, FilterFn.by(OriginCollectionComposerTest::isValidWord));

        // When.
        final Result<PCollection<String>, Failure> result1 = composer.getResult();
        final Result<PCollection<String>, Failure> result2 = composer.getResult();

        // Then.
        assertThat(result2).isSameAs(result1);
        assertThat(transformNames(pipeline)).contains(
                PARSE + " - keep origin",
                PARSE,
                VALIDATE,
                "Remove origin of " + VALIDATE,
                "Get all failures of " + VALIDATE
        );

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void givenOutputTypeWithoutDefaultCoder_whenSetCoder_thenOutputsWithTheGivenCoder() {
        // Given.
        final PCollection<String> names = pipeline.apply("Create names", Create.of("Avro test"));

        // When.
        final Result<PCollection<GenericRecord>, Failure> result = CollectionComposer.of(names)
                .withOriginElement(name -> name)
                .apply("To record", MapElementFn
                        .into(TypeDescriptor.of(GenericRecord.class))
                        .via(OriginCollectionComposerTest::toGenericRecord))
                .setCoder(AvroCoder.of(GenericRecord.class, AvroTest.getClassSchema()))
                .getResult();

        // Then.
        PAssert.that(result.output().apply(MapElements.into(TypeDescriptors.strings()).via(record -> record.get("name").toString())))
                .containsInAnyOrder("Avro test");

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void givenFailuresWithOrigin_whenRunPipeline_thenFailureCounterIncrementedByStep() {
        // Given.
        final PCollection<String> words = pipeline.apply("Create words", Create.of("a", "bad", "bad"));

        // When.
        CollectionComposer.of(words)
                .withOriginElement(word -> word)
                .apply(VALIDATE, FilterFn.by(OriginCollectionComposerTest::isValidWord))
                .getResult();

        final PipelineResult pipelineResult = pipeline.run();
        pipelineResult.waitUntilFinish();

        // Then.
        final Iterable<MetricResult<Long>> counters = pipelineResult.metrics()
                .queryMetrics(MetricsFilter.builder()
                        .addNameFilter(MetricNameFilter.named(FailureMetrics.NAMESPACE, VALIDATE))
                        .build())
                .getCounters();

        assertThat(StreamSupport.stream(counters.spliterator(), false).mapToLong(MetricResult::getAttempted).sum())
                .isEqualTo(2L);
    }

    private static boolean isValidWord(final String word) {
        if (word.equalsIgnoreCase("bad")) {
            throw new IllegalArgumentException("Bad word " + word);
        }
        return true;
    }

    private static String failIfPrefixed(final String word) {
        if (word.startsWith("x")) {
            throw new IllegalStateException("Prefixed word " + word);
        }
        return word;
    }

    private static String countedOriginConversion(final String origin) {
        ORIGIN_CONVERSIONS.incrementAndGet();
        return origin;
    }

    private static String failingOriginConversion(final String origin) {
        throw new IllegalStateException("Origin conversion error");
    }

    private static GenericRecord toGenericRecord(final String name) {
        final GenericRecord record = new GenericData.Record(AvroTest.getClassSchema());
        record.put("id", 1);
        record.put("name", name);
        return record;
    }

    private static PCollection<String> toStepInputAndOrigin(final PCollection<Failure> failures) {
        return failures.apply("To step, input and origin", MapElements
                .into(TypeDescriptors.strings())
                .via((Failure failure) -> failure.getPipelineStep() + "|" + failure.getInputElement() + "|" + failure.getOriginElement()));
    }

    private static List<String> transformNames(final Pipeline pipeline) {
        final List<String> names = new ArrayList<>();
        pipeline.traverseTopologically(new Pipeline.PipelineVisitor.Defaults() {
            @Override
            public CompositeBehavior enterCompositeTransform(final TransformHierarchy.Node node) {
                names.add(node.getFullName());
                return CompositeBehavior.ENTER_TRANSFORM;
            }

            @Override
            public void visitPrimitiveTransform(final TransformHierarchy.Node node) {
                names.add(node.getFullName());
            }
        });
        return names;
    }
}
