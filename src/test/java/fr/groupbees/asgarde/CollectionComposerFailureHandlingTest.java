package fr.groupbees.asgarde;

import fr.groupbees.asgarde.transforms.FlatMapElementFn;
import fr.groupbees.asgarde.transforms.MapElementFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.PipelineResult;
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
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Contains the tests of the failure handling edge cases of the {@link CollectionComposer} class:
 * reused DoFn, non serializable exceptions, JVM errors, transform names and metrics.
 */
public class CollectionComposerFailureHandlingTest implements Serializable {

    private static final String STEP_1 = "Step 1";
    private static final String STEP_2 = "Step 2";

    @Rule
    public transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void givenSameDoFnInstanceInTwoSteps_whenApplyComposer_thenEachFailureHasItsOwnStepName() {
        // Given.
        final MapElementFn<String, String> failOnPrefixedWords = MapElementFn
                .into(TypeDescriptors.strings())
                .via(CollectionComposerFailureHandlingTest::prefixOrFailIfPrefixed);

        final PCollection<String> words = pipeline.apply("Create words", Create.of("a", "xb"));

        // When.
        final Result<PCollection<String>, Failure> result = CollectionComposer.of(words)
                .apply(STEP_1, failOnPrefixedWords)
                .apply(STEP_2, failOnPrefixedWords)
                .getResult();

        // Then.
        PAssert.that(result.output()).empty();
        PAssert.that(toPipelineSteps(result.failures())).containsInAnyOrder(STEP_1, STEP_2);

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void givenExceptionNotSerializable_whenApplyComposer_thenFailuresWithSerializableCopyOfException() {
        // Given.
        final PCollection<String> words = pipeline.apply("Create words", Create.of("a"));

        // When.
        final Result<PCollection<String>, Failure> resultMapElements = CollectionComposer.of(words)
                .apply(STEP_1, MapElements
                        .into(TypeDescriptors.strings())
                        .via(CollectionComposerFailureHandlingTest::failWithNonSerializableException))
                .getResult();

        final Result<PCollection<String>, Failure> resultMapElementFn = CollectionComposer.of(words)
                .apply(STEP_2, MapElementFn
                        .into(TypeDescriptors.strings())
                        .via(CollectionComposerFailureHandlingTest::failWithNonSerializableException))
                .getResult();

        // Then.
        final String expectedException = NonSerializableException.class.getName() + ": " + NonSerializableException.MESSAGE;

        PAssert.that(toExceptions(resultMapElements.failures())).containsInAnyOrder(expectedException);
        PAssert.that(toExceptions(resultMapElementFn.failures())).containsInAnyOrder(expectedException);

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void givenJvmErrorInDoFn_whenRunPipeline_thenErrorIsRethrownAndNotSentToFailures() {
        // Given.
        final PCollection<String> words = pipeline.apply("Create words", Create.of("a"));

        // When.
        CollectionComposer.of(words)
                .apply(STEP_1, MapElementFn
                        .into(TypeDescriptors.strings())
                        .via(CollectionComposerFailureHandlingTest::failWithJvmError))
                .getResult();

        // Then.
        assertThatThrownBy(() -> pipeline.run().waitUntilFinish())
                .isInstanceOf(PipelineExecutionException.class)
                .hasRootCauseInstanceOf(StackOverflowError.class);
    }

    @Test
    public void givenComposerWithSteps_whenGetResult_thenFailuresTransformNameIsDeterministic() {
        // Given.
        final PCollection<String> words = pipeline.apply("Create words", Create.of("a"));

        // When.
        CollectionComposer.of(words)
                .apply(STEP_1, MapElements.into(TypeDescriptors.strings()).via((String word) -> word))
                .apply(STEP_2, MapElementFn.into(TypeDescriptors.strings()).via((String word) -> word))
                .getResult();

        // Then.
        assertThat(transformNames(pipeline)).contains("Get all failures of " + STEP_2);

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void givenComposerWithoutStep_whenGetResult_thenInputAsOutputAndNoFailure() {
        // Given.
        final PCollection<String> words = pipeline.apply("Create words", Create.of("a", "b"));

        // When.
        final Result<PCollection<String>, Failure> result = CollectionComposer.of(words).getResult();

        // Then.
        PAssert.that(result.output()).containsInAnyOrder("a", "b");
        PAssert.that(result.failures()).empty();

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void givenFailingSteps_whenRunPipeline_thenFailureCounterIncrementedByStep() {
        // Given.
        final PCollection<String> words = pipeline.apply("Create words", Create.of("a", "xb", "xc"));

        // When.
        CollectionComposer.of(words)
                .apply(STEP_1, MapElements
                        .into(TypeDescriptors.strings())
                        .via(CollectionComposerFailureHandlingTest::failIfPrefixed))
                .apply(STEP_2, MapElementFn
                        .into(TypeDescriptors.strings())
                        .via(CollectionComposerFailureHandlingTest::prefixOrFailIfPrefixed))
                .getResult();

        final PipelineResult pipelineResult = pipeline.run();
        pipelineResult.waitUntilFinish();

        // Then.
        assertThat(failureCounter(pipelineResult, STEP_1)).isEqualTo(2L);
        assertThat(failureCounter(pipelineResult, STEP_2)).isEqualTo(0L);
    }

    @Test
    public void givenIterableFailingInTheMiddle_whenApplyFlatMapElementFn_thenOnlyFailureAndNoPartialOutput() {
        // Given.
        final PCollection<String> words = pipeline.apply("Create words", Create.of("a"));

        // When.
        final Result<PCollection<String>, Failure> result = CollectionComposer.of(words)
                .apply(STEP_1, FlatMapElementFn
                        .into(TypeDescriptors.strings())
                        .via(CollectionComposerFailureHandlingTest::oneOutputThenFail))
                .getResult();

        // Then.
        PAssert.that(result.output()).empty();
        PAssert.that(toPipelineSteps(result.failures())).containsInAnyOrder(STEP_1);

        pipeline.run().waitUntilFinish();
    }

    private static Iterable<String> oneOutputThenFail(final String word) {
        return () -> new Iterator<String>() {
            private boolean first = true;

            @Override
            public boolean hasNext() {
                return true;
            }

            @Override
            public String next() {
                if (first) {
                    first = false;
                    return word;
                }
                throw new IllegalStateException("Failing in the middle of the iterable");
            }
        };
    }

    private static String failIfPrefixed(final String word) {
        if (word.startsWith("x")) {
            throw new IllegalStateException("Prefixed word " + word);
        }
        return word;
    }

    private static String prefixOrFailIfPrefixed(final String word) {
        return "x" + failIfPrefixed(word);
    }

    private static String failWithNonSerializableException(final String word) {
        throw new NonSerializableException();
    }

    private static String failWithJvmError(final String word) {
        throw new StackOverflowError("Simulated JVM error");
    }

    private static PCollection<String> toPipelineSteps(final PCollection<Failure> failures) {
        return failures.apply("To pipeline steps", MapElements
                .into(TypeDescriptors.strings())
                .via(Failure::getPipelineStep));
    }

    private static PCollection<String> toExceptions(final PCollection<Failure> failures) {
        return failures.apply("To exceptions " + failures.getName(), MapElements
                .into(TypeDescriptors.strings())
                .via((Failure failure) -> failure.getException().toString()));
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

    private static long failureCounter(final PipelineResult pipelineResult, final String pipelineStep) {
        final Iterable<MetricResult<Long>> counters = pipelineResult.metrics()
                .queryMetrics(MetricsFilter.builder()
                        .addNameFilter(MetricNameFilter.named(FailureMetrics.NAMESPACE, pipelineStep))
                        .build())
                .getCounters();

        return StreamSupport.stream(counters.spliterator(), false)
                .mapToLong(MetricResult::getAttempted)
                .sum();
    }

    /**
     * Exception holding a non serializable field, like an exception holding a client or a lock.
     */
    private static class NonSerializableException extends RuntimeException {
        private static final String MESSAGE = "Error with a non serializable field";

        @SuppressWarnings("unused")
        private final transient Object serializable = null;
        @SuppressWarnings("unused")
        private final Thread notSerializable = Thread.currentThread();

        NonSerializableException() {
            super(MESSAGE);
        }
    }
}
