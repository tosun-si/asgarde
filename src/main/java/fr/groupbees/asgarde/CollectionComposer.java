package fr.groupbees.asgarde;

import fr.groupbees.asgarde.transforms.BaseElementFn;
import fr.groupbees.asgarde.transforms.FilterFn;
import fr.groupbees.asgarde.transforms.MapElementFn;
import fr.groupbees.asgarde.transforms.MapProcessContextFn;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.values.*;

import java.util.Collections;

import static java.util.Objects.requireNonNull;

/**
 * This class allows to compose some transforms from an input {@link PCollection}.
 *
 * <p>
 * The purpose of this class is to handle and centralize the eventual errors in a pipeline flow.
 * This class captures all the failures met in a flow.
 * </p>
 *
 * <p>
 * This composer class can take the existing transforms like {@link MapElements} and {@link FlatMapElements}
 * but take also custom classes that corresponds to custom {@link DoFn}, and extends {@link BaseElementFn}.
 * These classes internally handle errors in order to not break the Job.
 * </p>
 *
 * <p>
 * For {@link BaseElementFn} the logic of Coder are handled via {@link TypeDescriptor} for map operations and
 * previous {@link PCollection} for {@link FilterFn}
 * </p>
 *
 * <p>
 * Finally a Result with output PCollection and failures PCollection is returned by the composition class :
 * From this, a {@link PCollection} of output T can be recovered and a {@link PCollection} of {@link Failure}
 * </p>
 *
 * @param <T> the type of elements in Composer class
 */
public class CollectionComposer<T> {

    private static final String FAILURES_STEP_NAME = "Get all failures";

    private final PCollection<T> outputPCollection;
    private final PCollectionList<Failure> failuresPCollection;
    private final String lastStepName;

    // Failure settings of the next steps, see withInputElementToString and withEncodedElements.
    private final SerializableFunction<Object, String> inputElementToString;
    private final boolean encodeElements;

    // Built once: calling getResult() several times must not apply the same (deterministic) transform name twice.
    private PCollection<Failure> allFailures;

    private CollectionComposer(PCollection<T> outputPCollection,
                               PCollectionList<Failure> failuresPCollection,
                               String lastStepName,
                               SerializableFunction<Object, String> inputElementToString,
                               boolean encodeElements) {
        this.outputPCollection = outputPCollection;
        this.failuresPCollection = failuresPCollection;
        this.lastStepName = lastStepName;
        this.inputElementToString = inputElementToString;
        this.encodeElements = encodeElements;
    }

    /**
     * Initializes a composer with given PCollection with an empty side collection.
     *
     * @param inputPCollection the input PCollection
     * @param <T>              the type of the input PCollection
     * @return The CollectionComposer instance from the input PCollection
     */
    public static <T> CollectionComposer<T> of(final PCollection<T> inputPCollection) {
        return new CollectionComposer<>(inputPCollection, PCollectionList.empty(inputPCollection.getPipeline()), null, null, false);
    }

    /**
     * Applies a {@link MapElements} in the input PCollection.
     *
     * <p>Internally, default {@link MapElements.MapWithFailures#exceptionsInto(TypeDescriptor)}
     * and {@link MapElements.MapWithFailures#exceptionsVia(ProcessFunction)} are applied on the {@link Failure} object.</p>
     *
     * <p>Example usage:</p>
     *
     * <pre>{@code
     *     Result<PCollection<String>, Failure>> result = CollectionComposer.of(words)
     *         .apply("Map", MapElements
     *                          .into(TypeDescriptors.integers())
     *                          .via((String word) -> 1 / word.length)  // Could throw ArithmeticException
     *               )
     *               .getResult();
     *     PCollection<String> output = result.output();
     *     PCollection<Failure> failures = result.failures();
     *     }</pre>
     *
     * @param name        name of the current transform application
     * @param mapElements mapElements
     * @param <OutputT>   the output type of the output PCollection
     * @return CollectionComposer of output
     */
    public <OutputT> CollectionComposer<OutputT> apply(final String name,
                                                       final MapElements<T, OutputT> mapElements) {
        final SerializableFunction<Object, String> elementToString = inputElementToString;
        final Coder<T> inputCoder = encodedInputCoder();

        return apply(
                name, mapElements
                        .exceptionsInto(TypeDescriptor.of(Failure.class))
                        .exceptionsVia(exceptionElement -> toFailure(name, exceptionElement, elementToString, inputCoder))
        );
    }

    /**
     * Applies a {@link FlatMapElements} in the input PCollection.
     * <p>Internally, default {@link FlatMapElements.FlatMapWithFailures#exceptionsInto(TypeDescriptor)}
     * and {@link FlatMapElements.FlatMapWithFailures#exceptionsVia(ProcessFunction)} are applied on the {@link Failure} object.</p>
     *
     * <p>Example usage:</p>
     *
     * <pre>{@code
     *     Result<PCollection<String>, Failure>> result = CollectionComposer.of(words)
     *        .apply("FlatMap", FlatMapElements
     *                              .into(TypeDescriptors.strings())
     *                              // Could throw ArrayIndexOutOfBoundsException
     *                              .via((String line) -> Arrays.asList(Arrays.copyOfRange(line.split(" "), 1, 5)))
     *              )
     *              .getResult();
     *     PCollection<String> output = result.output();
     *     PCollection<Failure> failures = result.failures();
     *     }</pre>
     *
     * @param name            name of the current transform application
     * @param flatMapElements flatMapElements
     * @param <OutputT>       the output type of the output PCollection
     * @return CollectionComposer of output
     */
    public <OutputT> CollectionComposer<OutputT> apply(final String name,
                                                       final FlatMapElements<T, OutputT> flatMapElements) {
        final SerializableFunction<Object, String> elementToString = inputElementToString;
        final Coder<T> inputCoder = encodedInputCoder();

        return apply(
                name, flatMapElements
                        .exceptionsInto(TypeDescriptor.of(Failure.class))
                        .exceptionsVia(exceptionElement -> toFailure(name, exceptionElement, elementToString, inputCoder))
        );
    }

    /**
     * Applies a {@link PTransform} with error handling in the input PCollection.
     * <p>In this case the elements concerned by this method, are {@link org.apache.beam.sdk.transforms.MapElements.MapWithFailures}
     * or {@link org.apache.beam.sdk.transforms.FlatMapElements.FlatMapWithFailures} elements.
     * </p>
     *
     * <p>These objects are provided by {@link MapElements} and {@link FlatMapElements}
     * When this method is called, the client of api must handle the error externally
     * </p>
     *
     * <p>Example usage:</p>
     *
     * <pre>{@code
     *     public static <T> Failure from(final String pipelineStep, final WithFailures.ExceptionElement<T> exceptionElement) {
     *         final T inputElement = exceptionElement.element();
     *         final String inputElementAsString = .....
     *
     *         return Failure.from(pipelineStep,
     *                             inputElementAsString,
     *                             exceptionElement.exception())
     *     }
     *
     *     // Example with MapElements.
     *     Result<PCollection<Integer>, Failure>> result = CollectionComposer.of(words)
     *        .apply("Map", MapElements
     *                         .into(TypeDescriptors.integers())
     *                         .via((String word) -> 1 / word.length())  // Could throw ArithmeticException
     *                         .exceptionsInto(TypeDescriptor.of(Failure.class))
     *                         .exceptionsVia(excElement -> Failure.from(name, excElement))
     *              )
     *              .getResult();
     *
     *     // Example with FlatMapElements.
     *     Result<PCollection<String>, Failure>> result = CollectionComposer.of(words)
     *        .apply("FlatMap", FlatMapElements
     *                              .into(TypeDescriptors.strings())
     *                              .via((String line) -> Arrays.asList(Arrays.copyOfRange(line.split(" "), 1, 5)))
     *                              .exceptionsInto(TypeDescriptor.of(Failure.class))
     *                              .exceptionsVia(excElement -> Failure.from(name, excElement))
     *              )
     *              .getResult();
     *
     *     PCollection<String> output = result.output();
     *     PCollection<Failure> failures = result.failures();
     *     }</pre>
     *
     * @param name      name of the current transform application
     * @param transform transform with error handling provided by {@link MapElements} and {@link FlatMapElements} for example
     * @param <OutputT> the output type of the output PCollection
     * @return CollectionComposer of output
     */
    public <OutputT> CollectionComposer<OutputT> apply(final String name,
                                                       final PTransform<PCollection<T>, Result<PCollection<OutputT>, Failure>> transform) {
        final Result<PCollection<OutputT>, Failure> result = outputPCollection.apply(name, transform);
        return new CollectionComposer<>(result.output(), failuresPCollection.and(result.failures()), name, inputElementToString, encodeElements);
    }

    /**
     * The same as {@link #apply(String, BaseElementFn, Iterable)} but without side inputs.
     *
     * @param name      the name of the current step
     * @param doFn      current transformation
     * @param <OutputT> the output after transformation
     * @return CollectionComposer of output
     */
    public <OutputT> CollectionComposer<OutputT> apply(final String name,
                                                       final BaseElementFn<T, OutputT> doFn) {
        return apply(name, doFn, Collections.emptyList());
    }

    /**
     * The same as {@link #apply(String, BaseElementFn, Iterable)} but specific to {@link FilterFn}.
     *
     * <p>
     * A separate method is used for filter FN because the logic of {@link org.apache.beam.sdk.coders.Coder}
     * is different in this case. In this case, it's preferred to doesn't force the client of api to
     * give an output {@link TypeDescriptor} because this operation is used for filtering (input same as output).
     * </p>
     *
     * <p>
     * The approach in this case is to retrieve the Coder and typ descriptor from the previous {@link PCollection}
     * in the flow
     * </p>
     *
     * @param name name of current operation
     * @param doFn filter Fn
     * @return CollectionComposer of filtered element
     */
    public CollectionComposer<T> apply(final String name,
                                       final FilterFn<T> doFn) {
        final BaseElementFn<T, T> stepFn = doFn.forPipelineStep(name, inputElementToString, encodedInputCoder());

        final PCollectionTuple tuple = outputPCollection.apply(name,
                ParDo.of(stepFn).withOutputTags(stepFn.getOutputTag(), TupleTagList.of(stepFn.getFailuresTag())));

        final PCollection<T> outputCollection = tuple
                .get(stepFn.getOutputTag())
                .setTypeDescriptor(outputPCollection.getCoder().getEncodedTypeDescriptor())
                .setCoder(outputPCollection.getCoder());

        return new CollectionComposer<>(outputCollection, failuresPCollection.and(tuple.get(stepFn.getFailuresTag())), name, inputElementToString, encodeElements);
    }

    /**
     * Applies a mapper that extends {@link BaseElementFn} in the input PCollection.
     * <p>A {@link BaseElementFn} is a generic {@link DoFn} that handle eventual errors, with {@link TupleTag}</p>
     *
     * <p>
     * These objects are provided by for example by {@link MapElementFn},
     * {@link MapProcessContextFn}
     * </p>
     *
     * <p>We can pass side inputs for this DoFn</p>
     *
     * <p>
     * In this case, the output {@link TypeDescriptor} is given by the given {@link BaseElementFn}
     * and by default the code is inferred from this type descriptor.
     * </p>
     *
     * <p>Example usage:</p>
     *
     * <pre>{@code
     *     // Example with MapElementFn.
     *     Result<PCollection<String>, Failure>> result = CollectionComposer.of(words)
     *        .apply("MapElementFn", MapElementFn
     *                                  .into(TypeDescriptors.integers())
     *                                  .via((String word) -> 1 / word.length)  // Could throw ArithmeticException
     *              )
     *              .getResult();
     *
     *     // Example with MapProcessContextFn.
     *     Result<PCollection<String>, Failure>> result = CollectionComposer.of(words)
     *        .apply("MapProcessContextFn", MapProcessContextFn
     *                                         .into(TypeDescriptors.strings())
     *                                         .via((ProcessContext ctx) -> 1 / ctx.element().length)
     *              )
     *              .getResult();
     *
     *     PCollection<String> output = result.output();
     *     PCollection<Failure> failures = result.failures();
     *     }</pre>
     *
     * @param name       name of the current transform application
     * @param doFn       current base element fn, for example by
     *                   {@link MapElementFn} and
     *                   {@link MapProcessContextFn}
     * @param sideInputs it's possible to pass and retrieve side inputs
     * @param <OutputT>  the output type of the output PCollection
     * @return CollectionComposer of output
     */
    public <OutputT> CollectionComposer<OutputT> apply(final String name,
                                                       final BaseElementFn<T, OutputT> doFn,
                                                       final Iterable<? extends PCollectionView<?>> sideInputs) {
        final BaseElementFn<T, OutputT> stepFn = doFn.forPipelineStep(name, inputElementToString, encodedInputCoder());

        final PCollectionTuple tuple = outputPCollection.apply(name,
                ParDo.of(stepFn)
                        .withOutputTags(stepFn.getOutputTag(), TupleTagList.of(stepFn.getFailuresTag()))
                        .withSideInputs(sideInputs));

        final PCollection<OutputT> outputCollection = tuple
                .get(stepFn.getOutputTag())
                .setTypeDescriptor(stepFn.getOutputTypeDescriptor());

        return new CollectionComposer<>(outputCollection, failuresPCollection.and(tuple.get(stepFn.getFailuresTag())), name, inputElementToString, encodeElements);
    }

    /**
     * Keeps the origin element of each element for the next steps: their failures give, with
     * {@link Failure#getOriginElement()}, the element that entered the flow (the current output of this composer).
     *
     * <p>
     * The given function converts the origin element to a string. It's evaluated <b>only when a failure occurs</b>:
     * e.g. the full payload to replay the failure from the start, or an identifier (message id, business key).
     * </p>
     *
     * <p>
     * The next steps must be function based Asgarde DoFn classes ({@link MapElementFn}, {@code FlatMapElementFn},
     * {@link FilterFn}), see {@link OriginCollectionComposer}.
     * </p>
     *
     * @param originToString converts the origin element to a string, evaluated only when a failure occurs
     * @return a composer keeping the origin element of each element
     */
    public OriginCollectionComposer<T, T> withOriginElement(final SerializableFunction<T, String> originToString) {
        return OriginCollectionComposer.of(
                outputPCollection,
                failuresPCollection,
                lastStepName,
                originToString,
                inputElementToString,
                encodeElements
        );
    }

    /**
     * Converts the input elements of the next steps to a string in the failures with the given function, instead of
     * {@code toString()}: e.g. JSON, a format masking sensitive data, or an identifier.
     *
     * <p>
     * The function is evaluated <b>only when a failure occurs</b>, for all the kinds of steps. It never breaks the
     * job: if it throws or returns {@code null}, the element is converted with {@code toString()}.
     * </p>
     *
     * @param inputElementToString converts an input element to a string
     * @return a composer converting the input elements of the next steps with the given function
     */
    public CollectionComposer<T> withInputElementToString(final SerializableFunction<Object, String> inputElementToString) {
        return new CollectionComposer<>(outputPCollection, failuresPCollection, lastStepName, requireNonNull(inputElementToString), encodeElements);
    }

    /**
     * Also keeps, in the failures of the next steps, the input element (and the origin element) encoded with its
     * coder: the coder of the PCollection consumed by the step, to replay the element exactly.
     *
     * <p>
     * The element is encoded <b>only when a failure occurs</b>. It never breaks the job: if the element can't be
     * encoded, the failure has no bytes.
     * </p>
     *
     * @return a composer encoding the elements of the next steps in the failures
     */
    public CollectionComposer<T> withEncodedElements() {
        return new CollectionComposer<>(outputPCollection, failuresPCollection, lastStepName, inputElementToString, true);
    }

    /**
     * Set the given {@link Coder} to the current output {@link PCollection} in the flow.
     *
     * @return CollectionComposer with current output and failure
     */
    public CollectionComposer<T> setCoder(final Coder<T> coder) {
        outputPCollection.setCoder(coder);

        return this;
    }

    /**
     * Get the result of flow.
     * It corresponds to the output PCollection and failures PCollection.
     *
     * @return the {@link Result} with output PCollection and failures PCollection
     */
    public Result<PCollection<T>, Failure> getResult() {
        return Result.of(outputPCollection, getFailurePCollection());
    }

    /**
     * Gets all the failures in a PCollection.
     *
     * @return all failures in a PCollection
     */
    private PCollection<Failure> getFailurePCollection() {
        if (allFailures == null) {
            allFailures = flattenFailures();
        }

        return allFailures;
    }

    private PCollection<Failure> flattenFailures() {
        // Deterministic name: Dataflow streaming updates (--update) need stable transform names.
        final String stepName = lastStepName == null ? FAILURES_STEP_NAME : FAILURES_STEP_NAME + " of " + lastStepName;

        if (failuresPCollection.size() == 0) {
            return outputPCollection.getPipeline().apply(stepName, Create.empty(TypeDescriptor.of(Failure.class)));
        }

        return failuresPCollection.apply(stepName, Flatten.pCollections());
    }

    /**
     * Coder of the elements consumed by the next step when they're encoded in the failures, {@code null} otherwise.
     * Taken when the step is built: the coder the pipeline uses for these elements.
     */
    private Coder<T> encodedInputCoder() {
        return encodeElements ? outputPCollection.getCoder() : null;
    }

    private static <T> Failure toFailure(final String pipelineStep,
                                         final WithFailures.ExceptionElement<T> exceptionElement,
                                         final SerializableFunction<Object, String> elementToString,
                                         final Coder<T> inputCoder) {
        FailureMetrics.counter(pipelineStep).inc();

        final T element = exceptionElement.element();
        final Failure failure = Failure.from(pipelineStep, element, exceptionElement.exception(), elementToString);

        return inputCoder == null ? failure : failure.withEncodedInputElement(element, inputCoder);
    }
}