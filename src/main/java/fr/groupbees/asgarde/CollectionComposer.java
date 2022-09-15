package fr.groupbees.asgarde;

import fr.groupbees.asgarde.transforms.BaseElementFn;
import fr.groupbees.asgarde.transforms.FilterFn;
import fr.groupbees.asgarde.transforms.MapElementFn;
import fr.groupbees.asgarde.transforms.MapProcessContextFn;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.values.*;

import java.util.Collections;
import java.util.List;

import static java.util.stream.Collectors.toList;

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

    private final PCollection<T> outputPCollection;
    private final PCollectionList<Failure> failuresPCollection;
    private final Coder<Failure> failuresCoder;

    private CollectionComposer(PCollection<T> outputPCollection,
                               PCollectionList<Failure> failuresPCollection,
                               Coder<Failure> failuresCoder) {
        this.outputPCollection = outputPCollection;
        this.failuresPCollection = failuresPCollection;
        this.failuresCoder = failuresCoder;
    }

    /**
     * Initializes a composer with given PCollection with an empty side collection.
     *
     * @param inputPCollection the input PCollection
     * @param <T>              the type of the input PCollection
     * @return The CollectionComposer instance from the input PCollection
     */
    public static <T> CollectionComposer<T> of(final PCollection<T> inputPCollection) {
        return new CollectionComposer<>(
                inputPCollection,
                PCollectionList.empty(inputPCollection.getPipeline()),
                SerializableCoder.of(TypeDescriptor.of(Failure.class))
        );
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
        return apply(
                name, mapElements
                        .exceptionsInto(TypeDescriptor.of(Failure.class))
                        .exceptionsVia(exceptionElement -> Failure.from(name, exceptionElement))
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
        return apply(
                name, flatMapElements
                        .exceptionsInto(TypeDescriptor.of(Failure.class))
                        .exceptionsVia(exceptionElement -> Failure.from(name, exceptionElement))
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
        return new CollectionComposer<>(result.output(), failuresPCollection.and(result.failures()), failuresCoder);
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
        doFn.setPipelineStep(name);

        final PCollectionTuple tuple = outputPCollection.apply(name,
                ParDo.of(doFn).withOutputTags(doFn.getOutputTag(), TupleTagList.of(doFn.getFailuresTag())));

        final PCollection<T> outputCollection = tuple
                .get(doFn.getOutputTag())
                .setTypeDescriptor(outputPCollection.getCoder().getEncodedTypeDescriptor())
                .setCoder(outputPCollection.getCoder());

        return new CollectionComposer<>(outputCollection,
                failuresPCollection.and(tuple.get(doFn.getFailuresTag())),
                failuresCoder
        );
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
        doFn.setPipelineStep(name);

        final PCollectionTuple tuple = outputPCollection.apply(name,
                ParDo.of(doFn)
                        .withOutputTags(doFn.getOutputTag(), TupleTagList.of(doFn.getFailuresTag()))
                        .withSideInputs(sideInputs));

        final PCollection<OutputT> outputCollection = tuple
                .get(doFn.getOutputTag())
                .setTypeDescriptor(doFn.getOutputTypeDescriptor());

        return new CollectionComposer<>(
                outputCollection,
                failuresPCollection.and(tuple.get(doFn.getFailuresTag())),
                failuresCoder
        );
    }

    /**
     * Set the given {@link Coder} to the current output {@link PCollection} in the flow.
     *
     * @return CollectionComposer with current output and failure
     */
    public CollectionComposer<T> setCoder(final Coder<T> coder) {
        final PCollection<T> outputPCollectionWithCoder = outputPCollection.setCoder(coder);

        return new CollectionComposer<>(outputPCollectionWithCoder, failuresPCollection, failuresCoder);
    }

    /**
     * Set the given {@link Coder} to the current failure {@link PCollection} in the flow.
     *
     * @return CollectionComposer with current output and failure
     */
    public CollectionComposer<T> setFailureCoder(final Coder<Failure> failuresCoder) {
        return new CollectionComposer<T>(outputPCollection, failuresPCollection, failuresCoder);
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
        final List<PCollection<Failure>> failureCollectionsWithCoder = failuresPCollection.getAll()
                .stream()
                .map(failureCollection -> failureCollection.setCoder(failuresCoder))
                .collect(toList());

        return PCollectionList.of(failureCollectionsWithCoder)
                .apply("Get all failures" + this, Flatten.pCollections());
    }
}