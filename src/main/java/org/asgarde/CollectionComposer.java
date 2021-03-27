package org.asgarde;

import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.values.*;
import org.asgarde.transforms.BaseElementFn;
import org.asgarde.transforms.FilterFn;
import org.asgarde.transforms.MapElementFn;
import org.asgarde.transforms.MapProcessContextFn;

import java.io.Serializable;
import java.util.Collections;

/**
 * This class allows to composes some transforms from an input {@link PCollection}.
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
 * Finally a Result<PCollection<T>, Failures>> is returned by the composition class :
 * From this, a {@link PCollection} of output T can be recovered and a {@link PCollection} of {@link Failure}
 * <p/>
 */
public class CollectionComposer<T> {

    private final PCollection<T> outputPCollection;
    private final PCollectionList<Failure> failuresPCollection;

    private CollectionComposer(PCollection<T> outputPCollection, PCollectionList<Failure> failuresPCollection) {
        this.outputPCollection = outputPCollection;
        this.failuresPCollection = failuresPCollection;
    }

    /**
     * Initializes a composer with given PCollection & an empty side collection.
     */
    public static <T> CollectionComposer<T> of(final PCollection<T> inputPCollection) {
        return new CollectionComposer<>(inputPCollection, PCollectionList.empty(inputPCollection.getPipeline()));
    }

    /**
     * Applies a {@link MapElements} in the input PCollection.
     *
     * <p>Internally, default {@link MapElements.MapWithFailures#exceptionsInto(TypeDescriptor)}
     * and {@link MapElements.MapWithFailures#exceptionsVia(ProcessFunction) are applied on the {@link Failure} object.</p>
     *
     * <p>Example usage:
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
     * </p>
     *
     * @param name        name of the current transform application
     * @param mapElements mapElements
     * @param <OutputT>   the output type of the output PCollection
     * @return CollectionComposer<OutputT>
     */
    public <OutputT> CollectionComposer<OutputT> apply(final String name,
                                                       final MapElements<T, OutputT> mapElements) {
        return apply(name, mapElements.exceptionsInto(TypeDescriptor.of(Failure.class)).exceptionsVia(Failure::from));
    }

    /**
     * Applies a {@link FlatMapElements} in the input PCollection.
     * <p>Internally, default {@link FlatMapElements.FlatMapWithFailures#exceptionsInto(TypeDescriptor)}
     * and {@link FlatMapElements.FlatMapWithFailures#exceptionsVia(ProcessFunction) are applied on the {@link Failure} object.</p>
     *
     * <p>Example usage:
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
     * </p>
     *
     * @param name            name of the current transform application
     * @param flatMapElements flatMapElements
     * @param <OutputT>       the output type of the output PCollection
     * @return CollectionComposer<OutputT>
     */
    public <OutputT> CollectionComposer<OutputT> apply(final String name,
                                                       final FlatMapElements<T, OutputT> flatMapElements) {
        return apply(name, flatMapElements.exceptionsInto(TypeDescriptor.of(Failure.class)).exceptionsVia(Failure::from));
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
     * <p>Example usage:
     *
     * <pre>{@code
     *     public static <T> Failure from(final WithFailures.ExceptionElement<T> exceptionElement) {
     *         final T inputElement = exceptionElement.element();
     *         final String inputElementAsString = .....
     *
     *         return Failure.builder()
     *                 .inputElementStep(inputElementAsString)
     *                 .exception(exceptionElement.exception())
     *                 .build();
     *     }
     *
     *     // Example with MapElements.
     *     Result<PCollection<Integer>, Failure>> result = CollectionComposer.of(words)
     *        .apply("Map", MapElements
     *                         .into(TypeDescriptors.integers())
     *                         .via((String word) -> 1 / word.length())  // Could throw ArithmeticException
     *                         .exceptionsInto(TypeDescriptor.of(Failure.class))
     *                         .exceptionsVia(Failure::from)
     *              )
     *              .getResult();
     *
     *     // Example with FlatMapElements.
     *     Result<PCollection<String>, Failure>> result = CollectionComposer.of(words)
     *        .apply("FlatMap", FlatMapElements
     *                              .into(TypeDescriptors.strings())
     *                              .via((String line) -> Arrays.asList(Arrays.copyOfRange(line.split(" "), 1, 5)))
     *                              .exceptionsInto(TypeDescriptor.of(Failure.class))
     *                              .exceptionsVia(Failure::from)
     *              )
     *              .getResult();
     *
     *     PCollection<String> output = result.output();
     *     PCollection<Failure> failures = result.failures();
     *     }</pre>
     * </p>
     *
     * @param name      name of the current transform application
     * @param transform transform with error handling provided by {@link MapElements} and {@link FlatMapElements} for example
     * @param <OutputT> the output type of the output PCollection
     * @return CollectionComposer<OutputT>
     */
    public <OutputT> CollectionComposer<OutputT> apply(final String name,
                                                       final PTransform<PCollection<T>, Result<PCollection<OutputT>, Failure>> transform) {
        final Result<PCollection<OutputT>, Failure> result = outputPCollection.apply(name, transform);
        return new CollectionComposer<>(result.output(), failuresPCollection.and(result.failures()));
    }

    /**
     * The same as {@link #apply(String, BaseElementFn, Iterable)} but without side inputs.
     */
    public <OutputT extends Serializable> CollectionComposer<OutputT> apply(final String name,
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
     * @return CollectionComposer<T>
     */
    public CollectionComposer<T> apply(final String name,
                                       final FilterFn<T> doFn) {
        final PCollectionTuple tuple = outputPCollection.apply(name,
                ParDo.of(doFn).withOutputTags(doFn.getOutputTag(), TupleTagList.of(doFn.getFailuresTag())));

        final PCollection<T> outputCollection = tuple
                .get(doFn.getOutputTag())
                .setTypeDescriptor(outputPCollection.getCoder().getEncodedTypeDescriptor())
                .setCoder(outputPCollection.getCoder());

        return new CollectionComposer<>(outputCollection, failuresPCollection.and(tuple.get(doFn.getFailuresTag())));
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
     * and by default a {@link SerializableCoder} code is applied on this type descriptor.
     * <p>
     * In order to do that the OutputT type should extend {@link Serializable}.
     * </p>
     *
     * <p>Example usage:
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
     * </p>
     *
     * @param name      name of the current transform application
     * @param doFn      current base element fn, for example by
     *                  {@link MapElementFn} and
     *                  {@link MapProcessContextFn}
     * @param <OutputT> the output type of the output PCollection
     * @return CollectionComposer<OutputT>
     */
    public <OutputT extends Serializable> CollectionComposer<OutputT> apply(final String name,
                                                                            final BaseElementFn<T, OutputT> doFn,
                                                                            final Iterable<? extends PCollectionView<?>> sideInputs) {
        final PCollectionTuple tuple = outputPCollection.apply(name,
                ParDo.of(doFn)
                        .withOutputTags(doFn.getOutputTag(), TupleTagList.of(doFn.getFailuresTag()))
                        .withSideInputs(sideInputs));

        final PCollection<OutputT> outputCollection = tuple
                .get(doFn.getOutputTag())
                .setTypeDescriptor(doFn.getOutputType())
                .setCoder(SerializableCoder.of(doFn.getOutputType()));

        return new CollectionComposer<>(outputCollection, failuresPCollection.and(tuple.get(doFn.getFailuresTag())));
    }

    /**
     * Get the result of flow.
     * It corresponds to the output PCollection and failures PCollection.
     */
    public Result<PCollection<T>, Failure> getResult() {
        return Result.of(outputPCollection, getFailurePCollection());
    }

    /**
     * Gets all the failures in a PCollection.
     */
    private PCollection<Failure> getFailurePCollection() {
        return failuresPCollection.apply(Flatten.pCollections());
    }
}