package fr.groupbees.asgarde;

import fr.groupbees.asgarde.transforms.BaseElementFn;
import fr.groupbees.asgarde.transforms.FilterFn;
import fr.groupbees.asgarde.transforms.FlatMapElementFn;
import fr.groupbees.asgarde.transforms.MapElementFn;
import fr.groupbees.asgarde.transforms.OriginElementFn;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;

import static java.util.Objects.requireNonNull;

/**
 * Composer keeping the origin element of each element: the failures of the next steps give, with
 * {@link Failure#getOriginElement()}, the element that entered the flow, to debug and replay from the start.
 *
 * <p>
 * Created with {@link CollectionComposer#withOriginElement(SerializableFunction)}:
 * </p>
 *
 * <pre>{@code
 * CollectionComposer.of(messages)
 *     .withOriginElement(message -> message.getPayload())
 *     .apply("Parse", MapElementFn.into(TypeDescriptor.of(Order.class)).via(OrderParser::parse))
 *     .apply("Validate", FilterFn.by(Order::isValid))   // A failure here gives the payload of the input message
 *     .getResult();
 * }</pre>
 *
 * <p>
 * The origin is light by design: each element keeps a reference to its origin (in a fused stage, an element already
 * in memory, not a copy), and the function converting the origin to a string is evaluated <b>only when a failure
 * occurs</b>.
 * </p>
 *
 * <p>
 * Only the function based Asgarde DoFn classes are accepted ({@link MapElementFn}, {@link FlatMapElementFn} and
 * {@link FilterFn}): the composer applies their function on the values and keeps the origin next to them.
 * </p>
 *
 * @param <OriginT> the type of the origin elements
 * @param <T>       the type of elements in Composer class
 */
public final class OriginCollectionComposer<OriginT, T> {

    private static final String FAILURES_STEP_NAME = "Get all failures";
    private static final String REMOVE_ORIGIN_STEP_NAME = "Remove origin";
    private static final String KEEP_ORIGIN_STEP_SUFFIX = " - keep origin";

    // Before the first step: the elements are their own origin, they are wrapped in KV<origin, value> once,
    // by the first step (a deterministic name is needed, the name of the first step gives it).
    private final PCollection<T> untrackedPCollection;
    private final PCollection<KV<OriginT, T>> trackedPCollection;
    private final Coder<OriginT> originCoder;
    private final Coder<T> valueCoder;
    private final SerializableFunction<OriginT, String> originToString;
    private final PCollectionList<Failure> failuresPCollection;
    private final String lastStepName;

    // Failure settings of the next steps, see withInputElementToString and withEncodedElements.
    private final SerializableFunction<Object, String> inputElementToString;
    private final boolean encodeElements;

    // Built once: calling getResult() several times must not apply the same (deterministic) transform names twice.
    private Result<PCollection<T>, Failure> result;

    private OriginCollectionComposer(final PCollection<T> untrackedPCollection,
                                     final PCollection<KV<OriginT, T>> trackedPCollection,
                                     final Coder<OriginT> originCoder,
                                     final Coder<T> valueCoder,
                                     final SerializableFunction<OriginT, String> originToString,
                                     final PCollectionList<Failure> failuresPCollection,
                                     final String lastStepName,
                                     final SerializableFunction<Object, String> inputElementToString,
                                     final boolean encodeElements) {
        this.untrackedPCollection = untrackedPCollection;
        this.trackedPCollection = trackedPCollection;
        this.originCoder = originCoder;
        this.valueCoder = valueCoder;
        this.originToString = originToString;
        this.failuresPCollection = failuresPCollection;
        this.lastStepName = lastStepName;
        this.inputElementToString = inputElementToString;
        this.encodeElements = encodeElements;
    }

    static <T> OriginCollectionComposer<T, T> of(final PCollection<T> inputPCollection,
                                                  final PCollectionList<Failure> failuresPCollection,
                                                  final String lastStepName,
                                                  final SerializableFunction<T, String> originToString,
                                                  final SerializableFunction<Object, String> inputElementToString,
                                                  final boolean encodeElements) {
        return new OriginCollectionComposer<>(
                inputPCollection,
                null,
                null,
                null,
                requireNonNull(originToString),
                failuresPCollection,
                lastStepName,
                inputElementToString,
                encodeElements
        );
    }

    /**
     * Converts the input elements of the next steps to a string in the failures with the given function, see
     * {@link CollectionComposer#withInputElementToString(SerializableFunction)}.
     *
     * @param inputElementToString converts an input element to a string
     * @return a composer converting the input elements of the next steps with the given function
     */
    public OriginCollectionComposer<OriginT, T> withInputElementToString(final SerializableFunction<Object, String> inputElementToString) {
        return new OriginCollectionComposer<>(
                untrackedPCollection,
                trackedPCollection,
                originCoder,
                valueCoder,
                originToString,
                failuresPCollection,
                lastStepName,
                requireNonNull(inputElementToString),
                encodeElements
        );
    }

    /**
     * Also keeps, in the failures of the next steps, the input element and the origin element encoded with their
     * coders, see {@link CollectionComposer#withEncodedElements()}.
     *
     * @return a composer encoding the elements of the next steps in the failures
     */
    public OriginCollectionComposer<OriginT, T> withEncodedElements() {
        return new OriginCollectionComposer<>(
                untrackedPCollection,
                trackedPCollection,
                originCoder,
                valueCoder,
                originToString,
                failuresPCollection,
                lastStepName,
                inputElementToString,
                true
        );
    }

    /**
     * Applies a map operation on the values, keeping their origin.
     *
     * @param name         the step name
     * @param mapElementFn the map DoFn
     * @param <OutputT>    the output type
     * @return the composer with the outputs of the step and their origin
     */
    public <OutputT> OriginCollectionComposer<OriginT, OutputT> apply(final String name,
                                                                      final MapElementFn<T, OutputT> mapElementFn) {
        return applyOriginFn(name, OriginElementFn.of(mapElementFn, originToString), coderOf(mapElementFn));
    }

    /**
     * Applies a flatMap operation on the values, the outputs keep the origin of their input element.
     *
     * @param name             the step name
     * @param flatMapElementFn the flatMap DoFn
     * @param <OutputT>        the output type
     * @return the composer with the outputs of the step and their origin
     */
    public <OutputT> OriginCollectionComposer<OriginT, OutputT> apply(final String name,
                                                                      final FlatMapElementFn<T, OutputT> flatMapElementFn) {
        return applyOriginFn(name, OriginElementFn.of(flatMapElementFn, originToString), coderOf(flatMapElementFn));
    }

    /**
     * Applies a filter on the values, keeping their origin. The coder of the values is kept.
     *
     * @param name     the step name
     * @param filterFn the filter DoFn
     * @return the composer with the filtered values and their origin
     */
    public OriginCollectionComposer<OriginT, T> apply(final String name, final FilterFn<T> filterFn) {
        return applyOriginFn(name, OriginElementFn.of(filterFn, originToString), currentValueCoder());
    }

    /**
     * Set the given {@link Coder} to the current values in the flow.
     *
     * @param coder the coder of the values
     * @return the composer with the coder set on the values
     */
    public OriginCollectionComposer<OriginT, T> setCoder(final Coder<T> coder) {
        if (trackedPCollection == null) {
            untrackedPCollection.setCoder(coder);
            return this;
        }

        trackedPCollection.setCoder(KvCoder.of(originCoder, coder));
        return new OriginCollectionComposer<>(
                null,
                trackedPCollection,
                originCoder,
                coder,
                originToString,
                failuresPCollection,
                lastStepName,
                inputElementToString,
                encodeElements
        );
    }

    /**
     * Returns the result of the flow: the values of the last step (without their origin) and the failures of all the
     * steps, with their origin element for the steps of this composer.
     *
     * @return the result with the output values and the failures
     */
    public Result<PCollection<T>, Failure> getResult() {
        if (result == null) {
            result = Result.of(outputValues(), flattenFailures());
        }

        return result;
    }

    private <OutputT> OriginCollectionComposer<OriginT, OutputT> applyOriginFn(
            final String name,
            final OriginElementFn<OriginT, T, OutputT> originFn,
            final Coder<OutputT> outputValueCoder) {

        final Coder<OriginT> stepOriginCoder = resolvedOriginCoder();
        final PCollection<KV<OriginT, T>> input = trackedPCollection(name, stepOriginCoder);
        final BaseElementFn<KV<OriginT, T>, KV<OriginT, OutputT>> stepFn =
                originFn.forPipelineStep(name, inputElementToString, encodedInputCoder(stepOriginCoder));

        final PCollectionTuple tuple = input.apply(name,
                ParDo.of(stepFn).withOutputTags(stepFn.getOutputTag(), TupleTagList.of(stepFn.getFailuresTag())));

        final PCollection<KV<OriginT, OutputT>> output = tuple.get(stepFn.getOutputTag());
        if (outputValueCoder != null) {
            output.setCoder(KvCoder.of(stepOriginCoder, outputValueCoder));
        }

        return new OriginCollectionComposer<>(
                null,
                output,
                stepOriginCoder,
                outputValueCoder,
                originToString,
                failuresPCollection.and(tuple.get(stepFn.getFailuresTag())),
                name,
                inputElementToString,
                encodeElements
        );
    }

    /**
     * {@code KvCoder<origin, value>} of the elements consumed by the next step when they're encoded in the failures:
     * its components encode the value and the origin. {@code null} if the elements aren't encoded, or if the coder of
     * the values is unknown (not inferred by Beam and not set with {@link #setCoder(Coder)}).
     */
    private KvCoder<OriginT, T> encodedInputCoder(final Coder<OriginT> stepOriginCoder) {
        if (!encodeElements) {
            return null;
        }

        final Coder<T> currentValueCoder = currentValueCoder();
        return currentValueCoder == null ? null : KvCoder.of(stepOriginCoder, currentValueCoder);
    }

    /**
     * Before the first step, wraps the elements in {@code KV<origin, value>}, the origin being the element itself.
     */
    private PCollection<KV<OriginT, T>> trackedPCollection(final String firstStepName, final Coder<OriginT> stepOriginCoder) {
        if (trackedPCollection != null) {
            return trackedPCollection;
        }

        return untrackedPCollection
                .apply(firstStepName + KEEP_ORIGIN_STEP_SUFFIX, WithKeys.of(OriginCollectionComposer.<T, OriginT>elementAsOrigin()))
                .setCoder(KvCoder.of(stepOriginCoder, untrackedPCollection.getCoder()));
    }

    /**
     * Before the first step, the origin coder is the coder of the input elements (OriginT is T, see the factory).
     */
    @SuppressWarnings("unchecked")
    private Coder<OriginT> resolvedOriginCoder() {
        return originCoder != null ? originCoder : (Coder<OriginT>) untrackedPCollection.getCoder();
    }

    @SuppressWarnings("unchecked")
    private static <T, OriginT> SerializableFunction<T, OriginT> elementAsOrigin() {
        return element -> (OriginT) element;
    }

    private PCollection<T> outputValues() {
        if (trackedPCollection == null) {
            return untrackedPCollection;
        }

        final PCollection<T> values = trackedPCollection.apply(REMOVE_ORIGIN_STEP_NAME + " of " + lastStepName, Values.create());
        return valueCoder == null ? values : values.setCoder(valueCoder);
    }

    private PCollection<Failure> flattenFailures() {
        // Deterministic name: Dataflow streaming updates (--update) need stable transform names.
        final String stepName = lastStepName == null ? FAILURES_STEP_NAME : FAILURES_STEP_NAME + " of " + lastStepName;

        if (failuresPCollection.size() == 0) {
            return failuresPCollection.getPipeline().apply(stepName, Create.empty(TypeDescriptor.of(Failure.class)));
        }

        return failuresPCollection.apply(stepName, Flatten.pCollections());
    }

    private Coder<T> currentValueCoder() {
        return trackedPCollection == null ? untrackedPCollection.getCoder() : valueCoder;
    }

    /**
     * Coder of the output values inferred from the output type of the DoFn, {@code null} if Beam can't infer it:
     * the coder must then be set with {@link #setCoder(Coder)}, as with a usual {@link PCollection}.
     */
    private <OutputT> Coder<OutputT> coderOf(final BaseElementFn<?, OutputT> doFn) {
        final TypeDescriptor<OutputT> outputType = doFn.getOutputTypeDescriptor();

        try {
            return outputType == null ? null : failuresPCollection.getPipeline().getCoderRegistry().getCoder(outputType);
        } catch (CannotProvideCoderException e) {
            return null;
        }
    }
}
