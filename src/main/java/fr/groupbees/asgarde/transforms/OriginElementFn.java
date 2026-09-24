package fr.groupbees.asgarde.transforms;

import fr.groupbees.asgarde.Failure;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;

import java.util.List;

/**
 * DoFn applied by the {@link fr.groupbees.asgarde.OriginCollectionComposer}: it applies the function of an Asgarde
 * DoFn ({@link MapElementFn}, {@link FlatMapElementFn} or {@link FilterFn}) on the value of {@code KV<origin, value>}
 * elements and keeps the origin with each output.
 *
 * <p>
 * The origin is converted to a string <b>only when a failure occurs</b>: the good elements never pay for it, and in
 * a fused stage the origin is a reference to an element already in memory, not a copy.
 * </p>
 *
 * <p>
 * The origin can't be modified by the steps: Beam forbids mutating the input elements of a DoFn.
 * </p>
 *
 * @param <OriginT> the origin element type
 * @param <InputT>  the input value type
 * @param <OutputT> the output value type
 */
public final class OriginElementFn<OriginT, InputT, OutputT> extends BaseElementFn<KV<OriginT, InputT>, KV<OriginT, OutputT>> {

    private final SerializableFunction<InputT, Iterable<OutputT>> function;
    private final SerializableFunction<OriginT, String> originToString;
    private final SerializableAction setupAction;
    private final SerializableAction startBundleAction;
    private final SerializableAction finishBundleAction;
    private final SerializableAction teardownAction;

    OriginElementFn(final SerializableFunction<InputT, Iterable<OutputT>> function,
                    final SerializableFunction<OriginT, String> originToString,
                    final SerializableAction setupAction,
                    final SerializableAction startBundleAction,
                    final SerializableAction finishBundleAction,
                    final SerializableAction teardownAction) {
        super();
        this.function = function;
        this.originToString = originToString;
        this.setupAction = setupAction;
        this.startBundleAction = startBundleAction;
        this.finishBundleAction = finishBundleAction;
        this.teardownAction = teardownAction;
    }

    /**
     * @param mapElementFn   the map DoFn to apply on the values
     * @param originToString the conversion of the origin element to a string, evaluated only on failure
     * @param <OriginT>      the origin element type
     * @param <InputT>       the input value type
     * @param <OutputT>      the output value type
     * @return the DoFn applying the map on the values and keeping the origin
     */
    public static <OriginT, InputT, OutputT> OriginElementFn<OriginT, InputT, OutputT> of(
            final MapElementFn<InputT, OutputT> mapElementFn,
            final SerializableFunction<OriginT, String> originToString) {
        return mapElementFn.toOriginElementFn(originToString);
    }

    /**
     * @param flatMapElementFn the flatMap DoFn to apply on the values
     * @param originToString   the conversion of the origin element to a string, evaluated only on failure
     * @param <OriginT>        the origin element type
     * @param <InputT>         the input value type
     * @param <OutputT>        the output value type
     * @return the DoFn applying the flatMap on the values and keeping the origin
     */
    public static <OriginT, InputT, OutputT> OriginElementFn<OriginT, InputT, OutputT> of(
            final FlatMapElementFn<InputT, OutputT> flatMapElementFn,
            final SerializableFunction<OriginT, String> originToString) {
        return flatMapElementFn.toOriginElementFn(originToString);
    }

    /**
     * @param filterFn       the filter DoFn to apply on the values
     * @param originToString the conversion of the origin element to a string, evaluated only on failure
     * @param <OriginT>      the origin element type
     * @param <InputT>       the value type
     * @return the DoFn applying the filter on the values and keeping the origin
     */
    public static <OriginT, InputT> OriginElementFn<OriginT, InputT, InputT> of(
            final FilterFn<InputT> filterFn,
            final SerializableFunction<OriginT, String> originToString) {
        return filterFn.toOriginElementFn(originToString);
    }

    @Setup
    public void setup() {
        setupAction.execute();
    }

    @StartBundle
    public void startBundle() {
        startBundleAction.execute();
    }

    @FinishBundle
    public void finishBundle() {
        finishBundleAction.execute();
    }

    @Teardown
    public void teardown() {
        teardownAction.execute();
    }

    @ProcessElement
    public void processElement(final ProcessContext ctx) {
        final KV<OriginT, InputT> element = ctx.element();

        // Same guarantees as the flatMap DoFn classes: no partial outputs next to a failure.
        final List<OutputT> outputs;
        try {
            outputs = materialize(function.apply(element.getValue()));
        } catch (Throwable throwable) {
            final Failure failure = toFailure(element.getValue(), throwable);
            ctx.output(failuresTag, failure.withOriginElement(originAsString(element.getKey())));
            return;
        }

        outputs.forEach(output -> ctx.output(KV.of(element.getKey(), output)));
    }

    /**
     * Never fails: the conversion of the origin must not break the error handling.
     */
    private String originAsString(final OriginT origin) {
        try {
            return String.valueOf(originToString.apply(origin));
        } catch (RuntimeException e) {
            return "<conversion of the origin element failed: " + e + ">";
        }
    }
}
