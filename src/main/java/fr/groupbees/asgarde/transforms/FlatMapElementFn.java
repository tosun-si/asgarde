package fr.groupbees.asgarde.transforms;

import fr.groupbees.asgarde.Failure;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

import static java.util.Objects.requireNonNull;

/**
 * This class allows to handle a generic and custom {@link org.apache.beam.sdk.transforms.DoFn} for flatMap operation
 * with error handling.
 *
 * <br>
 *
 * <p>
 * This class is based on a output type descriptor and take a {@link SerializableFunction} to execute the mapping treatment
 * lazily. This output type allows to give type information and handle default coder for output.
 * This function is mandatory and executed in the ProcessElement stage of Beam lifecycle.
 * </p>
 *
 * <br>
 *
 * <p>
 * This class can take actions {@link SerializableAction}, used in the DoFn Beam lifecycle.
 * <ul>
 *     <li>withSetupAction : executed in the setup method</li>
 *     <li>withStartBundleAction : executed in the start bundle method</li>
 *     <li>withFinishBundleAction : executed in the finish bundle method</li>
 *     <li>withTeardownAction : executed in the teardown method</li>
 * </ul>
 * <p>
 * These functions are not required and if they are given, they are executed lazily in the dedicated method.
 * </p>
 *
 * <br>
 *
 * <p>
 * If there are errors in the process, an failure Tag based on {@link Failure} object is used to handle
 * the failure output (and side outputs)
 * </p>
 *
 * <br>
 *
 * <p>Example usage:</p>
 *
 * <pre>{@code
 *        // With serializable function but without lifecycle actions.
 *        FlatMapElementFn.into(TypeDescriptor.of(Player.class))
 *                        .via(team -> team.getPlayers())  // Could throw an exception
 *
 *        // With serializable function and some lifecycle actions.
 *        FlatMapElementFn.into(TypeDescriptor.of(Player.class))
 *                        .via(team -> team.getPlayers())
 *                        .withSetupAction(() -> System.out.println("Starting of mapping...")
 *                        .withStartBundleAction(() -> System.out.println("Starting bundle of mapping...")
 *                        .withFinishBundleAction(() -> System.out.println("Ending bundle of mapping...")
 *                        .withTeardownAction(() -> System.out.println("Ending of mapping...")
 *      }
 * </pre>
 *
 * @author mazlum
 */
public class FlatMapElementFn<InputT, OutputT> extends BaseElementFn<InputT, OutputT> {

    private final SerializableAction setupAction;
    private final SerializableAction startBundleAction;
    private final SerializableAction finishBundleAction;
    private final SerializableAction teardownAction;
    private final SerializableFunction<InputT, Iterable<OutputT>> inputElementMapper;

    private FlatMapElementFn(final TypeDescriptor<InputT> inputType,
                             final TypeDescriptor<OutputT> outputType,
                             final SerializableAction setupAction,
                             final SerializableAction startBundleAction,
                             final SerializableAction finishBundleAction,
                             final SerializableAction teardownAction,
                             final SerializableFunction<InputT, Iterable<OutputT>> inputElementMapper) {
        super(inputType, outputType);
        this.setupAction = setupAction;
        this.startBundleAction = startBundleAction;
        this.finishBundleAction = finishBundleAction;
        this.teardownAction = teardownAction;
        this.inputElementMapper = inputElementMapper;
    }

    /**
     * Factory method of class, that take the output {@link TypeDescriptor}.
     *
     * @param outputType a {@link TypeDescriptor} object
     * @param <OutputT>  a OutputT class
     * @return a {@link FlatMapElementFn} object
     */
    public static <OutputT> FlatMapElementFn<?, OutputT> into(final TypeDescriptor<OutputT> outputType) {
        final SerializableAction defaultAction = () -> {
        };

        return new FlatMapElementFn<>(
                null,
                outputType,
                defaultAction,
                defaultAction,
                defaultAction,
                defaultAction,
                null
        );
    }

    /**
     * Method that takes the {@link SerializableFunction} that will be evaluated in the process element phase.
     * <p>
     * This function is mandatory in process element phase.
     *
     * @param inputElementMapper serializable function from input and to an iterable of output
     * @param <NewInputT>        a NewInputT class
     * @return a {@link FlatMapElementFn} object
     */
    public <NewInputT> FlatMapElementFn<NewInputT, OutputT> via(final SerializableFunction<NewInputT, Iterable<OutputT>> inputElementMapper) {
        requireNonNull(inputElementMapper);

        final TypeDescriptor<NewInputT> inputDescriptor = TypeDescriptors.inputOf(inputElementMapper);
        return new FlatMapElementFn<>(
                inputDescriptor,
                outputType,
                setupAction,
                startBundleAction,
                finishBundleAction,
                teardownAction,
                inputElementMapper
        );
    }

    /**
     * Method that takes the {@link SerializableAction} that will be evaluated in the setup phase.
     * <p>
     * This function is not mandatory in the setup phase.
     *
     * @param setupAction setup action
     * @return a {@link FlatMapElementFn} object
     */
    public FlatMapElementFn<InputT, OutputT> withSetupAction(final SerializableAction setupAction) {
        requireNonNull(setupAction);
        return new FlatMapElementFn<>(
                inputType,
                outputType,
                setupAction,
                startBundleAction,
                finishBundleAction,
                teardownAction,
                inputElementMapper
        );
    }

    /**
     * Method that takes the {@link SerializableAction} that will be evaluated in the start bundle phase.
     * <p>
     * This function is not mandatory in the start bundle phase.
     *
     * @param startBundleAction start bundle action
     * @return a {@link FlatMapElementFn} object
     */
    public FlatMapElementFn<InputT, OutputT> withStartBundleAction(final SerializableAction startBundleAction) {
        requireNonNull(startBundleAction);
        return new FlatMapElementFn<>(
                inputType,
                outputType,
                setupAction,
                startBundleAction,
                finishBundleAction,
                teardownAction,
                inputElementMapper
        );
    }

    /**
     * Method that takes the {@link SerializableAction} that will be evaluated in the finish bundle phase.
     * <p>
     * This function is not mandatory in the finish bundle phase.
     *
     * @param finishBundleAction start bundle action
     * @return a {@link FlatMapElementFn} object
     */
    public FlatMapElementFn<InputT, OutputT> withFinishBundleAction(final SerializableAction finishBundleAction) {
        requireNonNull(finishBundleAction);
        return new FlatMapElementFn<>(
                inputType,
                outputType,
                setupAction,
                startBundleAction,
                finishBundleAction,
                teardownAction,
                inputElementMapper
        );
    }

    /**
     * Method that takes the {@link SerializableAction} that will be evaluated in the teardown phase.
     * <p>
     * This function is not mandatory in the teardown phase.
     *
     * @param teardownAction teardown action
     * @return a {@link FlatMapElementFn} object
     */
    public FlatMapElementFn<InputT, OutputT> withTeardownAction(final SerializableAction teardownAction) {
        requireNonNull(teardownAction);
        return new FlatMapElementFn<>(
                inputType,
                outputType,
                setupAction,
                startBundleAction,
                finishBundleAction,
                teardownAction,
                inputElementMapper
        );
    }

    /**
     * <p>Setup action in the DoFn worker lifecycle.</p>
     */
    @Setup
    public void setup() {
        setupAction.execute();
    }

    /**
     * <p>Start bundle action in the DoFn worker lifecycle.</p>
     */
    @StartBundle
    public void startBundle() {
        startBundleAction.execute();
    }

    /**
     * <p>Finish bundle action in the DoFn worker lifecycle.</p>
     */
    @FinishBundle
    public void finishBundle() {
        finishBundleAction.execute();
    }

    /**
     * <p>Teardown action in the DoFn worker lifecycle.</p>
     */
    @Teardown
    public void teardown() {
        teardownAction.execute();
    }

    /**
     * <p>processElement.</p>
     *
     * @param ctx a ProcessContext object
     */
    @ProcessElement
    public void processElement(ProcessContext ctx) {
        requireNonNull(inputElementMapper);

        try {
            final Iterable<OutputT> outputs = inputElementMapper.apply(ctx.element());

            outputs.forEach(ctx::output);
        } catch (Throwable throwable) {
            final Failure failure = Failure.from(pipelineStep, ctx.element(), throwable);
            ctx.output(failuresTag, failure);
        }
    }
}
