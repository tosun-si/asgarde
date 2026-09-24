package fr.groupbees.asgarde.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.TypeDescriptor;

import static java.util.Objects.requireNonNull;

/**
 * This class allows to handle a generic and custom {@link org.apache.beam.sdk.transforms.DoFn} for map operation
 * with error handling.
 *
 * <br>
 *
 * <p>
 * This class is based on an input class and output type descriptor and take a {@link org.apache.beam.sdk.transforms.SerializableFunction} to execute
 * the mapping treatment lazily.
 * These types allows to give type information and handle default coders.
 * This function is from {@link org.apache.beam.sdk.transforms.DoFn.ProcessContext} object to the output type.
 * In some case, developers need to access to ProcessContext, to get technical data (timestamp...) or handle side inputs.
 * This function is mandatory and executed in the ProcessElement stage of Beam lifecycle.
 * </p>
 *
 * <br>
 *
 * <p>
 * This class can take actions {@link SerializableAction}, used in the DoFn Beam lifecycle.
 * <ul>
 *    <li>withSetupAction : executed in the setup method</li>
 *    <li>withStartBundleAction : executed in the start bundle method</li>
 *    <li>withFinishBundleAction : executed in the finish bundle method</li>
 *    <li>withTeardownAction : executed in the teardown method</li>
 * </ul>
 * <p>
 * These functions are not required and if they are given, they are executed lazily in the dedicated method.
 * </p>
 *
 * <br>
 *
 * <p>
 * If there are errors in the process, an failure Tag based on {@link fr.groupbees.asgarde.Failure} object is used to handle
 * the failure output (and side outputs)
 * </p>
 *
 * <br>
 *
 * <p>Example usage:</p>
 *
 * <pre>{@code
 *        // With serializable function but without lifecycle actions.
 *        MapProcessContextFn.from(String.class)
 *                           .into(TypeDescriptors.integers())
 *                           .via((ProcessContext ctx) -> 1 / ctx.element().length)  // Could throw ArithmeticException
 *
 *        // With serializable function and some lifecycle actions.
 *        MapProcessContextFn.from(String.class)
 *                           .into(TypeDescriptors.integers())
 *                           .via((String word) -> 1 / word.length)
 *                           .withSetupAction(() -> System.out.println("Starting of mapping...")
 *                           .withStartBundleAction(() -> System.out.println("Starting bundle of mapping...")
 *                           .withFinishBundleAction(() -> System.out.println("Ending bundle of mapping...")
 *                           .withTeardownAction(() -> System.out.println("Ending of mapping...")
 *      }
 * </pre>
 *
 * @author mazlum
 */
public class MapProcessContextFn<InputT, OutputT> extends BaseElementFn<InputT, OutputT> {

    private final SerializableAction setupAction;
    private final SerializableAction startBundleAction;
    private final SerializableAction finishBundleAction;
    private final SerializableAction teardownAction;
    private final SerializableFunction<DoFn<InputT, OutputT>.ProcessContext, OutputT> processContextMapper;

    private MapProcessContextFn(final TypeDescriptor<InputT> inputType,
                                final TypeDescriptor<OutputT> outputType,
                                final SerializableAction setupAction,
                                final SerializableAction startBundleAction,
                                final SerializableAction finishBundleAction,
                                final SerializableAction teardownAction,
                                final SerializableFunction<DoFn<InputT, OutputT>.ProcessContext, OutputT> processContextMapper) {
        super(inputType, outputType);
        this.setupAction = setupAction;
        this.startBundleAction = startBundleAction;
        this.finishBundleAction = finishBundleAction;
        this.teardownAction = teardownAction;
        this.processContextMapper = processContextMapper;
    }

    /**
     * Factory method of class, that take the input type class.
     *
     * @param inputClass a {@link java.lang.Class} object
     * @param <InputT>   a InputT class
     * @return a {@link fr.groupbees.asgarde.transforms.MapProcessContextFn} object
     */
    public static <InputT> MapProcessContextFn<InputT, ?> from(final Class<InputT> inputClass) {
        final SerializableAction defaultAction = () -> {
        };

        return new MapProcessContextFn<>(
                TypeDescriptor.of(inputClass),
                null,
                defaultAction,
                defaultAction,
                defaultAction,
                defaultAction,
                null
        );
    }

    /**
     * Add the output type descriptors, it's required because it allows to add default coder for Output.
     *
     * @param outputType   a {@link org.apache.beam.sdk.values.TypeDescriptor} object
     * @param <NewOutputT> a NewOutputT class
     * @return a {@link fr.groupbees.asgarde.transforms.MapProcessContextFn} object
     */
    public <NewOutputT> MapProcessContextFn<InputT, NewOutputT> into(final TypeDescriptor<NewOutputT> outputType) {
        final SerializableAction defaultAction = () -> {
        };

        return new MapProcessContextFn<>(
                inputType,
                outputType,
                defaultAction,
                defaultAction,
                defaultAction,
                defaultAction,
                null
        );
    }

    /**
     * Method that takes the {@link org.apache.beam.sdk.transforms.SerializableFunction} that will be evaluated in the process element phase.
     * This function is based on a {@link org.apache.beam.sdk.transforms.DoFn.ProcessContext} as input and a generic ouput.
     * <p>
     * This function is mandatory in process element phase.
     *
     * @param processContextMapper serializable function from process context and to output
     * @return a {@link fr.groupbees.asgarde.transforms.MapProcessContextFn} object
     */
    public MapProcessContextFn<InputT, OutputT> via(final SerializableFunction<DoFn<InputT, OutputT>.ProcessContext, OutputT> processContextMapper) {
        requireNonNull(processContextMapper);

        return new MapProcessContextFn<>(
                inputType,
                outputType,
                setupAction,
                startBundleAction,
                finishBundleAction,
                teardownAction,
                processContextMapper
        );
    }

    /**
     * Method that takes the {@link fr.groupbees.asgarde.transforms.SerializableAction} that will be evaluated in the setup phase.
     * <p>
     * This function is not mandatory in the setup phase.
     *
     * @param setupAction setup action
     * @return a {@link fr.groupbees.asgarde.transforms.MapProcessContextFn} object
     */
    public MapProcessContextFn<InputT, OutputT> withSetupAction(final SerializableAction setupAction) {
        requireNonNull(setupAction);

        return new MapProcessContextFn<>(
                inputType,
                outputType,
                setupAction,
                startBundleAction,
                finishBundleAction,
                teardownAction,
                processContextMapper
        );
    }

    /**
     * Method that takes the {@link fr.groupbees.asgarde.transforms.SerializableAction} that will be evaluated in the start bundle phase.
     * <p>
     * This function is not mandatory in the start bundle phase.
     *
     * @param startBundleAction start bundle action
     * @return a {@link fr.groupbees.asgarde.transforms.MapProcessContextFn} object
     */
    public MapProcessContextFn<InputT, OutputT> withStartBundleAction(final SerializableAction startBundleAction) {
        requireNonNull(startBundleAction);

        return new MapProcessContextFn<>(
                inputType,
                outputType,
                setupAction,
                startBundleAction,
                finishBundleAction,
                teardownAction,
                processContextMapper
        );
    }

    /**
     * Method that takes the {@link fr.groupbees.asgarde.transforms.SerializableAction} that will be evaluated in the finish bundle phase.
     * <p>
     * This function is not mandatory in the finish bundle phase.
     *
     * @param finishBundleAction finish bundle action
     * @return a {@link fr.groupbees.asgarde.transforms.MapProcessContextFn} object
     */
    public MapProcessContextFn<InputT, OutputT> withFinishBundleAction(final SerializableAction finishBundleAction) {
        requireNonNull(finishBundleAction);

        return new MapProcessContextFn<>(
                inputType,
                outputType,
                setupAction,
                startBundleAction,
                finishBundleAction,
                teardownAction,
                processContextMapper
        );
    }

    /**
     * Method that takes the {@link fr.groupbees.asgarde.transforms.SerializableAction} that will be evaluated in the teardown phase.
     * <p>
     * This function is not mandatory in the teardown phase.
     *
     * @param teardownAction teardown action
     * @return a {@link fr.groupbees.asgarde.transforms.MapProcessContextFn} object
     */
    public MapProcessContextFn<InputT, OutputT> withTeardownAction(final SerializableAction teardownAction) {
        requireNonNull(teardownAction);

        return new MapProcessContextFn<>(
                inputType,
                outputType,
                setupAction,
                startBundleAction,
                finishBundleAction,
                teardownAction,
                processContextMapper
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
     * @param ctx a {@link org.apache.beam.sdk.transforms.DoFn.ProcessContext} object
     */
    @ProcessElement
    public void processElement(DoFn<InputT, OutputT>.ProcessContext ctx) {
        requireNonNull(processContextMapper);
        try {
            ctx.output(processContextMapper.apply(ctx));
        } catch (Throwable throwable) {
            outputFailure(ctx, throwable);
        }
    }
}
