package org.asgarde.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.asgarde.Failure;

import static java.util.Objects.requireNonNull;

/**
 * This class allows to handle a generic and custom {@link org.apache.beam.sdk.transforms.DoFn} for map operation
 * with error handling.
 *
 * <p>
 * This class is based on an input class and output type descriptor and take a {@link SerializableFunction} to execute
 * the mapping treatment lazily.
 * These types allows to give type information and handle default coders.
 * This function is from {@link org.apache.beam.sdk.transforms.DoFn.ProcessContext} object to the output type.
 * In some case, developers need to access to ProcessContext, to get technical data (timestamp...) or handle side inputs.
 * This function is mandatory and executed in the ProcessElement stage of Beam lifecycle.
 * </p>
 *
 * <p>
 * This class can take a start action {@link SerializableAction}, used in the setup of Beam lifecycle.
 * This function is not required and if passed, it is executed lazily in the setup process.
 * </p>
 *
 * <p>
 * If there are errors in the process, an failure Tag based on {@link Failure} object is used to handle
 * the failure output (and side outputs)
 * </p>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 *        // With serializable function but without start action.
 *        MapProcessContextFn.from(String.class)
 *                           .into(TypeDescriptors.integers())
 *                           .via((ProcessContext ctx) -> 1 / ctx.element().length)  // Could throw ArithmeticException
 *
 *        // With serializable function but without start action.
 *        MapElementFn.from(String.class)
 *                    .into(TypeDescriptors.integers())
 *                    .via((String word) -> 1 / word.length)
 *                    .withSetupAction(() -> System.out.println("Starting of mapping...")
 *      }
 * </pre>
 * </p>
 */
public class MapProcessContextFn<InputT, OutputT> extends BaseElementFn<InputT, OutputT> {

    private final SerializableAction setupAction;
    private final SerializableFunction<DoFn<InputT, OutputT>.ProcessContext, OutputT> processContextMapper;

    private MapProcessContextFn(final TypeDescriptor<InputT> inputType,
                                final TypeDescriptor<OutputT> outputType,
                                final SerializableAction setupAction,
                                final SerializableFunction<DoFn<InputT, OutputT>.ProcessContext, OutputT> processContextMapper) {
        super(inputType, outputType);
        this.setupAction = setupAction;
        this.processContextMapper = processContextMapper;
    }

    /**
     * Factory method of class, that take the input type class.
     */
    public static <InputT> MapProcessContextFn<InputT, ?> from(final Class<InputT> inputClass) {
        final SerializableAction defaultSetupAction = () -> {
        };

        return new MapProcessContextFn<>(TypeDescriptor.of(inputClass), null, defaultSetupAction, null);
    }

    /**
     * Add the output type descriptors, it's required because it allows to add default coder for Output.
     */
    public <NewOutputT> MapProcessContextFn<InputT, NewOutputT> into(final TypeDescriptor<NewOutputT> outputType) {
        final SerializableAction defaultSetupAction = () -> {
        };

        return new MapProcessContextFn<>(inputType, outputType, defaultSetupAction, null);
    }

    /**
     * Method that takes the {@link SerializableFunction} that will be evaluated in the process element phase.
     * This function is based on a {@link org.apache.beam.sdk.transforms.DoFn.ProcessContext} as input and a generic ouput.
     * <p>
     * This function is mandatory in process element phase.
     *
     * @param processContextMapper serializable function from process context and to output
     */
    public MapProcessContextFn<InputT, OutputT> via(final SerializableFunction<DoFn<InputT, OutputT>.ProcessContext, OutputT> processContextMapper) {
        requireNonNull(processContextMapper);

        return new MapProcessContextFn<>(inputType, outputType, setupAction, processContextMapper);
    }

    /**
     * Method that takes the {@link SerializableAction} that will be evaluated in the setup element phase.
     * <p>
     * This function is not mandatory in the setup phase.
     *
     * @param setupAction serializable action
     */
    public MapProcessContextFn<InputT, OutputT> withSetupAction(final SerializableAction setupAction) {
        requireNonNull(setupAction);

        return new MapProcessContextFn<>(inputType, outputType, setupAction, processContextMapper);
    }

    @Setup
    public void setup() {
        setupAction.execute();
    }

    @ProcessElement
    public void processElement(DoFn<InputT, OutputT>.ProcessContext ctx) {
        requireNonNull(processContextMapper);
        try {
            ctx.output(processContextMapper.apply(ctx));
        } catch (Throwable throwable) {
            final Failure failure = Failure.from(ctx.element(), throwable);
            ctx.output(failuresTag, failure);
        }
    }
}