package fr.groupbees.asgarde.transforms;

import fr.groupbees.asgarde.Failure;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

import static java.util.Objects.requireNonNull;

/**
 * This class allows to handle a generic and custom {@link org.apache.beam.sdk.transforms.DoFn} for map operation
 * with error handling.
 *
 * <p>
 * This class is based on a output type descriptor and take a {@link SerializableFunction} to execute the mapping treatment
 * lazily. This output type allows to give type information and handle default coder for output.
 * This function is mandatory and executed in the ProcessElement stage of Beam lifecycle.
 * </p>
 *
 * <p>
 * This class can take a start action {@link SerializableAction}, used in the setup of Beam lifecycle.
 * This function is not required and if it passed, it is executed lazily in the setup process.
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
 *        MapElementFn.into(TypeDescriptors.integers())
 *                    .via((String word) -> 1 / word.length)  // Could throw ArithmeticException
 *
 *        // With serializable function but without start action.
 *        MapElementFn.into(TypeDescriptors.integers())
 *                    .via((String word) -> 1 / word.length)
 *                    .withSetupAction(() -> System.out.println("Starting of mapping...")
 *      }
 * </pre>
 * </p>
 */
public class MapElementFn<InputT, OutputT> extends BaseElementFn<InputT, OutputT> {

    private final SerializableAction setupAction;
    private final SerializableFunction<InputT, OutputT> inputElementMapper;

    private MapElementFn(TypeDescriptor<InputT> inputType,
                         TypeDescriptor<OutputT> outputType,
                         SerializableAction setupAction,
                         SerializableFunction<InputT, OutputT> inputElementMapper) {
        super(inputType, outputType);
        this.setupAction = setupAction;
        this.inputElementMapper = inputElementMapper;
    }

    /**
     * Factory method of class, that take the output {@link TypeDescriptor}.
     */
    public static <OutputT> MapElementFn<?, OutputT> into(final TypeDescriptor<OutputT> outputType) {
        final SerializableAction defaultSetupAction = () -> {
        };

        return new MapElementFn<>(null, outputType, defaultSetupAction, null);
    }

    /**
     * Method that takes the {@link SerializableFunction} that will be evaluated in the process element phase.
     * <p>
     * This function is mandatory in process element phase.
     *
     * @param inputElementMapper serializable function from input and to output
     */
    public <NewInputT> MapElementFn<NewInputT, OutputT> via(final SerializableFunction<NewInputT, OutputT> inputElementMapper) {
        requireNonNull(inputElementMapper);

        final TypeDescriptor<NewInputT> inputDescriptor = TypeDescriptors.inputOf(inputElementMapper);
        return new MapElementFn<>(inputDescriptor, outputType, setupAction, inputElementMapper);
    }

    /**
     * Method that takes the {@link SerializableAction} that will be evaluated in the setup element phase.
     * <p>
     * This function is not mandatory in the setup phase.
     *
     * @param setupAction serializable action
     */
    public MapElementFn<InputT, OutputT> withSetupAction(final SerializableAction setupAction) {
        requireNonNull(inputElementMapper);
        return new MapElementFn<>(inputType, outputType, setupAction, inputElementMapper);
    }

    @Setup
    public void start() {
        setupAction.execute();
    }

    @ProcessElement
    public void processElement(ProcessContext ctx) {
        requireNonNull(inputElementMapper);
        try {
            ctx.output(inputElementMapper.apply(ctx.element()));
        } catch (Throwable throwable) {
            final Failure failure = Failure.from(ctx.element(), throwable);
            ctx.output(failuresTag, failure);
        }
    }
}