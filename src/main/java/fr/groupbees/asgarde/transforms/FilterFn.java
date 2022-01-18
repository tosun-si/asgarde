package fr.groupbees.asgarde.transforms;

import fr.groupbees.asgarde.Failure;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

import static java.util.Objects.requireNonNull;

/**
 * <p>
 * This class allows to add a filter based on an input, with error handling, via a custom
 * {@link org.apache.beam.sdk.transforms.DoFn}.
 * This take a {@link org.apache.beam.sdk.transforms.SerializableFunction} that corresponds to a Predicate, this predicate is evaluated in
 * the ProcessElement phase.
 * </p>
 *
 * <p>
 * The input descriptor is recovered from the input type, only for information purpose.
 * The output descriptor is not passed in this case, because the output coder, will be recovered from the previous PCollection
 * in the {@link fr.groupbees.asgarde.CollectionComposer} class.
 * </p>
 *
 * <p>In this case the input and output in DoFn are identical</p>
 *
 * <p>
 * If there are errors in the process, an failure Tag based on {@link fr.groupbees.asgarde.Failure} object is used to handle
 * the failure output (and side outputs)
 * </p>
 *
 * <p>Example usage:</p>
 *
 * <pre>{@code
 *         FilterFn.by(word -> word.length > 1)
 *      }
 * </pre>
 *
 * @author mazlum
 */
public class FilterFn<InputT> extends BaseElementFn<InputT, InputT> {

    private final SerializableFunction<InputT, Boolean> predicate;

    private FilterFn(final TypeDescriptor<InputT> inputType,
                     final SerializableFunction<InputT, Boolean> predicate) {
        super(inputType, inputType);
        this.predicate = predicate;
    }

    /**
     * Factory method that take a {@link org.apache.beam.sdk.transforms.SerializableFunction} predicate.
     * <p>
     * This {@link org.apache.beam.sdk.transforms.SerializableFunction} has a generic input type a Boolean in output.
     *
     * @param predicate a {@link org.apache.beam.sdk.transforms.SerializableFunction} object
     * @param <InputT>  a InputT class
     * @return a {@link fr.groupbees.asgarde.transforms.FilterFn} object
     */
    public static <InputT> FilterFn<InputT> by(final SerializableFunction<InputT, Boolean> predicate) {
        final TypeDescriptor<InputT> inputDescriptor = TypeDescriptors.inputOf(predicate);
        return new FilterFn<>(inputDescriptor, predicate);
    }

    /**
     * <p>processElement.</p>
     *
     * @param ctx a ProcessContext object
     */
    @ProcessElement
    public void processElement(ProcessContext ctx) {
        requireNonNull(predicate);
        try {
            final InputT inputElement = ctx.element();
            if (predicate.apply(inputElement)) {
                ctx.output(inputElement);
            }
        } catch (Throwable throwable) {
            final Failure failure = Failure.from(pipelineStep, ctx.element(), throwable);
            ctx.output(failuresTag, failure);
        }
    }
}
