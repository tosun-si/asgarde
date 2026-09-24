package fr.groupbees.asgarde.transforms;

import fr.groupbees.asgarde.Failure;
import fr.groupbees.asgarde.FailureMetrics;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.util.ArrayList;
import java.util.List;

/**
 * Base class for the custom DoFn that handle errors with {@link org.apache.beam.sdk.values.TupleTag}.
 * <p>
 * This class gives shared elements like type descriptors and output and failure tags
 *
 * <p>
 * The class that extend {@link fr.groupbees.asgarde.transforms.BaseElementFn} must give input and output type descriptors and
 * uses the tuple tags to handle good output and failure.
 * </p>
 *
 * @param <InputT>  input of {@link org.apache.beam.sdk.transforms.DoFn}
 * @param <OutputT> output of {@link org.apache.beam.sdk.transforms.DoFn}
 * @author mazlum
 */
public abstract class BaseElementFn<InputT, OutputT> extends DoFn<InputT, OutputT> {

    protected transient TypeDescriptor<InputT> inputType;
    protected transient TypeDescriptor<OutputT> outputType;
    protected final TupleTag<OutputT> outputTag = new TupleTag<OutputT>() {
    };
    protected final TupleTag<Failure> failuresTag = new TupleTag<Failure>() {
    };

    protected String pipelineStep;

    // Set by the composer for each step (see forPipelineStep): format of the input element in the failures, and coder
    // of the input elements when they're encoded in the failures (null otherwise).
    protected SerializableFunction<Object, String> inputElementToString;
    protected Coder<InputT> inputCoder;

    /**
     * <p>Constructor for BaseElementFn.</p>
     */
    protected BaseElementFn() {
        inputType = super.getInputTypeDescriptor();
        outputType = super.getOutputTypeDescriptor();
    }

    /**
     * <p>Constructor for BaseElementFn.</p>
     *
     * @param inputType  a {@link org.apache.beam.sdk.values.TypeDescriptor} object
     * @param outputType a {@link org.apache.beam.sdk.values.TypeDescriptor} object
     */
    protected BaseElementFn(TypeDescriptor<InputT> inputType, TypeDescriptor<OutputT> outputType) {
        this.inputType = inputType;
        this.outputType = outputType;
    }

    /**
     * <p>Getter for the field <code>inputType</code>.</p>
     *
     * @return a {@link org.apache.beam.sdk.values.TypeDescriptor} object
     */
    public TypeDescriptor<InputT> getInputTypeDescriptor() {
        return inputType;
    }

    /**
     * <p>Getter for the field <code>outputType</code>.</p>
     *
     * @return a {@link org.apache.beam.sdk.values.TypeDescriptor} object
     */
    public TypeDescriptor<OutputT> getOutputTypeDescriptor() {
        return outputType;
    }

    /**
     * <p>Getter for the field <code>outputTag</code>.</p>
     *
     * @return a {@link org.apache.beam.sdk.values.TupleTag} object
     */
    public TupleTag<OutputT> getOutputTag() {
        return outputTag;
    }

    /**
     * <p>Getter for the field <code>failuresTag</code>.</p>
     *
     * @return a {@link org.apache.beam.sdk.values.TupleTag} object
     */
    public TupleTag<Failure> getFailuresTag() {
        return failuresTag;
    }

    /**
     * <p>Setter for the field <code>pipelineStep</code>.</p>
     *
     * @param pipelineStep pipeline step concerned by the current transformation
     */
    public void setPipelineStep(final String pipelineStep) {
        this.pipelineStep = pipelineStep;
    }

    /**
     * Returns a copy of this DoFn bound to the given pipeline step.
     *
     * <p>
     * A copy is needed because the same DoFn instance can be applied in several steps: mutating it would give
     * the name of the last step to the failures of all the steps.
     * </p>
     *
     * @param pipelineStep pipeline step concerned by the current transformation
     * @return a copy of this DoFn with the given pipeline step
     */
    public BaseElementFn<InputT, OutputT> forPipelineStep(final String pipelineStep) {
        return forPipelineStep(pipelineStep, null, null);
    }

    /**
     * Returns a copy of this DoFn bound to the given pipeline step, with the failure settings of the composer.
     *
     * @param pipelineStep         pipeline step concerned by the current transformation
     * @param inputElementToString converts the input element to a string in the failures, {@code toString()} if
     *                             {@code null}
     * @param inputCoder           coder of the input elements, to encode them in the failures, {@code null} to not
     *                             encode them
     * @return a copy of this DoFn with the given pipeline step and failure settings
     */
    public BaseElementFn<InputT, OutputT> forPipelineStep(final String pipelineStep,
                                                         final SerializableFunction<Object, String> inputElementToString,
                                                         final Coder<InputT> inputCoder) {
        final BaseElementFn<InputT, OutputT> copy = SerializableUtils.clone(this);
        copy.inputType = inputType;
        copy.outputType = outputType;
        copy.pipelineStep = pipelineStep;
        copy.inputElementToString = inputElementToString;
        copy.inputCoder = inputCoder;

        return copy;
    }

    /**
     * Outputs a {@link Failure} for the current element in the failures tag and increments the failure counter
     * of the pipeline step (see {@link FailureMetrics}).
     *
     * <p>
     * JVM errors ({@link VirtualMachineError} like {@link OutOfMemoryError} or {@link StackOverflowError}) are
     * rethrown: the worker is in an unstable state and the runner must handle them, not the dead letter queue.
     * </p>
     *
     * @param ctx       the current process context
     * @param throwable the error raised for the current element
     */
    protected void outputFailure(final ProcessContext ctx, final Throwable throwable) {
        final Failure failure = toFailure(ctx.element(), throwable);
        ctx.output(failuresTag, inputCoder == null ? failure : failure.withEncodedInputElement(ctx.element(), inputCoder));
    }

    /**
     * Builds the {@link Failure} of the given element and increments the failure counter of the pipeline step.
     * JVM errors ({@link VirtualMachineError}) are rethrown, see {@link #outputFailure}.
     *
     * @param element   the element concerned by the error
     * @param throwable the error raised for the element
     * @return the failure of the element
     */
    protected Failure toFailure(final Object element, final Throwable throwable) {
        if (throwable instanceof VirtualMachineError) {
            throw (VirtualMachineError) throwable;
        }

        FailureMetrics.counter(pipelineStep).inc();
        return Failure.from(pipelineStep, element, throwable, inputElementToString);
    }

    /**
     * Copies the given outputs in a list, to consume the whole {@link Iterable} before emitting any output.
     *
     * @param outputs  the outputs of a flatMap operation
     * @param <OutputT> the output type
     * @return the outputs as a list
     */
    protected static <OutputT> List<OutputT> materialize(final Iterable<OutputT> outputs) {
        final List<OutputT> result = new ArrayList<>();
        outputs.forEach(result::add);

        return result;
    }
}
