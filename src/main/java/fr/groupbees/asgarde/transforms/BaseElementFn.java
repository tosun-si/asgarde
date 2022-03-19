package fr.groupbees.asgarde.transforms;

import fr.groupbees.asgarde.Failure;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;

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

    /**
     * <p>Constructor for BaseElementFn.</p>
     */
    protected BaseElementFn() {
        inputType = getInputTypeDescriptor();
        outputType = getOutputTypeDescriptor();
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
    public TypeDescriptor<InputT> getInputType() {
        return inputType;
    }

    /**
     * <p>Getter for the field <code>outputType</code>.</p>
     *
     * @return a {@link org.apache.beam.sdk.values.TypeDescriptor} object
     */
    public TypeDescriptor<OutputT> getOutputType() {
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
}
