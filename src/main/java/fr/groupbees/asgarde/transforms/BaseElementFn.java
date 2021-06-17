package fr.groupbees.asgarde.transforms;

import fr.groupbees.asgarde.Failure;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Base class for the custom DoFn that handle errors with {@link TupleTag}.
 * <p>
 * This class gives shared elements like type descriptors and output and failure tags
 *
 * <p>
 * The class that extend {@link BaseElementFn} must give input and output type descriptors and
 * uses the tuple tags to handle good output and failure.
 * </p>
 *
 * @param <InputT>  input of {@link DoFn}
 * @param <OutputT> output of {@link DoFn}
 */
public abstract class BaseElementFn<InputT, OutputT> extends DoFn<InputT, OutputT> {

    protected transient TypeDescriptor<InputT> inputType;
    protected transient TypeDescriptor<OutputT> outputType;
    protected final TupleTag<OutputT> outputTag = new TupleTag<OutputT>() {
    };
    protected final TupleTag<Failure> failuresTag = new TupleTag<Failure>() {
    };

    protected BaseElementFn() {
        inputType = getInputTypeDescriptor();
        outputType = getOutputTypeDescriptor();
    }

    protected BaseElementFn(TypeDescriptor<InputT> inputType, TypeDescriptor<OutputT> outputType) {
        this.inputType = inputType;
        this.outputType = outputType;
    }

    public TypeDescriptor<InputT> getInputType() {
        return inputType;
    }

    public TypeDescriptor<OutputT> getOutputType() {
        return outputType;
    }

    public TupleTag<OutputT> getOutputTag() {
        return outputTag;
    }

    public TupleTag<Failure> getFailuresTag() {
        return failuresTag;
    }
}
