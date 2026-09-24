package fr.groupbees.asgarde;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;

/**
 * Beam metrics published by Asgarde: one counter per pipeline step, incremented for each failure.
 *
 * <p>
 * The counters are visible in the runner UI (e.g. Dataflow job metrics) under the {@link #NAMESPACE} namespace,
 * with the pipeline step name as counter name.
 * </p>
 */
public final class FailureMetrics {

    /**
     * Namespace of the failure counters.
     */
    public static final String NAMESPACE = "asgarde-failures";

    /**
     * Counter name used when the pipeline step is unknown.
     */
    public static final String UNKNOWN_STEP = "unknown-step";

    private FailureMetrics() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    /**
     * @param pipelineStep the pipeline step name, {@value #UNKNOWN_STEP} if null or empty
     *                     (DoFn applied outside a {@link CollectionComposer})
     * @return the failure counter of the given pipeline step
     */
    public static Counter counter(final String pipelineStep) {
        final boolean noStep = pipelineStep == null || pipelineStep.isEmpty();
        return Metrics.counter(NAMESPACE, noStep ? UNKNOWN_STEP : pipelineStep);
    }
}
