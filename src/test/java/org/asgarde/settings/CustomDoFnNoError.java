package org.asgarde.settings;

import org.asgarde.Failure;
import org.asgarde.transforms.BaseElementFn;

/**
 * Custom DoFn without error for testing purpose.
 */
public class CustomDoFnNoError extends BaseElementFn<Datasets.Team, Datasets.OtherTeam> {

    public CustomDoFnNoError() {
        super();
    }

    @ProcessElement
    public void processElement(ProcessContext ctx) {
        try {
            ctx.output(TestSettings.toOtherTeam(ctx.element()));
        } catch (Throwable throwable) {
            final Failure failure = Failure.from(ctx.element(), throwable);
            ctx.output(failuresTag, failure);
        }
    }
}
