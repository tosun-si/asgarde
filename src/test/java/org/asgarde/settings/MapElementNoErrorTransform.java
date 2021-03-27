package org.asgarde.settings;

import org.asgarde.CollectionComposer;
import org.asgarde.Failure;
import org.asgarde.settings.Datasets.OtherTeam;
import org.asgarde.settings.Datasets.Team;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.values.PCollection;

import static org.apache.beam.sdk.values.TypeDescriptor.of;

/**
 * Custom PTransform that uses a MapElement without error.
 */
public class MapElementNoErrorTransform extends PTransform<PCollection<Team>, Result<PCollection<OtherTeam>, Failure>> {

    @Override
    public Result<PCollection<OtherTeam>, Failure> expand(PCollection<Team> input) {
        return CollectionComposer.of(input)
                .apply("Map", MapElements.into(of(OtherTeam.class)).via(TestSettings::toOtherTeam))
                .getResult();
    }
}
