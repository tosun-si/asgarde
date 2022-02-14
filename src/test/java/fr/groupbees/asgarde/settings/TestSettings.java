package fr.groupbees.asgarde.settings;

import fr.groupbees.asgarde.Failure;
import fr.groupbees.asgarde.settings.Datasets.OtherTeam;
import fr.groupbees.asgarde.settings.Datasets.Team;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.List;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This class gives some settings and useful methods for the testing part.
 */
public class TestSettings {

    private TestSettings() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    public static final String ERROR_OTHER_TEAM = "Error in other team ";
    public static final String ERROR_PLAYERS = "Error in players ";

    public static OtherTeam toOtherTeam(final Team team) {
        final OtherTeam otherTeam = new OtherTeam();
        otherTeam.setName(team.getName());
        otherTeam.setOtherName("other " + team.getName());

        return otherTeam;
    }

    public static OtherTeam toOtherTeamWithSideInputField(final PCollectionView<String> sideInput,
                                                          final DoFn<Team, OtherTeam>.ProcessContext context) {
        final Team inputTeam = context.element();

        final OtherTeam otherTeam = toOtherTeam(inputTeam);
        otherTeam.setSideInputField(context.sideInput(sideInput));

        return otherTeam;
    }

    public static OtherTeam toOtherTeamWithException(final Team team) {
        throw new IllegalArgumentException(ERROR_OTHER_TEAM + team.getName());
    }

    public static List<Datasets.Player> toPlayersWithException(final Team team) {
        throw new IllegalStateException(ERROR_PLAYERS + team.getName());
    }

    /**
     * Asserts the result other teams list.
     */
    public static Void assertOtherTeam(final Iterable<OtherTeam> otherTeams) {
        assertThat(otherTeams).isNotNull().isNotEmpty();
        assertThat(otherTeams).hasSize(1);

        final OtherTeam otherTeam = StreamSupport.stream(otherTeams.spliterator(), false)
                .findFirst()
                .get();

        assertThat(otherTeam.getName())
                .isNotNull()
                .isEqualTo(Datasets.TeamNames.PSG.toString());

        assertThat(otherTeam.getOtherName())
                .isNotNull()
                .isEqualTo("other " + Datasets.TeamNames.PSG);

        return null;
    }

    /**
     * Asserts the {@link OtherTeam} result list with side input field.
     * <p>
     * This object has a field dedicated to check if element provided by side input is correctly mapped to
     * {@link OtherTeam} objects.
     */
    public static Void assertOtherTeamWithSideInputField(final String sideInputFieldValue,
                                                         final Iterable<OtherTeam> otherTeams) {
        assertOtherTeam(otherTeams);

        assertThat(otherTeams)
                .allSatisfy(team -> assertThat(team.getSideInputField()).isNotNull().isEqualTo(sideInputFieldValue));

        return null;
    }

    /**
     * Asserts failures for the mapping from {@link Team} object.
     * <p>
     * The exception class and expected error are given, assertions are executed on them in the result failures.
     */
    public static Void assertFailuresFromInputTeam(final Iterable<Failure> failures,
                                                   final Class<? extends Throwable> currentExceptionClass,
                                                   final String currentExceptionMessage) {
        assertThat(failures).isNotNull().isNotEmpty();
        assertThat(failures).hasSize(1);

        final Failure failure = StreamSupport.stream(failures.spliterator(), false)
                .findFirst()
                .get();

        assertThat(failure.getInputElement())
                .isNotNull()
                .isNotEmpty();

        final Team inputTeam = JsonUtil.deserialize(failure.getInputElement(), Team.class);
        assertThat(inputTeam).isNotNull();
        assertThat(inputTeam.getName()).isNotNull().isNotEmpty();

        assertThat(failure.getException())
                .isNotNull()
                .isInstanceOf(currentExceptionClass)
                .hasMessage(currentExceptionMessage + inputTeam.getName());

        return null;
    }
}
