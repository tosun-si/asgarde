package fr.groupbees.asgarde.settings;

import fr.groupbees.asgarde.Failure;
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

    public static Datasets.OtherTeam toOtherTeam(final Datasets.Team team) {
        return Datasets.OtherTeam.builder()
                .name(team.getName())
                .otherName("other " + team.getName())
                .build();
    }

    public static Datasets.OtherTeam toOtherTeamWithSideInputField(final PCollectionView<String> sideInput,
                                                                   final DoFn<Datasets.Team, Datasets.OtherTeam>.ProcessContext context) {
        final Datasets.Team inputTeam = context.element();
        return toOtherTeam(inputTeam)
                .toBuilder()
                .sideInputField(context.sideInput(sideInput))
                .build();
    }

    public static Datasets.OtherTeam toOtherTeamWithException(final Datasets.Team team) {
        throw new IllegalArgumentException(ERROR_OTHER_TEAM + team.getName());
    }

    public static List<Datasets.Player> toPlayersWithException(final Datasets.Team team) {
        throw new IllegalStateException(ERROR_PLAYERS + team.getName());
    }

    /**
     * Asserts the result other teams list.
     */
    public static Void assertOtherTeam(final Iterable<Datasets.OtherTeam> otherTeams) {
        assertThat(otherTeams).isNotNull().isNotEmpty();
        assertThat(otherTeams).hasSize(1);

        final Datasets.OtherTeam otherTeam = StreamSupport.stream(otherTeams.spliterator(), false)
                .findFirst()
                .get();

        assertThat(otherTeam.getName())
                .isNotNull()
                .isEqualTo(Datasets.TeamNames.PSG.toString());

        assertThat(otherTeam.getOtherName())
                .isNotNull()
                .isEqualTo("other " + Datasets.TeamNames.PSG.toString());

        return null;
    }

    /**
     * Asserts the {@link Datasets.OtherTeam} result list with side input field.
     * <p>
     * This object has a field dedicated to check if element provided by side input is correctly mapped to
     * {@link Datasets.OtherTeam} objects.
     */
    public static Void assertOtherTeamWithSideInputField(final String sideInputFieldValue,
                                                         final Iterable<Datasets.OtherTeam> otherTeams) {
        assertOtherTeam(otherTeams);

        assertThat(otherTeams)
                .allSatisfy(team -> assertThat(team.getSideInputField()).isNotNull().isEqualTo(sideInputFieldValue));

        return null;
    }

    /**
     * Asserts failures for the mapping from {@link Datasets.Team} object.
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

        final Datasets.Team inputTeam = JsonUtil.deserialize(failure.getInputElement(), Datasets.Team.class);
        assertThat(inputTeam).isNotNull();
        assertThat(inputTeam.getName()).isNotNull().isNotEmpty();

        assertThat(failure.getException())
                .isNotNull()
                .isInstanceOf(currentExceptionClass)
                .hasMessage(currentExceptionMessage + inputTeam.getName());

        return null;
    }
}
