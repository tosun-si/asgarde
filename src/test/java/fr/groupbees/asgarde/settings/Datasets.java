package fr.groupbees.asgarde.settings;

import avro.shaded.com.google.common.collect.ImmutableMap;
import com.fasterxml.jackson.core.type.TypeReference;
import fr.groupbees.asgarde.CollectionComposerTest;
import lombok.*;
import fr.groupbees.asgarde.Failure;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toList;

/**
 * This class gives datasets for {@link CollectionComposerTest} class.
 */
public class Datasets {

    public static final String NO_AUTH_SCORE_PSG = "No authorized score for PSG team";
    public static final String BAYERN_NOT_HAVE_NICKNAME = "Bayern team doesn't have a nickname";
    public static final String NO_AUTH_PROFIT_JUVE = "No authorized profit for Juventus team";

    public static final Supplier<RuntimeException> NO_AUTH_SCORE_PSG_EXCEPTION = () -> new IllegalStateException(NO_AUTH_SCORE_PSG);
    public static final Supplier<RuntimeException> BAYERN_NOT_HAVE_NICKNAME_EXCEPTION = () -> new IllegalArgumentException(BAYERN_NOT_HAVE_NICKNAME);
    public static final Supplier<RuntimeException> NO_AUTH_PROFIT_JUVE_EXCEPTION = () -> new IllegalStateException(NO_AUTH_PROFIT_JUVE);

    public static final TypeReference<List<Team>> TEAM_TYPE_REF = new TypeReference<List<Team>>() {
    };
    public static final List<Team> INPUT_TEAMS_WITH_THREE_FAILURES = JsonUtil.deserializeFromResourcePath(
            "inputs/input-five-teams-with-three-failures-and-one-good-output.json", TEAM_TYPE_REF);

    public static final List<Team> INPUT_TEAMS_WITH_ALL_FAILURES = JsonUtil.deserializeFromResourcePath(
            "inputs/input-five-teams-with-all-failures.json", TEAM_TYPE_REF);

    public static final List<Team> INPUT_TEAMS_WITH_ONE_FAILURE = JsonUtil.deserializeFromResourcePath(
            "inputs/input-one-team-with-one-failure-and-no-good-output.json", TEAM_TYPE_REF);

    public static final List<Team> FAILURE_THREE_TEAMS = JsonUtil.deserializeFromResourcePath(
            "failures/failures-five-teams-with-three-failures-and-one-good-output.json", TEAM_TYPE_REF);

    public static final List<Team> INPUT_TEAMS_NO_FAILURE = JsonUtil.deserializeFromResourcePath(
            "inputs/input-five-teams-no-failure.json", TEAM_TYPE_REF);

    public static final Map<String, Supplier<RuntimeException>> TEAMS_WITH_ERROR = ImmutableMap.of(
            TeamNames.PSG.toString(), NO_AUTH_SCORE_PSG_EXCEPTION,
            TeamNames.BAYERN.toString(), BAYERN_NOT_HAVE_NICKNAME_EXCEPTION,
            TeamNames.JUVENTUS.toString(), NO_AUTH_PROFIT_JUVE_EXCEPTION
    );

    public static final List<Failure> EXPECTED_THREE_FAILURES = FAILURE_THREE_TEAMS.stream()
            .map(Datasets::toFailure)
            .collect(toList());

    public static final List<Failure> EXPECTED_ONE_FAILURES = INPUT_TEAMS_WITH_ONE_FAILURE.stream()
            .map(Datasets::toFailure)
            .collect(toList());

    public static Failure toFailure(final Team team) {
        return Failure.from(team, TEAMS_WITH_ERROR.get(team.getName()).get());
    }

    @Builder(toBuilder = true)
    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @EqualsAndHashCode
    public static class Team implements Serializable {
        private String name;

        private int score;

        private Integer profit;

        private String nickName;

        private boolean checkError;

        @Singular
        private List<Player> players;

        @Override
        public String toString() {
            return JsonUtil.serialize(this);
        }
    }

    @Builder(toBuilder = true)
    @AllArgsConstructor
    @Getter
    public static class OtherTeam implements Serializable {
        private String name;
        private String otherName;
        private String sideInputField;

        @Override
        public String toString() {
            return JsonUtil.serialize(this);
        }
    }

    @Builder(toBuilder = true)
    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @EqualsAndHashCode
    public static class Player implements Serializable {
        private String firstName;
        private String lastName;
        private Integer age;

        @Override
        public String toString() {
            return JsonUtil.serialize(this);
        }
    }

    @AllArgsConstructor
    @Getter
    public enum TeamNames {
        PSG, BAYERN, JUVENTUS, REAL, BARCELONA;
    }
}
