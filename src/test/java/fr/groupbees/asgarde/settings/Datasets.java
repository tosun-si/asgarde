package fr.groupbees.asgarde.settings;

import avro.shaded.com.google.common.collect.ImmutableMap;
import com.fasterxml.jackson.core.type.TypeReference;
import fr.groupbees.asgarde.CollectionComposerTest;
import fr.groupbees.asgarde.Failure;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
        return Failure.from(
                team.getName(),
                team,
                TEAMS_WITH_ERROR.get(team.getName()).get()
        );
    }

    public static class Team implements Serializable {
        private String name;
        private int score;
        private Integer profit;
        private String nickName;
        private boolean checkError;
        private List<Player> players;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getScore() {
            return score;
        }

        public void setScore(int score) {
            this.score = score;
        }

        public Integer getProfit() {
            return profit;
        }

        public void setProfit(Integer profit) {
            this.profit = profit;
        }

        public String getNickName() {
            return nickName;
        }

        public void setNickName(String nickName) {
            this.nickName = nickName;
        }

        public boolean isCheckError() {
            return checkError;
        }

        public void setCheckError(boolean checkError) {
            this.checkError = checkError;
        }

        public List<Player> getPlayers() {
            return players;
        }

        public void setPlayers(List<Player> players) {
            this.players = players;
        }

        @Override
        public String toString() {
            return JsonUtil.serialize(this);
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Team team = (Team) o;
            return score == team.score
                    && checkError == team.checkError
                    && Objects.equals(name, team.name)
                    && Objects.equals(profit, team.profit)
                    && Objects.equals(nickName, team.nickName)
                    && Objects.equals(players, team.players);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, score, profit, nickName, checkError, players);
        }

        public Team copy() {
            final Team newTeam = new Team();

            newTeam.setName(this.getName());
            newTeam.setScore(this.getScore());
            newTeam.setProfit(this.getProfit());
            newTeam.setNickName(this.getNickName());
            newTeam.setCheckError(this.isCheckError());
            newTeam.setPlayers(this.getPlayers());

            return newTeam;
        }
    }

    public static class OtherTeam implements Serializable {
        private String name;
        private String otherName;
        private String sideInputField;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getOtherName() {
            return otherName;
        }

        public void setOtherName(String otherName) {
            this.otherName = otherName;
        }

        public String getSideInputField() {
            return sideInputField;
        }

        public void setSideInputField(String sideInputField) {
            this.sideInputField = sideInputField;
        }

        @Override
        public String toString() {
            return JsonUtil.serialize(this);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            OtherTeam otherTeam = (OtherTeam) o;
            return Objects.equals(name, otherTeam.name)
                    && Objects.equals(otherName, otherTeam.otherName)
                    && Objects.equals(sideInputField, otherTeam.sideInputField);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, otherName, sideInputField);
        }
    }

    public static class Player implements Serializable {
        private String firstName;
        private String lastName;
        private Integer age;

        public String getFirstName() {
            return firstName;
        }

        public void setFirstName(String firstName) {
            this.firstName = firstName;
        }

        public String getLastName() {
            return lastName;
        }

        public void setLastName(String lastName) {
            this.lastName = lastName;
        }

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }

        @Override
        public String toString() {
            return JsonUtil.serialize(this);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Player player = (Player) o;
            return Objects.equals(firstName, player.firstName)
                    && Objects.equals(lastName, player.lastName)
                    && Objects.equals(age, player.age);
        }

        @Override
        public int hashCode() {
            return Objects.hash(firstName, lastName, age);
        }
    }

    public enum TeamNames {
        PSG, BAYERN, JUVENTUS, REAL, BARCELONA
    }
}
