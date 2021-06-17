package fr.groupbees.asgarde;

import fr.groupbees.asgarde.settings.*;
import fr.groupbees.asgarde.settings.Datasets.OtherTeam;
import fr.groupbees.asgarde.settings.Datasets.Team;
import fr.groupbees.asgarde.transforms.BaseElementFn;
import fr.groupbees.asgarde.transforms.FilterFn;
import fr.groupbees.asgarde.transforms.MapElementFn;
import fr.groupbees.asgarde.transforms.MapProcessContextFn;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;
import static org.apache.beam.sdk.values.TypeDescriptor.of;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Contains all the test of the {@link CollectionComposer} class.
 */
@RunWith(JUnitParamsRunner.class)
public class CollectionComposerTest implements Serializable {

    private static final String MAP_TO_TEAM = "Map to team";
    private static final String MAP_TO_OTHER_TEAM = "Map to other team";
    private static final String CONTEXT_TO_TEAM = "Process context to team";
    private static final String FLAT_MAP_TO_PLAYER = "Flat map to player";
    private static final String CONTEXT_TO_OTHER_TEAM = "Process context to other team";
    private static final String FILTER_TEAMS = "Filter teams";

    /**
     * Contains all the params to test the result with all the MapElement functions without error.
     */
    public Object[] resultCorrectMapElementsParams() {
        final Function<PCollection<Team>, Result<PCollection<OtherTeam>, Failure>> resultMapElements =
                teams -> CollectionComposer.of(teams)
                        .apply(MAP_TO_OTHER_TEAM, MapElements
                                .into(of(OtherTeam.class))
                                .via(TestSettings::toOtherTeam)
                                .exceptionsInto(of(Failure.class))
                                .exceptionsVia(Failure::from))
                        .getResult();

        final Function<PCollection<Team>, Result<PCollection<OtherTeam>, Failure>> resultMapElementsInternalErrorHandling =
                teams -> CollectionComposer.of(teams)
                        .apply(MAP_TO_OTHER_TEAM, MapElements.into(of(OtherTeam.class)).via(TestSettings::toOtherTeam))
                        .getResult();

        final Function<PCollection<Team>, Result<PCollection<OtherTeam>, Failure>> resultSeparateTransform =
                teams -> CollectionComposer.of(teams)
                        .apply(MAP_TO_OTHER_TEAM, new MapElementNoErrorTransform())
                        .getResult();

        final Function<PCollection<Team>, Result<PCollection<OtherTeam>, Failure>> resultSeparateDoFn =
                teams -> CollectionComposer.of(teams)
                        .apply(MAP_TO_OTHER_TEAM, new CustomDoFnNoError())
                        .getResult();

        final Function<PCollection<Team>, Result<PCollection<OtherTeam>, Failure>> resultMapElementFn =
                teams -> CollectionComposer.of(teams)
                        .apply(MAP_TO_OTHER_TEAM, MapElementFn.into(of(OtherTeam.class)).via(TestSettings::toOtherTeam))
                        .getResult();

        final Function<PCollection<Team>, Result<PCollection<OtherTeam>, Failure>> resultMapProcessElementFn =
                teams -> CollectionComposer.of(teams)
                        .apply(CONTEXT_TO_OTHER_TEAM, MapProcessContextFn
                                .from(Team.class)
                                .into(of(OtherTeam.class))
                                .via(context -> TestSettings.toOtherTeam(context.element())))
                        .getResult();

        return new Object[][]{
                {resultMapElements},
                {resultMapElementsInternalErrorHandling},
                {resultSeparateTransform},
                {resultSeparateDoFn},
                {resultMapElementFn},
                {resultMapProcessElementFn}
        };
    }

    /**
     * Contains all the params to test the result with all the MapElement functions with error.
     */
    public Object[] resultErrorMapElementsParams() {
        final Function<PCollection<Team>, Result<PCollection<OtherTeam>, Failure>> resultMapElements =
                teams -> CollectionComposer.of(teams)
                        .apply(MAP_TO_OTHER_TEAM, MapElements
                                .into(of(OtherTeam.class))
                                .via(TestSettings::toOtherTeamWithException)
                                .exceptionsInto(of(Failure.class))
                                .exceptionsVia(Failure::from))
                        .getResult();

        final Function<PCollection<Team>, Result<PCollection<OtherTeam>, Failure>> resultMapElementsInternalErrorHandling =
                teams -> CollectionComposer.of(teams)
                        .apply(MAP_TO_OTHER_TEAM, MapElements.into(of(OtherTeam.class)).via(TestSettings::toOtherTeamWithException))
                        .getResult();

        final Function<PCollection<Team>, Result<PCollection<OtherTeam>, Failure>> resultMapElementFn =
                teams -> CollectionComposer.of(teams)
                        .apply(MAP_TO_OTHER_TEAM, MapElementFn.into(of(OtherTeam.class)).via(TestSettings::toOtherTeamWithException))
                        .getResult();

        final Function<PCollection<Team>, Result<PCollection<OtherTeam>, Failure>> resultMapProcessElementFn =
                teams -> CollectionComposer.of(teams)
                        .apply(CONTEXT_TO_OTHER_TEAM, MapProcessContextFn
                                .from(Team.class)
                                .into(of(OtherTeam.class))
                                .via(context -> TestSettings.toOtherTeamWithException(context.element())))
                        .getResult();

        return new Object[][]{
                {resultMapElements},
                {resultMapElementsInternalErrorHandling},
                {resultMapElementFn},
                {resultMapProcessElementFn}
        };
    }

    /**
     * Contains all the params to test the result with all the FlaMapElement functions without error.
     */
    public Object[] resultCorrectFlatMapElementsParams() {
        final Function<PCollection<Team>, Result<PCollection<Datasets.Player>, Failure>> resultMapElements =
                teams -> CollectionComposer.of(teams)
                        .apply(FLAT_MAP_TO_PLAYER, FlatMapElements
                                .into(of(Datasets.Player.class))
                                .via(Team::getPlayers)
                                .exceptionsInto(of(Failure.class))
                                .exceptionsVia(Failure::from))
                        .getResult();

        final Function<PCollection<Team>, Result<PCollection<Datasets.Player>, Failure>> resultMapElementsInternalErrorHandling =
                teams -> CollectionComposer.of(teams)
                        .apply(FLAT_MAP_TO_PLAYER, FlatMapElements.into(of(Datasets.Player.class)).via(Team::getPlayers))
                        .getResult();


        return new Object[][]{
                {resultMapElements},
                {resultMapElementsInternalErrorHandling}
        };
    }

    /**
     * Contains all the params to test the result with all the FlaMapElement functions without error.
     */
    public Object[] resultErrorFlatMapElementsParams() {
        final Function<PCollection<Team>, Result<PCollection<Datasets.Player>, Failure>> resultFlatMapElements =
                teams -> CollectionComposer.of(teams)
                        .apply(FLAT_MAP_TO_PLAYER, FlatMapElements
                                .into(of(Datasets.Player.class))
                                .via(TestSettings::toPlayersWithException)
                                .exceptionsInto(of(Failure.class))
                                .exceptionsVia(Failure::from))
                        .getResult();

        final Function<PCollection<Team>, Result<PCollection<Datasets.Player>, Failure>> resultFlatMapElementsInternalErrorHandling =
                teams -> CollectionComposer.of(teams)
                        .apply(FLAT_MAP_TO_PLAYER, FlatMapElements.into(of(Datasets.Player.class)).via(TestSettings::toPlayersWithException))
                        .getResult();

        return new Object[][]{
                {resultFlatMapElements},
                {resultFlatMapElementsInternalErrorHandling}
        };
    }

    /**
     * Contains all the params for the custom DoFn ({@link BaseElementFn}),
     * in order to test the output coders and type descriptors.
     */
    public Object[] resultBaseElementFnCodersAndTypeDescriptorsParams() {
        final Function<PCollection<Team>, Result<PCollection<OtherTeam>, Failure>> resultSeparateTransform =
                teams -> CollectionComposer.of(teams)
                        .apply(MAP_TO_OTHER_TEAM, new MapElementNoErrorTransform())
                        .getResult();

        final Function<PCollection<Team>, Result<PCollection<OtherTeam>, Failure>> resultSeparateDoFn =
                teams -> CollectionComposer.of(teams)
                        .apply(MAP_TO_OTHER_TEAM, new CustomDoFnNoError())
                        .getResult();

        final Function<PCollection<Team>, Result<PCollection<OtherTeam>, Failure>> resultMapElementFn =
                teams -> CollectionComposer.of(teams)
                        .apply(MAP_TO_OTHER_TEAM, MapElementFn.into(of(OtherTeam.class)).via(TestSettings::toOtherTeam))
                        .getResult();

        final Function<PCollection<Team>, Result<PCollection<OtherTeam>, Failure>> resultMapProcessElementFn =
                teams -> CollectionComposer.of(teams)
                        .apply(CONTEXT_TO_OTHER_TEAM, MapProcessContextFn
                                .from(Team.class)
                                .into(of(OtherTeam.class))
                                .via(context -> TestSettings.toOtherTeam(context.element())))
                        .getResult();

        final Function<PCollection<Team>, Result<PCollection<Team>, Failure>> resultFilterFn =
                teams -> CollectionComposer.of(teams)
                        .apply(MAP_TO_OTHER_TEAM, FilterFn.by(this::isNotBarcelona))
                        .getResult();

        return new Object[][]{
                {
                        resultSeparateTransform,
                        SerializableCoder.of(OtherTeam.class),
                        of(OtherTeam.class)
                },
                {
                        resultSeparateDoFn,
                        SerializableCoder.of(OtherTeam.class),
                        of(OtherTeam.class)
                },
                {
                        resultMapElementFn,
                        SerializableCoder.of(OtherTeam.class),
                        of(OtherTeam.class)
                },
                {
                        resultMapProcessElementFn,
                        SerializableCoder.of(OtherTeam.class),
                        of(OtherTeam.class)
                },
                {
                        resultFilterFn,
                        SerializableCoder.of(Team.class),
                        of(Team.class)
                }
        };
    }

    /**
     * Contains all the params for the custom DoFn, {@link MapElementFn} and {@link MapProcessContextFn}
     * with start action.
     * In this case we can check if the action was correctly executed
     */
    public Object[] resultMapElementFnWithSetupActionParams() {
        final String consoleMessageMapElementFn = "Test start action MapElementFn";
        final Function<PCollection<Team>, Result<PCollection<OtherTeam>, Failure>> resultMapElementFn =
                teams -> CollectionComposer.of(teams)
                        .apply(MAP_TO_OTHER_TEAM, MapElementFn
                                .into(of(OtherTeam.class))
                                .via(TestSettings::toOtherTeam)
                                .withSetupAction(() -> System.out.print(consoleMessageMapElementFn)))
                        .getResult();

        final String consoleMessageMapProcessElementFn = "Test start action MapProcessContextFn";
        final Function<PCollection<Team>, Result<PCollection<OtherTeam>, Failure>> resultMapProcessElementFn =
                teams -> CollectionComposer.of(teams)
                        .apply(MAP_TO_OTHER_TEAM, MapProcessContextFn
                                .from(Team.class)
                                .into(of(OtherTeam.class))
                                .via(ctx -> TestSettings.toOtherTeam(ctx.element()))
                                .withSetupAction(() -> System.out.print(consoleMessageMapProcessElementFn)))
                        .getResult();

        return new Object[][]{
                {
                        resultMapElementFn,
                        consoleMessageMapElementFn
                },
                {
                        resultMapProcessElementFn,
                        consoleMessageMapProcessElementFn
                }
        };
    }

    @Rule
    public transient TestPipeline pipeline = TestPipeline.create();

    @Test
    @Category(ValidatesRunner.class)
    @Parameters(method = "resultCorrectMapElementsParams")
    public void givenOneTeam_whenApplyComposerWithMapElementToOtherTeamObjectWithoutError_thenOneOtherTeamOutputAndEmptyFailure(
            final Function<PCollection<Team>, Result<PCollection<OtherTeam>, Failure>> resultFunction) {

        // Given.
        final List<Team> psgTeam = getTeamsByName(Datasets.TeamNames.PSG, Datasets.INPUT_TEAMS_NO_FAILURE);
        final PCollection<Team> teamCollection = pipeline.apply("Reads people", Create.of(psgTeam));

        // When.
        Result<PCollection<OtherTeam>, Failure> result = resultFunction.apply(teamCollection);

        // Then.
        final PCollection<Failure> failures = result.failures();
        PAssert.that(failures).empty();

        final PCollection<OtherTeam> output = result.output();
        PAssert.that(output).satisfies(TestSettings::assertOtherTeam);

        pipeline.run().waitUntilFinish();
    }

    @Test
    @Category(ValidatesRunner.class)
    @Parameters(method = "resultErrorMapElementsParams")
    public void givenOneTeam_whenApplyComposerWithMapElementToOtherTeamObjectWithError_thenEmptyOtherTeamOutputAndOneFailure(
            final Function<PCollection<Team>, Result<PCollection<OtherTeam>, Failure>> resultFunction) {

        // Given.
        final List<Team> psgTeam = getTeamsByName(Datasets.TeamNames.PSG, Datasets.INPUT_TEAMS_NO_FAILURE);
        final PCollection<Team> teamCollection = pipeline.apply("Reads people", Create.of(psgTeam));

        // When.
        Result<PCollection<OtherTeam>, Failure> result = resultFunction.apply(teamCollection);

        // Then.
        PAssert.that(result.failures())
                .satisfies(failures -> TestSettings.assertFailuresFromInputTeam(failures, IllegalArgumentException.class, TestSettings.ERROR_OTHER_TEAM));

        final PCollection<OtherTeam> output = result.output();
        PAssert.that(output).empty();

        pipeline.run().waitUntilFinish();
    }

    @Test
    @Category(ValidatesRunner.class)
    @Parameters(method = "resultCorrectFlatMapElementsParams")
    public void givenOneTeam_whenApplyComposerWithOneFlatMapWithoutError_thenExpectedOutputPlayersAndNoFailure(
            final Function<PCollection<Team>, Result<PCollection<Datasets.Player>, Failure>> resultFunction) {
        // Given.
        final List<Team> psgTeam = getTeamsByName(Datasets.TeamNames.PSG, Datasets.INPUT_TEAMS_NO_FAILURE);
        final PCollection<Team> teamCollection = pipeline.apply("Reads people", Create.of(psgTeam));

        // When.
        final Result<PCollection<Datasets.Player>, Failure> result = resultFunction.apply(teamCollection);

        // Then.
        final List<Datasets.Player> expectedPlayers = psgTeam.stream()
                .map(Team::getPlayers)
                .flatMap(Collection::stream)
                .collect(toList());

        final PCollection<Failure> failures = result.failures();
        PAssert.that(failures).empty();

        final PCollection<Datasets.Player> output = result.output();
        PAssert.that(output).containsInAnyOrder(expectedPlayers);

        pipeline.run().waitUntilFinish();
    }

    @Test
    @Category(ValidatesRunner.class)
    @Parameters(method = "resultErrorFlatMapElementsParams")
    public void givenOneTeam_whenApplyComposerWithFlatMapElementWithError_thenEmptyPlayersOutputAndOneFailure(
            final Function<PCollection<Team>, Result<PCollection<Datasets.Player>, Failure>> resultFunction) {

        // Given.
        final List<Team> psgTeam = getTeamsByName(Datasets.TeamNames.PSG, Datasets.INPUT_TEAMS_NO_FAILURE);
        final PCollection<Team> teamCollection = pipeline.apply("Reads people", Create.of(psgTeam));

        // When.
        Result<PCollection<Datasets.Player>, Failure> result = resultFunction.apply(teamCollection);

        // Then.
        PAssert.that(result.failures())
                .satisfies(failures -> TestSettings.assertFailuresFromInputTeam(failures, IllegalStateException.class, TestSettings.ERROR_PLAYERS));

        final PCollection<Datasets.Player> output = result.output();
        PAssert.that(output).empty();

        pipeline.run().waitUntilFinish();
    }

    @Test
    @Category(ValidatesRunner.class)
    @Parameters(method = "resultBaseElementFnCodersAndTypeDescriptorsParams")
    public <T> void givenOneTeam_whenApplyComposerWithBaseElementFnOutputCoderAndTypeDescriptor_thenOutputHasExpectedCoderAndTypeDescriptor(
            final Function<PCollection<Team>, Result<PCollection<T>, Failure>> resultFunction,
            final Coder<T> outputCoder,
            final TypeDescriptor<T> outputDescriptor) {

        // Given.
        final List<Team> psgTeam = getTeamsByName(Datasets.TeamNames.PSG, Datasets.INPUT_TEAMS_NO_FAILURE);
        final PCollection<Team> teamCollection = pipeline.apply("Reads people", Create.of(psgTeam));

        // When.
        Result<PCollection<T>, Failure> result = resultFunction.apply(teamCollection);

        // Then.
        final PCollection<Failure> failures = result.failures();
        PAssert.that(failures).empty();

        final PCollection<T> output = result.output();

        assertThat(output.getCoder()).isEqualTo(outputCoder);
        assertThat(output.getTypeDescriptor()).isEqualTo(outputDescriptor);

        pipeline.run().waitUntilFinish();
    }

    @Test
    @Category(ValidatesRunner.class)
    public void givenOneTeams_whenApplyComposerWithMapProcessElementAndSideInput_thenOutputOtherTeamWithSideInputFieldAndNoFailure() {
        // Given.
        final List<Team> psgTeam = getTeamsByName(Datasets.TeamNames.PSG, Datasets.INPUT_TEAMS_NO_FAILURE);
        final PCollection<Team> teamCollection = pipeline.apply("Reads people", Create.of(psgTeam));
        final String sideInputFieldValue = "Side input test";

        final PCollectionView<String> sideInput = pipeline
                .apply("String side input", Create.of(sideInputFieldValue))
                .apply("Create as collection view", View.asSingleton());

        // When.
        final Result<PCollection<OtherTeam>, Failure> result = CollectionComposer.of(teamCollection)
                .apply(CONTEXT_TO_OTHER_TEAM, MapProcessContextFn
                                .from(Team.class)
                                .into(of(OtherTeam.class))
                                .via(context -> TestSettings.toOtherTeamWithSideInputField(sideInput, context)),
                        Collections.singletonList(sideInput))
                .getResult();

        final PCollection<Failure> failures = result.failures();
        PAssert.that(failures).empty();

        final PCollection<OtherTeam> output = result.output();
        PAssert.that(output)
                .satisfies(otherTeams -> TestSettings.assertOtherTeamWithSideInputField(sideInputFieldValue, otherTeams));

        pipeline.run().waitUntilFinish();
    }

    @Test
    @Category(ValidatesRunner.class)
    public void givenFiveTeams_whenApplyComposerWithoutMapErrorAndWithoutFilter_thenAllTeamsInOutputAndNoFailure() {
        // Given.
        final PCollection<Team> teamCollection = pipeline.apply("Reads people", Create.of(Datasets.INPUT_TEAMS_NO_FAILURE));

        // When.
        final Result<PCollection<Team>, Failure> result = CollectionComposer.of(teamCollection)
                .apply(MAP_TO_TEAM, MapElements.into(of(Team.class)).via(this::toTeamWithScore))
                .apply(CONTEXT_TO_TEAM, MapProcessContextFn.from(Team.class).into(of(Team.class)).via(this::toTeamWithProfit))
                .apply(MAP_TO_TEAM + "fn", MapElementFn.into(of(Team.class)).via(this::toTeamWithNickName))
                .getResult();

        final PCollection<Failure> failures = result.failures();
        PAssert.that(failures).empty();

        final List<Team> expectedTeams = JsonUtil.deserializeFromResourcePath(
                "outputs/output-five-teams-no-error.json", Datasets.TEAM_TYPE_REF);

        final PCollection<Team> output = result.output();
        PAssert.that(output).containsInAnyOrder(expectedTeams);

        pipeline.run().waitUntilFinish();
    }

    @Test
    @Category(ValidatesRunner.class)
    public void givenFiveTeams_whenApplyComposerWithDifferentMapErrorsAndACorrectFilter_thenThreeFailuresAndOneSuccess() {
        // Given.
        final PCollection<Team> teamCollection = pipeline.apply("Reads people", Create.of(Datasets.INPUT_TEAMS_WITH_THREE_FAILURES));

        // When.
        final Result<PCollection<Team>, Failure> result = CollectionComposer.of(teamCollection)
                .apply(MAP_TO_TEAM, MapElements.into(of(Team.class)).via(this::toTeamWithScore))
                .apply(CONTEXT_TO_TEAM, MapProcessContextFn.from(Team.class).into(of(Team.class)).via(this::toTeamWithProfit))
                .apply(MAP_TO_TEAM + "2", MapElementFn.into(of(Team.class)).via(this::toTeamWithNickName))
                .apply(FILTER_TEAMS, FilterFn.by(this::isNotBarcelona))
                .getResult();

        final PCollection<Failure> failures = result.failures();
        PAssert.that(failures).satisfies(resultFailures -> assertFailures(resultFailures, Datasets.EXPECTED_THREE_FAILURES));

        final List<Team> expectedTeams = JsonUtil.deserializeFromResourcePath(
                "outputs/output-five-teams-with-three-failures-and-one-good-output.json", Datasets.TEAM_TYPE_REF);

        final PCollection<Team> output = result.output();
        PAssert.that(output).containsInAnyOrder(expectedTeams);

        pipeline.run().waitUntilFinish();
    }

    @Test
    @Category(ValidatesRunner.class)
    public void givenFiveTeams_whenApplyComposerWithSimulatingAllElementsWithError_thenAllResultInFailures() {
        // Given.
        final PCollection<Team> teamCollection = pipeline.apply("Reads people", Create.of(Datasets.INPUT_TEAMS_WITH_ALL_FAILURES));

        // When.
        final Result<PCollection<Team>, Failure> result = CollectionComposer.of(teamCollection)
                .apply(MAP_TO_TEAM,
                        MapElements
                                .into(of(Team.class))
                                .via(this::toTeamWithScore)
                                .exceptionsInto(of(Failure.class))
                                .exceptionsVia(Failure::from))
                .apply(CONTEXT_TO_TEAM, MapProcessContextFn.from(Team.class).into(of(Team.class)).via(this::toTeamWithProfit))
                .apply(MAP_TO_TEAM + "2", MapElementFn.into(of(Team.class)).via(this::toTeamWithNickName))
                .apply(FILTER_TEAMS, FilterFn.by(this::isNotBarcelona))
                .getResult();

        final PCollection<Failure> failures = result.failures();
        PAssert.that(failures).satisfies(resultFailures -> assertFailures(resultFailures, Datasets.EXPECTED_THREE_FAILURES));

        final PCollection<Team> output = result.output();
        PAssert.that(output).empty();

        pipeline.run().waitUntilFinish();
    }

    @Test
    @Category(ValidatesRunner.class)
    public void givenFiveTeams_whenApplyComposerWithOneFilterError_thenEmptyCorrectOutputAndOneFailure() {
        // Given.
        final PCollection<Team> teamCollection = pipeline.apply("Reads people", Create.of(Datasets.INPUT_TEAMS_WITH_ONE_FAILURE));

        // When.
        final Result<PCollection<Team>, Failure> result = CollectionComposer.of(teamCollection)
                .apply(FILTER_TEAMS, FilterFn.by(this::simulateFilterErrorPsgTeam))
                .getResult();

        final PCollection<Failure> failures = result.failures();
        PAssert.that(failures).satisfies(resultFailures -> assertFailures(resultFailures, Datasets.EXPECTED_ONE_FAILURES));

        final PCollection<Team> output = result.output();
        PAssert.that(output).empty();

        pipeline.run().waitUntilFinish();
    }

    @Test
    @Category(ValidatesRunner.class)
    public void givenFiveTeams_whenApplyComposerWithOneFlatMapError_thenEmptyCorrectOutputAndOneFailure() {
        // Given.
        final PCollection<Team> teamCollection = pipeline.apply("Reads people", Create.of(Datasets.INPUT_TEAMS_WITH_ONE_FAILURE));

        // When.
        final Result<PCollection<Datasets.Player>, Failure> result = CollectionComposer.of(teamCollection)
                .apply(FLAT_MAP_TO_PLAYER, FlatMapElements
                        .into(of(Datasets.Player.class))
                        .via(this::simulateFlatMapErrorPsgTeam)
                        .exceptionsInto(of(Failure.class))
                        .exceptionsVia(Failure::from))
                .getResult();

        final PCollection<Failure> failures = result.failures();
        PAssert.that(failures).satisfies(resultFailures -> assertFailures(resultFailures, Datasets.EXPECTED_ONE_FAILURES));

        final PCollection<Datasets.Player> output = result.output();
        PAssert.that(output).empty();

        pipeline.run().waitUntilFinish();
    }

    @Test
    @Category(ValidatesRunner.class)
    public void givenOneTeam_whenApplyComposerWithOneFilterWithoutError_thenExpectedOutputTeamsAndNoFailure() {
        // Given.
        final List<Team> psgTeam = getTeamsByName(Datasets.TeamNames.PSG, Datasets.INPUT_TEAMS_NO_FAILURE);
        final PCollection<Team> teamCollection = pipeline.apply("Reads people", Create.of(psgTeam));

        // When.
        final Result<PCollection<Team>, Failure> result = CollectionComposer.of(teamCollection)
                .apply(FILTER_TEAMS, FilterFn.by(this::isNotBarcelona))
                .getResult();

        // Then.
        final PCollection<Failure> failures = result.failures();
        PAssert.that(failures).empty();

        final PCollection<Team> output = result.output();
        PAssert.that(output).containsInAnyOrder(psgTeam);

        pipeline.run().waitUntilFinish();
    }

    @Test
    @Category(ValidatesRunner.class)
    @Parameters(method = "resultMapElementFnWithSetupActionParams")
    public void givenOneTeam_whenApplyComposerWithMapFnWithSetupAction_thenActionIsCorrectlyExecuted(
            final Function<PCollection<Team>, Result<PCollection<OtherTeam>, Failure>> resultFunction,
            final String setupActionExpectedMessageConsole) {

        // Allows to test side effect withSetupAction.
        // We perform a System.out.print and checks if the message has been correctly printed in the console.
        final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        final PrintStream originalOut = System.out;
        System.setOut(new PrintStream(outContent));

        // Given.
        final List<Team> psgTeam = getTeamsByName(Datasets.TeamNames.PSG, Datasets.INPUT_TEAMS_NO_FAILURE);
        final PCollection<Team> teamCollection = pipeline.apply("Reads people", Create.of(psgTeam));

        // When.
        resultFunction.apply(teamCollection);

        pipeline.run().waitUntilFinish();

        // Then.
        assertThat(outContent.toString()).isEqualTo(setupActionExpectedMessageConsole);

        // Adds the original out at the end of test.
        System.setOut(originalOut);
    }

    private List<Team> getTeamsByName(final Datasets.TeamNames name, final List<Team> teams) {
        return teams.stream()
                .filter(team -> name.toString().equals(team.getName()))
                .collect(toList());
    }

    private boolean isNotBarcelona(final Team team) {
        return !Datasets.TeamNames.BARCELONA.toString().equals(team.getName());
    }

    private boolean simulateFilterErrorPsgTeam(final Team team) {
        applyCheckOnTeam(team, t -> !Datasets.TeamNames.PSG.toString().equals(t.getName()), Datasets.NO_AUTH_SCORE_PSG_EXCEPTION);

        return true;
    }

    private List<Datasets.Player> simulateFlatMapErrorPsgTeam(final Team team) {
        applyCheckOnTeam(team, t -> !Datasets.TeamNames.PSG.toString().equals(t.getName()), Datasets.NO_AUTH_SCORE_PSG_EXCEPTION);

        return team.getPlayers();
    }

    private Team toTeamWithScore(final Team team) {
        applyCheckOnTeam(team, t -> !Datasets.TeamNames.PSG.toString().equals(t.getName()), Datasets.NO_AUTH_SCORE_PSG_EXCEPTION);

        return team
                .toBuilder()
                .score(5)
                .build();
    }

    private Team toTeamWithNickName(final Team team) {
        applyCheckOnTeam(team, t -> !Datasets.TeamNames.BAYERN.toString().equals(t.getName()), Datasets.BAYERN_NOT_HAVE_NICKNAME_EXCEPTION);

        return team
                .toBuilder()
                .nickName("Nick name " + team.getName())
                .build();
    }

    private Team toTeamWithProfit(final ProcessContext context) {
        final Team team = (Team) context.element();
        applyCheckOnTeam(team, t -> !Datasets.TeamNames.JUVENTUS.toString().equals(t.getName()), Datasets.NO_AUTH_PROFIT_JUVE_EXCEPTION);

        return team
                .toBuilder()
                .profit(10)
                .build();
    }

    private Void assertFailures(final Iterable<Failure> failures,
                                final List<Failure> expectedFailures) {
        final List<String> resultFailures = StreamSupport.stream(failures.spliterator(), false)
                .map(Failure::toString)
                .collect(toList());

        final List<String> expectedFailuresString = expectedFailures.stream()
                .map(Failure::toString)
                .collect(toList());

        assertThat(failures).isNotNull().isNotEmpty();
        assertThat(failures).hasSize(expectedFailures.size());
        assertThat(resultFailures).containsExactlyInAnyOrderElementsOf(expectedFailuresString);

        return null;
    }

    private void applyCheckOnTeam(final Team team,
                                  final Predicate<Team> predicateOnTeam,
                                  final Supplier<RuntimeException> eventualException) {

        final Predicate<Team> isCheckError = Team::isCheckError;
        final Predicate<Team> noError = isCheckError.negate().or(predicateOnTeam);

        Optional.of(team)
                .filter(noError)
                .orElseThrow(eventualException);
    }
}