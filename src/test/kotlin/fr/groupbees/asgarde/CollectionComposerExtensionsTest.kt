package fr.groupbees.asgarde

import fr.groupbees.asgarde.settings.*
import fr.groupbees.asgarde.settings.Datasets.*
import fr.groupbees.asgarde.settings.TestSettings.assertOtherTeamWithSideInputField
import fr.groupbees.asgarde.settings.TestSettings.toOtherTeamWithSideInputField
import junitparams.JUnitParamsRunner
import junitparams.Parameters
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.coders.SerializableCoder
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.testing.TestPipeline
import org.apache.beam.sdk.testing.ValidatesRunner
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.transforms.WithFailures.ExceptionElement
import org.apache.beam.sdk.transforms.WithFailures.Result
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionView
import org.apache.beam.sdk.values.TypeDescriptor
import org.assertj.core.api.Assertions.assertThat
import org.junit.Rule
import org.junit.Test
import org.junit.experimental.categories.Category
import org.junit.runner.RunWith
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.io.Serializable
import java.util.*
import java.util.function.Predicate
import java.util.function.Supplier
import java.util.stream.Collectors.toList
import java.util.stream.StreamSupport


@RunWith(JUnitParamsRunner::class)
class CollectionComposerExtensionsTest : Serializable {

    @Transient
    private val pipeline: TestPipeline = TestPipeline.create()

    @Rule
    fun pipeline(): TestPipeline = pipeline

    /**
     * Contains all the params to test the result with all the MapElement functions without error.
     */
    fun resultCorrectMapElementsParams(): Array<Any> {
        val resultMapElements = { teams: PCollection<Team> ->
            CollectionComposer.of(teams)
                .mapWithFailure(MAP_TO_OTHER_TEAM,
                    { team -> TestSettings.toOtherTeam(team) },
                    { exElt -> Failure.from(MAP_TO_OTHER_TEAM, exElt) }
                )
                .result
        }

        val resultMapElementsInternalErrorHandling = { teams: PCollection<Team> ->
            CollectionComposer.of(teams)
                .map(MAP_TO_OTHER_TEAM) { team -> TestSettings.toOtherTeam(team) }
                .result
        }
        val resultSeparateTransform = { teams: PCollection<Team> ->
            CollectionComposer.of(teams)
                .apply(MAP_TO_OTHER_TEAM, MapElementNoErrorTransform())
                .result
        }

        val resultSeparateDoFn = { teams: PCollection<Team> ->
            CollectionComposer.of(teams)
                .apply(MAP_TO_OTHER_TEAM, CustomDoFnNoError())
                .result
        }

        val resultMapElementFn = { teams: PCollection<Team> ->
            CollectionComposer.of(teams)
                .mapFn(MAP_TO_OTHER_TEAM, { team -> TestSettings.toOtherTeam(team) })
                .result
        }

        val resultMapProcessElementFn = { teams: PCollection<Team> ->
            CollectionComposer.of(teams)
                .mapFnWithContext(
                    CONTEXT_TO_OTHER_TEAM,
                    { c: DoFn<Team, OtherTeam>.ProcessContext -> TestSettings.toOtherTeam(c.element()) })
                .result
        }

        return arrayOf(
            arrayOf(resultMapElements),
            arrayOf(resultMapElementsInternalErrorHandling),
            arrayOf(resultSeparateTransform),
            arrayOf(resultSeparateDoFn),
            arrayOf(resultMapElementFn),
            arrayOf(resultMapProcessElementFn)
        )
    }

    /**
     * Contains all the params to test the result with all the MapElement functions with error.
     */
    fun resultErrorMapElementsParams(): Array<Any> {
        val resultMapElements = { teams: PCollection<Team> ->
            CollectionComposer.of(teams)
                .mapWithFailure(MAP_TO_OTHER_TEAM,
                    { team -> TestSettings.toOtherTeamWithException(team) },
                    { exElt -> Failure.from(MAP_TO_OTHER_TEAM, exElt) }
                )
                .result
        }

        val resultMapElementsInternalErrorHandling = { teams: PCollection<Team> ->
            CollectionComposer.of(teams)
                .map(MAP_TO_OTHER_TEAM) { team -> TestSettings.toOtherTeamWithException(team) }
                .result
        }

        val resultMapElementFn = { teams: PCollection<Team> ->
            CollectionComposer.of(teams)
                .mapFn(MAP_TO_OTHER_TEAM, { team -> TestSettings.toOtherTeamWithException(team) })
                .result
        }

        val resultMapProcessElementFn = { teams: PCollection<Team> ->
            CollectionComposer.of(teams)
                .mapFnWithContext(
                    CONTEXT_TO_OTHER_TEAM,
                    { c: DoFn<Team, OtherTeam>.ProcessContext -> TestSettings.toOtherTeamWithException(c.element()) }
                )
                .result
        }
        return arrayOf(
            arrayOf(resultMapElements),
            arrayOf(resultMapElementsInternalErrorHandling),
            arrayOf(resultMapElementFn),
            arrayOf(resultMapProcessElementFn)
        )
    }

    /**
     * Contains all the params to test the result with all the FlaMapElement functions without error.
     */
    fun resultCorrectFlatMapElementsParams(): Array<Any> {
        val resultFlatMapElements = { teams: PCollection<Team> ->
            CollectionComposer.of(teams)
                .flatMapWithFailure(FLAT_MAP_TO_PLAYER,
                    { obj: Team -> obj.players },
                    { exElt: ExceptionElement<Team> -> Failure.from(FLAT_MAP_TO_PLAYER, exElt) }
                )
                .result
        }

        val resultFlatMapElementsInternalErrorHandling = { teams: PCollection<Team> ->
            CollectionComposer.of(teams)
                .flatMap(FLAT_MAP_TO_PLAYER) { it.players }
                .result
        }

        val resultFlatMapElementsFn = { teams: PCollection<Team> ->
            CollectionComposer.of(teams)
                .flatMapFn(FLAT_MAP_TO_PLAYER, { it.players })
                .result
        }

        val resultFlatMapProcessElementFn = { teams: PCollection<Team> ->
            CollectionComposer.of(teams)
                .flatMapFnWithContext<Team, Player>(FLAT_MAP_TO_PLAYER, { it.element().players })
                .result
        }

        return arrayOf(
            arrayOf(resultFlatMapElements),
            arrayOf(resultFlatMapElementsInternalErrorHandling),
            arrayOf(resultFlatMapElementsFn),
            arrayOf(resultFlatMapProcessElementFn)
        )
    }

    /**
     * Contains all the params to test the result with all the FlaMapElement functions without error.
     */
    fun resultErrorFlatMapElementsParams(): Array<Any> {
        val resultFlatMapElements = { teams: PCollection<Team> ->
            CollectionComposer.of(teams)
                .flatMapWithFailure(FLAT_MAP_TO_PLAYER,
                    { team: Team -> TestSettings.toPlayersWithException(team) },
                    { exElt: ExceptionElement<Team> -> Failure.from(FLAT_MAP_TO_PLAYER, exElt) }
                )
                .result
        }

        val resultFlatMapElementsInternalErrorHandling = { teams: PCollection<Team> ->
            CollectionComposer.of(teams)
                .flatMap(FLAT_MAP_TO_PLAYER) { TestSettings.toPlayersWithException(it) }
                .result
        }

        val resultFlatMapElementsFn = { teams: PCollection<Team> ->
            CollectionComposer.of(teams)
                .flatMapFn(FLAT_MAP_TO_PLAYER, { TestSettings.toPlayersWithException(it) })
                .result
        }

        val resultFlatMapProcessContextFn = { teams: PCollection<Team> ->
            CollectionComposer.of(teams)
                .flatMapFnWithContext<Team, Player>(
                    FLAT_MAP_TO_PLAYER,
                    { TestSettings.toPlayersWithException(it.element()) })
                .result
        }

        return arrayOf(
            arrayOf(resultFlatMapElements),
            arrayOf(resultFlatMapElementsInternalErrorHandling),
            arrayOf(resultFlatMapElementsFn),
            arrayOf(resultFlatMapProcessContextFn)
        )
    }

    /**
     * Contains all the params for the custom DoFn ([BaseElementFn]),
     * in order to test the output coders and type descriptors.
     */
    fun resultBaseElementFnCodersAndTypeDescriptorsParams(): Array<Any> {
        val resultSeparateTransform = { teams: PCollection<Team> ->
            CollectionComposer.of(teams)
                .apply(MAP_TO_OTHER_TEAM, MapElementNoErrorTransform())
                .result
        }

        val resultSeparateDoFn = { teams: PCollection<Team> ->
            CollectionComposer.of(teams)
                .apply(MAP_TO_OTHER_TEAM, CustomDoFnNoError())
                .result
        }

        val resultMapElementFn = { teams: PCollection<Team> ->
            CollectionComposer.of(teams)
                .mapFn(MAP_TO_OTHER_TEAM, { team -> TestSettings.toOtherTeam(team) })
                .result
        }

        val resultMapProcessElementFn = { teams: PCollection<Team> ->
            CollectionComposer.of(teams)
                .mapFnWithContext(CONTEXT_TO_OTHER_TEAM,
                    { context: DoFn<Team, OtherTeam>.ProcessContext -> TestSettings.toOtherTeam(context.element()) }
                )
                .result
        }
        val resultFilterFn = { teams: PCollection<Team> ->
            CollectionComposer.of(teams)
                .filter(MAP_TO_OTHER_TEAM) { team: Team -> this.isNotBarcelona(team) }
                .result
        }

        return arrayOf(
            arrayOf(
                resultSeparateTransform,
                SerializableCoder.of(OtherTeam::class.java),
                TypeDescriptor.of(OtherTeam::class.java)
            ), arrayOf(
                resultSeparateDoFn,
                SerializableCoder.of(OtherTeam::class.java),
                TypeDescriptor.of(OtherTeam::class.java)
            ), arrayOf(
                resultMapElementFn,
                SerializableCoder.of(OtherTeam::class.java),
                TypeDescriptor.of(OtherTeam::class.java)
            ), arrayOf(
                resultMapProcessElementFn,
                SerializableCoder.of(OtherTeam::class.java),
                TypeDescriptor.of(OtherTeam::class.java)
            ), arrayOf(
                resultFilterFn,
                SerializableCoder.of(Team::class.java),
                TypeDescriptor.of(Team::class.java)
            )
        )
    }

    fun resultOperationsWithSetupActionParams(): Array<Any> {
        val consoleMessageMapElementFn = "Test start action MapElementFn"
        val resultMapElementFn = { teams: PCollection<Team> ->
            CollectionComposer.of(teams)
                .mapFn(MAP_TO_OTHER_TEAM,
                    { team -> TestSettings.toOtherTeam(team) },
                    setupAction = { print(consoleMessageMapElementFn) }
                )
                .result
        }

        val consoleMessageMapProcessElementFn = "Test start action MapProcessContextFn"
        val resultMapProcessElementFn = { teams: PCollection<Team> ->
            CollectionComposer.of(teams)
                .mapFnWithContext<Team, OtherTeam>(MAP_TO_OTHER_TEAM,
                    { TestSettings.toOtherTeam(it.element()) },
                    setupAction = { print(consoleMessageMapProcessElementFn) }
                )
                .result
        }

        val consoleMessageFlatMapElementFn = "Test start action FlatMapElementFn"
        val resultFlatMapElementFn = { teams: PCollection<Team> ->
            CollectionComposer.of(teams)
                .flatMapFn(FLAT_MAP_TO_PLAYER,
                    { it.players },
                    setupAction = { print(consoleMessageFlatMapElementFn) }
                )
                .result
        }

        val consoleMessageFlatMapProcessElementFn = "Test start action FlatMapProcessContextFn"
        val resultFlatMapProcessElementFn = { teams: PCollection<Team> ->
            CollectionComposer.of(teams)
                .flatMapFnWithContext<Team, Player>(FLAT_MAP_TO_PLAYER,
                    { it.element().players },
                    setupAction = { print(consoleMessageFlatMapProcessElementFn) }
                )
                .result
        }

        return arrayOf(
            arrayOf(
                resultMapElementFn,
                consoleMessageMapElementFn
            ), arrayOf(
                resultMapProcessElementFn,
                consoleMessageMapProcessElementFn
            ), arrayOf(
                resultFlatMapElementFn,
                consoleMessageFlatMapElementFn
            ), arrayOf(
                resultFlatMapProcessElementFn,
                consoleMessageFlatMapProcessElementFn
            )
        )
    }

    fun resultOperationsWithStartBundleActionParams(): Array<Any> {
        val consoleMessageMapElementFn = "Test start bundle action MapElementFn"
        val resultMapElementFn = { teams: PCollection<Team> ->
            CollectionComposer.of(teams)
                .mapFn(MAP_TO_OTHER_TEAM,
                    { team -> TestSettings.toOtherTeam(team) },
                    startBundleAction = { print(consoleMessageMapElementFn) }
                )
                .result
        }

        val consoleMessageMapProcessElementFn = "Test start bundle action MapProcessContextFn"
        val resultMapProcessElementFn = { teams: PCollection<Team> ->
            CollectionComposer.of(teams)
                .mapFnWithContext<Team, OtherTeam>(MAP_TO_OTHER_TEAM,
                    { TestSettings.toOtherTeam(it.element()) },
                    startBundleAction = { print(consoleMessageMapProcessElementFn) }
                )
                .result
        }

        val consoleMessageFlatMapElementFn = "Test start bundle action FlatMapElementFn"
        val resultFlatMapElementFn = { teams: PCollection<Team> ->
            CollectionComposer.of(teams)
                .flatMapFn(FLAT_MAP_TO_PLAYER,
                    { it.players },
                    startBundleAction = { print(consoleMessageFlatMapElementFn) }
                )
                .result
        }

        val consoleMessageFlatMapProcessElementFn = "Test start bundle action FlatMapProcessContextFn"
        val resultFlatMapProcessElementFn = { teams: PCollection<Team> ->
            CollectionComposer.of(teams)
                .flatMapFnWithContext<Team, Player>(FLAT_MAP_TO_PLAYER,
                    { it.element().players },
                    startBundleAction = { print(consoleMessageFlatMapProcessElementFn) }
                )
                .result
        }

        return arrayOf(
            arrayOf(
                resultMapElementFn,
                consoleMessageMapElementFn
            ), arrayOf(
                resultMapProcessElementFn,
                consoleMessageMapProcessElementFn
            ), arrayOf(
                resultFlatMapElementFn,
                consoleMessageFlatMapElementFn
            ), arrayOf(
                resultFlatMapProcessElementFn,
                consoleMessageFlatMapProcessElementFn
            )
        )
    }

    fun resultOperationsWithFinishBundleActionParams(): Array<Any> {
        val consoleMessageMapElementFn = "Test finish bundle action MapElementFn"
        val resultMapElementFn = { teams: PCollection<Team> ->
            CollectionComposer.of(teams)
                .mapFn(MAP_TO_OTHER_TEAM,
                    { team -> TestSettings.toOtherTeam(team) },
                    finishBundleAction = { print(consoleMessageMapElementFn) }
                )
                .result
        }

        val consoleMessageMapProcessElementFn = "Test finish bundle action MapProcessContextFn"
        val resultMapProcessElementFn = { teams: PCollection<Team> ->
            CollectionComposer.of(teams)
                .mapFnWithContext<Team, OtherTeam>(MAP_TO_OTHER_TEAM,
                    { TestSettings.toOtherTeam(it.element()) },
                    finishBundleAction = { print(consoleMessageMapProcessElementFn) }
                )
                .result
        }

        val consoleMessageFlatMapElementFn = "Test finish bundle action FlatMapElementFn"
        val resultFlatMapElementFn = { teams: PCollection<Team> ->
            CollectionComposer.of(teams)
                .flatMapFn(FLAT_MAP_TO_PLAYER,
                    { it.players },
                    finishBundleAction = { print(consoleMessageFlatMapElementFn) }
                )
                .result
        }

        val consoleMessageFlatMapProcessElementFn = "Test finish bundle action FlatMapProcessContextFn"
        val resultFlatMapProcessElementFn = { teams: PCollection<Team> ->
            CollectionComposer.of(teams)
                .flatMapFnWithContext<Team, Player>(FLAT_MAP_TO_PLAYER,
                    { it.element().players },
                    finishBundleAction = { print(consoleMessageFlatMapProcessElementFn) }
                )
                .result
        }

        return arrayOf(
            arrayOf(
                resultMapElementFn,
                consoleMessageMapElementFn
            ), arrayOf(
                resultMapProcessElementFn,
                consoleMessageMapProcessElementFn
            ), arrayOf(
                resultFlatMapElementFn,
                consoleMessageFlatMapElementFn
            ), arrayOf(
                resultFlatMapProcessElementFn,
                consoleMessageFlatMapProcessElementFn
            )
        )
    }

    fun resultOperationsWithTeardownActionParams(): Array<Any> {
        val consoleMessageMapElementFn = "Test teardown action MapElementFn"
        val resultMapElementFn = { teams: PCollection<Team> ->
            CollectionComposer.of(teams)
                .mapFn(MAP_TO_OTHER_TEAM,
                    { team -> TestSettings.toOtherTeam(team) },
                    teardownAction = { print(consoleMessageMapElementFn) }
                )
                .result
        }

        val consoleMessageMapProcessElementFn = "Test teardown action MapProcessContextFn"
        val resultMapProcessElementFn = { teams: PCollection<Team> ->
            CollectionComposer.of(teams)
                .mapFnWithContext<Team, OtherTeam>(MAP_TO_OTHER_TEAM,
                    { TestSettings.toOtherTeam(it.element()) },
                    teardownAction = { print(consoleMessageMapProcessElementFn) }
                )
                .result
        }

        val consoleMessageFlatMapElementFn = "Test teardown action FlatMapElementFn"
        val resultFlatMapElementFn = { teams: PCollection<Team> ->
            CollectionComposer.of(teams)
                .flatMapFn(FLAT_MAP_TO_PLAYER,
                    { it.players },
                    teardownAction = { print(consoleMessageFlatMapElementFn) }
                )
                .result
        }

        val consoleMessageFlatMapProcessElementFn = "Test teardown action FlatMapProcessContextFn"
        val resultFlatMapProcessElementFn = { teams: PCollection<Team> ->
            CollectionComposer.of(teams)
                .flatMapFnWithContext<Team, Player>(FLAT_MAP_TO_PLAYER,
                    { it.element().players },
                    teardownAction = { print(consoleMessageFlatMapProcessElementFn) }
                )
                .result
        }

        return arrayOf(
            arrayOf(
                resultMapElementFn,
                consoleMessageMapElementFn
            ), arrayOf(
                resultMapProcessElementFn,
                consoleMessageMapProcessElementFn
            ), arrayOf(
                resultFlatMapElementFn,
                consoleMessageFlatMapElementFn
            ), arrayOf(
                resultFlatMapProcessElementFn,
                consoleMessageFlatMapProcessElementFn
            )
        )
    }

    @Test
    @Category(ValidatesRunner::class)
    @Parameters(method = "resultCorrectMapElementsParams")
    fun givenOneTeam_whenApplyComposerWithMapElementToOtherTeamObjectWithoutError_thenOneOtherTeamOutputAndEmptyFailure(
        resultFunction: (PCollection<Team>) -> Result<PCollection<OtherTeam>, Failure>
    ) {

        // Given.
        val psgTeam = getTeamsByName(TeamNames.PSG, INPUT_TEAMS_NO_FAILURE)
        val teamCollection = pipeline.apply("Reads people", Create.of(psgTeam))

        // When.
        val result = resultFunction(teamCollection)

        // Then.
        val failures = result.failures()
        PAssert.that(failures).empty()

        val output = result.output()
        PAssert.that(output).satisfies { otherTeams: Iterable<OtherTeam> -> TestSettings.assertOtherTeam(otherTeams) }

        pipeline.run().waitUntilFinish()
    }

    @Test
    @Category(ValidatesRunner::class)
    @Parameters(method = "resultErrorMapElementsParams")
    fun givenOneTeam_whenApplyComposerWithMapElementToOtherTeamObjectWithError_thenEmptyOtherTeamOutputAndOneFailure(
        resultFunction: (PCollection<Team>) -> Result<PCollection<OtherTeam>, Failure>
    ) {

        // Given.
        val psgTeam = getTeamsByName(TeamNames.PSG, INPUT_TEAMS_NO_FAILURE)
        val teamCollection = pipeline.apply("Reads people", Create.of(psgTeam))

        // When.
        val result = resultFunction(teamCollection)

        // Then.
        PAssert.that(result.failures())
            .satisfies { failures ->
                TestSettings.assertFailuresFromInputTeam(
                    failures,
                    IllegalArgumentException::class.java, TestSettings.ERROR_OTHER_TEAM
                )
            }

        val output = result.output()
        PAssert.that(output).empty()
        pipeline.run().waitUntilFinish()
    }

    @Test
    @Category(ValidatesRunner::class)
    @Parameters(method = "resultCorrectFlatMapElementsParams")
    fun givenOneTeam_whenApplyComposerWithOneFlatMapWithoutError_thenExpectedOutputPlayersAndNoFailure(
        resultFunction: (PCollection<Team>) -> Result<PCollection<Player>, Failure>
    ) {
        // Given.
        val psgTeam = getTeamsByName(TeamNames.PSG, INPUT_TEAMS_NO_FAILURE)
        val teamCollection = pipeline.apply("Reads people", Create.of(psgTeam))

        // When.
        val result = resultFunction(teamCollection)

        // Then.
        val expectedPlayers = psgTeam.stream()
            .map { it.players }
            .flatMap { it.stream() }
            .collect(toList())

        val failures = result.failures()
        PAssert.that(failures).empty()

        val output = result.output()
        PAssert.that(output).containsInAnyOrder(expectedPlayers)
        pipeline.run().waitUntilFinish()
    }

    @Test
    @Category(ValidatesRunner::class)
    @Parameters(method = "resultErrorFlatMapElementsParams")
    fun givenOneTeam_whenApplyComposerWithFlatMapElementWithError_thenEmptyPlayersOutputAndOneFailure(
        resultFunction: (PCollection<Team>) -> Result<PCollection<Player>, Failure>
    ) {

        // Given.
        val psgTeam = getTeamsByName(TeamNames.PSG, INPUT_TEAMS_NO_FAILURE)
        val teamCollection = pipeline.apply("Reads people", Create.of(psgTeam))

        // When.
        val result = resultFunction(teamCollection)

        // Then.
        PAssert.that(result.failures())
            .satisfies { failures: Iterable<Failure> ->
                TestSettings.assertFailuresFromInputTeam(
                    failures,
                    IllegalStateException::class.java, TestSettings.ERROR_PLAYERS
                )
            }

        PAssert.that(result.output()).empty()
        pipeline.run().waitUntilFinish()
    }

    @Test
    @Category(ValidatesRunner::class)
    @Parameters(method = "resultBaseElementFnCodersAndTypeDescriptorsParams")
    fun <T> givenOneTeam_whenApplyComposerWithBaseElementFnOutputCoderAndTypeDescriptor_thenOutputHasExpectedCoderAndTypeDescriptor(
        resultFunction: (PCollection<Team>) -> Result<PCollection<T>, Failure>,
        outputCoder: Coder<T>,
        outputDescriptor: TypeDescriptor<T>
    ) {

        // Given.
        val psgTeam = getTeamsByName(TeamNames.PSG, INPUT_TEAMS_NO_FAILURE)
        val teamCollection = pipeline.apply("Reads people", Create.of(psgTeam))

        // When.
        val result = resultFunction(teamCollection)

        // Then.
        val failures = result.failures()
        PAssert.that(failures).empty()

        val output = result.output()
        assertThat(output.coder).isEqualTo(outputCoder)
        assertThat(output.typeDescriptor).isEqualTo(outputDescriptor)

        pipeline.run().waitUntilFinish()
    }

    @Test
    @Category(ValidatesRunner::class)
    fun givenOneTeams_whenApplyComposerWithMapProcessElementAndSideInput_thenOutputOtherTeamWithSideInputFieldAndNoFailure() {
        // Given.
        val psgTeam = getTeamsByName(TeamNames.PSG, INPUT_TEAMS_NO_FAILURE)
        val teamCollection = pipeline.apply("Reads people", Create.of(psgTeam))
        val sideInputFieldValue = "Side input test"
        val sideInput: PCollectionView<String> = pipeline
            .apply("String side input", Create.of(sideInputFieldValue))
            .apply("Create as collection view", View.asSingleton())

        // When.
        val result = CollectionComposer.of(teamCollection)
            .mapFnWithContext(
                CONTEXT_TO_OTHER_TEAM,
                { context: DoFn<Team, OtherTeam>.ProcessContext -> toOtherTeamWithSideInputField(sideInput, context) },
                sideInputs = listOf(sideInput)
            )
            .result

        val failures = result.failures()
        PAssert.that(failures).empty()

        val output = result.output()
        PAssert.that(output)
            .satisfies { otherTeams: Iterable<OtherTeam> ->
                assertOtherTeamWithSideInputField(
                    sideInputFieldValue,
                    otherTeams
                )
            }

        pipeline.run().waitUntilFinish()
    }

    @Test
    @Category(ValidatesRunner::class)
    fun givenFiveTeams_whenApplyComposerWithoutMapErrorAndWithoutFilter_thenAllTeamsInOutputAndNoFailure() {
        // Given.
        val teamCollection = pipeline.apply("Reads people", Create.of(INPUT_TEAMS_NO_FAILURE))

        // When.
        val result = CollectionComposer.of(teamCollection)
            .map(TeamNames.PSG.name) { toTeamWithPsgError(it) }
            .mapFnWithContext(
                TeamNames.JUVENTUS.name,
                { context: DoFn<Team, Team>.ProcessContext -> toTeamWithJuveError(context) }
            )
            .mapFn(TeamNames.BAYERN.name, { toTeamWithBayernError(it) })
            .result

        val failures = result.failures()
        PAssert.that(failures).empty()

        val expectedTeams = JsonUtil.deserializeFromResourcePath(
            "outputs/output-five-teams-no-error.json", TEAM_TYPE_REF
        )

        val output: PCollection<Team> = result.output()
        PAssert.that(output).containsInAnyOrder(expectedTeams)

        pipeline.run().waitUntilFinish()
    }

    @Test
    @Category(ValidatesRunner::class)
    fun givenFiveTeams_whenApplyComposerWithDifferentMapErrorsAndACorrectFilter_thenThreeFailuresAndOneSuccess() {
        // Given.
        val teamCollection = pipeline.apply("Reads people", Create.of(INPUT_TEAMS_WITH_THREE_FAILURES))

        // When.
        val result = CollectionComposer.of(teamCollection)
            .map(TeamNames.PSG.name) { toTeamWithPsgError(it) }
            .mapFnWithContext(
                TeamNames.JUVENTUS.name,
                { context: DoFn<Team, Team>.ProcessContext -> toTeamWithJuveError(context) }
            )
            .mapFn(TeamNames.BAYERN.name, { toTeamWithBayernError(it) })
            .filter(FILTER_TEAMS) { isNotBarcelona(it) }
            .result

        val failures = result.failures()
        PAssert.that(failures).satisfies { resultFailures: Iterable<Failure> ->
            assertFailures(resultFailures, EXPECTED_THREE_FAILURES)
        }

        val expectedTeams = JsonUtil.deserializeFromResourcePath(
            "outputs/output-five-teams-with-three-failures-and-one-good-output.json", TEAM_TYPE_REF
        )
        val output = result.output()
        PAssert.that(output).containsInAnyOrder(expectedTeams)
        pipeline.run().waitUntilFinish()
    }

    @Test
    @Category(ValidatesRunner::class)
    fun givenFiveTeams_whenApplyComposerWithSimulatingAllElementsWithError_thenAllResultInFailures() {
        // Given.
        val teamCollection = pipeline.apply("Reads people", Create.of(INPUT_TEAMS_WITH_ALL_FAILURES))

        // When.
        val result = CollectionComposer.of(teamCollection)
            .mapWithFailure(
                TeamNames.PSG.name,
                { toTeamWithPsgError(it) },
                { Failure.from(TeamNames.PSG.name, it) }
            )
            .mapFnWithContext(
                TeamNames.JUVENTUS.name,
                { context: DoFn<Team, Team>.ProcessContext -> toTeamWithJuveError(context) }
            )
            .map(TeamNames.BAYERN.name) { toTeamWithBayernError(it) }
            .filter(FILTER_TEAMS) { isNotBarcelona(it) }
            .result

        val failures = result.failures()
        PAssert.that(failures).satisfies { resultFailures: Iterable<Failure> ->
            assertFailures(resultFailures, EXPECTED_THREE_FAILURES)
        }

        val output = result.output()
        PAssert.that(output).empty()
        pipeline.run().waitUntilFinish()
    }

    @Test
    @Category(ValidatesRunner::class)
    fun givenFiveTeams_whenApplyComposerWithOneFilterError_thenEmptyCorrectOutputAndOneFailure() {
        // Given.
        val teamCollection = pipeline.apply("Reads people", Create.of(INPUT_TEAMS_WITH_ONE_FAILURE))

        // When.
        val result = CollectionComposer.of(teamCollection)
            .filter(TeamNames.PSG.name) { simulateFilterErrorPsgTeam(it) }
            .result

        val failures = result.failures()
        PAssert.that(failures).satisfies { resultFailures: Iterable<Failure> ->
            assertFailures(resultFailures, EXPECTED_ONE_FAILURES)
        }

        val output = result.output()
        PAssert.that(output).empty()
        pipeline.run().waitUntilFinish()
    }

    @Test
    @Category(ValidatesRunner::class)
    fun givenFiveTeams_whenApplyComposerWithOneFlatMapError_thenEmptyCorrectOutputAndOneFailure() {
        // Given.
        val teamCollection = pipeline.apply("Reads people", Create.of(INPUT_TEAMS_WITH_ONE_FAILURE))

        // When.
        val result = CollectionComposer.of(teamCollection)
            .flatMapWithFailure(
                TeamNames.PSG.name,
                { simulateFlatMapErrorPsgTeam(it) },
                { Failure.from(TeamNames.PSG.name, it) }
            )
            .result

        val failures = result.failures()
        PAssert.that(failures).satisfies { resultFailures: Iterable<Failure> ->
            assertFailures(resultFailures, EXPECTED_ONE_FAILURES)
        }

        val output = result.output()
        PAssert.that(output).empty()
        pipeline.run().waitUntilFinish()
    }

    @Test
    @Category(ValidatesRunner::class)
    fun givenOneTeam_whenApplyComposerWithOneFilterWithoutError_thenExpectedOutputTeamsAndNoFailure() {
        // Given.
        val psgTeam = getTeamsByName(TeamNames.PSG, INPUT_TEAMS_NO_FAILURE)
        val teamCollection = pipeline.apply("Reads people", Create.of(psgTeam))

        // When.
        val result = CollectionComposer.of(teamCollection)
            .filter(FILTER_TEAMS) { isNotBarcelona(it) }
            .result

        // Then.
        val failures = result.failures()
        PAssert.that(failures).empty()

        val output = result.output()
        PAssert.that(output).containsInAnyOrder(psgTeam)
        pipeline.run().waitUntilFinish()
    }

    @Test
    @Category(ValidatesRunner::class)
    @Parameters(method = "resultOperationsWithSetupActionParams")
    fun givenOneTeam_whenApplyComposerWithMapFnWithSetupAction_thenActionIsCorrectlyExecuted(
        resultFunction: (PCollection<Team>) -> Result<PCollection<OtherTeam>, Failure>,
        setupActionExpectedMessageConsole: String
    ) {
        testLifecycleActionInPipeline(resultFunction, setupActionExpectedMessageConsole)
    }

    @Test
    @Category(ValidatesRunner::class)
    @Parameters(method = "resultOperationsWithStartBundleActionParams")
    fun givenOneTeam_whenApplyComposerWithMapFnWithStartBundleAction_thenActionIsCorrectlyExecuted(
        resultFunction: (PCollection<Team>) -> Result<PCollection<OtherTeam>, Failure>,
        startBundleActionExpectedMessageConsole: String
    ) {
        testLifecycleActionInPipeline(resultFunction, startBundleActionExpectedMessageConsole)
    }

    @Test
    @Category(ValidatesRunner::class)
    @Parameters(method = "resultOperationsWithFinishBundleActionParams")
    fun givenOneTeam_whenApplyComposerWithMapFnWithFinishBundleAction_thenActionIsCorrectlyExecuted(
        resultFunction: (PCollection<Team>) -> Result<PCollection<OtherTeam>, Failure>,
        finishBundleActionExpectedMessageConsole: String
    ) {
        testLifecycleActionInPipeline(resultFunction, finishBundleActionExpectedMessageConsole)
    }

    @Test
    @Category(ValidatesRunner::class)
    @Parameters(method = "resultOperationsWithTeardownActionParams")
    fun givenOneTeam_whenApplyComposerWithMapFnWithTeardownAction_thenActionIsCorrectlyExecuted(
        resultFunction: (PCollection<Team>) -> Result<PCollection<OtherTeam>, Failure>,
        setupActionExpectedMessageConsole: String
    ) {
        testLifecycleActionInPipeline(resultFunction, setupActionExpectedMessageConsole)
    }

    private fun getTeamsByName(name: TeamNames, teams: List<Team>): List<Team> {
        return teams.stream()
            .filter { name.toString() == it.name }
            .collect(toList())
    }

    private fun isNotBarcelona(team: Team): Boolean {
        return TeamNames.BARCELONA.toString() != team.name
    }

    private fun simulateFilterErrorPsgTeam(team: Team): Boolean {
        applyCheckOnTeam(
            team = team,
            predicateOnTeam = { t -> TeamNames.PSG.toString() != t.name },
            eventualException = NO_AUTH_SCORE_PSG_EXCEPTION
        )
        return true
    }

    private fun simulateFlatMapErrorPsgTeam(team: Team): List<Player> {
        applyCheckOnTeam(
            team = team,
            predicateOnTeam = { t -> TeamNames.PSG.toString() != t.name },
            eventualException = NO_AUTH_SCORE_PSG_EXCEPTION
        )
        return team.players
    }

    private fun toTeamWithPsgError(team: Team): Team {
        applyCheckOnTeam(
            team = team,
            predicateOnTeam = { t -> TeamNames.PSG.toString() != t.name },
            eventualException = NO_AUTH_SCORE_PSG_EXCEPTION
        )

        val copiedTeam = team.copy()
        copiedTeam.score = 5

        return copiedTeam
    }

    private fun toTeamWithBayernError(team: Team): Team {
        applyCheckOnTeam(
            team = team,
            predicateOnTeam = { t: Team -> TeamNames.BAYERN.toString() != t.name },
            eventualException = BAYERN_NOT_HAVE_NICKNAME_EXCEPTION
        )

        val copiedTeam = team.copy()
        copiedTeam.nickName = "Nick name " + team.name

        return copiedTeam
    }

    private fun toTeamWithJuveError(context: DoFn<Team, Team>.ProcessContext): Team {
        val team = context.element() as Team

        applyCheckOnTeam(
            team = team,
            predicateOnTeam = { t -> TeamNames.JUVENTUS.toString() != t.name }, NO_AUTH_PROFIT_JUVE_EXCEPTION
        )

        val copiedTeam = team.copy()
        copiedTeam.profit = 10

        return copiedTeam
    }

    private fun assertFailures(failures: Iterable<Failure>, expectedFailures: List<Failure>): Void? {
        val resultFailures = StreamSupport.stream(failures.spliterator(), false)
            .map { it.toString() }
            .collect(toList())

        val expectedFailuresString = expectedFailures.stream()
            .map { it.toString() }
            .collect(toList())

        assertThat(failures).isNotNull.isNotEmpty
        assertThat(failures).hasSize(expectedFailures.size)
        assertThat(resultFailures).containsExactlyInAnyOrderElementsOf(expectedFailuresString)

        return null
    }

    private fun applyCheckOnTeam(
        team: Team,
        predicateOnTeam: Predicate<Team>,
        eventualException: Supplier<RuntimeException>
    ) {
        val isCheckError = Predicate { obj: Team -> obj.isCheckError }
        val noError = isCheckError.negate().or(predicateOnTeam)

        Optional.of(team)
            .filter(noError)
            .orElseThrow(eventualException)
    }

    private fun testLifecycleActionInPipeline(
        resultFunction: (PCollection<Team>) -> Result<PCollection<OtherTeam>, Failure>,
        actionExpectedMessageConsole: String
    ) {

        // Allows testing side effect.
        // We perform a System.out.print and checks if the message has been correctly printed in the console.
        val outContent = ByteArrayOutputStream()
        val originalOut = System.out
        System.setOut(PrintStream(outContent))

        // Given.
        val psgTeam = getTeamsByName(TeamNames.PSG, INPUT_TEAMS_NO_FAILURE)
        val teamCollection = pipeline.apply("Reads people", Create.of(psgTeam))

        // When.
        resultFunction(teamCollection)
        pipeline.run().waitUntilFinish()

        // Then.
        assertThat(outContent.toString()).isEqualTo(actionExpectedMessageConsole)

        // Adds the original out at the end of test.
        System.setOut(originalOut)
    }

    companion object {
        private const val MAP_TO_OTHER_TEAM = "Map to other team"
        private const val FLAT_MAP_TO_PLAYER = "Flat map to player"
        private const val CONTEXT_TO_OTHER_TEAM = "Process context to other team"
        private const val FILTER_TEAMS = "Filter teams"
    }
}