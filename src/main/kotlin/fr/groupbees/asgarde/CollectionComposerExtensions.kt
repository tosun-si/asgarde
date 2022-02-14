package fr.groupbees.asgarde

import fr.groupbees.asgarde.transforms.FilterFn
import fr.groupbees.asgarde.transforms.MapElementFn
import fr.groupbees.asgarde.transforms.MapProcessContextFn
import fr.groupbees.asgarde.transforms.SerializableAction
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.transforms.WithFailures.ExceptionElement
import org.apache.beam.sdk.values.PCollectionView
import org.apache.beam.sdk.values.TypeDescriptor
import java.io.Serializable

/**
 * Extension for [MapElements] class with all parameters.
 *
 * ```kotlin
 * CollectionComposer.of(teams)
 *     .map("Step name") { team -> TestSettings.toOtherTeam(team) }
 *     .result
 * ```
 *
 * @param name pipeline step name
 * @param transform current transformation function
 * @return current [CollectionComposer] class with result output and failures
 */
inline fun <I, reified O : Serializable> CollectionComposer<I>.map(
    name: String = "map to ${O::class.simpleName}",
    transform: SerializableFunction<I, O>
): CollectionComposer<O> {
    return this.apply(name, MapElements.into(TypeDescriptor.of(O::class.java)).via(transform))
}

/**
 * Extension for [FlatMapElements] class with all parameters.
 *
 * ```kotlin
 * CollectionComposer.of(teams)
 *     .flatMap("Step name") { team -> team.players }
 *     .result
 * ```
 *
 * @param name pipeline step name
 * @param transform current transformation function
 * @return current [CollectionComposer] class with result output and failures
 */
inline fun <I, reified O : Serializable> CollectionComposer<I>.flatMap(
    name: String = "flatMap to ${O::class.simpleName}",
    transform: SerializableFunction<I, Iterable<O>>
): CollectionComposer<O> {
    return this.apply(name, FlatMapElements.into(TypeDescriptor.of(O::class.java)).via(transform))
}

/**
 * Extension for [MapElements] class with all parameters and exception handler function.
 * The exception handler is a function an [ExceptionElement] with input element to [Failure] object.
 *
 * ```kotlin
 * CollectionComposer.of(teamCollection)
 *       .mapWithFailure(
 *            "Step name",
 *            { team -> toTeamWithPsgError(team) },
 *            { exElt -> Failure.from(TeamNames.PSG.name, exElt) }
 *       )
 *       .result
 * ```
 *
 * @param name pipeline step name
 * @param transform current transformation function
 * @return current [CollectionComposer] class with result output and failures
 */
inline fun <I, reified O : Serializable> CollectionComposer<I>.mapWithFailure(
    name: String = "map to ${O::class.simpleName}",
    transform: SerializableFunction<I, O>,
    exceptionHandler: ProcessFunction<ExceptionElement<I>, Failure>
): CollectionComposer<O> {
    return this.apply(
        name, MapElements
            .into(TypeDescriptor.of(O::class.java))
            .via(transform)
            .exceptionsInto(TypeDescriptor.of(Failure::class.java))
            .exceptionsVia(exceptionHandler)
    )
}

/**
 * Extension for [FlatMapElements] class with all parameters and exception handler function.
 * The exception handler is a function an [ExceptionElement] with input element to [Failure] object.
 *
 * ```kotlin
 *  CollectionComposer.of(teamCollection)
 *       .flatMapWithFailure(
 *           "Step name",
 *           { team -> team.players },
 *           { Failure.from(TeamNames.PSG.name, it) }
 *       )
 *       .result
 * ```
 *
 * @param name pipeline step name
 * @param transform current transformation function
 * @return current [CollectionComposer] class with result output and failures
 */
inline fun <I, reified O : Serializable> CollectionComposer<I>.flatMapWithFailure(
    name: String = "flatMap to ${O::class.simpleName}",
    transform: SerializableFunction<I, Iterable<O>>,
    exceptionHandler: ProcessFunction<ExceptionElement<I>, Failure>
): CollectionComposer<O> {
    return this.apply(
        name, FlatMapElements
            .into(TypeDescriptor.of(O::class.java))
            .via(transform)
            .exceptionsInto(TypeDescriptor.of(Failure::class.java))
            .exceptionsVia(exceptionHandler)
    )
}

/**
 * Extension for [MapElementFn] class with all parameters.
 *
 * ```kotlin
 *  CollectionComposer.of(teams)
 *      .mapFn(
 *          "Step name",
 *          { team -> TestSettings.toOtherTeam(team) },
 *          { print("Test setup action") }
 *      )
 *      .result
 * ```
 *
 * @param name step name
 * @param transform current transformation function
 * @param setupAction setup action function
 */
inline fun <I, reified O : Serializable> CollectionComposer<I>.mapFn(
    name: String = "map to ${O::class.simpleName}",
    transform: SerializableFunction<I, O>,
    setupAction: SerializableAction = SerializableAction { }
): CollectionComposer<O> {
    return this.apply(
        name, MapElementFn
            .into(TypeDescriptor.of(O::class.java))
            .via(transform)
            .withSetupAction(setupAction)
    )
}

/**
 * Extension for [MapProcessContextFn] class with all parameters.
 * Side inputs as [PCollectionView] can be passed to this DoFn.
 *
 * ```kotlin
 * CollectionComposer.of(teamCollection)
 *      .mapFnWithContext(
 *          "Step name",
 *          { context: DoFn<Team, OtherTeam>.ProcessContext -> toOtherTeamWithSideInputField(sideInput, context) },
 *          setupAction = { print("Test setup action") },
 *          sideInputs = listOf(sideInput)
 *      )
 *      .result
 * ```
 *
 * @param name pipeline step name
 * @param transform current transformation function
 * @param setupAction setup action function
 * @param sideInputs side inputs associated to this DoFn class
 * @return current [CollectionComposer] class with result output and failures
 */
inline fun <reified I, reified O : Serializable> CollectionComposer<I>.mapFnWithContext(
    name: String = "map to ${O::class.simpleName}",
    transform: SerializableFunction<DoFn<I, O>.ProcessContext, O>,
    setupAction: SerializableAction = SerializableAction { },
    sideInputs: Iterable<PCollectionView<*>> = emptyList()
): CollectionComposer<O> {
    return this.apply(
        name, MapProcessContextFn
            .from(I::class.java)
            .into(TypeDescriptor.of(O::class.java))
            .via(transform)
            .withSetupAction(setupAction),
        sideInputs
    )
}

/**
 * Extension for [FilterFn] class with all parameters.
 *
 * ```kotlin
 * CollectionComposer.of(teams)
 *      .filter("Step name") { team: Team -> this.isNotBarcelona(team) }
 *      .result
 * ```
 *
 * @param name pipeline step name
 * @param transform current transformation function
 * @return current [CollectionComposer] class with result output and failures
 */
inline fun <reified I> CollectionComposer<I>.filter(
    name: String = "filter to ${I::class.simpleName}",
    transform: SerializableFunction<I, Boolean>
): CollectionComposer<I> {
    return this.apply(name, FilterFn.by(transform))
}
