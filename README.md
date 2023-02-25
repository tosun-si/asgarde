![Logo](asgarde_logo_small.gif) 

# Asgarde

This module allows simplifying error handling with Apache Beam Java.

## Versions compatibility between Beam and Asgarde

| Asgarde | Beam   |
|---------|--------|
| 0.10.0  | 2.31.0 |
| 0.11.0  | 2.32.0 |
| 0.12.0  | 2.33.0 |
| 0.13.0  | 2.34.0 |
| 0.14.0  | 2.35.0 |
| 0.15.0  | 2.36.0 |
| 0.16.0  | 2.37.0 |
| 0.17.0  | 2.38.0 |
| 0.18.0  | 2.39.0 |
| 0.19.0  | 2.40.0 |
| 0.20.0  | 2.41.0 |
| 0.21.0  | 2.42.0 |
| 0.22.0  | 2.43.0 |
| 0.23.0  | 2.44.0 |
| 0.24.0  | 2.45.0 |

## Installation of project

The project is hosted on Maven repository.\
You can install it with all the build tools compatibles with Maven.

Example with Maven and Gradle :

#### Maven

```xml
<dependency>
    <groupId>fr.groupbees</groupId>
    <artifactId>asgarde</artifactId>
    <version>0.23.0</version>
</dependency>
```

#### Gradle

```text
implementation group: 'fr.groupbees', name: 'asgarde', version: '0.23.0'
```

## Error logic with Beam ParDo and DoFn

Beam recommends treating errors with Dead letters.\
It means catching errors in the flow and, using side outputs, sinking errors to a file, database or any other output...

Beam suggests handling side outputs with `TupleTags` in a `DoFn` class, example :

```java
// Failure object.
public class Failure implements Serializable {
    private final String pipelineStep;
    private final Integer inputElement;
    private final Throwable exception;

    public static <T> Failure from(final String pipelineStep,
                                   final T element,
                                   final Throwable exception) {
        return new Failure(pipelineStep, element.toString(), exception);
    }
}

// Word count DoFn class.
public class WordCountFn extends DoFn<String, Integer> {

     private final TupleTag<Integer> outputTag = new TupleTag<Integer>() {};
     private final TupleTag<Failure> failuresTag = new TupleTag<Failure>() {};
    
     @ProcessElement
     public void processElement(ProcessContext ctx) {
        try {
            // Could throw ArithmeticException.
            final String word = ctx.element();
            ctx.output(1 / word.length());
        } catch (Throwable throwable) {
            final Failure failure = Failure.from("step", ctx.element(), throwable);
            ctx.output(failuresTag, failure);
        }
    }
    
    public TupleTag<Integer> getOutputTag() {
       return outputTag;
    }
    
    public TupleTag<Failure> getFailuresTag() {
       return failuresTag;
    }
}
```

```java
// In Beam pipeline flow.
final PCollection<String> wordPCollection....

final WordCountFn wordCountFn = new WordCountFn();

final PCollectionTuple tuple = wordPCollection
           .apply("ParDo", ParDo.of(wordCountFn).withOutputTags(wordCountFn.getOutputTag(), TupleTagList.of(wordCountFn.getFailuresTag())));

// Output PCollection via outputTag.
PCollection<Integer> outputCollection = tuple.get(wordCountFn.getOutputTag());

// Failures PCollection via failuresTag.
PCollection<Failure> failuresCollection = tuple.get(wordCountFn.getFailuresTag());
```

With this approach we can, in all steps, get the output and failures result PCollections. 

## Error logic with Beam MapElements and FlatMapElements

Beam also allows handling errors with built-in components like `MapElements` and `FlatMapElements` (it's currently an experimental feature as of april of 2020).

Behind the scene, in these classes Beam use the same concept explained above.

Example: 

```java
public class Failure implements Serializable {
    private final String pipelineStep;
    private final String inputElement;
    private final Throwable exception;

    public static <T> Failure from(final String pipelineStep,
                                   final WithFailures.ExceptionElement<T> exceptionElement) {
        final T inputElement = exceptionElement.element();
        return new Failure(pipelineStep, inputElement.toString(), exceptionElement.exception());
    }
}

// In Beam pipeline flow.
final PCollection<String> wordPCollection....

WithFailures.Result<PCollection<Integer>, Failure> result = wordPCollection
   .apply("Map", MapElements
           .into(TypeDescriptors.integers())
           .via((String word) -> 1 / word.length())  // Could throw ArithmeticException
           .exceptionsInto(TypeDescriptor.of(Failure.class))
           .exceptionsVia(exElt -> Failure.from("step", exElt))
   );

PCollection<String> output = result.output();
PCollection<Failure> failures = result.failures();
```

The logic is the same for FlatMapElements : 

```java
final PCollection<String> wordPCollection....

WithFailures.Result<PCollection<String>, Failure>> result = wordPCollection
   .apply("FlatMap", FlatMapElements
           .into(TypeDescriptors.strings())
           .via((String line) -> Arrays.asList(Arrays.copyOfRange(line.split(" "), 1, 5)))
           .exceptionsInto(TypeDescriptor.of(Failure.class))
           .exceptionsVia(exElt -> Failure.from("step", exElt))
   )

PCollection<String> output = result.output();
PCollection<Failure> failures = result.failures();
```

## Comparison between approaches

### Usual Beam pipeline

In a usual Beam pipeline flow, steps are chained fluently: 

```java
final PCollection<Integer> outputPCollection = inputPCollection
                 .apply("Map", MapElements .into(TypeDescriptors.strings()).via((String word) -> word + "Test"))
                 .apply("FlatMap", FlatMapElements
                                         .into(TypeDescriptors.strings())
                                         .via((String line) -> Arrays.asList(Arrays.copyOfRange(line.split(" "), 1, 5))))
                 .apply("Map ParDo", ParDo.of(new WordCountFn()));
```

### Usual Beam pipeline with error handling

Here's the same flow with error handling in each step:

```java
WithFailures.Result<PCollection<String>, Failure> result1 = input
        .apply("Map", MapElements
                        .into(TypeDescriptors.strings())
                        .via((String word) -> word + "Test")
                        .exceptionsInto(TypeDescriptor.of(Failure.class))
                        .exceptionsVia(exElt -> Failure.from("step", exElt)));

final PCollection<String> output1 = result1.output();
final PCollection<Failure> failure1 = result1.failures();

WithFailures.Result<PCollection<String>, Failure> result2 = output1
        .apply("FlatMap", FlatMapElements
                            .into(TypeDescriptors.strings())
                            .via((String line) -> Arrays.asList(Arrays.copyOfRange(line.split(" "), 1, 5)))
                            .exceptionsInto(TypeDescriptor.of(Failure.class))
                            .exceptionsVia(exElt -> Failure.from("step", exElt)));

final PCollection<String> output2 = result1.output();
final PCollection<Failure> failure2 = result1.failures();

final PCollectionTuple result3 = output2
        .apply("Map ParDo", ParDo.of(wordCountFn).withOutputTags(wordCountFn.getOutputTag(), TupleTagList.of(wordCountFn.getFailuresTag())));

final PCollection<Integer> output3 = result3.get(wordCountFn.getOutputTag());
final PCollection<Failure> failure3 = result3.get(wordCountFn.getFailuresTag());

final PCollection<Failure> allFailures = PCollectionList
        .of(failure1)
        .and(failure2)
        .and(failure3)
        .apply(Flatten.pCollections());
```

Problems with this approach: 

- We loose the native fluent style on apply chains, because we have to handle output and error for each step.
- For `MapElements` and `FlatMapElements` we have to always add `exceptionsInto` and `exceptionsVia` (can be centralized).
- For each custom DoFn, we have to duplicate the code of `TupleTag` logic and the try catch block (can be centralized).
- The code is verbose.
- There is no centralized code to concat all the errors, we have to concat all failures (can be centralized).
 

### Usual Beam pipeline with error handling using Asgarde

Here's the same flow with error handling, but using this library instead:

```java
final WithFailures.Result<PCollection<Integer>, Failure> resultComposer = CollectionComposer.of(input)
        .apply("Map", MapElements.into(TypeDescriptors.strings()).via((String word) -> word + "Test"))
        .apply("FlatMap", FlatMapElements
                .into(TypeDescriptors.strings())
                .via((String line) -> Arrays.asList(Arrays.copyOfRange(line.split(" "), 1, 5))))
        .apply("ParDo", MapElementFn.into(TypeDescriptors.integers()).via(word -> 1 / word.length()))
        .getResult();
```

Some explanations:
- The `CollectionComposer` class allows to centralize all the error logic, fluently compose the applies and concat all the failures occurring in the flow. 
- For `MapElements` and `FlatMapElements`, behind the scene, the `apply` method adds `exceptionsInto` and `exceptionsVia` on `Failure` object. 
We can also explicitely use `exceptionsInto` and `exceptionsVia` if needed, if you have some custom logic based on `Failure` object.
- The `MapElementFn` class is a custom `DoFn` class internally wraps the shared logic for `DoFn` like try/catch block and Tuple tags.
We will detail concepts in the next sections.


## Purpose of the library

- Wrap all error handling logic in a composer class.
- Wrap `exceptionsInto` and `exceptionsVia` usage in the native Beam classes `MapElements` and `FlatMapElements`.
- Keep the fluent style natively proposed by Beam in `apply` methods while checking for failures and offer a less verbose way of handling errors.
- Expose custom `DoFn` classes with centralized try/catch blocks (loan pattern) and Tuple tags.
- Expose an easier access to the `@Setup`, `@StartBundle`, `@FinishBundle`, `@Teardown` steps of `DoFn` classes.
- Expose a way to handle errors in filtering logic (currently not available with Beam's `Filter.by`).

Some resources for Loan pattern : 

https://dzone.com/articles/functional-programming-patterns-with-java-8

https://blog.knoldus.com/scalaknol-understanding-loan-pattern/

### Custom DoFn classes

#### `MapElementFn`

This class is the equivalent of a Beam `MapElements`.

It must be created by the `outputTypeDescriptor` and takes a `SerializableFunction` on generic input and output (input and output types of the mapper).

This `SerializableFunction` will be invoked lazily in the `@ProcessElement` method and lifecycle of the `DoFn`.

We can also give to this class actions related to `DoFn` lifecycle :
- `setupAction` executed in the `@Setup` method
- `startBundleAction` executed in the `@StartBundle` method
- `finishBundleAction` executed in the `@FinishBundle` method
- `teardownAction` executed in the `@Teardown` method

This action is represented by a `SerializableAction`: 

```java
@FunctionalInterface
public interface SerializableAction extends Serializable {
    
    void execute();
}
```

The `SerializableAction` is like a `java.lang.Runnable` that has to implement `Serializable`.

When writing a `DoFn`, Beam can infer the output type from `DoFn<Input, Output>` and deduce the output type descriptor from it. 

A default `Coder` can be added for this descriptor.

When we write a generic `DoFn`, Beam is unable to infer the output type and create the output descriptor, 
that's why in our custom `DoFn` classes we have to give the output descriptor in the `into` method.

Usage example:

```java
final PCollection<Integer> outputMapElementFn = CollectionComposer.of(inputPCollection)
        .apply("PaDo", MapElementFn
                .into(TypeDescriptors.integers())
                .via((String word) -> 1 / word.length())
                .withSetupAction(() -> LOGGER.info("Start word count action in the worker"))
                .withTeardownAction(() -> LOGGER.info("End word count action in the worker")))
        .getResult()
        .output();
```

Behind the scene the `CollectionComposer` class adds a `ParDo` on this `DoFn` and handles errors with tuple tags.

#### `MapProcessElementFn` 

This class works exactly as `MapElementFn`, but gives access to Beam's `ProcessContext` object.

It must be created from the input type of `DoFn`, which allows giving more information on the input, 
because the `SerializableFunction` is from the `ProcessContext` and not from the `Input` (doesn't bring the input type):

```java
SerializableFunction<ProcessContext, Output>
```

This class can take a `DoFn` lifecycle methods as the `MapElementFn` : 
- `setupAction`
- `startBundleAction`
- `finishBundleAction`
- `teardownAction`

and expects an output descriptor too.

Usage example: 

```java
final PCollection<Integer> resMapProcessElementFn = CollectionComposer.of(input)
        .apply("PaDo", MapProcessContextFn
                .from(String.class)
                .into(TypeDescriptors.integers())
                .via(ctx -> 1 / ctx.element().length())
                .withSetupAction(() -> LOGGER.info("Setup word count action in the worker"))
                .withStartBundleAction(() -> LOGGER.info("Start bundle word count action in the worker"))
                .withFinishBundleAction(() -> LOGGER.info("Finish bundle word count action in the worker"))
                .withTeardownAction(() -> LOGGER.info("End word count action in the worker"))))
        .getResult()
        .output();
```

Sometimes we need access to Beam's `ProcessContext` to get technical fields or handle side inputs.

For `MapElementFn` and `MapProcessContextFn`, we can give side inputs to the `CollectionComposer`.

Here's an example using side inputs:

```java
// Simulates a side input, from Beam pipeline.
final String wordDescription = "Word to describe Football teams";

final PCollectionView<String> sideInputs = pipeline
        .apply("Create word description side input", Create.of(wordDescription))
        .apply("Create as collection view", View.asSingleton());

final PCollection<WordStats> resMapProcessElementFn = CollectionComposer.of(input)
        .apply("PaDo", MapProcessContextFn
                        .from(String.class)
                        .into(TypeDescriptor.of(WordStats.class))
                        .via(ctx -> toWordStats(sideInputs, ctx))
                        .withSetupAction(() -> LOGGER.info("Start word count action in the worker")),
                
                Collections.singleton(sideInputs))
        .getResult()
        .output();
```

```java
private WordStats toWordStats(final PCollectionView<String> sideInputs, final DoFn<String, WordStats>.ProcessContext context) {
    final String word = context.element();
    final Integer dividedWordCount = 1 / word.length();

    // Gets the current timestamp in context.
    final Instant timestamp = context.timestamp();

    // Gets the side input value from PCollectionView and context.
    final String wordDescription = context.sideInput(sideInputs);

    return new WordStats(word, dividedWordCount, timestamp, wordDescription);
}

private static class WordStats implements Serializable {
    private final String word;
    private final Integer dividedWordCount;
    private final Instant timestamp;
    private final String wordDescription;

    public WordStats(String word, Integer dividedWordCount, Instant timestamp, String wordDescription) {
        this.word = word;
        this.dividedWordCount = dividedWordCount;
        this.timestamp = timestamp;
        this.wordDescription = wordDescription;
    }
}
```

Behind the scene the `CollectionComposer` class adds the `ParDo` on this `DoFn` and handles errors with tuple tags.

#### `FlatMapElementFn`

Same principle as `MapElementFn` but for `flatMap` operator.

```java
PCollection<Player> players =
    FlatMapElementFn.into(TypeDescriptor.of(Player.class))
        .via(team -> team.getPlayers())
        .withSetupAction(() -> System.out.println("Starting of mapping...")
        .withStartBundleAction(() -> System.out.println("Starting bundle of mapping...")
        .withFinishBundleAction(() -> System.out.println("Ending bundle of mapping...")
        .withTeardownAction(() -> System.out.println("Ending of mapping...")
```

#### `FlatMapProcessContextFn`

Same principle as `MapProcessContextFn` but for `flatMap` operator.

```java
PCollection<Player> players =
    FlatMapProcessContextFn.from(Team.class)
        .into(TypeDescriptor.of(Player.class))
        .via((ProcessContext ctx) -> ctx.element().getPlayers())
        .withSetupAction(() -> System.out.println("Starting of mapping...")
        .withStartBundleAction(() -> System.out.println("Starting bundle of mapping...")
        .withFinishBundleAction(() -> System.out.println("Ending bundle of mapping...")
        .withTeardownAction(() -> System.out.println("Ending of mapping...")
```

#### `FilterFn` 

This class is like the `Filter` class exposed by Beam but with built-in error handling.
It's a generic `DoFn` implementation just like `MapElementFn` and `MapProcessElementFn`.

Usage example:

```java
final PCollection<String> resFilterFn = CollectionComposer.of(wordCollection)
            .apply("FilterFn", FilterFn.by(word -> word.length() > 3))
            .getResult()
            .output();
```

It takes a predicate via: 

```java
final SerializableFunction<InputT, Boolean> predicate
```

Behind the scene, the `CollectionComposer` takes the output from the previous `PCollection`.
No need to pass any output descriptor in this case, because it's only a filtering operation on the same type.


### Collection composer class

Centralizes error handling in one place.

Create an instance from a PCollection.

```java
CollectionComposer.of(wordPCollection)
```


* It accepts native `MapElements` and `FlatMapElements` without adding `exceptionsType` and `exceptionsVia`:

```java
final PCollection<Integer> resMapElements = CollectionComposer.of(input)
        .apply("ParDo", MapElements.into(TypeDescriptors.integers()).via((String word) -> 1 / word.length()))
        .getResult()
        .output();

final PCollection<String> resFlatMapElements = CollectionComposer.of(input)
        .apply("FlatMapElements", FlatMapElements
                .into(TypeDescriptors.strings())
                .via((String line) -> Arrays.asList(Arrays.copyOfRange(line.split(" "), 1, 5))))
        .getResult()
        .output();
```

* It accepts native `MapElements` and `FlatMapElements` with `exceptionsType` and `exceptionsVia` 
(if external` exceptionsType` and `exceptionsVia` are given, they have to be based on the `Failure` object exposed by 
the library):

```java
final PCollection<Integer> resMapElements2 = CollectionComposer.of(input)
        .apply("MapElements", MapElements
                .into(TypeDescriptors.integers())
                .via((String word) -> 1 / word.length())
                .exceptionsInto(of(Failure.class))
                .exceptionsVia(exElt -> Failure.from("step", exElt)))
        .getResult()
        .output();

final PCollection<String> resFlatMapElements2 = CollectionComposer.of(input)
        .apply("FlatMapElements", FlatMapElements
                .into(TypeDescriptors.strings())
                .via((String line) -> Arrays.asList(Arrays.copyOfRange(line.split(" "), 1, 5)))
                .exceptionsInto(of(Failure.class))
                .exceptionsVia(exElt -> Failure.from("step", exElt)))
        .getResult()
        .output();
```

The `Failure` object exposed by the library, contains the `inputElement` of the step in a String format (toString method), and the occurred exception.
For the input element used in transforms, developers can override toString method and implement the string representation of object.
A possible approach is to serialize the object to Json string via a framework like Jackson for better readability.
Beam already uses Jackson as an internal dependency.

Example with a `Team` class used in a Beam Transform : 

```java
public static class Team implements Serializable {
    ...

    @Override
    public String toString() {
        return JsonUtil.serialize(this);
    }
}

public static <T> String serialize(final T obj) {
    try {
        return OBJECT_MAPPER.writeValueAsString(obj);
    } catch (JsonProcessingException e) {
        throw new IllegalStateException("The serialization of object fails : " + obj);
    }
}
```

In this case, we use Jackson to serialize the current object to Json String, in the toString method of the Team object.

 
The Failure class exposes factory methods to build from inputs:

```java
public class Failure implements Serializable {
    private final String pipelineStep;
    private final String inputElement;
    private final Throwable exception;

    private Failure(String pipelineStep,
                    String inputElement,
                    Throwable exception) {
        this.pipelineStep = pipelineStep;
        this.inputElement = inputElement;
        this.exception = exception;
    }

    public static <T> Failure from(final String pipelineStep,
                                   final WithFailures.ExceptionElement<T> exceptionElement) {
        final T inputElement = exceptionElement.element();
        return new Failure(pipelineStep, inputElement.toString(), exceptionElement.exception());
    }
    
    public static <T> Failure from(final String pipelineStep,
                                   final T element,
                                   final Throwable exception) {
        return new Failure(pipelineStep, element.toString(), exception);
    }
}
```

* It accepts `MapElementFn`, `MapProcessElementFn`, `MapElementFn`, `MapProcessElementFn`, `FilterFn` and custom `DoFn`s:

```java
// Map element Fn.
final PCollection<Integer> resMapElementFn = CollectionComposer.of(input)
            .apply("PaDo", MapElementFn
                    .into(TypeDescriptors.integers())
                    .via((String word) -> 1 / word.length())
                    .withSetupAction(() -> LOGGER.info("Start word count action in the worker")))
            .getResult()
            .output();

// Map process context Fn without side inputs.
final PCollection<Integer> resMapProcessElementFn = CollectionComposer.of(input)
            .apply("PaDo", MapProcessContextFn
                    .from(String.class)
                    .into(TypeDescriptors.integers())
                    .via(ctx -> 1 / ctx.element().length())
                    .withSetupAction(() -> LOGGER.info("Start word count action in the worker")))
            .getResult()
            .output();


// Map process context Fn with side inputs.
final String wordDescription = "Word to describe Football teams";

final PCollectionView<String> sideInputs = input.getPipeline()
            .apply("String side input", Create.of(wordDescription))
            .apply("Create as collection view", View.asSingleton());

final PCollection<WordStats> resMapProcessElementFn2 = CollectionComposer.of(input)
            .apply("PaDo", MapProcessContextFn
                            .from(String.class)
                            .into(TypeDescriptor.of(WordStats.class))
                            .via(ctx -> toWordStats(sideInputs, ctx))
                            .withSetupAction(() -> LOGGER.info("Start word count action in the worker")),
    
                    Collections.singleton(sideInputs))
            .getResult()
            .output();

// Filter Fn.
final PCollection<String> resFilterFn = CollectionComposer.of(input)
            .apply("FilterFn", FilterFn.by(word -> word.length() > 3))
            .getResult()
            .output();
```

* Custom non-generic `DoFn` classes are also supported, they must extend the `BaseElementFn` (it initializes the tuple tags logic) 
and the type descriptors will be deduced from non-generic types:

```java
import fr.groupbees.asgarde.Failure;

final PCollection<WordStats> resCustomDoFn=CollectionComposer.of(input)
        .apply("PaDo",new WordStatsFn(sideInputs),Collections.singleton(sideInputs))
        .getResult()
        .output();

// Custom DoFn class.
public class WordStatsFn extends BaseElementFn<String, WordStats> {

    private PCollectionView<String> sideInputs;

    public WordStatsFn(final PCollectionView<String> sideInputs) {
        // Do not forget to call this!
        super();

        this.sideInputs = sideInputs
    }

    @ProcessElement
    public void processElement(ProcessContext ctx) {
        try {
            ctx.output(toWordStats(sideInputs, ctx));
        } catch (Throwable throwable) {
            final Failure failure = Failure.from("step", ctx.element(), throwable);
            ctx.output(failuresTag, failure);
        }
    }
}
```

* It keeps the `apply` methods' fluent style and internally concats all the occurring failures.

```java
final WithFailures.Result<PCollection<Integer>, Failure> resultComposer = CollectionComposer.of(input)
            .apply("Map", MapElements.into(TypeDescriptors.strings()).via((String word) -> word + "Test"))
            .apply("FlatMap", FlatMapElements
                    .into(TypeDescriptors.strings())
                    .via((String line) -> Arrays.asList(Arrays.copyOfRange(line.split(" "), 1, 5))))
            .apply("ParDo", MapElementFn.into(TypeDescriptors.integers()).via(word -> 1 / word.length()))
            .getResult();

final PCollection<String> output = resultComposer.output();
final PCollection<Failure> failures = resultComposer.failures();
```

### Coder in CollectionComposer class

#### Coder for output PCollection

A coder can be set for the current output `PCollection` in the flow, example : 

```java
// Generate and instantiate an Avro specific object.
final AvroTest inputAvroObject = AvroTest.newBuilder()
    .setId(34444)
    .setName("test")
    .build();

// Read an Avro file with a type corresponding to the previous specific object.
final PCollection<AvroTest> avroObjectsCollection = pipeline.apply("Reads Avro objects", AvroIO
    .read(AvroTest.class)
    .from("filePath"));

private GenericRecord avroObjectToGenericRecord(final AvroTest avroTest) {
    GenericRecord record = new GenericData.Record(avroTest.getSchema());
    record.put("id", avroTest.getId());
    record.put("name", avroTest.getName());

    return record;
}

// Flow with CollectionComposer.
final Result<PCollection<GenericRecord>, Failure> result2 = CollectionComposer.of(avroObjectsCollection)
    .apply("Map", MapElements.into(of(GenericRecord.class)).via(this::avroObjectToGenericRecord))
    .setCoder(AvroCoder.of(GenericRecord.class, inputAvroObject.getSchema()))
    .getResult();
```

In this example, an `Avro` file is read and mapped to a typed and specific object `AvroTest`.\
Then we simulate a transformation from this `AvroTest` instance to a `GenericRecord` object.\
The `GenericRecord` doesn't contain any information about `Serialization` and in this case `Beam` can't infer
a default `Coder`.

We have to set a `Coder` for the output `PCollection` of `GenericRecord` :

```java
  .setCoder(AvroCoder.of(GenericRecord.class, inputAvroObject.getSchema()))
```

`Asgarde` and the `CollectionComposer` class work as the usual `PCollection` for coders and 
propose the same `setCoder` method for good outputs.


## Asgarde with Kotlin

`Apache Beam Java` can be used with `Kotlin`, and it's make the experience more enjoyable.

`Asgarde` proposes also `Kotlin` extensions to use `CollectionComposer` class with more expressive/concise code and with
functional programming style.

> :warning: **Kotlin Asgarde is proposed from 0.15.0 and Beam 2.36.0 versions**

Let's take a previous example of `Asgarde Java` pipeline with error handling :

```java
final WithFailures.Result<PCollection<Integer>, Failure> resultComposer = CollectionComposer.of(input)
        .apply("Map", MapElements.into(TypeDescriptors.strings()).via((String word)-> word + "Test"))
        .apply("FlatMap", FlatMapElements
                          .into(TypeDescriptors.strings())
                          .via((String line) -> Arrays.asList(Arrays.copyOfRange(line.split(" "), 1, 5))))
        .apply("ParDo", MapElementFn.into(TypeDescriptors.integers()).via(word -> 1/word.length()))
        .getResult();
```

In `Asgarde Kotlin` the same pipeline is :

```kotlin
import fr.groupbees.asgarde.*

val result: Result<PCollection<Int>, Failure> = CollectionComposer.of(words)
    .map("Map") { word -> word + "Test" }
    .flatMap("FlatMap") { Arrays.asList(*Arrays.copyOfRange(it.split(" ").toTypedArray(), 1, 5)) }
    .mapFn("ParDo", { word -> 1 / word.length })
    .result
```

The Kotlin code of `Asgarde` uses `extensions`.\
To use these extensions, the following `import` must be added :

```kotlin
import fr.groupbees.asgarde.*
```

This feature is great because we can mix native `Asgarde` code with 
functions dedicated to Kotlin, example : 

```kotlin
val result: Result<PCollection<Int>, Failure> = CollectionComposer.of(words)
    .apply("Map", MapElements.into(TypeDescriptors.strings()).via(SerializableFunction { String word -> word + "Test" })
    .flatMap("FlatMap") { Arrays.asList(*Arrays.copyOfRange(it.split(" ").toTypedArray(), 1, 5)) }
    .mapFn("ParDo", { word -> 1 / word.length })
    .result
```

In this case, the first `map` function has been replaced by native `apply` function with `MapElements`\
The type of `lambda expression` in `via` function needs to be specified in `Kotlin`, because this can take `ProcessFunction` 
or `SerializableFunction`.\
This causes an ambiguity (`SerializableFunction` is used in this example).


In the following sections, all `Asgarde` native components and their equivalents in `Kotlin` are proposed.

### Extension for MapElements

`Asgarde Java` :

```java
CollectionComposer.of(teams)
        .apply("Step name", MapElements.into(TypeDescriptor.of(OtherTeam.class)).via(team -> TestSettings.toOtherTeam(team)))
        .getResult();
```

With method reference

```java
CollectionComposer.of(teams)
        .apply("Step name", MapElements.into(TypeDescriptor.of(OtherTeam.class)).via(TestSettings::toOtherTeam))
        .getResult();
```

`Asgarde Kotlin` :

```kotlin
CollectionComposer.of(teams)
    .map("Step name") { team -> TestSettings.toOtherTeam(team) }
    .result
```

With `Kotlin` `it`

```kotlin
CollectionComposer.of(teams)
    .map("Step name") { TestSettings.toOtherTeam(it) }
    .result
```

The `Kotlin` version takes only a lambda type in the inline function, 
`Kotlin` recommends writing the lambda in a separated parenthesis.

### Extension for FlatMapElements

`Asgarde Java` :

```java
CollectionComposer.of(teams)
        .apply("Step name", FlatMapElements.into(of(Player.class)).via(team -> team.getPlayers()))
        .getResult();
```

With method reference

```java
CollectionComposer.of(teams)
        .apply("Step name", FlatMapElements.into(of(Player.class)).via(Team::getPlayers))
        .getResult();
```

`Asgarde Kotlin` :

```kotlin
CollectionComposer.of(teams)
    .flatMap("Step name") { team -> team.players }
    .result
```

With `Kotlin` `it`

```kotlin
CollectionComposer.of(teams)
    .flatMap("Step name") { it.players }
    .result
```

### Extension for MapElement with failure

`Asgarde Java` :

```java
CollectionComposer.of(teams)
        .apply("Step name", MapElements
                            .into(of(Team.class))
                            .via(team -> toTeamWithPsgError(team))
                            .exceptionsInto(of(Failure.class))
                            .exceptionsVia(exElt -> Failure.from("Step name", exElt)))
        .getResult();
```

With method reference

```java
CollectionComposer.of(teams)
        .apply("Step name", MapElements
                            .into(of(Team.class))
                            .via(this::toTeamWithPsgError)
                            .exceptionsInto(of(Failure.class))
                            .exceptionsVia(exElt -> Failure.from("Step name", exElt)))
        .getResult();
```

`Asgarde Kotlin` :

```kotlin
CollectionComposer.of(teamCollection)
    .mapWithFailure(
      "Step name",
      { team -> toTeamWithPsgError(team) },
      { exElt -> Failure.from("Step name", exElt) }
    )
```

With `Kotlin` `it`

```kotlin
CollectionComposer.of(teamCollection)
    .mapWithFailure(
      "Step name",
      { toTeamWithPsgError(it) },
      { Failure.from("Step name", it) }
    )
```

### Extension for FlatMapElement with failure

`Asgarde Java` :

```java
CollectionComposer.of(teams)
        .apply("Step name", FlatMapElements
                            .into(of(Player.class))
                            .via(team -> simulateFlatMapErrorPsgTeam(team))
                            .exceptionsInto(of(Failure.class))
                            .exceptionsVia(exElt -> Failure.from("Step name", exElt)))
        .getResult();
```

With method reference

```java
CollectionComposer.of(teams)
        .apply("Step name", FlatMapElements
                            .into(of(Player.class))
                            .via(this::simulateFlatMapErrorPsgTeam)
                            .exceptionsInto(of(Failure.class))
                            .exceptionsVia(exElt -> Failure.from("Step name", exElt)))
        .getResult();
```

`Asgarde Kotlin` :

```kotlin
CollectionComposer.of(teamCollection)
    .flatMapWithFailure(
      "Step name",
      { team -> simulateFlatMapErrorPsgTeam(team) },
      { exElt -> Failure.from("Step name", exElt) }
    )
    .result
```

With `Kotlin` `it`

```kotlin
CollectionComposer.of(teamCollection)
    .flatMapWithFailure(
      "Step name",
      { simulateFlatMapErrorPsgTeam(it) },
      { Failure.from("Step name", it) }
    )
    .result
```


### Extension for MapElementFn

`Asgarde Java` :

```java
CollectionComposer.of(teams)
        .apply("Step name", MapElementFn
                              .into(of(OtherTeam.class))
                              .via(team -> TestSettings.toOtherTeam(team))
                              .withSetupAction(() -> System.out.print("Test"))
                              .withStartBundleAction(() -> System.out.print("Test"))
                              .withFinishBundleAction(() -> System.out.print("Test"))
                              .withTeardownAction(() -> System.out.print("Test")))
        .getResult();
```

With method reference

```java
CollectionComposer.of(teams)
        .apply("Step name", MapElementFn
                                  .into(of(OtherTeam.class))
                                  .via(TestSettings::toOtherTeam)
                                  .withSetupAction(() -> System.out.print("Test"))
                                  .withStartBundleAction(() -> System.out.print("Test"))
                                  .withFinishBundleAction(() -> System.out.print("Test"))
                                  .withTeardownAction(() -> System.out.print("Test")))
        .getResult();
```

`Asgarde Kotlin` :

```kotlin
CollectionComposer.of(teams)
      .mapFn("Step name",
        { team -> TestSettings.toOtherTeam(team) },
        setupAction = { print("Test") }, 
        startBundleAction = { print("Test") }, 
        finishBundleAction = { print("Test") }, 
        teardownAction = { print("Test") }
      )
      .result
```

With `Kotlin` `it`

```kotlin
CollectionComposer.of(teams)
    .mapFn("Step name",
      { TestSettings.toOtherTeam(it) },
      setupAction = { print("Test") }, 
      startBundleAction = { print("Test") }, 
      finishBundleAction = { print("Test") },
      teardownAction = { print("Test") }
    )
    .result
```

### Extension for FlatMapElementFn

`Asgarde Java` :

```java
CollectionComposer.of(teams)
        .apply("Step name", FlatMapElementFn
                              .into(of(Player.class))
                              .via(team -> team.getPlayers())
                              .withSetupAction(() -> System.out.print("Test"))
                              .withStartBundleAction(() -> System.out.print("Test"))
                              .withFinishBundleAction(() -> System.out.print("Test"))
                              .withTeardownAction(() -> System.out.print("Test")))
        .getResult();
```

With method reference

```java
CollectionComposer.of(teams)
        .apply("Step name", FlatMapElementFn
                                  .into(of(Player.class))
                                  .via(Team::getPlayers)
                                  .withSetupAction(() -> System.out.print("Test"))
                                  .withStartBundleAction(() -> System.out.print("Test"))
                                  .withFinishBundleAction(() -> System.out.print("Test"))
                                  .withTeardownAction(() -> System.out.print("Test")))
        .getResult();
```

`Asgarde Kotlin` :

```kotlin
CollectionComposer.of(teams)
      .flatMapFn("Step name",
        { team -> team.players },
        setupAction = { print("Test") }, 
        startBundleAction = { print("Test") }, 
        finishBundleAction = { print("Test") }, 
        teardownAction = { print("Test") }
      )
      .result
```

With `Kotlin` `it`

```kotlin
CollectionComposer.of(teams)
    .flatMapFn("Step name",
      { it.players },
      setupAction = { print("Test") },
      startBundleAction = { print("Test") },
      finishBundleAction = { print("Test") },
      teardownAction = { print("Test") }
    )
    .result
```

### Extension for MapProcessContextFn

`Asgarde Java` :

```java
CollectionComposer.of(teams)
        .apply("Step name", MapProcessContextFn
                              .from(Team.class)
                              .into(of(OtherTeam.class))
                              .via(ctx -> TestSettings.toOtherTeam(ctx))
                              .withSetupAction(() -> System.out.print("Test"))
                              .withStartBundleAction(() -> System.out.print("Test"))
                              .withFinishBundleAction(() -> System.out.print("Test"))
                              .withTeardownAction(() -> System.out.print("Test")))
        .getResult();
```

With method reference

```java
CollectionComposer.of(teams)
        .apply("Step name", MapProcessContextFn
                            .from(Team.class)
                            .into(of(OtherTeam.class))
                            .via(TestSettings::toOtherTeam)
                            .withSetupAction(() -> System.out.print("Test"))
                            .withStartBundleAction(() -> System.out.print("Test"))
                            .withFinishBundleAction(() -> System.out.print("Test"))
                            .withTeardownAction(() -> System.out.print("Test")))
        .getResult();
```

`Asgarde Kotlin` :

```kotlin
CollectionComposer.of(teams)
    .mapFnWithContext("Step name",
      { ctx: DoFn<Team, OtherTeam>.ProcessContext -> TestSettings.toOtherTeam(ctx) },
      setupAction =  { print("Test") }, 
      startBundleAction =  { print("Test") }, 
      finishBundleAction =  { print("Test") },
      teardownAction =  { print("Test") }
    )
    .result
```

With `Kotlin` `it`

```kotlin
CollectionComposer.of(teams)
    .mapFnWithContext<Team, OtherTeam>("Step name",
      { TestSettings.toOtherTeam(it) },
      setupAction =  { print("Test") },
      startBundleAction =  { print("Test") },
      finishBundleAction =  { print("Test") },
      teardownAction =  { print("Test") }
    )
    .result
```

### Extension for FlatMapProcessContextFn

`Asgarde Java` :

```java
CollectionComposer.of(teams)
        .apply("Step name", FlatMapProcessContextFn
                              .from(Team.class)
                              .into(of(Player.class))
                              .via(ctx -> TestSettins.toPlayers(ctx))
                              .withSetupAction(() -> System.out.print("Test"))
                              .withStartBundleAction(() -> System.out.print("Test"))
                              .withFinishBundleAction(() -> System.out.print("Test"))
                              .withTeardownAction(() -> System.out.print("Test")))
        .getResult();
```

With method reference

```java
CollectionComposer.of(teams)
        .apply("Step name", FlatMapProcessContextFn
                             .from(Team.class)
                             .into(of(Player.class))
                             .via(TestSettins::toPlayers)
                             .withSetupAction(() -> System.out.print("Test"))
                             .withStartBundleAction(() -> System.out.print("Test"))
                             .withFinishBundleAction(() -> System.out.print("Test"))
                             .withTeardownAction(() -> System.out.print("Test")))
        .getResult();
```

`Asgarde Kotlin` :

```kotlin
CollectionComposer.of(teams)
    .flatMapFnWithContext("Step name",
      { ctx: DoFn<Team, Player>.ProcessContext -> TestSettings.toPlayers(ctx) },
      setupAction =  { print("Test") }, 
      startBundleAction =  { print("Test") },
      finishBundleAction =  { print("Test") }, 
      teardownAction =  { print("Test") }
    )
    .result
```

With `Kotlin` `it`

```kotlin
CollectionComposer.of(teams)
    .flatMapFnWithContext<Team, Player>("Step name",
      { TestSettings.toPlayers(it) },
      setupAction =  { print("Test") },
      startBundleAction =  { print("Test") },
      finishBundleAction =  { print("Test") },
      teardownAction =  { print("Test") }
    )
    .result
```

### Extension for FilterFn

`Asgarde Java` :

```java
 CollectionComposer.of(teamCollection)
        .apply("Step name", FilterFn.by(team -> simulateFilterErrorPsgTeam(team)))
        .getResult();
```

With method reference

```java
 CollectionComposer.of(teamCollection)
        .apply("Step name", FilterFn.by(this::simulateFilterErrorPsgTeam))
        .getResult();
```

`Asgarde Kotlin` :

```kotlin
CollectionComposer.of(teamCollection)
    .filter("Step name") { team -> simulateFilterErrorPsgTeam(team) }
    .result
```

With `Kotlin` `it`

```kotlin
CollectionComposer.of(teamCollection)
    .filter("Step name") { simulateFilterErrorPsgTeam(it) }
    .result
```

## Possible evolutions in the future

- Maybe allow injecting a custom `Failure` object and error handling function to be used in all `apply` calls.