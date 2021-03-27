
# Asgarde

This module allows error handling with Apache Beam.

## Error logic with Beam ParDo and DoFn

Beam recommends treating errors with Dead letters.
It means catching errors in the flow and, using side outputs, sinking errors to a file, database or any other output...

Beam suggests handling side outputs with `TupleTags` in a `DoFn` class, example :

```java
// Failure object.
public class Failure implements Serializable {
    private Integer inputElement;
    private Throwable exception;
    
    public static <T> Failure from(final T element, final Throwable exception) {
        return new Failure(element.toString(), exception);
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
            final Failure failure = Failure.from(ctx.element(), throwable);
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

Beam also allows handling errors with built in components like `MapElements` and `FlatMapElements` (it's currently an experimental feature as of april of 2020).

Behind the scene, in these classes Beam use the same concept explained above.

Example: 

```java
public class Failure implements Serializable {
    private String inputElement;
    private Throwable exception;

    public static <T> Failure from(final T element, final Throwable exception) {
        return new Failure(element.toString(), exception);
    }
}

// In Beam pipeline flow.
final PCollection<String> wordPCollection....

WithFailures.Result<PCollection<Integer>, Failure> result = wordPCollection
   .apply("Map", MapElements
           .into(TypeDescriptors.integers())
           .via((String word) -> 1 / word.length())  // Could throw ArithmeticException
           .exceptionsInto(TypeDescriptor.of(Failure.class))
           .exceptionsVia(Failure::from)
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
           .exceptionsVia(Failure::from)
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
                        .exceptionsVia(Failure::from));

final PCollection<String> output1 = result1.output();
final PCollection<Failure> failure1 = result1.failures();

WithFailures.Result<PCollection<String>, Failure> result2 = output1
        .apply("FlatMap", FlatMapElements
                            .into(TypeDescriptors.strings())
                            .via((String line) -> Arrays.asList(Arrays.copyOfRange(line.split(" "), 1, 5)))
                            .exceptionsInto(TypeDescriptor.of(Failure.class))
                            .exceptionsVia(Failure::from));

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
- Expose an easier access to the `@Setup` step of `DoFn` classes.
- Expose a way to handle errors in filtering logic (currently not available with Beam's `Filter.by`).

Some resources for Loan pattern : 

https://dzone.com/articles/functional-programming-patterns-with-java-8

https://blog.knoldus.com/scalaknol-understanding-loan-pattern/

### Custom DoFn classes

#### `MapElementFn`

This class is the equivalent of a Beam `MapElements`.

It must be created by the `outputTypeDescriptor` and takes a `SerializableFunction` on generic input and output (input and output types of the mapper).

This `SerializableFunction` will be invoked lazily in the `@ProcessElement` method and lifecycle of the `DoFn`.

We can also give a `setupAction` to this class, that will be executed in the `@Setup` method and lifecycle.

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
                .withSetupAction(() -> LOGGER.info("Start word count action in the worker")))
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

This class can take a `setupAction` as the `MapElementFn` and expects an output descriptor too.

Usage example: 

```java
final PCollection<Integer> resMapProcessElementFn = CollectionComposer.of(input)
        .apply("PaDo", MapProcessContextFn
                .from(String.class)
                .into(TypeDescriptors.integers())
                .via(ctx -> 1 / ctx.element().length())
                .withSetupAction(() -> LOGGER.info("Start word count action in the worker")))
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
A default Serializable Coder is created from the output type descriptor (same behaviour as MapElementFn).

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
                .exceptionsVia(Failure::from))
        .getResult()
        .output();

final PCollection<String> resFlatMapElements2 = CollectionComposer.of(input)
        .apply("FlatMapElements", FlatMapElements
                .into(TypeDescriptors.strings())
                .via((String line) -> Arrays.asList(Arrays.copyOfRange(line.split(" "), 1, 5)))
                .exceptionsInto(of(Failure.class))
                .exceptionsVia(Failure::from))
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
    private String inputElement;
    private Throwable exception;
    
    private Failure(String inputElement, Throwable exception) {
        this.inputElement = inputElement;
        this.exception = exception;
    }

    public static <T> Failure from(final WithFailures.ExceptionElement<T> exceptionElement) {
        final T inputElement = exceptionElement.element();
        return new Failure(inputElement.toString(), exceptionElement.exception());
    }
    
    public static <T> Failure from(final T element, final Throwable exception) {
        return new Failure(element.toString(), exception);
    }
}
```

* It accepts `MapElementFn`, `MapProcessElementFn`, `FilterFn` and custom `DoFn`s:

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
and the type descriptors will be deduced from non generic types:

```java
final PCollection<WordStats> resCustomDoFn = CollectionComposer.of(input)
            .apply("PaDo", new WordStatsFn(sideInputs), Collections.singleton(sideInputs))
            .getResult()
            .output();

// Custom DoFn class.
public class WordStatsFn extends BaseElementFn<String, WordStats> {

    private PCollectionView<String> sideInputs;

    public WordStatsFn(final PCollectionView<String> sideInputs) {
        // Do not forget to call this!
        super();
    }

    @ProcessElement
    public void processElement(ProcessContext ctx) {
        try {
            ctx.output(toWordStats(sideInputs, ctx));
        } catch (Throwable throwable) {
            val failure = Failure.from(ctx.element(), throwable);
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


## Possible evolutions in the future

- Allow passing a custom coder in the `apply` method of `CollectionComposer`.
- Allow injecting a custom `Failure` object and error handling function to be used in all `apply` calls.
- Add FlatMapElementFn and FlatMapProcessElementFn classes.