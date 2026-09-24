<img src="asgarde_logo.png" alt="Asgarde logo" width="200">

# Asgarde

[![Maven Central](https://img.shields.io/maven-central/v/fr.groupbees/asgarde?logo=apachemaven&label=Maven%20Central&color=blue)](https://central.sonatype.com/artifact/fr.groupbees/asgarde)
[![Build](https://github.com/tosun-si/asgarde/actions/workflows/build-and-quality-check.yml/badge.svg?branch=main)](https://github.com/tosun-si/asgarde/actions/workflows/build-and-quality-check.yml)
[![Apache Beam](https://img.shields.io/badge/Apache%20Beam-%E2%89%A5%202.70.0-E25A1C?logo=apache&logoColor=white)](https://beam.apache.org/)
[![Java](https://img.shields.io/badge/Java-8%2B-ED8B00?logo=openjdk&logoColor=white)](https://tosun-si.github.io/asgarde/project/compatibility/)
[![Kotlin](https://img.shields.io/badge/Kotlin-extensions-7F52FF?logo=kotlin&logoColor=white)](https://tosun-si.github.io/asgarde/java/kotlin/)
[![Quality Gate](https://sonarcloud.io/api/project_badges/measure?project=tosun-si_asgarde&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=tosun-si_asgarde)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=tosun-si_asgarde&metric=coverage)](https://sonarcloud.io/summary/new_code?id=tosun-si_asgarde)
[![Docs](https://img.shields.io/badge/docs-tosun--si.github.io%2Fasgarde-E25A1C?logo=astro&logoColor=white)](https://tosun-si.github.io/asgarde/)
[![License: MIT](https://img.shields.io/github/license/tosun-si/asgarde)](LICENSE)
[![GitHub stars](https://img.shields.io/github/stars/tosun-si/asgarde?style=social)](https://github.com/tosun-si/asgarde)

**Error handling and dead letter queues for Apache Beam, without the boilerplate.** For Java and Kotlin, and for
Python with [pasgarde](https://github.com/tosun-si/pasgarde).

📖 **Documentation: https://tosun-si.github.io/asgarde/**

## Why Asgarde

With plain Beam, each step needs its own `try/catch`, tuple tags or `exceptionsInto`/`exceptionsVia`, and all the
failures must be gathered at the end. Asgarde keeps the fluent style and gathers the failures of all the steps:

```java
final WithFailures.Result<PCollection<Integer>, Failure> result = CollectionComposer.of(values)
        .apply("Trim", MapElements.into(TypeDescriptors.strings()).via((String value) -> value.trim()))
        .apply("Parse", MapElementFn.into(TypeDescriptors.integers()).via((String value) -> Integer.parseInt(value)))
        .apply("Keep even numbers", FilterFn.by(number -> number % 2 == 0))
        .getResult();

final PCollection<Integer> outputs = result.output();
final PCollection<Failure> failures = result.failures();   // The failures of all the steps, for your dead letter queue
```

The same flow in Kotlin:

```kotlin
val result: Result<PCollection<Int>, Failure> = CollectionComposer.of(values)
    .map("Trim") { value -> value.trim() }
    .mapFn("Parse", { value -> value.toInt() })
    .filter("Keep even numbers") { number -> number % 2 == 0 }
    .result
```

## Installation

Asgarde is published on [Maven Central](https://central.sonatype.com/artifact/fr.groupbees/asgarde). Beam is a
`provided` dependency: your pipeline brings its own Beam version.

```xml
<dependency>
    <groupId>fr.groupbees</groupId>
    <artifactId>asgarde</artifactId>
    <version>1.3.0</version>
</dependency>
```

```kotlin
implementation("fr.groupbees:asgarde:1.3.0")
```

## Features

- **One place for all the errors**: each step catches its errors in a
  [`Failure`](https://tosun-si.github.io/asgarde/concepts/failure/), the
  [`CollectionComposer`](https://tosun-si.github.io/asgarde/concepts/collection-composer/) gathers the failures of
  all the steps.
- **Beam transforms and Asgarde DoFn classes**: `MapElements`, `FlatMapElements`, and
  [`MapElementFn`, `FlatMapElementFn`, `FilterFn`, `*ProcessContextFn`](https://tosun-si.github.io/asgarde/java/transforms/)
  with side inputs and DoFn lifecycle actions, or [your own DoFn](https://tosun-si.github.io/asgarde/java/custom-dofn/).
- **[Origin element](https://tosun-si.github.io/asgarde/concepts/origin-element/)**: the failures can give the element
  that entered the flow, to debug and replay from the start. Evaluated only when a failure occurs.
- **[Never breaks your job](https://tosun-si.github.io/asgarde/concepts/guarantees/)**: non serializable exceptions,
  failing `toString`, partial flatMap outputs, reused DoFn instances, stable transform names for Dataflow updates.
- **[Write the failures](https://tosun-si.github.io/asgarde/concepts/write-failures/)** to BigQuery or any
  schema-aware sink, with a documented schema (exception type, message, stack trace, timestamp...).
- **[Beam native error handling](https://tosun-si.github.io/asgarde/concepts/beam-error-handling/)**: the failures
  can be added to a Beam `ErrorHandler` as `BadRecord`s, for a single dead letter queue with the Beam IOs.
- **[Failure metrics](https://tosun-si.github.io/asgarde/concepts/metrics/)**: a Beam counter per step.
- **[Kotlin extensions](https://tosun-si.github.io/asgarde/java/kotlin/)** for a concise syntax.

## Compatibility

Asgarde is not tied to a Beam version: it's compiled against Beam `2.70.0` (the minimum supported version), and the
CI tests every push and every week against the latest Beam release. Java 8+ (11+ with Beam `>= 2.74.0`, required by
Beam). See [Compatibility](https://tosun-si.github.io/asgarde/project/compatibility/), including the versions before
`1.0.0` (one Asgarde release per Beam release).

## Roadmap

See the [roadmap](https://tosun-si.github.io/asgarde/project/roadmap/).

## Contributing

Contributions are welcome, see [CONTRIBUTING.md](CONTRIBUTING.md).

## License

[MIT](LICENSE)
