# Contributing to Asgarde

Thanks for your interest in Asgarde! Bug reports, ideas and pull requests are welcome.

## Reporting a bug or proposing a feature

Open a [GitHub issue](https://github.com/tosun-si/asgarde/issues) with:

- the Asgarde, Beam and Java versions, and the runner (Direct, Dataflow, Flink, Spark...);
- a minimal pipeline reproducing the problem, and the expected vs actual behavior.

For a new feature, open an issue first to discuss the design before writing code.

## Development setup

- **JDK 17** (recommended) or any JDK 8+. Beam >= 2.74.0 needs Java 11+ to compile against.
- **Maven 3.9+**.

```bash
# Build and run the tests against the baseline Beam version (pom)
mvn verify

# Run the tests against another Beam version (e.g. the latest release)
mvn verify -Dbeam.version=2.76.0
```

The CI runs the tests on the baseline Beam version with JDK 8, and on the latest Beam release with JDK 17 and 21.

## Rules for code changes

- **Beam stays `provided`**: `beam.version` in the pom is the minimum supported Beam version, it's not bumped to
  follow Beam releases. Only use stable Beam APIs available in this baseline version.
- **No new runtime dependency**: Asgarde has no transitive dependency, keep it that way.
- **Java 8 bytecode and APIs**: the code is compiled with `release 8`, don't use APIs added after Java 8.
- **Kotlin stays on 1.9.x**, Kotlin 2 would break the Kotlin 1.x users of the extensions.
- **Backward compatibility**: the public API (`CollectionComposer`, `Failure`, the `*Fn` classes, the Kotlin
  extensions) must stay compatible in minor and patch versions. A breaking change needs a major version.
- **Tests**: every bug fix or feature comes with tests, following the existing style:
  `givenX_whenY_thenZ` method names and `// Given.`, `// When.`, `// Then.` blocks, with `TestPipeline` and
  `PAssert`.
- **Documentation**: update the `README.md` when the behavior or the public API changes.

## Pull requests

1. Fork the repo and create a branch from `main` (e.g. `feature/metrics-per-step`).
2. Use descriptive commit messages, e.g. `Add failure counters per pipeline step` (no Conventional Commits
   prefixes).
3. Open a pull request to `main` describing what changed and why, and how it was tested.
4. The CI must be green. Maintainers add the labels used to generate the release notes
   (`feature`, `bug`, `breaking-change`, `documentation`, `dependencies`, `security`...).

## Release process (maintainers)

1. Bump the version in `pom.xml` and in the installation snippets of `README.md`.
2. Merge to `main`, then tag it: `git tag -a vX.Y.Z -m "..." && git push origin vX.Y.Z`.
3. The `Release` workflow checks that the tag matches the pom version, publishes to Maven Central, then creates
   the GitHub Release with the notes generated from the pull request labels.

A new Beam version doesn't need an Asgarde release: the weekly CI run tests the latest Beam release and opens an
issue if something breaks.

## License

By contributing, you agree that your contributions are licensed under the [MIT License](LICENSE).
