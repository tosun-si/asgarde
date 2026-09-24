<!-- agents-md-manager:start — managed section, do not edit by hand -->
# Asgarde — Project context

> Project-level context for AI coding agents (GitHub Copilot, Claude Code,
> Cursor…). **Conventions live in the referenced skills; this file only holds what
> is NOT in a skill and NOT derivable from code.**

## Identity

- **Domain**: open-source library (Maven Central `fr.groupbees:asgarde`)
- **Project**: Asgarde
- **Purpose**: error handling and dead letter queues for Apache Beam Java/Kotlin pipelines
- **Repo layout**: standalone single-module Maven repo

## Conventions — source of truth (referenced, NOT copied)

- `commit-open-source` — commit message style for this personal open-source repo
- `tag-opensource` — semver `vX.Y.Z` tags, release-prep (pom version + README snippets); the tag must equal `v<pom version>` or the release workflow fails

## Project specifics (not in any skill, not derivable from code)

- **Beam decoupling (since 1.0.0)**: Beam is `provided`; `beam.version` in the pom is the compile BASELINE (minimum supported), NOT the latest. Never bump it just because Beam released — CI tests the latest Beam via `-Dbeam.version=<latest>` (push + weekly cron, opens an issue on regression). Release Asgarde only when a Beam release forces a code change.
- **Bytecode stays Java 8** (`release 8`) so users on Beam < 2.74 keep working; Beam >= 2.74 itself needs Java 11+, hence JDK 17/21 in the "latest" CI legs.
- **Kotlin stays on 1.9.x**: Kotlin 2.x metadata is unreadable by Kotlin 1.x consumers of the extensions; a Kotlin 2 move is a deliberate breaking release.
- **Public API is the product**: `CollectionComposer`, `Failure`, `*Fn` transforms and Kotlin extensions — keep changes backward compatible outside a major version.
<!-- agents-md-manager:end -->

<!-- Free zone — add durable, project-specific notes below. -->
<!-- Keep code-derivable facts (structure, deps, schedule) OUT. -->
