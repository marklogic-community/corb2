# Copilot instructions for `corb2`

## Build and test commands

- Use the Gradle wrapper from the repository root.
- Build the distributable fat jar: `./gradlew shadowJar`
- Run unit tests: `./gradlew test`
- Run integration tests: `./gradlew integrationTest`
- Run performance tests: `./gradlew performanceTest`
- Run the full verification pipeline used by the build: `./gradlew check`
- Generate coverage reports: `./gradlew jacocoTestReport`
- If MarkLogic is not available locally, use the CI-style verification command: `./gradlew check jacocoTestReport -PskipIntegrationTest -PskipPerformanceTest`

Single-test patterns:

- Run one unit test class: `./gradlew test --tests 'com.marklogic.developer.corb.ManagerTest'`
- Run one integration test class: `./gradlew integrationTest --tests 'com.marklogic.developer.corb.ManagerIT'`
- Run one performance test class: `./gradlew performanceTest --tests 'com.marklogic.developer.corb.ManagerPT'`

There is no dedicated lint task in this repository. Formatting is governed by `.editorconfig`, and CI quality checks are centered on `check`, JaCoCo, Sonar, and Coveralls.

## High-level architecture

- `com.marklogic.developer.corb.Manager` is the main CoRB entrypoint. It extends `AbstractManager`, loads options from CLI/system properties/`OPTIONS-FILE`, initializes XCC connections, and orchestrates the full job lifecycle: init task/module, URI loading, worker pool execution, monitoring, optional job server, and post-batch work.
- `TaskFactory` is the wiring hub for runtime behavior. It chooses the task class for each phase (`INIT`, `PRE-BATCH`, `PROCESS`, `POST-BATCH`) and resolves modules as:
  - installed module URIs under `MODULE-ROOT`
  - filesystem/classpath adhoc modules via `|ADHOC`
  - inline code via `INLINE-XQUERY|...` or `INLINE-JAVASCRIPT|...`
- `AbstractTask` contains the shared execution mechanics for task implementations: XCC request creation, request variable setup, retries, fail-on-error behavior, and error-file output. `Transform` is the default process/pre/post implementation when only a module is configured. File-oriented tasks such as `ExportToFileTask`, `ExportBatchToFileTask`, `PreBatchUpdateFileTask`, and `PostBatchUpdateFileTask` layer file handling on top of that base.
- URI acquisition is pluggable through the `UrisLoader` interface and `AbstractUrisLoader`. The built-in loaders cover MarkLogic query-based loading and client-side file/directory/XML/ZIP inputs. Loaders can set `URIS_BATCH_REF` and URI totals that are then exposed to pre/post batch processing.
- `ModuleExecutor` is a separate entrypoint for running a single XQuery or JavaScript module without the full CoRB batch lifecycle.
- Monitoring/UI is built into the main artifact. `JobServer` and `JobServicesHandler` serve the dashboard and metrics endpoints, while `src/main/resources/web` holds the static UI and `src/main/resources/jobStatsToJson.xsl` handles XML-to-JSON transformation for metrics output.
- MarkLogic deployment support is in `src/main/ml-config` and the root `build.gradle` configures ml-gradle to deploy only the XDBC-focused app resources; the REST API deploy command is explicitly removed.

## Key repository conventions

- CoRB option names are centralized in `Options.java`. If you add a new option, define it there and add its `@Usage` metadata so CLI/help/docs stay aligned with code.
- Property lookup intentionally accepts both kebab-case and underscore forms for option names. Keep using the canonical option constants (for example `PROCESS-MODULE`) instead of hard-coded string variants.
- Test partitioning is name-based, not directory-based. All tests live under `src/test/java`, and Gradle filters them by suffix:
  - unit tests: anything except `*IT` and `*PT`
  - integration tests: `*IT`
  - performance tests: `*PT`
- The published artifact is the shadow/fat jar. `jar` is disabled, `shadowJar` becomes the main output, and web assets plus `externalsortinginjava` are intentionally bundled into it.
- Web UI HTML in `src/main/resources/web` is tokenized during `processResources` so WebJar versions come from Gradle properties; do not hand-inline dependency versions into generated HTML.
- Module handling has a few important rules that show up across `README.md`, `TaskFactory`, and task code:
  - deployed XQuery modules use `.xqy`
  - deployed JavaScript modules use `.sjs`
  - adhoc JavaScript modules may use `.sjs` or `.js`
  - JavaScript modules that return multiple values must return a MarkLogic `Sequence`
- Custom inputs are passed by prefixing properties with the module type (for example `PROCESS-MODULE.startDate=...`). `URIS-MODULE` can also emit those prefixed values before the URI count so later phases receive dynamic inputs.
- The `examples/` project is the reference for local end-to-end usage. `./gradlew setup` provisions MarkLogic resources via ml-gradle and generates `corb.sh`/`corb.bat`; `./gradlew corb -PcorbOptionsFile=...` is the standard example execution path.
- The project targets Java 8 (`sourceCompatibility`/`targetCompatibility` are `1.8`). Keep dependency and language choices compatible with Java 8 unless the build is deliberately upgraded.
- Use the diamond operator `<>` on the right-hand side of generic type assignments instead of repeating the type (e.g., `List<String> list = new ArrayList<>()`, not `new ArrayList<String>()`).
- Do not rely upon default encoding in code or tests. Use explicit encodings (for example, `UTF-8`) when reading/writing files or streams.
- Unit tests must avoid external dependencies. They should use mocks, and focus on testing logic in isolation. Integration tests can use MarkLogic. Performance tests may use an external MarkLogic instance and can be more flexible with dependencies, but should be clearly marked and separated from the main verification pipeline. Try to avoid relying on MarkLogic converters.
