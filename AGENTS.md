# AI Agent Guide for CoRB2

## What is CoRB2?

**CoRB** (Content Reprocessing in Bulk) is a Java tool for parallel batch processing of documents stored in MarkLogic. It orchestrates multi-threaded, distributed work with flexible task execution and real-time monitoring.

## Architecture Overview

### Job Execution Pipeline

CoRB follows a strict execution order managed by `Manager`:
1. **INIT-TASK/MODULE** — Optional one-time setup (via `TaskFactory.newInitTask()`)
2. **URIS-MODULE/URIS-FILE** — Load URIs to process (via pluggable `UrisLoader` interface)
3. **PRE-BATCH-TASK/MODULE** — One-time pre-processing (headers, setup)
4. **PROCESS-TASK/MODULE** — Run for each URI in parallel threads (`PausableThreadPoolExecutor`)
5. **POST-BATCH-TASK/MODULE** — One-time post-processing (footers, aggregation)

Each phase is orchestrated by `Manager` and configured via `TaskFactory`, which resolves modules and injects dependencies.

### Core Components

| Component | Role | Key Files |
|-----------|------|-----------|
| **Manager** | Main entry point; orchestrates full job lifecycle | `Manager.java`, `AbstractManager.java` |
| **TaskFactory** | Wiring hub; creates and configures task instances for each phase | `TaskFactory.java` |
| **AbstractTask** | Base class for all executable tasks; handles XCC request setup, retries, error reporting | `AbstractTask.java` |
| **UrisLoader** | Pluggable interface for URI acquisition (queries, files, directories, ZIP, XML) | `UrisLoader.java`, `AbstractUrisLoader.java`, `QueryUrisLoader.java`, `FileUrisDirectoryLoader.java`, etc. |
| **ContentSourcePool** | Manages XCC connections; supports load balancing and IP renewal | `ContentSourcePool.java`, `DefaultContentSourcePool.java` |
| **JobServer** | Embedded HTTP dashboard and metrics API | `JobServer.java`, `JobServicesHandler.java`, `src/main/resources/web/` |
| **Options** | Centralized option constants with `@Usage` metadata | `Options.java` (1600+ lines) |

## Key Conventions & Patterns

### Option Names (Critical!)

- **Always use constants from `Options.java`** instead of string literals (e.g., `Options.PROCESS_MODULE` not `"PROCESS-MODULE"`)
- Options accept kebab-case or underscore format (both `PROCESS-MODULE` and `PROCESS_MODULE` work)
- Precedence: command-line args > system properties > OPTIONS-FILE > defaults
- Custom inputs are passed by prefixing with module type: `PROCESS-MODULE.startDate=2025-01-01`

Example from tests:
```java
properties.setProperty(Options.PROCESS_MODULE, "transform.xqy|ADHOC");
properties.setProperty(Options.THREAD_COUNT, "4");
```

### Module Resolution

`TaskFactory` resolves modules in this order:
1. **Inline:** `INLINE-XQUERY|xquery code` or `INLINE-JAVASCRIPT|js code`
2. **Adhoc:** `path/to/file.xqy|ADHOC` (loaded from classpath/filesystem)
3. **Deployed:** `module-uri` (with MODULE-ROOT prefix, installed to MarkLogic)

Module extensions matter:
- **XQuery:** `.xqy` (deployed) or `.xqy|ADHOC` (adhoc)
- **JavaScript:** `.sjs` (deployed/adhoc) or `.js` (adhoc only)
- JavaScript modules returning multiple values **must use `Sequence`** return type

### Task Patterns

**File Export Tasks:**
- `ExportBatchToFileTask` — single file for all results (e.g., reports)
- `ExportToFileTask` — multiple files, one per URI
- `PreBatchUpdateFileTask` — write headers/static content
- `PostBatchUpdateFileTask` — write footers/aggregations; can ZIP output

**Custom Tasks:**
- Implement `Task` interface or extend `AbstractTask`
- TaskFactory injects ContentSourcePool, properties, and URIs automatically
- Use `AbstractTask.executeRequest()` to invoke MarkLogic modules with retry logic

### Plugins & Extensibility

- **Custom URI Loaders:** Extend `AbstractUrisLoader`, register via `URIS-LOADER` option
- **Custom Content Source Pools:** Implement `ContentSourcePool`, set via `CONTENT-SOURCE-POOL`
- **Custom Decrypters:** Implement `Decrypter`, set via `DECRYPTER` (for encrypted connection URIs/credentials)
- **Custom Comparators:** Implement `Comparator` for `EXPORT-FILE-SORT-COMPARATOR`

## Build & Test Commands

Use **Gradle wrapper** from repo root:

```bash
# Build distributable fat jar
./gradlew shadowJar

# Test commands
./gradlew test                              # Unit tests only (*Test.java)
./gradlew integrationTest                   # Integration tests (*IT.java)
./gradlew performanceTest                   # Performance tests (*PT.java)
./gradlew check                             # Full CI pipeline (unit + integration + JaCoCo)
./gradlew check -PskipIntegrationTest       # Without MarkLogic server (CI-style)

# Single test
./gradlew test --tests 'com.marklogic.developer.corb.ManagerTest'
./gradlew integrationTest --tests 'com.marklogic.developer.corb.ManagerIT'
```

**Test Partitioning:** Tests are split by suffix, not directory. All live in `src/test/java`, Gradle filters via class name.

## Configuration & Properties

All options are documented in `Options.java` with `@Usage` annotations. Key option families:

- **Connection:** `XCC-CONNECTION-URI`, `XCC-USERNAME`, `XCC-PASSWORD`, `XCC-DBNAME`, `XCC-TIME-ZONE`
- **Modules:** `INIT-MODULE`, `URIS-MODULE`, `PROCESS-MODULE`, `PRE-BATCH-MODULE`, `POST-BATCH-MODULE`
- **Tasks:** `INIT-TASK`, `PROCESS-TASK`, `PRE-BATCH-TASK`, `POST-BATCH-TASK`
- **Execution:** `THREAD-COUNT`, `BATCH-SIZE`, `BATCH-URI-DELIM`, `FAIL-ON-ERROR`
- **Export:** `EXPORT-FILE-NAME`, `EXPORT-FILE-DIR`, `EXPORT-FILE-SORT`, `EXPORT-FILE-SPLIT-MAX-LINES/SIZE`
- **Monitoring:** `JOB-SERVER-PORT`, `METRICS-DATABASE`, `METRICS-LOG-LEVEL`, `METRICS-MODULE`
- **Advanced:** `DISK-QUEUE`, `CONTENT-SOURCE-POOL`, `CONTENT-SOURCE-RENEW`, `CONNECTION-POLICY`

Example job.properties:
```ini
XCC-CONNECTION-URI=xcc://user:pass@localhost:8202/content-db
URIS-MODULE=selector.xqy|ADHOC
PROCESS-MODULE=transform.xqy|ADHOC
PROCESS-TASK=com.marklogic.developer.corb.ExportBatchToFileTask
EXPORT-FILE-NAME=output/results.txt
THREAD-COUNT=8
BATCH-SIZE=10
FAIL-ON-ERROR=false
```

## Development Workflow

### Adding a New Option

1. Define constant in `Options.java` with `@Usage` annotation
2. Add getter/setter in `TransformOptions.java`
3. Use via `Options.YOUR_OPTION` constant throughout codebase
4. Property lookup normalizes kebab/underscore automatically

### Implementing a Custom Task

1. Extend `AbstractTask` (has retry logic, error handling)
2. Override `processItem()` or `call()` for batch behavior
3. Use `getContentSourcePool()`, `getProperties()`, `getUris()` for injected state
4. Register via `PROCESS-TASK`, `INIT-TASK`, etc. option

### Testing Patterns

- Use `ManagerTest.getDefaultProperties()` as test fixture
- Mock `ContentSourcePool`, `ContentSource`, `Session` for unit tests
- Integration tests (`*IT`) can connect to real MarkLogic via `XCC-CONNECTION-URI` system property
- Mockito used for stubbing XCC request/response objects

## Important Files & Directories

| Path | Purpose |
|------|---------|
| `src/main/java/com/marklogic/developer/corb/` | Core classes |
| `src/main/resources/` | Bundled XQuery/JS modules (metrics, UI assets) |
| `src/main/resources/web/` | Job dashboard HTML/JS (tokenized during build) |
| `bin/test/` | Test resources (sample modules, config files) |
| `examples/` | End-to-end example jobs; run via `./gradlew setup` then `./gradlew corb -PcorbOptionsFile=...` |
| `build.gradle` | Build config; disables plain jar, enables shadowJar (fat jar) |
| `.editorconfig` | Formatting rules (no dedicated lint task) |

## Metrics & Monitoring

CoRB includes real-time job monitoring:
- **JobServer:** HTTP dashboard on configurable port (see `JOB-SERVER-PORT`)
- **Metrics:** Can log to MarkLogic error log (`METRICS-LOG-LEVEL`) or save as document (`METRICS-DATABASE`)
- **Custom Metrics Module:** Use `saveMetrics.sjs|ADHOC` to format as JSON instead of XML
- **Collections:** Metrics added to collection named after job (via `METRICS-COLLECTIONS`)

See `METRICS.md` for full options.

## Java Version & Dependencies

- **Target:** Java 8 (`sourceCompatibility`/`targetCompatibility` set to `1.8`)
- **Key deps:** MarkLogic XCC 12.0.1, externalsortinginjava, Alpine.js (UI)
- **Testing:** JUnit 5.14.3, Mockito 4.0.0 (5.x requires Java 11)

## CI & Quality

- **Quality checks:** `./gradlew check` runs unit tests, integration tests, JaCoCo code coverage
- **Sonar:** `sonar-project.properties` configured
- **Coverage:** JaCoCo reports in `build/reports/jacoco/`
- **No lint task:** Formatting enforced by `.editorconfig` + CI checks

## Quick Reference: Common Tasks

**Debug a failing test:**
```bash
./gradlew test --tests 'ManagerTest.testSomething' --info
```

**Check option documentation:**
- Read `Options.java` and search for `@Usage` on your option constant
- CLI help embedded in Manager via `--help` flag

**Run example job locally:**
```bash
cd examples
./gradlew setup                              # Deploy MarkLogic resources
./gradlew corb -PcorbOptionsFile=test/job.properties
```

**Generate coverage report:**
```bash
./gradlew check jacocoTestReport
# Reports in build/reports/jacoco/
```

