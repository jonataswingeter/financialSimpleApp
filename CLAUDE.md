# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`transfer-service`: a Spring Boot 4.1 / Java 25 service for asynchronous internal bank transfers. It accepts a transfer over HTTP, writes the transfer and an outbox event in one PostgreSQL transaction, relays the event to Kafka, and a Kafka listener moves the money. It is built as a hexagonal (ports & adapters) application, and ArchUnit enforces the boundaries.

## Commands

Use the installed `mvn`. `./mvnw` is broken because `.mvn/wrapper/` is not in the repo.

```bash
mvn test                                   # all tests (integration tests need Docker running)
mvn verify                                 # tests + formatter check + Checkstyle — run before committing
mvn spotless:apply                         # format everything (fixes all formatter violations)
mvn checkstyle:check                       # lint only
mvn test -Dtest=TransferTest               # one test class
mvn test -Dtest='TransferTest$Execute'     # one @Nested class
mvn test -Dtest=OutboxRelayServiceTest#shouldScheduleRetryWithBackoffWhenPublishingFails   # one method
mvn verify -Dtest='TransferTest,OutboxRelayServiceTest,ArchitectureTest' -Dsurefire.failIfNoSpecifiedTests=false   # everything that runs without Docker
```

Run locally:
- **Testcontainers:** run the `TestTransferServiceApplication` main class under `src/test`. It starts Postgres and Kafka in containers.
- **Against `compose.yaml`:** `docker compose up -d`, then run `TransferServiceApplication`. Compose starts Postgres 18 and a 3-controller/3-broker KRaft Kafka cluster on `localhost:29092,39092,49092`.

## Formatting and lint

- **Formatter:** Spotless with google-java-format, Google style. That means 2-space indentation, 4-space continuation and 100 columns. It also does import ordering and removes unused imports.
- **Edit with this style:** write new code in it and run `mvn spotless:apply` after editing. The VS Code `redhat.java` formatter does **not** produce the same output, so don't rely on it.
- **google-java-format is pinned to 1.36.1.** Version 1.37.0 crashes inside Spotless 3.10.4, with an `InvocationTargetException` on every file. Only bump it together with a newer Spotless.
- **Linter:** Checkstyle, with rules in `config/checkstyle/checkstyle.xml` and suppressions in `suppressions.xml` next to it. They are written to agree with google-java-format, so a lint failure is a real issue and not a formatting conflict.
  - Star imports are banned; static star imports are allowed.
  - Loggers are named `log`/`logger`. All other constants are `UPPER_SNAKE`.
- `.editorconfig` sets 2 spaces, LF line endings and UTF-8 for all files.

## Architecture

Everything lives under `com.financialsimpleapp.transfer_service`:

```
transfer/domain/                 pure Java — no Spring, Jackson, JDBC (ArchUnit-enforced)
transfer/application/port/in/    use-case interfaces + commands, results, and the exceptions they throw
transfer/application/port/out/   repository / publisher interfaces the application needs
transfer/application/service/    use-case implementations
transfer/adapter/in/web/         REST controller, DTOs, exception handler
transfer/adapter/in/messaging/   Kafka listener, @Scheduled outbox trigger, topic + DLT config
transfer/adapter/out/persistence JdbcTemplate repositories (no JPA)
transfer/adapter/out/messaging/  Kafka publisher
config/                          composition root (beans, settings) — exempt from layer rules
```

Dependency rules, all checked by `src/test/.../ArchitectureTest.java`:
- Adapters depend only on **ports** and the domain, never on `application.service`.
- Implementations of `port.in` live in `application.service`; implementations of `port.out` live in `adapter.out`.
- The application layer must not import web, Kafka, JDBC, servlet or Jackson classes. `@Service` and `@Transactional` are the deliberate exceptions.
- Adding a new adapter package means adding it to the `onionArchitecture()` rule in that test.

### The transfer flow (spans many files)

1. **Accept.** `POST /api/v1/transfers` is called with the `X-Client-Id` and `Idempotency-Key` headers. `TransferService.acceptTransfer` then runs **one transaction** that:
   - inserts the transfer (status `ACCEPTED`);
   - reserves the idempotency key with `INSERT … ON CONFLICT DO NOTHING`;
   - inserts a `TransferAcceptedEvent` row into `outbox_events`.

   The response is `202 Accepted` with a `Location` header. If the key is already taken, the outcome depends on the SHA-256 request hash:
   - **Same hash:** the candidate transfer is deleted and the original is returned with `replayed=true`.
   - **Different hash:** `IdempotencyConflictException` is thrown, which becomes `409`.
2. **Relay.** `OutboxRelayScheduler` (`@Scheduled`) calls `OutboxRelayService` through `RelayOutboxEventsUseCase`. The relay:
   - claims a batch with `FOR UPDATE SKIP LOCKED` and sets a lease (`locked_until` + `lock_token`);
   - publishes each event through `TransferEventPublisher`, waiting for the broker to confirm;
   - marks the event published only after that confirmation;
   - on a send failure, schedules a retry with exponential backoff.

   The relay is **deliberately not `@Transactional`**: Kafka must never be called while a DB transaction is open. Claim, mark and retry each run in their own short transaction via `OutboxTransactionService`. A lost or expired lease means the event is re-sent, so delivery is **at-least-once**.
3. **Process.** `TransferAcceptedListener` deserializes the event and calls `ProcessTransferUseCase`. `TransferProcessingService` then:
   - locks the transfer row and both account rows (`FOR UPDATE`, accounts ordered by id to avoid deadlocks);
   - returns early if the transfer is already final, which makes duplicate deliveries harmless;
   - calls `Transfer.execute(source, destination)`;
   - saves the balances, the ledger entries and the outcome.

   The outcome `UPDATE`s are guarded with `WHERE status = 'ACCEPTED'`. If processing still fails, it is retried 3 times and then sent to `<topic>.DLT`.

### Where business rules live

`Transfer.execute` in the domain decides the outcome, applying these checks in order:
1. source account active;
2. destination account active;
3. currencies match;
4. sufficient funds.

It either rejects with a `RejectionReason` or debits and credits the `Account`s and returns the `LedgerEntry`s to persist. The application service only orchestrates: load, call the domain, save.

Keep new rules in the domain, not in services or SQL. The one exception is a missing account: that is a lookup result, so the service checks it and calls `transfer.rejectForMissingAccount()`.

- `PROCESSING` is a domain-only intermediate state. It is never written to the database, because the final state is committed in the same transaction.
- Rejection codes and ledger entry types are stored as the enum `name()`.

### Event contract

`domain/TransferAcceptedEvent` is the single definition of the Kafka payload (`{"eventId","transferId"}`, type `TRANSFER_ACCEPTED`):
- `JdbcOutboxRepository` serializes it into `outbox_events.payload`;
- the listener deserializes the same record.

Change it in one place, and keep it backward compatible: rows already in the outbox and messages already in Kafka still use the old shape. The Kafka key is the transfer ID, so all events of one transfer go to the same partition.

### Schema

The schema is defined only by the Flyway migrations in `src/main/resources/db/migration`. Add a new `V<n>__*.sql`; never edit applied migrations. The database also enforces invariants that the code relies on:
- non-negative balances;
- the `status`/`completed_at` consistency check;
- the ledger sign by entry type, unique per `(transfer_id, entry_type)`;
- the outbox lock columns being both set or both null.

## Testing

| Test | Kind | What it covers |
|---|---|---|
| `domain/TransferTest` | plain unit | domain rules and state machine |
| `application/service/OutboxRelayServiceTest` | Mockito unit | relay: claim, publish, mark, retry/backoff, lease |
| `application/service/TransferServiceIntegrationTest` | `@SpringBootTest` + Testcontainers | acceptance, idempotency, concurrency, rollback when the outbox insert fails |
| `application/service/OutboxRelayIntegrationTest` | `@SpringBootTest`, mocked publisher | outbox SQL against real Postgres: lease expiry, stale lock tokens, concurrent relays, backoff stored in `next_attempt_at`, send order |
| `application/service/TransferProcessingConcurrencyIntegrationTest` | `@SpringBootTest` + Testcontainers | parallel processing on shared accounts: no overdraft, no deadlock, ledger consistent |
| `transfer/TransferEndToEndIntegrationTest` | full end-to-end | HTTP → outbox → Kafka → listener: completion, rejection, duplicate delivery, broker outage, rollback + Kafka retry, DLT, message key; uses `RestTestClient` + Awaitility |
| `ArchitectureTest` | ArchUnit | layer boundaries |

Things that are easy to get wrong:
- **Docker:** every `@SpringBootTest` imports `TestcontainersConfiguration` (Postgres 16 + single-node `apache/kafka-native`) and needs Docker running.
- **Outbox scheduling is off by default in tests** (`app.outbox.scheduling.enabled: false` in `src/test/resources/application.yaml`). Tests that need the relay turn it back on, as the end-to-end test does.
- **`src/test/resources/application.yaml` replaces the main one; it doesn't add to it.** Every setting it leaves out falls back to the `@Value` default in code. For example, `batch-size` is 100 in tests, not 20. Tests read `OutboxRelaySettings` instead of hard-coding these values.
- **Single-broker Kafka:** the test config sets `app.kafka.replication-factor: 1` and `app.kafka.min-insync-replicas: 1`. With the production defaults (3 and 2) and `acks=all`, every send fails against one broker.
- **Consumer offset:** end-to-end tests set `spring.kafka.consumer.auto-offset-reset=earliest`. Otherwise the listener can join after the event was published and miss it.
- **Test data:** tests clean the tables themselves in `@BeforeEach`, deleting children first: `outbox_events`, `ledger_entries`, `idempotency_records`, `transfer_requests`, `accounts`.
- **Container reuse:** a test class with different `properties` gets its own Spring context, and therefore its own containers.
- **Stale classes from the IDE:** the VS Code Java extension compiles into `target/` too. After editing, a plain `mvn test` can run a mix of IDE- and Maven-compiled classes, which shows up as tests behaving as if a change were not there. Use `mvn clean test` / `mvn clean verify` when results look wrong, and always after reverting a change to `src/main`.
- **Time in tests:** don't sleep for leases or backoff. Move `locked_until` / `next_attempt_at` into the past with SQL instead (see `OutboxRelayIntegrationTest`).

## Other gotchas

- **Two Jacksons:** the app defines its own Jackson 2 `ObjectMapper` bean (`config/JacksonConfig`, `com.fasterxml`). The listener and the outbox repository use it. Spring Boot 4's HTTP layer uses Jackson 3 (`tools.jackson`).
- **Outbox settings:** these are `app.outbox.*` properties, bound into `OutboxRelaySettings` by `config/OutboxRelayConfig`. The lease is automatically raised to at least `send-timeout × batch-size + 30s`.
- **Package name:** it is `transfer_service`, with an underscore, because the hyphenated name is invalid in Java.
- **Portuguese comments:** some code comments are in Portuguese. Keep them when moving that code.
