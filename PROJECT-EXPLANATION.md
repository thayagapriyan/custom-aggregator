# Custom Aggregator - Project Explanation

## What This Project Does

This is a **MuleSoft custom SDK extension** that provides **database-backed message aggregation** for Mule 4 applications. It collects individual messages over time or by group, stores them in a relational database, and when a completion condition is met (group size reached, time period expired, or eviction timeout), it delivers all collected messages as a single aggregated list to an `onComplete` route.

Unlike Mule's built-in in-memory aggregator (`mule-aggregators-module`), this extension **persists aggregation state to a database**, making it survive application restarts, cluster failovers, and memory constraints.

**All database configuration is bundled inside the extension JAR.** The user only selects the environment name (dev/sit/prod) in the config — no external DataSource, Spring beans, or JDBC parameters are needed.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│  Mule Application                                           │
│                                                             │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ Custom Aggregator Config                                │ │
│  │ - Environment (dev/sit/prod)                            │ │
│  │ - Aggregator Name                                       │ │
│  │ - Config Table Name (optional)                          │ │
│  │ - Items Table Name (optional)                           │ │
│  └──────────┬─────────────────────────────────────────────┘ │
│             │                                               │
│             ▼                                               │
│  ┌─────────────────────────┐                                │
│  │ ConnectionProvider       │                                │
│  │ reads db-config-{env}.yaml                               │
│  │ from extension JAR      │                                │
│  └──────────┬──────────────┘                                │
│             │                                               │
│     ┌───────┴────────┐                                      │
│     ▼                ▼                                      │
│  ┌──────────────┐ ┌──────────────┐                          │
│  │ Aggregate    │ │ Aggregate    │                          │
│  │ By Group     │ │ By Time      │                          │
│  └──────┬───────┘ └──────┬───────┘                          │
│         └───────┬────────┘                                  │
│                 ▼                                            │
│      ┌─────────────────────┐                                │
│      │  DbAggregatorStore  │                                │
│      └────────┬────────────┘                                │
│               ▼                                             │
│  ┌─────────────────────────────────────────────┐            │
│  │  Database                                   │            │
│  │  ┌──────────────────┐  ┌─────────────────┐  │            │
│  │  │ AGGREGATOR_CONFIG│  │ AGGREGATOR_ITEMS │  │            │
│  │  │ (1)──────────(N) │──│ via reference_id │  │            │
│  │  └──────────────────┘  └─────────────────┘  │            │
│  └─────────────────────────────────────────────┘            │
└─────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
custom-aggregator/
├── pom.xml                                                  # Maven build config
├── mule-artifact.json                                       # Mule runtime descriptor
├── src/main/java/com/priyan/router/internal/
│   ├── CustomaggregatorExtension.java                       # Extension entry point
│   ├── CustomaggregatorConfiguration.java                   # Config (env, name, tables)
│   ├── CustomaggregatorConnection.java                      # JDBC connection wrapper
│   ├── CustomaggregatorConnectionProvider.java              # Internal YAML-based provider
│   ├── DbConfigLoader.java                                  # YAML config file loader
│   └── aggregator/
│       ├── GroupBasedAggregator.java                        # Group-based operation
│       ├── TimeBasedAggregator.java                         # Time-based operation
│       ├── DbAggregatorStore.java                           # Database persistence layer
│       ├── AggregationAttributes.java                       # Output metadata model
│       ├── AggregationCompleteRoute.java                    # On-complete route
│       └── IncrementalAggregationRoute.java                 # Incremental route
└── src/main/resources/
    ├── db-config-dev.yaml                                   # Dev environment DB config
    ├── db-config-sit.yaml                                   # SIT environment DB config
    └── db-config-prod.yaml                                  # Prod environment DB config
```

---

## Database Connection: Internal YAML Configuration

The extension bundles all database configuration inside the JAR. No external setup is required from the user.

### YAML Config File Format

Each environment has a file named `db-config-{env}.yaml` under `src/main/resources/`:

```yaml
database:
  jdbcUrl: "jdbc:mysql://hostname:3306/database_name"
  driverClassName: "com.mysql.cj.jdbc.Driver"
  username: "db_user"
  password: "db_password"
  maxPoolSize: 10              # optional, default: 10
  minIdleConnections: 2        # optional, default: 2
  connectionTimeoutMs: 30000   # optional, default: 30000
  validationTimeoutSeconds: 5  # optional, default: 5
```

### How It Works

1. User sets `environment="dev"` in the `<custom-aggregator:config>` element
2. `CustomaggregatorConnectionProvider` calls `DbConfigLoader.load("dev")`
3. `DbConfigLoader` reads `db-config-dev.yaml` from the classpath
4. JDBC connection is established using the loaded URL, driver, username, and password

### Adding a New Environment

1. Create `src/main/resources/db-config-{newenv}.yaml` with the database section
2. Rebuild the extension (`mvn clean install`)
3. Use `environment="{newenv}"` in the Mule app config

### Mule XML Config (No External DB Setup Needed)

```xml
<custom-aggregator:config name="myAggConfig"
    environment="dev"
    configId="order-aggregator"/>
```

That's it — no Spring beans, no DataSource references, no JDBC parameters.

---

## Database Schema

### Table 1: `AGGREGATOR_CONFIG`

One row per aggregation group. Auto-managed by the extension.

```sql
CREATE TABLE AGGREGATOR_CONFIG (
    id                INT AUTO_INCREMENT PRIMARY KEY,
    aggregator_name   VARCHAR(255) NOT NULL,
    group_id          VARCHAR(255) NOT NULL,
    group_size        INT NOT NULL,
    is_complete       BOOLEAN DEFAULT FALSE,
    first_event_time  TIMESTAMP NULL,
    last_event_time   TIMESTAMP NULL,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_agg_config UNIQUE (aggregator_name, group_id)
);
```

### Table 2: `AGGREGATOR_ITEMS`

Individual payloads referencing a config row via foreign key.

```sql
CREATE TABLE AGGREGATOR_ITEMS (
    id                INT AUTO_INCREMENT PRIMARY KEY,
    reference_id      INT NOT NULL,
    items_payload     BLOB NOT NULL,
    sequence_number   INT NOT NULL,
    event_time        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_agg_items_config FOREIGN KEY (reference_id) REFERENCES AGGREGATOR_CONFIG(id)
);
CREATE INDEX idx_agg_items_ref ON AGGREGATOR_ITEMS (reference_id, sequence_number);
```

### Oracle DDL

```sql
CREATE TABLE AGGREGATOR_CONFIG (
    id                NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    aggregator_name   VARCHAR2(255) NOT NULL,
    group_id          VARCHAR2(255) NOT NULL,
    group_size        NUMBER NOT NULL,
    is_complete       NUMBER(1) DEFAULT 0,
    first_event_time  TIMESTAMP NULL,
    last_event_time   TIMESTAMP NULL,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_agg_config UNIQUE (aggregator_name, group_id)
);

CREATE TABLE AGGREGATOR_ITEMS (
    id                NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    reference_id      NUMBER NOT NULL,
    items_payload     BLOB NOT NULL,
    sequence_number   NUMBER NOT NULL,
    event_time        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_agg_items_config FOREIGN KEY (reference_id) REFERENCES AGGREGATOR_CONFIG(id)
);
CREATE INDEX idx_agg_items_ref ON AGGREGATOR_ITEMS (reference_id, sequence_number);
```

---

## File-by-File Details

### `CustomaggregatorExtension.java`

Extension entry point. Defines XML prefix `custom-aggregator` and extension display name.

| Annotation | Value | Effect |
|-----------|-------|--------|
| `@Xml(prefix)` | `custom-aggregator` | XML namespace: `<custom-aggregator:.../>` |
| `@Extension(name)` | `Custom-aggregator` | Anypoint Studio palette name |
| `@Configurations` | `CustomaggregatorConfiguration.class` | Registers the config element |

### `CustomaggregatorConfiguration.java`

Config element: `<custom-aggregator:config>`. The user only provides environment and aggregator name.

| Parameter | Display Name | Required | Default |
|-----------|-------------|----------|---------|
| `environment` | Environment | Yes | - |
| `configId` | Aggregator Name | Yes | - |
| `aggregatorConfigTable` | Aggregator Config Table | No | `AGGREGATOR_CONFIG` |
| `aggregatorItemsTable` | Aggregator Items Table | No | `AGGREGATOR_ITEMS` |

### `CustomaggregatorConnectionProvider.java`

Creates JDBC connections using database configuration loaded from internal YAML files. Uses `@Config` to access the environment parameter from `CustomaggregatorConfiguration`.

**Key behavior:**
- Loads YAML config on first `connect()` call via `DbConfigLoader`
- Loads JDBC driver class dynamically (`Class.forName`)
- Creates connections via `DriverManager.getConnection()`
- Sets `autoCommit = false` for transaction management
- Implements `CachedConnectionProvider` — Mule caches the connection

**Corner cases handled:**
- JDBC driver class not found → clear error with instructions
- DriverManager returns null → explicit error
- SQL connection failure → wrapped in `ConnectionException`
- Invalid YAML config → wrapped in `ConnectionException`
- Connection closed/invalid → validation catches it

### `DbConfigLoader.java`

Utility that loads and parses YAML config files from the classpath.

**Key behavior:**
- File naming convention: `db-config-{env}.yaml`
- Uses SnakeYAML to parse the YAML structure
- Validates all required properties (jdbcUrl, driverClassName, username, password)
- Provides sensible defaults for optional properties (maxPoolSize, etc.)
- Immutable — all fields set at construction time

**Corner cases handled:**
- Null/empty environment → `IllegalArgumentException`
- Config file not found → clear error with file name and instructions
- Missing `database` root key → descriptive error
- Missing required properties → error naming the missing property
- Invalid integer values → warning logged, default used

### `CustomaggregatorConnection.java`

Thin wrapper around `java.sql.Connection`. Safe `invalidate()` method (idempotent, swallows exceptions). `getId()` handles closed/null connections gracefully.

### `GroupBasedAggregator.java`

Operation: `<custom-aggregator:aggregate-by-group>`

| Parameter | Type | Required | Default |
|-----------|------|----------|---------|
| `groupId` | String | Yes | - |
| `groupSize` | int | Yes | - |
| `evictionTime` | int | No | `0` |
| `evictionTimeUnit` | TimeUnit | No | `SECONDS` |
| `content` | Object | Yes (body) | - |
| `onComplete` | Route | Yes | - |
| `incremental` | Route | No | - |

**Corner cases handled:**
- Null/empty groupId → `callback.error()` with descriptive message
- groupSize <= 0 → `callback.error()` with descriptive message
- Null content → `callback.error()` with descriptive message
- SQL failure → rollback + `callback.error()`
- Rollback failure → logged but doesn't mask original error
- Incremental route failure → logged as warning, doesn't fail the operation
- onComplete route failure → logged and forwarded to `callback.error()`
- Zero items at completion → warning logged
- Concurrent insert race condition → handled in `DbAggregatorStore`

### `TimeBasedAggregator.java`

Operation: `<custom-aggregator:aggregate-by-time>`

| Parameter | Type | Required | Default |
|-----------|------|----------|---------|
| `aggregatorName` | String | Yes | - |
| `period` | int | Yes | - |
| `periodUnit` | TimeUnit | Yes | - |
| `maxSize` | int | No | `0` |
| `content` | Object | Yes (body) | - |
| `onComplete` | Route | Yes | - |
| `incremental` | Route | No | - |

Same corner case handling as GroupBasedAggregator. Additional validations: period must be positive, periodUnit must not be null, maxSize must be >= 0.

### `DbAggregatorStore.java`

Database persistence layer with full validation and enterprise error handling.

**Corner cases handled:**
- Table name SQL injection → regex validation (`^[A-Za-z_][A-Za-z0-9_.]{0,127}$`)
- Null/blank aggregatorName or groupId → `IllegalArgumentException`
- Null/empty payload → `IllegalArgumentException`
- Concurrent config insert (race condition) → catches duplicate key exception (handles ANSI SQL state `23xxx`, Oracle `ORA-00001`, MySQL `1062`) and retries with SELECT
- INSERT returns no generated key → explicit `SQLException`
- Config row already deleted by concurrent process → warning logged
- Zero rows updated on markComplete → warning logged
- Null/corrupt payload bytes → skipped with warning, doesn't fail the batch
- Empty deserialization result → returns unmodifiable empty list

### `AggregationAttributes.java`

Immutable metadata model. Available in DataWeave as `attributes.*`:
- `attributes.aggregationId` — group ID or aggregator name
- `attributes.firstItemArrivalTime` — `java.util.Date`
- `attributes.lastItemArrivalTime` — `java.util.Date`
- `attributes.isAggregationComplete` — always `true` on completion
- `attributes.itemCount` — number of collected items

### `AggregationCompleteRoute.java` / `IncrementalAggregationRoute.java`

Marker route classes extending Mule SDK `Route`. No parameters.

---

## Mule XML Usage Examples

### Group-Based Aggregation

```xml
<custom-aggregator:config name="myAggConfig"
    environment="dev"
    configId="order-aggregator"/>

<custom-aggregator:aggregate-by-group groupId="#[correlationId]" groupSize="5"
    evictionTime="30" evictionTimeUnit="SECONDS" config-ref="myAggConfig">
    <custom-aggregator:content>#[payload]</custom-aggregator:content>
    <custom-aggregator:aggregate-by-group-on-complete>
        <logger message="Group complete: #[sizeOf(payload)] items, id=#[attributes.aggregationId]"/>
        <foreach>
            <logger message="Item: #[payload]"/>
        </foreach>
    </custom-aggregator:aggregate-by-group-on-complete>
    <custom-aggregator:aggregate-by-group-incremental>
        <logger level="DEBUG" message="Item added to group"/>
    </custom-aggregator:aggregate-by-group-incremental>
</custom-aggregator:aggregate-by-group>
```

### Time-Based Aggregation

```xml
<custom-aggregator:config name="myAggConfig"
    environment="prod"
    configId="batch-processor"/>

<custom-aggregator:aggregate-by-time aggregatorName="batch-collector"
    period="60" periodUnit="SECONDS" maxSize="100" config-ref="myAggConfig">
    <custom-aggregator:content>#[payload]</custom-aggregator:content>
    <custom-aggregator:aggregate-by-time-on-complete>
        <logger message="Time batch: #[sizeOf(payload)] items"/>
    </custom-aggregator:aggregate-by-time-on-complete>
</custom-aggregator:aggregate-by-time>
```

---

## Build and Deploy

```bash
mvn clean install        # Build the extension
mvn deploy               # Publish to Anypoint Exchange
```

Mule app dependency:
```xml
<dependency>
    <groupId>be53b1bc-dcfe-4956-8984-bab672e25261</groupId>
    <artifactId>custom-aggregator</artifactId>
    <version>1.0.0</version>
    <classifier>mule-plugin</classifier>
</dependency>
```
