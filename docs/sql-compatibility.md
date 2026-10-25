# SQL compatibility and execution limits

PostgreSQL SQL and protocol compatibility is incremental. The differential suite runs the same queries against PostgreSQL 16 and compares values, column names, type OIDs, row counts, command tags and SQLSTATEs. Passing those cases is evidence for that coverage; it is not a full PostgreSQL conformance claim.

The relational executor supports scans, primary-key lookup pushdown, inner and outer ON joins, SQL three-valued predicates, grouping, COUNT/MIN/MAX (including DISTINCT), DISTINCT results, ordering, limits and offsets. Integer expressions retain PostgreSQL SMALLINT/INTEGER/BIGINT result widths and report overflow. Text ordering is by Rust string ordering, rather than a PostgreSQL locale/collation implementation.

Multi-row INSERT, expression UPDATE, predicate DELETE and RETURNING run in distributed transactions. Assignment expressions see each row's original values. Missing rows do not trigger assignment constraints. Later-row errors roll back earlier staged modifications. Single-row primary-key updates can move a tuple across shards. Multiple-row key changes are rejected before modification.

Tables have unquoted ASCII names, one inline integer primary key, and SMALLINT, INTEGER, BIGINT, TEXT or BOOLEAN columns. Constraints are primary-key uniqueness and NULL/NOT NULL. PostgreSQL catalogs, schemas, secondary indexes, non-primary unique constraints, foreign keys, defaults, sequences, generated columns, ALTER/DROP, views and triggers remain pending.

Expressions currently include checked integer arithmetic, comparisons, AND/OR/NOT, IS NULL, IN, BETWEEN, supported-type casts, concatenation, LOWER/UPPER/LENGTH/CHAR_LENGTH/COALESCE/ABS, and the listed aggregates. Numeric/float/date/JSON/array types, SUM/AVG, CASE, LIKE, window functions, subqueries, CTEs, set operations and JOIN USING/NATURAL remain pending.

Each scan is paginated at 1,024 rows, with a query limit of 100,000 input/intermediate rows and 64 MiB. Joins cap candidate comparisons at five million, and CPU work runs on four admitted blocking tasks. A mutation can modify at most 4,096 rows; RETURNING and cached portal results are limited to 16 MiB per session. Protocol frames are limited to one MiB and parameters to 128. Quota failures return 54000 and abort the affected transaction.

Extended-query Execute supports partial results with PortalSuspended and resumes without replaying mutations. Mutations remain staged until Sync unless an explicit transaction is active. Simple queries currently accept one statement per request. Savepoints, transactional DDL, COPY, cancellation requests, authentication roles, SCRAM and TLS remain pending.

Prepared parameter types are inferred from supported expressions. Some PostgreSQL overload and unknown-literal resolution rules are not implemented, including independently supplied parameter OIDs for a bare untyped parameter. The supported client matrix currently consists of Psycopg 3 and the tokio-postgres benchmark path. ORM compatibility is not yet established.
