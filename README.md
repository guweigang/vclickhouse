# vclickhouse

Native TCP ClickHouse client for V. It uses the official
[`ClickHouse/clickhouse-c`](https://github.com/ClickHouse/clickhouse-c) protocol
library rather than ClickHouse HTTP.

The core API deliberately follows V's `db.mysql` and `db.pg` modules:

- `Config`, `DB`, `connect`, `close`, and `ping`
- `Row`, `RowNoNull`, `Result`, and `Field`
- `query`, `exec`, `exec_result`, `exec_one`, `q_int`, and `q_string`
- `exec_param_many`, `exec_param`, `exec_param2`, and `StmtHandle`
- `StreamResult` for bounded-memory block/row iteration

`Row.vals` uses `[]?string`, matching `db.pg.Row`, so SQL NULL remains distinct
from an empty string. Native ClickHouse types are exposed in `Result.types` and
`Field.type_name`.

## Requirements

- V
- a C compiler

The first release uses uncompressed Native TCP blocks, so it does not add LZ4
or Zstandard runtime dependencies to applications that link it.

## Usage

```v
import vclickhouse

mut db := vclickhouse.connect(vclickhouse.Config{
	host: '127.0.0.1'
	port: 9000
	user: 'default'
	password: '<password>'
	dbname: 'default'
})!
defer {
	db.close() or {}
}

result := db.query('select number, toString(number) as label from numbers(3)')!
for row in result.rows {
	println(row.values())
}
```

### Streaming

`stream()` retains one Native TCP query on the connection and consumes server
blocks lazily. A connection can have only one active stream; close or fully
consume it before issuing the next query on that connection.

```v
mut rows := db.stream('select number from numbers(1000000)')!
defer {
	rows.close()
}

for rows.next()! {
	println(rows.row().val(0))
}
```

For batch-oriented consumers, use `fetch_batch(max_rows)`. `progress()` exposes
the latest rows/bytes/total-rows counters reported by ClickHouse, and `cancel()`
sends a Native protocol cancellation packet.

### Parameters

The MySQL-style helpers accept positional `?` placeholders and bind each value
as a ClickHouse `String` query parameter:

```v
rows := db.exec_param_many(
	'select ? as first, ? as second',
	['hello', "V's ClickHouse"],
)!
```

For explicitly typed ClickHouse parameters, use `query_with_params`. Values for
that low-level API are ClickHouse literals:

```v
result := db.query_with_params(
	'select {limit:UInt64}',
	{'limit': '10'},
)!
```

## Tests

Unit tests:

```sh
v test src
```

Native TCP integration tests are opt-in and read all credentials from the
environment:

```sh
VCLICKHOUSE_TEST=1 \
VCLICKHOUSE_HOST=127.0.0.1 \
VCLICKHOUSE_PORT=9000 \
VCLICKHOUSE_USER=default \
VCLICKHOUSE_PASSWORD='<password>' \
VCLICKHOUSE_DATABASE=default \
v test src/integration_test.v
```

The current transport is POSIX Native TCP. `ssl_mode: .require` returns an
explicit unsupported error until the TLS transport is wired in.
