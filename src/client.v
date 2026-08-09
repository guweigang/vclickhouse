module vclickhouse

import strconv

const error_buffer_size = 4096

pub enum SslMode {
	disable
	require
}

pub struct Config {
pub:
	host               string = '127.0.0.1'
	port               int    = 9000
	user               string = 'default'
	username           string
	password           string
	dbname             string = 'default'
	ssl_mode           SslMode
	connect_timeout_ms int = 5_000
	read_timeout_ms    int = 30_000
	write_timeout_ms   int = 30_000
}

fn (c Config) connection_user() string {
	if c.username != '' {
		return c.username
	}
	return c.user
}

@[heap]
pub struct DB {
mut:
	conn   &C.vch_conn = unsafe { nil }
	closed bool
}

pub fn connect(config Config) !&DB {
	if config.ssl_mode == .require {
		return error('native TLS is not implemented yet; use ssl_mode: .disable')
	}
	if config.port < 1 || config.port > 65535 {
		return error('ClickHouse port must be between 1 and 65535')
	}
	user := config.connection_user()
	opts := C.vch_options{
		host:               config.host.str
		port:               u16(config.port)
		user:               user.str
		password:           config.password.str
		database:           config.dbname.str
		connect_timeout_ms: config.connect_timeout_ms
		read_timeout_ms:    config.read_timeout_ms
		write_timeout_ms:   config.write_timeout_ms
	}
	mut error_buffer := []u8{len: error_buffer_size}
	conn := C.vch_connect(&opts, &char(error_buffer.data), error_buffer.len)
	if isnil(conn) {
		return error(buffer_error(error_buffer, 'failed to connect to ClickHouse'))
	}
	return &DB{
		conn: conn
	}
}

pub fn (mut db DB) close() ! {
	if db.closed {
		return
	}
	if !isnil(db.conn) {
		C.vch_close(db.conn)
	}
	db.closed = true
	db.conn = unsafe { nil }
}

pub fn (mut db DB) ping() !bool {
	db.ensure_open()!
	mut error_buffer := []u8{len: error_buffer_size}
	if C.vch_ping(db.conn, &char(error_buffer.data), error_buffer.len) != 0 {
		return error(buffer_error(error_buffer, 'ClickHouse ping failed'))
	}
	return true
}

pub fn (db &DB) stream(query string) !&StreamResult {
	return db.stream_with_params(query, map[string]string{})
}

pub fn (db &DB) stream_with_params(query string, params map[string]string) !&StreamResult {
	db.ensure_open()!
	mut names := params.keys()
	names.sort()
	mut values := []string{cap: names.len}
	for name in names {
		values << params[name]
	}
	mut name_ptrs := []&char{cap: names.len}
	mut value_ptrs := []&char{cap: values.len}
	for name in names {
		name_ptrs << name.str
	}
	for value in values {
		value_ptrs << value.str
	}
	mut error_buffer := []u8{len: error_buffer_size}
	name_data := &&char(name_ptrs.data)
	value_data := &&char(value_ptrs.data)
	raw := C.vch_query(db.conn, query.str, name_data, value_data, names.len,
		&char(error_buffer.data), error_buffer.len)
	if isnil(raw) {
		return error(buffer_error(error_buffer, 'ClickHouse query failed'))
	}
	return &StreamResult{
		raw: raw
	}
}

pub fn (db &DB) query(query string) !Result {
	return db.query_with_params(query, map[string]string{})
}

pub fn (db &DB) query_with_params(query string, params map[string]string) !Result {
	mut stream := db.stream_with_params(query, params)!
	defer {
		stream.close()
	}
	mut rows := []Row{}
	for {
		if !stream.next()! {
			break
		}
		rows << stream.row()
	}
	return Result{
		cols:  stream.columns()
		names: stream.field_names()
		types: stream.type_names()
		rows:  rows
	}
}

pub fn (db &DB) exec(query string) ![]Row {
	return db.query(query)!.rows
}

pub fn (db &DB) exec_result(query string) !Result {
	return db.query(query)
}

pub fn (db &DB) exec_no_null(query string) ![]RowNoNull {
	return db.query(query)!.rows_no_null()
}

pub fn (db &DB) exec_one(query string) !Row {
	rows := db.exec(query)!
	if rows.len == 0 {
		return error('query returned no rows')
	}
	return rows[0]
}

pub fn (db &DB) exec_none(query string) int {
	db.query(query) or { return 1 }
	return 0
}

pub fn (db &DB) q_int(query string) !int {
	value := db.q_string(query)!
	return strconv.atoi(value) or { return error('cannot convert "${value}" to int') }
}

pub fn (db &DB) q_string(query string) !string {
	row := db.exec_one(query)!
	if row.vals.len == 0 {
		return error('query returned a row without columns')
	}
	return row.val(0)
}

pub fn (db &DB) q_strings(query string) ![]Row {
	return db.exec(query)
}

pub fn (db &DB) exec_param_many(query string, params []string) ![]Row {
	return db.exec_param_many_result(query, params)!.rows
}

pub fn (db &DB) exec_param_many_result(query string, params []string) !Result {
	bound_query, bound_params := bind_positional_params(query, params)!
	return db.query_with_params(bound_query, bound_params)
}

pub fn (db &DB) exec_param(query string, param string) ![]Row {
	return db.exec_param_many(query, [param])
}

pub fn (db &DB) exec_param2(query string, param string, param2 string) ![]Row {
	return db.exec_param_many(query, [param, param2])
}

pub fn (db &DB) prepare(query string) !StmtHandle {
	db.ensure_open()!
	return StmtHandle{
		db:    db
		query: query
	}
}

fn (db &DB) ensure_open() ! {
	if db.closed || isnil(db.conn) {
		return error('ClickHouse connection is closed')
	}
}

fn buffer_error(buffer []u8, fallback string) string {
	unsafe {
		message := cstring_to_vstring(&char(buffer.data))
		if message != '' {
			return message
		}
	}
	return fallback
}
