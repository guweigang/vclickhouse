module vclickhouse

import os
import strconv

fn integration_config() ?Config {
	if os.getenv('VCLICKHOUSE_TEST') == '' {
		return none
	}
	port := strconv.atoi(os.getenv('VCLICKHOUSE_PORT')) or { 9000 }
	return Config{
		host:     os.getenv_opt('VCLICKHOUSE_HOST') or { '127.0.0.1' }
		port:     port
		user:     os.getenv_opt('VCLICKHOUSE_USER') or { 'default' }
		password: os.getenv('VCLICKHOUSE_PASSWORD')
		dbname:   os.getenv_opt('VCLICKHOUSE_DATABASE') or { 'default' }
	}
}

fn test_native_tcp_query_types_and_null() {
	config := integration_config() or { return }
	mut db := connect(config)!
	defer {
		db.close() or {}
	}
	assert db.ping()!
	result :=
		db.query("select toUInt64(42) as id, CAST(NULL, 'Nullable(String)') as missing, ['a', 'b'] as tags")!
	assert result.field_names() == ['id', 'missing', 'tags']
	assert result.types == ['UInt64', 'Nullable(String)', 'Array(String)']
	assert result.n_rows() == 1
	assert result.rows[0].val(0) == '42'
	assert result.rows[0].val_opt(1) == none
	assert result.rows[0].val(2) == "['a','b']"
}

fn test_native_query_parameters() {
	config := integration_config() or { return }
	mut db := connect(config)!
	defer {
		db.close() or {}
	}
	result := db.exec_param_many_result('select ? as first, ? as second',
		['hello', "V's ClickHouse"])!
	assert result.rows[0].values() == ['hello', "V's ClickHouse"]
}

fn test_native_stream_batches() {
	config := integration_config() or { return }
	mut db := connect(config)!
	defer {
		db.close() or {}
	}
	mut stream := db.stream('select number from numbers(5) settings max_block_size=2')!
	defer {
		stream.close()
	}
	first := stream.fetch_batch(3)!
	second := stream.fetch_batch(3)!
	assert first.map(it.val(0)) == ['0', '1', '2']
	assert second.map(it.val(0)) == ['3', '4']
	assert stream.finished
}
