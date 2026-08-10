module vclickhouse

fn test_row_preserves_nulls() {
	row := Row{
		vals: [?string('hello'), none, ?string('')]
	}
	assert row.val(0) == 'hello'
	assert row.val(1) == ''
	assert row.val_opt(1) == none
	assert row.val_opt(2)? == ''
	assert row.values() == ['hello', '', '']
}

fn test_result_metadata_and_maps() {
	result := Result{
		cols:  {
			'id':   0
			'name': 1
		}
		names: ['id', 'name']
		types: ['UInt64', 'Nullable(String)']
		rows:  [Row{ vals: [?string('42'), none] }]
	}
	assert result.n_rows() == 1
	assert result.n_fields() == 2
	assert result.field_names() == ['id', 'name']
	assert result.fields()[1].type_name == 'Nullable(String)'
	assert result.maps() == [{
		'id':   '42'
		'name': ''
	}]
	assert result.row(0)!.val(0) == '42'
}

fn test_positional_binding_skips_literals_and_comments() {
	query := "select ?, '?', `?` -- ?\n, ? /* ? */"
	bound, params := bind_positional_params(query, ['one', 'two'])!
	assert bound == "select {p1:String}, '?', `?` -- ?\n, {p2:String} /* ? */"
	assert params == {
		'p1': "'one'"
		'p2': "'two'"
	}
}

fn test_clickhouse_string_literal() {
	assert clickhouse_string_literal("V's \\ ClickHouse") == "'V\\'s \\\\ ClickHouse'"
}

fn test_positional_binding_checks_arity() {
	if _, _ := bind_positional_params('select ?, ?', ['one']) {
		assert false
	} else {
		assert err.msg().contains('not enough parameters')
	}
	if _, _ := bind_positional_params('select ?', ['one', 'two']) {
		assert false
	} else {
		assert err.msg().contains('too many parameters')
	}
}

fn test_tls_client_certificate_requires_matching_key() {
	connect(Config{
		tls_cert_file: 'client.crt'
	}) or {
		assert err.msg() == 'tls_cert_file and tls_key_file must be configured together'
		return
	}
	assert false
}

fn test_tls_reports_required_build_flag_before_connecting() {
	$if !vclickhouse_openssl ? {
		connect(Config{
			ssl_mode: .require
		}) or {
			assert err.msg() == 'native TLS requires rebuilding with -d vclickhouse_openssl'
			return
		}
		assert false
	}
}
