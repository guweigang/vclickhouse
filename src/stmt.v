module vclickhouse

import strings

// StmtHandle follows db.mysql's high-level prepared statement shape. ClickHouse
// native query parameters are used, so parameter data is never interpolated.
pub struct StmtHandle {
	db    &DB
	query string
}

pub fn (stmt &StmtHandle) execute(params []string) ![]Row {
	return stmt.execute_result(params)!.rows
}

pub fn (stmt &StmtHandle) execute_result(params []string) !Result {
	bound_query, bound_params := bind_positional_params(stmt.query, params)!
	return stmt.db.query_with_params(bound_query, bound_params)
}

pub fn (stmt &StmtHandle) close() {
	// Native query parameters are sent with each query and own no server resource.
}

enum SqlScanState {
	normal
	single_quote
	double_quote
	backtick
	line_comment
	block_comment
}

fn bind_positional_params(query string, params []string) !(string, map[string]string) {
	mut state := SqlScanState.normal
	mut builder := strings.new_builder(query.len + params.len * 12)
	mut param_index := 0
	mut i := 0
	for i < query.len {
		ch := query[i]
		next := if i + 1 < query.len { query[i + 1] } else { u8(0) }
		match state {
			.normal {
				if ch == `'` {
					state = .single_quote
				} else if ch == `"` {
					state = .double_quote
				} else if ch == u8(96) {
					state = .backtick
				} else if ch == `-` && next == `-` {
					state = .line_comment
				} else if ch == `/` && next == `*` {
					state = .block_comment
				} else if ch == `?` {
					if param_index >= params.len {
						return error('not enough parameters: placeholder ${param_index + 1} has no value')
					}
					param_index++
					builder.write_string('{p${param_index}:String}')
					i++
					continue
				}
			}
			.single_quote {
				if ch == `\\` && next != 0 {
					builder.write_u8(ch)
					builder.write_u8(next)
					i += 2
					continue
				}
				if ch == `'` {
					if next == `'` {
						builder.write_u8(ch)
						builder.write_u8(next)
						i += 2
						continue
					}
					state = .normal
				}
			}
			.double_quote {
				if ch == `"` {
					state = .normal
				}
			}
			.backtick {
				if ch == u8(96) {
					state = .normal
				}
			}
			.line_comment {
				if ch == `\n` {
					state = .normal
				}
			}
			.block_comment {
				if ch == `*` && next == `/` {
					builder.write_u8(ch)
					builder.write_u8(next)
					i += 2
					state = .normal
					continue
				}
			}
		}
		builder.write_u8(ch)
		i++
	}
	if param_index != params.len {
		return error('too many parameters: query has ${param_index} placeholders, got ${params.len}')
	}
	mut named := map[string]string{}
	for index, value in params {
		named['p${index + 1}'] = clickhouse_string_literal(value)
	}
	return builder.str(), named
}

fn clickhouse_string_literal(value string) string {
	mut builder := strings.new_builder(value.len + 2)
	builder.write_u8(`'`)
	for ch in value {
		if ch == `'` || ch == `\\` {
			builder.write_u8(`\\`)
		}
		builder.write_u8(ch)
	}
	builder.write_u8(`'`)
	return builder.str()
}
