module vclickhouse

// Row is compatible with db.pg.Row and preserves ClickHouse NULL values.
pub struct Row {
pub mut:
	vals []?string
}

pub fn (r Row) val(idx int) string {
	return r.vals[idx] or { '' }
}

pub fn (r Row) val_opt(idx int) ?string {
	return r.vals[idx]
}

pub fn (r Row) values() []string {
	return r.vals.map(it or { '' })
}

pub struct RowNoNull {
pub mut:
	vals []string
}

pub struct Field {
pub:
	name      string
	type_name string
}

// Result mirrors the useful core of db.pg.Result while retaining ClickHouse types.
pub struct Result {
pub:
	cols  map[string]int
	names []string
	types []string
	rows  []Row
}

pub fn (r Result) n_rows() int {
	return r.rows.len
}

pub fn (r Result) n_fields() int {
	return r.names.len
}

pub fn (r Result) field_names() []string {
	return r.names.clone()
}

pub fn (r Result) fields() []Field {
	mut fields := []Field{cap: r.names.len}
	for i, name in r.names {
		fields << Field{
			name:      name
			type_name: if i < r.types.len { r.types[i] } else { '' }
		}
	}
	return fields
}

pub fn (r Result) row(index int) !Row {
	if index < 0 || index >= r.rows.len {
		return error('row index ${index} is out of range')
	}
	return r.rows[index]
}

pub fn (r Result) maps() []map[string]string {
	mut records := []map[string]string{cap: r.rows.len}
	for row in r.rows {
		mut record := map[string]string{}
		for i, name in r.names {
			if i < row.vals.len {
				record[name] = row.val(i)
			}
		}
		records << record
	}
	return records
}

pub fn (r Result) rows_no_null() []RowNoNull {
	return r.rows.map(RowNoNull{ vals: it.values() })
}
