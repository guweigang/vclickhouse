module vclickhouse

pub struct Progress {
pub:
	rows       u64
	bytes      u64
	total_rows u64
}

@[heap]
pub struct StreamResult {
mut:
	raw        &C.vch_stream = unsafe { nil }
	block_row  int
	block_rows int
	names      []string
	types      []string
	current    Row
	closed     bool
pub mut:
	finished bool
}

// next advances to one row without materializing the complete result.
// Call row() after it returns true.
pub fn (mut s StreamResult) next() !bool {
	if s.closed || s.finished {
		return false
	}
	for s.block_row >= s.block_rows {
		if !s.load_block()! {
			return false
		}
	}
	mut vals := []?string{cap: s.names.len}
	for column in 0 .. s.names.len {
		mut value_len := usize(0)
		mut is_null := 0
		mut error_buffer := []u8{len: error_buffer_size}
		value := C.vch_stream_cell_text(s.raw, usize(s.block_row), usize(column), &value_len,
			&is_null, &char(error_buffer.data), error_buffer.len)
		if is_null != 0 {
			vals << none
		} else if isnil(value) {
			return error(buffer_error(error_buffer, 'could not decode ClickHouse value'))
		} else {
			// The C bridge reuses its scratch buffer for the next cell.
			vals << unsafe { value.vstring_with_len(int(value_len)).clone() }
		}
	}
	s.block_row++
	s.current = Row{
		vals: vals
	}
	return true
}

pub fn (s &StreamResult) row() Row {
	return s.current
}

pub fn (mut s StreamResult) fetch_batch(max_rows int) ![]Row {
	if max_rows < 1 {
		return error('max_rows must be greater than zero')
	}
	mut rows := []Row{cap: max_rows}
	for rows.len < max_rows {
		if !s.next()! {
			break
		}
		rows << s.row()
	}
	return rows
}

pub fn (s &StreamResult) field_names() []string {
	return s.names.clone()
}

pub fn (s &StreamResult) type_names() []string {
	return s.types.clone()
}

pub fn (s &StreamResult) fields() []Field {
	mut fields := []Field{cap: s.names.len}
	for i, name in s.names {
		fields << Field{
			name:      name
			type_name: s.types[i]
		}
	}
	return fields
}

pub fn (s &StreamResult) columns() map[string]int {
	mut columns := map[string]int{}
	for i, name in s.names {
		columns[name] = i
	}
	return columns
}

pub fn (s &StreamResult) progress() Progress {
	if isnil(s.raw) {
		return Progress{}
	}
	return Progress{
		rows:       C.vch_stream_progress_rows(s.raw)
		bytes:      C.vch_stream_progress_bytes(s.raw)
		total_rows: C.vch_stream_progress_total_rows(s.raw)
	}
}

pub fn (mut s StreamResult) cancel() ! {
	if s.closed || s.finished || isnil(s.raw) {
		return
	}
	mut error_buffer := []u8{len: error_buffer_size}
	if C.vch_stream_cancel(s.raw, &char(error_buffer.data), error_buffer.len) != 0 {
		return error(buffer_error(error_buffer, 'could not cancel ClickHouse query'))
	}
	s.finished = true
}

pub fn (mut s StreamResult) close() {
	if s.closed {
		return
	}
	if !isnil(s.raw) {
		C.vch_stream_close(s.raw)
	}
	s.raw = unsafe { nil }
	s.closed = true
	s.finished = true
}

fn (mut s StreamResult) load_block() !bool {
	mut error_buffer := []u8{len: error_buffer_size}
	for {
		status := C.vch_stream_next_block(s.raw, &char(error_buffer.data), error_buffer.len)
		if status < 0 {
			return error(buffer_error(error_buffer, 'could not read ClickHouse result block'))
		}
		if status == 0 {
			s.finished = true
			return false
		}
		columns := int(C.vch_stream_column_count(s.raw))
		if s.names.len == 0 && columns > 0 {
			s.read_fields(columns)
		}
		s.block_rows = int(C.vch_stream_row_count(s.raw))
		s.block_row = 0
		if s.block_rows > 0 {
			return true
		}
	}
	return false
}

fn (mut s StreamResult) read_fields(columns int) {
	for column in 0 .. columns {
		mut name_len := usize(0)
		mut type_len := usize(0)
		name := C.vch_stream_column_name(s.raw, usize(column), &name_len)
		type_name := C.vch_stream_column_type(s.raw, usize(column), &type_len)
		s.names << unsafe { name.vstring_with_len(int(name_len)) }
		s.types << unsafe { type_name.vstring_with_len(int(type_len)) }
	}
}
