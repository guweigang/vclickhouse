module vclickhouse

#flag -I @VMODROOT/src
#flag -I @VMODROOT/vendor/clickhouse-c
#flag @VMODROOT/src/vclickhouse.c
#include "vclickhouse.h"

struct C.vch_conn {}

struct C.vch_stream {}

@[typedef]
struct C.vch_options {
	host               &char
	port               u16
	user               &char
	password           &char
	database           &char
	connect_timeout_ms int
	read_timeout_ms    int
	write_timeout_ms   int
}

fn C.vch_connect(&C.vch_options, &char, usize) &C.vch_conn
fn C.vch_close(&C.vch_conn)
fn C.vch_ping(&C.vch_conn, &char, usize) int
fn C.vch_query(&C.vch_conn, &char, &&char, &&char, usize, &char, usize) &C.vch_stream
fn C.vch_stream_next_block(&C.vch_stream, &char, usize) int
fn C.vch_stream_row_count(&C.vch_stream) usize
fn C.vch_stream_column_count(&C.vch_stream) usize
fn C.vch_stream_column_name(&C.vch_stream, usize, &usize) &char
fn C.vch_stream_column_type(&C.vch_stream, usize, &usize) &char
fn C.vch_stream_cell_text(&C.vch_stream, usize, usize, &usize, &int, &char, usize) &char
fn C.vch_stream_progress_rows(&C.vch_stream) u64
fn C.vch_stream_progress_bytes(&C.vch_stream) u64
fn C.vch_stream_progress_total_rows(&C.vch_stream) u64
fn C.vch_stream_cancel(&C.vch_stream, &char, usize) int
fn C.vch_stream_close(&C.vch_stream)
