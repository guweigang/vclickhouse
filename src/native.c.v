module vclickhouse

#flag -I @VMODROOT/src
#flag -I @VMODROOT/vendor/clickhouse-c
#flag @VMODROOT/src/vclickhouse.c
#flag windows -l ws2_32
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
	secure             int
	tls_verify         int
	tls_ca_file        &char
	tls_cert_file      &char
	tls_key_file       &char
}

fn C.vch_connect(&C.vch_options, &char, uint) &C.vch_conn
fn C.vch_close(&C.vch_conn)
fn C.vch_ping(&C.vch_conn, &char, uint) int
fn C.vch_query(&C.vch_conn, &char, &&char, &&char, uint, &char, uint) &C.vch_stream
fn C.vch_stream_next_block(&C.vch_stream, &char, uint) int
fn C.vch_stream_row_count(&C.vch_stream) uint
fn C.vch_stream_column_count(&C.vch_stream) uint
fn C.vch_stream_column_name(&C.vch_stream, uint, &uint) &char
fn C.vch_stream_column_type(&C.vch_stream, uint, &uint) &char
fn C.vch_stream_cell_text(&C.vch_stream, uint, uint, &uint, &i32, &char, uint) &char
fn C.vch_stream_progress_rows(&C.vch_stream) u64
fn C.vch_stream_progress_bytes(&C.vch_stream) u64
fn C.vch_stream_progress_total_rows(&C.vch_stream) u64
fn C.vch_stream_cancel(&C.vch_stream, &char, uint) int
fn C.vch_stream_close(&C.vch_stream)
