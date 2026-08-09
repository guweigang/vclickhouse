#ifndef VCLICKHOUSE_H
#define VCLICKHOUSE_H

#include <stddef.h>
#include <stdint.h>

typedef struct vch_conn vch_conn;
typedef struct vch_stream vch_stream;

typedef struct vch_options {
    const char *host;
    uint16_t port;
    const char *user;
    const char *password;
    const char *database;
    int connect_timeout_ms;
    int read_timeout_ms;
    int write_timeout_ms;
} vch_options;

vch_conn *vch_connect(const vch_options *opts, char *error, size_t error_len);
void vch_close(vch_conn *conn);
int vch_ping(vch_conn *conn, char *error, size_t error_len);
vch_stream *vch_query(vch_conn *conn, const char *sql,
                      const char *const *param_names,
                      const char *const *param_values,
                      size_t param_count,
                      char *error, size_t error_len);
int vch_stream_next_block(vch_stream *stream, char *error, size_t error_len);
size_t vch_stream_row_count(const vch_stream *stream);
size_t vch_stream_column_count(const vch_stream *stream);
const char *vch_stream_column_name(const vch_stream *stream, size_t column, size_t *len);
const char *vch_stream_column_type(const vch_stream *stream, size_t column, size_t *len);
const char *vch_stream_cell_text(vch_stream *stream, size_t row, size_t column,
                                 size_t *len, int *is_null,
                                 char *error, size_t error_len);
uint64_t vch_stream_progress_rows(const vch_stream *stream);
uint64_t vch_stream_progress_bytes(const vch_stream *stream);
uint64_t vch_stream_progress_total_rows(const vch_stream *stream);
int vch_stream_cancel(vch_stream *stream, char *error, size_t error_len);
void vch_stream_close(vch_stream *stream);

#endif
