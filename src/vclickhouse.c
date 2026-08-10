#ifndef _WIN32
#define _POSIX_C_SOURCE 200112L
#else
#ifndef _WIN32_WINNT
#define _WIN32_WINNT 0x0600
#endif
#define WIN32_LEAN_AND_MEAN
#include <winsock2.h>
#include <ws2tcpip.h>
#endif

#include "vclickhouse.h"

#ifndef _WIN32
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>
#endif

#include <inttypes.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifdef VCLICKHOUSE_OPENSSL
#include <openssl/err.h>
#include <openssl/ssl.h>
#endif

#define CHC_PROVIDE_STDLIB_ALLOC
#define CHC_NO_LZ4
#define CHC_NO_ZSTD
#define CHC_IMPLEMENTATION
#include "clickhouse.h"
#ifdef _WIN32
#include "clickhouse-winsock-io.h"
#else
#include "clickhouse-posix-io.h"
#endif
#ifdef VCLICKHOUSE_OPENSSL
#include "clickhouse-openssl.h"
#endif
#include "clickhouse-compression.h"
#include "clickhouse-client.h"

#ifdef _WIN32
typedef SOCKET vch_socket;
#define VCH_INVALID_SOCKET INVALID_SOCKET
#else
typedef int vch_socket;
#define VCH_INVALID_SOCKET (-1)
#endif

struct vch_conn {
    vch_socket socket;
    chc_alloc allocator;
#ifdef _WIN32
    chc_winsock_io io_state;
#else
    chc_posix_io io_state;
#endif
#ifdef VCLICKHOUSE_OPENSSL
    SSL_CTX *ssl_ctx;
    SSL *ssl;
    chc_openssl_io ssl_io_state;
#endif
    chc_io io;
    chc_client *client;
    int active_stream;
#ifdef _WIN32
    int winsock_started;
#endif
};

struct vch_stream {
    vch_conn *conn;
    chc_block *block;
    size_t row_count;
    int done;
    uint64_t progress_rows;
    uint64_t progress_bytes;
    uint64_t progress_total_rows;
    char *scratch;
    size_t scratch_len;
    size_t scratch_cap;
};

static void vch_error(char *out, size_t cap, const char *fmt, ...)
{
    if (!out || cap == 0) return;
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(out, cap, fmt, ap);
    va_end(ap);
    out[cap - 1] = '\0';
}

static void vch_chc_error(char *out, size_t cap, const char *prefix, const chc_err *err)
{
    if (err && err->server_code)
        vch_error(out, cap, "%s: %s (server code %d, %s)", prefix, err->msg,
                  err->server_code, err->server_name);
    else
        vch_error(out, cap, "%s: %s", prefix, err ? err->msg : "unknown error");
}

static void vch_close_socket(vch_socket socket)
{
    if (socket == VCH_INVALID_SOCKET) return;
#ifdef _WIN32
    closesocket(socket);
#else
    close(socket);
#endif
}

static int vch_last_socket_error(void)
{
#ifdef _WIN32
    return WSAGetLastError();
#else
    return errno;
#endif
}

static vch_socket vch_open_socket(const vch_options *opts, char *error,
                                  size_t error_len)
{
    char port[16];
    snprintf(port, sizeof port, "%u", (unsigned)(opts->port ? opts->port : 9000));
    struct addrinfo hints;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

    struct addrinfo *addresses = NULL;
    const char *host = opts->host && opts->host[0] ? opts->host : "127.0.0.1";
    int gai = getaddrinfo(host, port, &hints, &addresses);
    if (gai != 0) {
#ifdef _WIN32
        vch_error(error, error_len, "resolve %s: Winsock error %d", host, gai);
#else
        vch_error(error, error_len, "resolve %s: %s", host, gai_strerror(gai));
#endif
        return VCH_INVALID_SOCKET;
    }

    vch_socket socket_handle = VCH_INVALID_SOCKET;
    int last_error = 0;
    for (struct addrinfo *it = addresses; it; it = it->ai_next) {
        socket_handle = socket(it->ai_family, it->ai_socktype, it->ai_protocol);
        if (socket_handle == VCH_INVALID_SOCKET) {
            last_error = vch_last_socket_error();
            continue;
        }
        int one = 1;
        setsockopt(socket_handle, IPPROTO_TCP, TCP_NODELAY,
#ifdef _WIN32
                   (const char *) &one, (int) sizeof one);

        u_long nonblocking = 1;
        bool timed_connect = opts->connect_timeout_ms > 0 &&
                             ioctlsocket(socket_handle, FIONBIO,
                                         &nonblocking) == 0;
#else
                   &one, sizeof one);

        int flags = fcntl(socket_handle, F_GETFL, 0);
        bool timed_connect = opts->connect_timeout_ms > 0 && flags >= 0;
        if (timed_connect)
            fcntl(socket_handle, F_SETFL, flags | O_NONBLOCK);
#endif
        int connected = connect(socket_handle, it->ai_addr,
#ifdef _WIN32
                                (int) it->ai_addrlen);
        int connect_error = connected == 0 ? 0 : WSAGetLastError();
        bool connect_in_progress = connect_error == WSAEWOULDBLOCK ||
                                   connect_error == WSAEINPROGRESS ||
                                   connect_error == WSAEINVAL;
#else
                                it->ai_addrlen);
        int connect_error = connected == 0 ? 0 : errno;
        bool connect_in_progress = connect_error == EINPROGRESS;
#endif
        if (connected != 0 && timed_connect && connect_in_progress) {
            fd_set write_set;
            FD_ZERO(&write_set);
            FD_SET(socket_handle, &write_set);
            struct timeval timeout = {
                .tv_sec = opts->connect_timeout_ms / 1000,
                .tv_usec = (opts->connect_timeout_ms % 1000) * 1000
            };
            int selected = select(
#ifdef _WIN32
                0,
#else
                socket_handle + 1,
#endif
                NULL, &write_set, NULL, &timeout);
            if (selected > 0) {
                int socket_error = 0;
#ifdef _WIN32
                int socket_error_len = sizeof socket_error;
                if (getsockopt(socket_handle, SOL_SOCKET, SO_ERROR,
                               (char *) &socket_error,
                               &socket_error_len) == 0 && socket_error == 0)
#else
                socklen_t socket_error_len = sizeof socket_error;
                if (getsockopt(socket_handle, SOL_SOCKET, SO_ERROR, &socket_error,
                               &socket_error_len) == 0 && socket_error == 0)
#endif
                    connected = 0;
                else
                    connect_error = socket_error ? socket_error :
                                    vch_last_socket_error();
            } else if (selected == 0) {
#ifdef _WIN32
                connect_error = WSAETIMEDOUT;
#else
                connect_error = ETIMEDOUT;
#endif
            } else {
                connect_error = vch_last_socket_error();
            }
        }
#ifdef _WIN32
        if (timed_connect) {
            u_long blocking = 0;
            ioctlsocket(socket_handle, FIONBIO, &blocking);
        }
#else
        if (timed_connect) fcntl(socket_handle, F_SETFL, flags);
#endif
        if (connected == 0) break;
        last_error = connect_error;
        vch_close_socket(socket_handle);
        socket_handle = VCH_INVALID_SOCKET;
    }
    freeaddrinfo(addresses);
    if (socket_handle == VCH_INVALID_SOCKET) {
#ifdef _WIN32
        vch_error(error, error_len, "connect %s:%s: Winsock error %d", host,
                  port, last_error ? last_error : WSAECONNREFUSED);
#else
        vch_error(error, error_len, "connect %s:%s: %s", host, port,
                  strerror(last_error ? last_error : ECONNREFUSED));
#endif
        return VCH_INVALID_SOCKET;
    }

#ifdef _WIN32
    if (opts->read_timeout_ms > 0) {
        DWORD timeout = (DWORD) opts->read_timeout_ms;
        setsockopt(socket_handle, SOL_SOCKET, SO_RCVTIMEO,
                   (const char *) &timeout, (int) sizeof timeout);
    }
    if (opts->write_timeout_ms > 0) {
        DWORD timeout = (DWORD) opts->write_timeout_ms;
        setsockopt(socket_handle, SOL_SOCKET, SO_SNDTIMEO,
                   (const char *) &timeout, (int) sizeof timeout);
    }
#else
    struct timeval tv;
    if (opts->read_timeout_ms > 0) {
        tv.tv_sec = opts->read_timeout_ms / 1000;
        tv.tv_usec = (opts->read_timeout_ms % 1000) * 1000;
        setsockopt(socket_handle, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    }
    if (opts->write_timeout_ms > 0) {
        tv.tv_sec = opts->write_timeout_ms / 1000;
        tv.tv_usec = (opts->write_timeout_ms % 1000) * 1000;
        setsockopt(socket_handle, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);
    }
#endif
    return socket_handle;
}

static int vch_init_transport(vch_conn *conn, const vch_options *opts,
                              char *error, size_t error_len)
{
    if (!opts->secure) {
#ifdef _WIN32
        chc_winsock_io_init(&conn->io_state, &conn->io, conn->socket,
                            NULL, NULL);
#else
        chc_posix_io_init(&conn->io_state, &conn->io, conn->socket,
                          NULL, NULL);
#endif
        return 0;
    }

#ifndef VCLICKHOUSE_OPENSSL
    vch_error(error, error_len,
              "native TLS requires rebuilding with -d vclickhouse_openssl");
    return -1;
#else
    conn->ssl_ctx = SSL_CTX_new(TLS_client_method());
    if (!conn->ssl_ctx) {
        vch_error(error, error_len, "create OpenSSL context failed");
        return -1;
    }
    if (opts->tls_verify) {
        SSL_CTX_set_verify(conn->ssl_ctx, SSL_VERIFY_PEER, NULL);
        if (opts->tls_ca_file && opts->tls_ca_file[0]) {
            if (SSL_CTX_load_verify_locations(conn->ssl_ctx,
                                              opts->tls_ca_file, NULL) != 1) {
                vch_error(error, error_len,
                          "load TLS CA file failed: %s", opts->tls_ca_file);
                return -1;
            }
        } else if (SSL_CTX_set_default_verify_paths(conn->ssl_ctx) != 1) {
            vch_error(error, error_len,
                      "load default TLS certificate authorities failed");
            return -1;
        }
    } else {
        SSL_CTX_set_verify(conn->ssl_ctx, SSL_VERIFY_NONE, NULL);
    }

    if (opts->tls_cert_file && opts->tls_cert_file[0]) {
        if (SSL_CTX_use_certificate_chain_file(conn->ssl_ctx,
                                               opts->tls_cert_file) != 1 ||
            SSL_CTX_use_PrivateKey_file(conn->ssl_ctx, opts->tls_key_file,
                                        SSL_FILETYPE_PEM) != 1 ||
            SSL_CTX_check_private_key(conn->ssl_ctx) != 1) {
            vch_error(error, error_len, "load TLS client certificate failed");
            return -1;
        }
    }

    conn->ssl = SSL_new(conn->ssl_ctx);
    if (!conn->ssl) {
        vch_error(error, error_len, "create OpenSSL connection failed");
        return -1;
    }
    const char *host = opts->host && opts->host[0] ?
                       opts->host : "127.0.0.1";
    if (SSL_set_tlsext_host_name(conn->ssl, host) != 1 ||
        (opts->tls_verify && SSL_set1_host(conn->ssl, host) != 1)) {
        vch_error(error, error_len, "configure TLS server name failed");
        return -1;
    }
    if (SSL_set_fd(conn->ssl, (int) conn->socket) != 1) {
        vch_error(error, error_len, "attach TLS socket failed");
        return -1;
    }
    if (SSL_connect(conn->ssl) != 1) {
        unsigned long code = ERR_peek_last_error();
        char detail[256] = "TLS handshake failed";
        if (code) ERR_error_string_n(code, detail, sizeof detail);
        vch_error(error, error_len, "ClickHouse TLS handshake failed: %s",
                  detail);
        return -1;
    }
    chc_openssl_io_init(&conn->ssl_io_state, &conn->io, conn->ssl,
                        NULL, NULL);
    return 0;
#endif
}

static void vch_free_transport(vch_conn *conn)
{
    if (!conn) return;
#ifdef VCLICKHOUSE_OPENSSL
    if (conn->ssl) {
        SSL_shutdown(conn->ssl);
        SSL_free(conn->ssl);
        conn->ssl = NULL;
    }
    if (conn->ssl_ctx) {
        SSL_CTX_free(conn->ssl_ctx);
        conn->ssl_ctx = NULL;
    }
#endif
    vch_close_socket(conn->socket);
    conn->socket = VCH_INVALID_SOCKET;
#ifdef _WIN32
    if (conn->winsock_started) {
        WSACleanup();
        conn->winsock_started = 0;
    }
#endif
}

vch_conn *vch_connect(const vch_options *opts, char *error, size_t error_len)
{
    vch_options defaults;
    memset(&defaults, 0, sizeof defaults);
    defaults.host = "127.0.0.1";
    defaults.port = 9000;
    defaults.user = "default";
    defaults.database = "default";
    if (!opts) opts = &defaults;

#ifndef VCLICKHOUSE_OPENSSL
    if (opts->secure) {
        vch_error(error, error_len,
                  "native TLS requires rebuilding with -d vclickhouse_openssl");
        return NULL;
    }
#endif

#ifdef _WIN32
    WSADATA winsock_data;
    int winsock_error = WSAStartup(MAKEWORD(2, 2), &winsock_data);
    if (winsock_error != 0) {
        vch_error(error, error_len, "initialize Winsock: error %d",
                  winsock_error);
        return NULL;
    }
#endif

    vch_socket socket_handle = vch_open_socket(opts, error, error_len);
    if (socket_handle == VCH_INVALID_SOCKET) {
#ifdef _WIN32
        WSACleanup();
#endif
        return NULL;
    }

    vch_conn *conn = calloc(1, sizeof *conn);
    if (!conn) {
        vch_close_socket(socket_handle);
#ifdef _WIN32
        WSACleanup();
#endif
        vch_error(error, error_len, "allocate ClickHouse connection: out of memory");
        return NULL;
    }
    conn->socket = socket_handle;
#ifdef _WIN32
    conn->winsock_started = 1;
#endif
    conn->allocator = chc_alloc_stdlib();
    if (vch_init_transport(conn, opts, error, error_len) != 0) {
        vch_free_transport(conn);
        free(conn);
        return NULL;
    }
    chc_client_opts client_opts;
    memset(&client_opts, 0, sizeof client_opts);
    client_opts.client_name = "vclickhouse";
    client_opts.client_version_major = 0;
    client_opts.client_version_minor = 1;
    client_opts.client_version_patch = 0;
    client_opts.database = opts->database && opts->database[0] ? opts->database : "default";
    client_opts.user = opts->user && opts->user[0] ? opts->user : "default";
    client_opts.password = opts->password ? opts->password : "";

    chc_err err;
    memset(&err, 0, sizeof err);
    int rc = chc_client_init(&conn->client, &client_opts, &conn->allocator, &conn->io, &err);
    if (rc != CHC_OK) {
        vch_chc_error(error, error_len, "ClickHouse handshake failed", &err);
        vch_free_transport(conn);
        free(conn);
        return NULL;
    }
    return conn;
}

void vch_close(vch_conn *conn)
{
    if (!conn) return;
    if (conn->client) chc_client_close(conn->client);
    vch_free_transport(conn);
    free(conn);
}

int vch_ping(vch_conn *conn, char *error, size_t error_len)
{
    if (!conn || !conn->client) {
        vch_error(error, error_len, "ClickHouse connection is closed");
        return -1;
    }
    if (conn->active_stream) {
        vch_error(error, error_len, "ClickHouse connection has an active stream");
        return -1;
    }
    chc_err err = {0};
    if (chc_client_send_ping(conn->client, &err) != CHC_OK) {
        vch_chc_error(error, error_len, "ClickHouse ping send failed", &err);
        return -1;
    }
    chc_packet packet = {0};
    int rc = chc_client_recv_packet(conn->client, &packet, &err);
    if (rc != CHC_OK || packet.kind != CHC_PKT_PONG) {
        if (rc != CHC_OK) vch_chc_error(error, error_len, "ClickHouse ping failed", &err);
        else vch_error(error, error_len, "ClickHouse ping returned packet %d", (int)packet.kind);
        chc_packet_clear(conn->client, &packet);
        return -1;
    }
    chc_packet_clear(conn->client, &packet);
    return 0;
}

vch_stream *vch_query(vch_conn *conn, const char *sql,
                      const char *const *param_names,
                      const char *const *param_values,
                      size_t param_count,
                      char *error, size_t error_len)
{
    if (!conn || !conn->client) {
        vch_error(error, error_len, "ClickHouse connection is closed");
        return NULL;
    }
    if (conn->active_stream) {
        vch_error(error, error_len, "ClickHouse connection already has an active stream");
        return NULL;
    }
    if (!sql || !sql[0]) {
        vch_error(error, error_len, "ClickHouse query is empty");
        return NULL;
    }

    chc_query_setting settings[] = {
        { "output_format_native_encode_types_in_binary_format", "0", false, false },
        { "output_format_native_write_use_sparse_columns_optimization", "0", false, false },
        { "output_format_native_write_json_as_string", "1", false, false },
    };
    chc_query_param *params = NULL;
    if (param_count) {
        params = calloc(param_count, sizeof *params);
        if (!params) {
            vch_error(error, error_len, "allocate ClickHouse query parameters: out of memory");
            return NULL;
        }
        for (size_t i = 0; i < param_count; i++) {
            params[i].name = param_names[i];
            params[i].value = param_values[i];
        }
    }
    chc_query_opts query_opts = {0};
    query_opts.settings = settings;
    query_opts.n_settings = sizeof settings / sizeof settings[0];
    query_opts.params = params;
    query_opts.n_params = param_count;

    chc_err err = {0};
    int rc = chc_client_send_query_ex(conn->client, sql, strlen(sql), &query_opts, &err);
    free(params);
    if (rc != CHC_OK) {
        vch_chc_error(error, error_len, "ClickHouse query send failed", &err);
        return NULL;
    }

    vch_stream *stream = calloc(1, sizeof *stream);
    if (!stream) {
        vch_error(error, error_len, "allocate ClickHouse result stream: out of memory");
        return NULL;
    }
    stream->conn = conn;
    conn->active_stream = 1;
    return stream;
}

static void vch_stream_drop_block(vch_stream *stream)
{
    if (stream && stream->block) {
        chc_block_destroy(stream->block, &stream->conn->allocator);
        stream->block = NULL;
        stream->row_count = 0;
    }
}

int vch_stream_next_block(vch_stream *stream, char *error, size_t error_len)
{
    if (!stream || !stream->conn || !stream->conn->client) {
        vch_error(error, error_len, "ClickHouse stream is closed");
        return -1;
    }
    if (stream->done) return 0;
    vch_stream_drop_block(stream);
    for (;;) {
        chc_packet packet = {0};
        chc_err err = {0};
        int rc = chc_client_recv_packet(stream->conn->client, &packet, &err);
        if (rc != CHC_OK) {
            vch_chc_error(error, error_len, "ClickHouse result receive failed", &err);
            stream->done = 1;
            stream->conn->active_stream = 0;
            chc_packet_clear(stream->conn->client, &packet);
            return -1;
        }
        if (packet.kind == CHC_PKT_PROGRESS) {
            stream->progress_rows += packet.progress.rows;
            stream->progress_bytes += packet.progress.bytes;
            stream->progress_total_rows = packet.progress.total_rows;
        } else if (packet.kind == CHC_PKT_EXCEPTION) {
            if (packet.exception)
                vch_error(error, error_len, "ClickHouse server error %d: %s",
                          packet.exception->code,
                          packet.exception->display_text ? packet.exception->display_text : packet.exception->name);
            else
                vch_error(error, error_len, "ClickHouse server returned an exception");
            stream->done = 1;
            stream->conn->active_stream = 0;
            chc_packet_clear(stream->conn->client, &packet);
            return -1;
        } else if (packet.kind == CHC_PKT_END_OF_STREAM) {
            stream->done = 1;
            stream->conn->active_stream = 0;
            chc_packet_clear(stream->conn->client, &packet);
            return 0;
        } else if (packet.kind == CHC_PKT_DATA && packet.block) {
            size_t columns = chc_block_n_columns(packet.block);
            for (size_t i = 0; i < columns; i++) {
                chc_err validation = {0};
                if (chc_column_validate(chc_block_column(packet.block, i), &validation) != CHC_OK) {
                    vch_chc_error(error, error_len, "invalid ClickHouse result column", &validation);
                    chc_packet_clear(stream->conn->client, &packet);
                    stream->done = 1;
                    stream->conn->active_stream = 0;
                    return -1;
                }
            }
            stream->block = packet.block;
            packet.block = NULL;
            stream->row_count = chc_block_n_rows(stream->block);
            chc_packet_clear(stream->conn->client, &packet);
            return 1;
        }
        chc_packet_clear(stream->conn->client, &packet);
    }
}

size_t vch_stream_row_count(const vch_stream *stream)
{ return stream ? stream->row_count : 0; }

size_t vch_stream_column_count(const vch_stream *stream)
{ return stream && stream->block ? chc_block_n_columns(stream->block) : 0; }

const char *vch_stream_column_name(const vch_stream *stream, size_t column, size_t *len)
{
    if (!stream || !stream->block) { if (len) *len = 0; return NULL; }
    return chc_block_column_name(stream->block, column, len);
}

const char *vch_stream_column_type(const vch_stream *stream, size_t column, size_t *len)
{
    if (!stream || !stream->block) { if (len) *len = 0; return NULL; }
    return chc_type_name(chc_block_column_type(stream->block, column), len);
}

typedef struct vch_buf { vch_stream *stream; int failed; } vch_buf;

static void vch_buf_reset(vch_buf *b)
{
    b->failed = 0;
    b->stream->scratch_len = 0;
    if (b->stream->scratch) b->stream->scratch[0] = '\0';
}

static void vch_buf_reserve(vch_buf *b, size_t add)
{
    if (b->failed) return;
    size_t need = b->stream->scratch_len + add + 1;
    if (need <= b->stream->scratch_cap) return;
    size_t cap = b->stream->scratch_cap ? b->stream->scratch_cap : 128;
    while (cap < need) cap *= 2;
    char *next = realloc(b->stream->scratch, cap);
    if (!next) { b->failed = 1; return; }
    b->stream->scratch = next;
    b->stream->scratch_cap = cap;
}

static void vch_buf_write(vch_buf *b, const void *data, size_t len)
{
    vch_buf_reserve(b, len);
    if (b->failed) return;
    memcpy(b->stream->scratch + b->stream->scratch_len, data, len);
    b->stream->scratch_len += len;
    b->stream->scratch[b->stream->scratch_len] = '\0';
}

static void vch_buf_puts(vch_buf *b, const char *text)
{ vch_buf_write(b, text, strlen(text)); }

static void vch_buf_printf(vch_buf *b, const char *fmt, ...)
{
    char local[160];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(local, sizeof local, fmt, ap);
    va_end(ap);
    if (n < 0) { b->failed = 1; return; }
    if ((size_t)n < sizeof local) { vch_buf_write(b, local, (size_t)n); return; }
    char *tmp = malloc((size_t)n + 1);
    if (!tmp) { b->failed = 1; return; }
    va_start(ap, fmt);
    vsnprintf(tmp, (size_t)n + 1, fmt, ap);
    va_end(ap);
    vch_buf_write(b, tmp, (size_t)n);
    free(tmp);
}

static void vch_buf_quoted(vch_buf *b, const uint8_t *data, size_t len)
{
    vch_buf_puts(b, "'");
    for (size_t i = 0; i < len; i++) {
        unsigned char ch = data[i];
        if (ch == '\\' || ch == '\'') { vch_buf_write(b, "\\", 1); vch_buf_write(b, &ch, 1); }
        else if (ch == '\n') vch_buf_puts(b, "\\n");
        else if (ch == '\r') vch_buf_puts(b, "\\r");
        else if (ch == '\t') vch_buf_puts(b, "\\t");
        else vch_buf_write(b, &ch, 1);
    }
    vch_buf_puts(b, "'");
}

static uint64_t vch_lc_key(const chc_column *column, size_t row)
{
    const uint8_t *keys = chc_column_lc_keys(column);
    int width = chc_column_lc_key_size(column);
    uint64_t key = 0;
    if (!keys || width <= 0 || width > 8) return 0;
    memcpy(&key, keys + row * (size_t)width, (size_t)width);
    return key;
}

static int64_t vch_read_signed(const void *ptr, size_t width)
{
    if (width == 1) return *(const int8_t *)ptr;
    if (width == 2) return *(const int16_t *)ptr;
    if (width == 4) return *(const int32_t *)ptr;
    if (width == 8) return *(const int64_t *)ptr;
    return 0;
}

static uint64_t vch_read_unsigned(const void *ptr, size_t width)
{
    if (width == 1) return *(const uint8_t *)ptr;
    if (width == 2) return *(const uint16_t *)ptr;
    if (width == 4) return *(const uint32_t *)ptr;
    if (width == 8) return *(const uint64_t *)ptr;
    return 0;
}

static void vch_decimal(vch_buf *b, int64_t value, int scale)
{
    if (scale <= 0) { vch_buf_printf(b, "%" PRId64, value); return; }
    uint64_t magnitude = value < 0 ? (uint64_t)(-(value + 1)) + 1 : (uint64_t)value;
    uint64_t divisor = 1;
    for (int i = 0; i < scale && i < 18; i++) divisor *= 10;
    if (value < 0) vch_buf_puts(b, "-");
    vch_buf_printf(b, "%" PRIu64 ".%0*" PRIu64, magnitude / divisor, scale,
                   magnitude % divisor);
}

static void vch_time_value(vch_buf *b, int64_t unix_seconds)
{
    time_t value = (time_t)unix_seconds;
    struct tm tm_value;
    if (!gmtime_r(&value, &tm_value)) { vch_buf_printf(b, "%" PRId64, unix_seconds); return; }
    char text[40];
    if (strftime(text, sizeof text, "%Y-%m-%d %H:%M:%S", &tm_value)) vch_buf_puts(b, text);
}

static void vch_date_value(vch_buf *b, int64_t days)
{
    time_t value = (time_t)(days * 86400);
    struct tm tm_value;
    if (!gmtime_r(&value, &tm_value)) { vch_buf_printf(b, "%" PRId64, days); return; }
    char text[20];
    if (strftime(text, sizeof text, "%Y-%m-%d", &tm_value)) vch_buf_puts(b, text);
}

static void vch_hex(vch_buf *b, const uint8_t *data, size_t len)
{
    static const char digits[] = "0123456789abcdef";
    for (size_t i = 0; i < len; i++) {
        char pair[2] = { digits[data[i] >> 4], digits[data[i] & 15] };
        vch_buf_write(b, pair, 2);
    }
}

static int vch_format_cell(vch_buf *b, const chc_type *type, const chc_column *column,
                           size_t row, int nested, int *is_null)
{
    if (!type || !column) return -1;
    chc_col_kind layout = chc_column_layout(column);
    if (layout == CHC_COL_NULLABLE) {
        const uint8_t *nulls = chc_column_null_map(column);
        if (nulls && nulls[row]) {
            if (is_null) *is_null = 1;
            if (nested) vch_buf_puts(b, "NULL");
            return 0;
        }
        return vch_format_cell(b, chc_type_child(type, 0),
                               chc_column_nullable_inner(column), row, nested, is_null);
    }
    if (layout == CHC_COL_LOW_CARDINALITY) {
        return vch_format_cell(b, chc_type_child(type, 0), chc_column_lc_dict(column),
                               (size_t)vch_lc_key(column, row), nested, is_null);
    }
    if (layout == CHC_COL_NOTHING || chc_type_kind(type) == CHC_NOTHING) {
        if (is_null) *is_null = 1;
        if (nested) vch_buf_puts(b, "NULL");
        return 0;
    }
    if (layout == CHC_COL_STRING) {
        const uint64_t *offsets = chc_column_string_offsets(column);
        const uint8_t *bytes = chc_column_string_data(column);
        uint64_t start = row ? offsets[row - 1] : 0;
        uint64_t end = offsets[row];
        if (nested) vch_buf_quoted(b, bytes + start, (size_t)(end - start));
        else vch_buf_write(b, bytes + start, (size_t)(end - start));
        return b->failed ? -1 : 0;
    }
    if (layout == CHC_COL_ARRAY) {
        const uint64_t *offsets = chc_column_array_offsets(column);
        uint64_t start = row ? offsets[row - 1] : 0;
        uint64_t end = offsets[row];
        const chc_type *child_type = chc_type_child(type, 0);
        const chc_column *values = chc_column_array_values(column);
        vch_buf_puts(b, "[");
        for (uint64_t i = start; i < end; i++) {
            if (i > start) vch_buf_puts(b, ",");
            int child_null = 0;
            if (vch_format_cell(b, child_type, values, (size_t)i, 1, &child_null)) return -1;
        }
        vch_buf_puts(b, "]");
        return b->failed ? -1 : 0;
    }
    if (layout == CHC_COL_TUPLE) {
        size_t arity = chc_column_tuple_arity(column);
        vch_buf_puts(b, "(");
        for (size_t i = 0; i < arity; i++) {
            if (i) vch_buf_puts(b, ",");
            int child_null = 0;
            if (vch_format_cell(b, chc_type_child(type, i),
                                chc_column_tuple_child(column, i), row, 1, &child_null)) return -1;
        }
        vch_buf_puts(b, ")");
        return b->failed ? -1 : 0;
    }
    if (layout != CHC_COL_FIXED) return -1;

    size_t width = 0;
    const uint8_t *base = chc_column_fixed_data(column, &width);
    const uint8_t *ptr = base + row * width;
    chc_kind kind = chc_type_kind(type);
    switch (kind) {
    case CHC_INT8: case CHC_INT16: case CHC_INT32: case CHC_INT64:
        vch_buf_printf(b, "%" PRId64, vch_read_signed(ptr, width)); break;
    case CHC_UINT8: case CHC_UINT16: case CHC_UINT32: case CHC_UINT64:
        vch_buf_printf(b, "%" PRIu64, vch_read_unsigned(ptr, width)); break;
    case CHC_FLOAT32: vch_buf_printf(b, "%.9g", (double)*(const float *)ptr); break;
    case CHC_FLOAT64: vch_buf_printf(b, "%.17g", *(const double *)ptr); break;
    case CHC_BOOL: vch_buf_puts(b, ptr[0] ? "true" : "false"); break;
    case CHC_DATE: vch_date_value(b, (int64_t)*(const uint16_t *)ptr); break;
    case CHC_DATE32: vch_date_value(b, (int64_t)*(const int32_t *)ptr); break;
    case CHC_DATETIME: vch_time_value(b, (int64_t)*(const uint32_t *)ptr); break;
    case CHC_DATETIME64: {
        int scale = chc_type_datetime64_scale(type);
        int64_t raw = *(const int64_t *)ptr;
        int64_t divisor = 1;
        for (int i = 0; i < scale; i++) divisor *= 10;
        vch_time_value(b, raw / divisor);
        if (scale > 0) vch_buf_printf(b, ".%0*" PRId64, scale, llabs(raw % divisor));
        break;
    }
    case CHC_DECIMAL32: case CHC_DECIMAL64:
        vch_decimal(b, vch_read_signed(ptr, width), chc_type_decimal_scale(type)); break;
    case CHC_ENUM8: case CHC_ENUM16: {
        int64_t value = vch_read_signed(ptr, width);
        size_t count = chc_type_enum_count(type);
        int found = 0;
        for (size_t i = 0; i < count; i++) {
            const char *name = NULL; size_t name_len = 0; int64_t enum_value = 0;
            chc_type_enum_at(type, i, &name, &name_len, &enum_value);
            if (enum_value == value) {
                if (nested) vch_buf_quoted(b, (const uint8_t *)name, name_len);
                else vch_buf_write(b, name, name_len);
                found = 1; break;
            }
        }
        if (!found) vch_buf_printf(b, "%" PRId64, value);
        break;
    }
    case CHC_FIXED_STRING: {
        size_t len = width;
        while (len && ptr[len - 1] == 0) len--;
        if (nested) vch_buf_quoted(b, ptr, len); else vch_buf_write(b, ptr, len);
        break;
    }
    case CHC_UUID:
        vch_hex(b, ptr, 4); vch_buf_puts(b, "-"); vch_hex(b, ptr + 4, 2);
        vch_buf_puts(b, "-"); vch_hex(b, ptr + 6, 2); vch_buf_puts(b, "-");
        vch_hex(b, ptr + 8, 2); vch_buf_puts(b, "-"); vch_hex(b, ptr + 10, 6); break;
    case CHC_IPV4: {
        char ip[INET_ADDRSTRLEN];
        if (inet_ntop(AF_INET, ptr, ip, sizeof ip)) vch_buf_puts(b, ip); else vch_hex(b, ptr, width);
        break;
    }
    case CHC_IPV6: {
        char ip[INET6_ADDRSTRLEN];
        if (inet_ntop(AF_INET6, ptr, ip, sizeof ip)) vch_buf_puts(b, ip); else vch_hex(b, ptr, width);
        break;
    }
    default:
        if (width <= 8) vch_buf_printf(b, "%" PRIu64, vch_read_unsigned(ptr, width));
        else { vch_buf_puts(b, "0x"); vch_hex(b, ptr, width); }
        break;
    }
    return b->failed ? -1 : 0;
}

const char *vch_stream_cell_text(vch_stream *stream, size_t row, size_t column,
                                 size_t *len, int *is_null,
                                 char *error, size_t error_len)
{
    if (len) *len = 0;
    if (is_null) *is_null = 0;
    if (!stream || !stream->block || row >= stream->row_count ||
        column >= chc_block_n_columns(stream->block)) {
        vch_error(error, error_len, "ClickHouse cell index is out of range");
        return NULL;
    }
    vch_buf buffer = { stream, 0 };
    vch_buf_reset(&buffer);
    if (vch_format_cell(&buffer, chc_block_column_type(stream->block, column),
                        chc_block_column(stream->block, column), row, 0, is_null) != 0) {
        vch_error(error, error_len, "unsupported or invalid ClickHouse value in column %zu", column);
        return NULL;
    }
    if (len) *len = stream->scratch_len;
    return stream->scratch ? stream->scratch : "";
}

uint64_t vch_stream_progress_rows(const vch_stream *stream)
{ return stream ? stream->progress_rows : 0; }
uint64_t vch_stream_progress_bytes(const vch_stream *stream)
{ return stream ? stream->progress_bytes : 0; }
uint64_t vch_stream_progress_total_rows(const vch_stream *stream)
{ return stream ? stream->progress_total_rows : 0; }

int vch_stream_cancel(vch_stream *stream, char *error, size_t error_len)
{
    if (!stream || !stream->conn || !stream->conn->client || stream->done) return 0;
    chc_err err = {0};
    if (chc_client_send_cancel(stream->conn->client, &err) != CHC_OK) {
        vch_chc_error(error, error_len, "ClickHouse cancel failed", &err);
        return -1;
    }
    return 0;
}

void vch_stream_close(vch_stream *stream)
{
    if (!stream) return;
    vch_stream_drop_block(stream);
    if (!stream->done && stream->conn && stream->conn->client) {
        chc_err err = {0};
        chc_client_send_cancel(stream->conn->client, &err);
        for (;;) {
            chc_packet packet = {0};
            memset(&err, 0, sizeof err);
            int rc = chc_client_recv_packet(stream->conn->client, &packet, &err);
            int done = rc != CHC_OK || packet.kind == CHC_PKT_END_OF_STREAM ||
                       packet.kind == CHC_PKT_EXCEPTION;
            chc_packet_clear(stream->conn->client, &packet);
            if (done) break;
        }
    }
    if (stream->conn) stream->conn->active_stream = 0;
    free(stream->scratch);
    free(stream);
}
