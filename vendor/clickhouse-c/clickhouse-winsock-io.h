/*
 * clickhouse-winsock-io.h -- blocking Winsock backend for chc_io.
 *
 * Exactly one TU must `#define CHC_IMPLEMENTATION` before including;
 * other TUs include for declarations only. Depends on clickhouse.h.
 * License: Apache-2.0. See LICENSE.
 */

#ifndef CLICKHOUSE_WINSOCK_IO_H
#define CLICKHOUSE_WINSOCK_IO_H

#ifndef _WIN32
#error "clickhouse-winsock-io.h requires Windows"
#endif

#include <stdbool.h>
#include <winsock2.h>

#include "clickhouse.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct chc_winsock_io {
    SOCKET socket;
    bool (*check_cancel)(void *ud);
    void *cancel_ud;
    /* Monotonic-us deadline applied to each blocking read. 0 disables. */
    int64_t deadline_us;
} chc_winsock_io;

void chc_winsock_io_init(chc_winsock_io *state, chc_io *out_io, SOCKET socket,
                         bool (*check_cancel)(void *), void *cancel_ud);

/* Bound subsequent reads by an absolute monotonic deadline; 0 = none. */
void chc_winsock_io_set_deadline(chc_winsock_io *state, int64_t deadline_us);

#ifdef CHC_IMPLEMENTATION

#include <limits.h>

static int64_t
chc__winsock_now_us(void)
{
    return (int64_t) GetTickCount64() * 1000;
}

static int
chc__winsock_wait_readable(SOCKET socket, int64_t deadline_us, chc_err *err)
{
    if (deadline_us == 0) return CHC_OK;
    for (;;) {
        int64_t now = chc__winsock_now_us();
        if (now >= deadline_us)
            return chc__err_set(err, CHC_ERR_IO, "read timeout");
        int64_t remaining_us = deadline_us - now;
        struct timeval timeout = {
            .tv_sec = (long) (remaining_us / 1000000),
            .tv_usec = (long) (remaining_us % 1000000),
        };
        fd_set read_set;
        FD_ZERO(&read_set);
        FD_SET(socket, &read_set);
        int selected = select(0, &read_set, NULL, NULL, &timeout);
        if (selected > 0) return CHC_OK;
        if (selected == 0)
            return chc__err_set(err, CHC_ERR_IO, "read timeout");
        int code = WSAGetLastError();
        if (code == WSAEINTR) continue;
        return chc__err_set(err, CHC_ERR_IO, "select(socket): Winsock error %d",
                            code);
    }
}

static int
chc__winsock_read(void *ud, void *buf, size_t len, size_t *out_n,
                  chc_err *err)
{
    chc_winsock_io *state = ud;
    if (len == 0) {
        *out_n = 0;
        return CHC_OK;
    }
    int want = len > (size_t) INT_MAX ? INT_MAX : (int) len;
    for (;;) {
        if (state->check_cancel && state->check_cancel(state->cancel_ud))
            return chc__err_set(err, CHC_ERR_CANCELLED, "cancelled");
        int rc = chc__winsock_wait_readable(state->socket,
                                            state->deadline_us, err);
        if (rc != CHC_OK) return rc;
        int n = recv(state->socket, (char *) buf, want, 0);
        if (n >= 0) {
            *out_n = (size_t) n;
            return CHC_OK;
        }
        int code = WSAGetLastError();
        if (code == WSAEINTR || code == WSAEWOULDBLOCK) continue;
        return chc__err_set(err, CHC_ERR_IO, "recv(socket): Winsock error %d",
                            code);
    }
}

static int
chc__winsock_write(void *ud, const void *buf, size_t len, chc_err *err)
{
    chc_winsock_io *state = ud;
    const unsigned char *cursor = buf;
    while (len) {
        if (state->check_cancel && state->check_cancel(state->cancel_ud))
            return chc__err_set(err, CHC_ERR_CANCELLED, "cancelled");
        int chunk = len > (size_t) INT_MAX ? INT_MAX : (int) len;
        int n = send(state->socket, (const char *) cursor, chunk, 0);
        if (n > 0) {
            cursor += n;
            len -= (size_t) n;
            continue;
        }
        int code = n == 0 ? WSAECONNRESET : WSAGetLastError();
        if (code == WSAEINTR || code == WSAEWOULDBLOCK) continue;
        return chc__err_set(err, CHC_ERR_IO, "send(socket): Winsock error %d",
                            code);
    }
    return CHC_OK;
}

static int
chc__winsock_cancel(void *ud)
{
    chc_winsock_io *state = ud;
    return state->check_cancel && state->check_cancel(state->cancel_ud);
}

void
chc_winsock_io_init(chc_winsock_io *state, chc_io *out_io, SOCKET socket,
                    bool (*check_cancel)(void *), void *cancel_ud)
{
    *state = (chc_winsock_io) {
        .socket = socket,
        .check_cancel = check_cancel,
        .cancel_ud = cancel_ud,
    };
    *out_io = (chc_io) {
        .ud = state,
        .read = chc__winsock_read,
        .write = chc__winsock_write,
        .check_cancel = check_cancel ? chc__winsock_cancel : NULL,
    };
}

void
chc_winsock_io_set_deadline(chc_winsock_io *state, int64_t deadline_us)
{
    state->deadline_us = deadline_us;
}

#endif /* CHC_IMPLEMENTATION */

#ifdef __cplusplus
}
#endif

#endif /* CLICKHOUSE_WINSOCK_IO_H */
