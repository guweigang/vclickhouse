# Vendored clickhouse-c

Source: https://github.com/ClickHouse/clickhouse-c

Pinned commit: `4bdd89a02438d1e81ba7cf95dc111af8a23e313b`

The upstream project is header-only. Its public headers are vendored so a V
consumer does not need to clone a second repository. The upstream LICENSE is
included in this directory.

`clickhouse-winsock-io.h` is a vclickhouse-local adapter because the pinned
upstream currently ships only a POSIX socket backend. The small `_WIN32`
branches in `clickhouse-openssl.h` make its read deadline portable to Winsock;
the remaining OpenSSL protocol code is unchanged from upstream.
