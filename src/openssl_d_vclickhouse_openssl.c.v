module vclickhouse

import net.openssl

#flag -DVCLICKHOUSE_OPENSSL

// Referencing the standard-library type makes V include net.openssl's
// platform-specific OpenSSL headers and linker flags for this optional build.
fn vclickhouse_openssl_linkage() openssl.SSLConnectConfig {
	return openssl.SSLConnectConfig{}
}
