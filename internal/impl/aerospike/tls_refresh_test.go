// Copyright 2026 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aerospike

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCertRefreshIntervalOffLeavesStaticTLS(t *testing.T) {
	dir := t.TempDir()
	_, certPEM, keyPEM := leafCert(t, nil, nil, "rpcn", 1, nil, true)
	certPath, keyPath := writePair(t, dir, "client.pem", "client.key", certPEM, keyPEM)

	c, err := parseClientYAML(t, `
hosts: ["127.0.0.1:1"]
tls:
  enabled: true
  skip_cert_verify: true
  client_certs:
    - cert_file: `+certPath+`
      key_file: `+keyPath+`
cert_refresh_interval: 0s
`)
	require.NoError(t, err)
	assert.Nil(t, c.tlsRefresh)
	require.NotNil(t, c.Policy.TlsConfig)
	assert.Nil(t, c.Policy.TlsConfig.GetClientCertificate)
	assert.NotEmpty(t, c.Policy.TlsConfig.Certificates)
}

func TestCertRefreshRejectsNegative(t *testing.T) {
	_, err := parseClientYAML(t, `
hosts: ["127.0.0.1:1"]
cert_refresh_interval: -1s
`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cert_refresh_interval")
}

func TestCertRefreshInlinePEMIsStatic(t *testing.T) {
	_, certPEM, keyPEM := leafCert(t, nil, nil, "rpcn", 1, nil, true)
	c, err := parseClientYAML(t, `
hosts: ["127.0.0.1:1"]
tls:
  enabled: true
  skip_cert_verify: true
  client_certs:
    - cert: |
`+indentPEM(certPEM, 8)+`
      key: |
`+indentPEM(keyPEM, 8)+`
cert_refresh_interval: 1h
`)
	require.NoError(t, err)
	assert.Nil(t, c.tlsRefresh)
	assert.NotEmpty(t, c.Policy.TlsConfig.Certificates)
	assert.Nil(t, c.Policy.TlsConfig.GetClientCertificate)
}

func TestCertRefreshClientCertOnNewHandshake(t *testing.T) {
	dir := t.TempDir()
	ca, caCertPEM, _ := caCert(t, "ca")
	first, firstPEM, firstKey := leafCert(t, ca.cert, ca.key, "rpcn", 1, nil, true)
	second, secondPEM, secondKey := leafCert(t, ca.cert, ca.key, "rpcn", 2, nil, true)
	certPath, keyPath := writePair(t, dir, "client.pem", "client.key", firstPEM, firstKey)
	srv, srvPEM, srvKey := leafCert(t, ca.cert, ca.key, "server.example", 10, []string{"server.example"}, false)
	_ = srv
	caPath := filepath.Join(dir, "ca.pem")
	require.NoError(t, os.WriteFile(caPath, caCertPEM, 0o600))

	c, err := parseClientYAML(t, `
hosts: ["127.0.0.1:1"]
tls:
  enabled: true
  root_cas_file: `+caPath+`
  client_certs:
    - cert_file: `+certPath+`
      key_file: `+keyPath+`
cert_refresh_interval: 1h
`)
	require.NoError(t, err)
	require.NotNil(t, c.tlsRefresh)
	require.NotNil(t, c.Policy.TlsConfig.GetClientCertificate)

	addr, peers, cleanup := echoServer(t, mustKeyPair(t, srvPEM, srvKey), tls.RequireAndVerifyClientCert, poolFromPEM(t, caCertPEM))
	defer cleanup()

	conn := dialTLS(t, addr, c.Policy.TlsConfig, "server.example")
	exchange(t, conn)
	require.Equal(t, 0, first.SerialNumber.Cmp(awaitSerial(t, peers)))

	require.NoError(t, os.WriteFile(certPath, secondPEM, 0o600))
	require.NoError(t, os.WriteFile(keyPath, secondKey, 0o600))
	c.tlsRefresh.reload(nil)

	exchange(t, conn)

	conn2 := dialTLS(t, addr, c.Policy.TlsConfig, "server.example")
	exchange(t, conn2)
	require.Equal(t, 0, second.SerialNumber.Cmp(awaitSerial(t, peers)))

	require.NoError(t, os.WriteFile(certPath, []byte("not a certificate"), 0o600))
	c.tlsRefresh.reload(nil)
	conn3 := dialTLS(t, addr, c.Policy.TlsConfig, "server.example")
	exchange(t, conn3)
	require.Equal(t, 0, second.SerialNumber.Cmp(awaitSerial(t, peers)))
}

func TestCertRefreshRootCAOnNewHandshake(t *testing.T) {
	dir := t.TempDir()
	ca1, ca1PEM, _ := caCert(t, "ca-1")
	ca2, ca2PEM, _ := caCert(t, "ca-2")
	_, srv1PEM, srv1Key := leafCert(t, ca1.cert, ca1.key, "server.example", 1, []string{"server.example"}, false)
	_, srv2PEM, srv2Key := leafCert(t, ca2.cert, ca2.key, "server.example", 2, []string{"server.example"}, false)
	caPath := filepath.Join(dir, "ca.pem")
	require.NoError(t, os.WriteFile(caPath, ca1PEM, 0o600))

	c, err := parseClientYAML(t, `
hosts: ["server.example:1"]
tls:
  enabled: true
  root_cas_file: `+caPath+`
cert_refresh_interval: 1h
`)
	require.NoError(t, err)
	require.NotNil(t, c.tlsRefresh)
	require.True(t, c.Policy.TlsConfig.InsecureSkipVerify)
	require.NotNil(t, c.Policy.TlsConfig.VerifyConnection)
	require.Equal(t, "server.example", c.Hosts[0].TLSName)

	addr1, _, cleanup1 := echoServer(t, mustKeyPair(t, srv1PEM, srv1Key), tls.NoClientCert, nil)
	defer cleanup1()
	conn := dialTLS(t, addr1, c.Policy.TlsConfig, "server.example")
	exchange(t, conn)

	_, err = dialRaw(t, addr1, c.Policy.TlsConfig, "other.example")
	require.Error(t, err)

	require.NoError(t, os.WriteFile(caPath, []byte("not a certificate"), 0o600))
	c.tlsRefresh.reload(nil)
	exchange(t, conn)
	still := dialTLS(t, addr1, c.Policy.TlsConfig, "server.example")
	exchange(t, still)

	require.NoError(t, os.WriteFile(caPath, ca2PEM, 0o600))
	c.tlsRefresh.reload(nil)
	exchange(t, conn)
	_, err = dialRaw(t, addr1, c.Policy.TlsConfig, "server.example")
	require.Error(t, err)

	addr2, _, cleanup2 := echoServer(t, mustKeyPair(t, srv2PEM, srv2Key), tls.NoClientCert, nil)
	defer cleanup2()
	conn2 := dialTLS(t, addr2, c.Policy.TlsConfig, "server.example")
	exchange(t, conn2)
}

func TestCertRefreshEncryptedKeyUsesConfiguredPassword(t *testing.T) {
	const password = "right-secret"
	dir := t.TempDir()
	firstPEM, firstKey := encryptedPKCS8Key(t, password)
	secondPEM, secondKey := encryptedPKCS8Key(t, password)
	certPath, keyPath := writePair(t, dir, "client.pem", "client.key", firstPEM, firstKey)

	c, err := parseClientYAML(t, `
hosts: ["127.0.0.1:1"]
tls:
  enabled: true
  skip_cert_verify: true
  client_certs:
    - cert_file: `+certPath+`
      key_file: `+keyPath+`
      password: `+password+`
cert_refresh_interval: 1h
`)
	require.NoError(t, err)
	before, err := c.Policy.TlsConfig.GetClientCertificate(nil)
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(certPath, secondPEM, 0o600))
	require.NoError(t, os.WriteFile(keyPath, secondKey, 0o600))
	c.tlsRefresh.reload(nil)
	after, err := c.Policy.TlsConfig.GetClientCertificate(nil)
	require.NoError(t, err)
	assert.NotEqual(t, before.Certificate[0], after.Certificate[0])

	require.NoError(t, os.WriteFile(keyPath, firstKey, 0o600))
	c.tlsRefresh.reload(nil)
	kept, err := c.Policy.TlsConfig.GetClientCertificate(nil)
	require.NoError(t, err)
	assert.Equal(t, after.Certificate[0], kept.Certificate[0])
}

func TestCertRefreshPollerStops(t *testing.T) {
	dir := t.TempDir()
	_, firstPEM, firstKey := leafCert(t, nil, nil, "rpcn", 1, nil, true)
	_, secondPEM, secondKey := leafCert(t, nil, nil, "rpcn", 2, nil, true)
	certPath, keyPath := writePair(t, dir, "client.pem", "client.key", firstPEM, firstKey)

	c, err := parseClientYAML(t, `
hosts: ["127.0.0.1:1"]
tls:
  enabled: true
  skip_cert_verify: true
  client_certs:
    - cert_file: `+certPath+`
      key_file: `+keyPath+`
cert_refresh_interval: 20ms
`)
	require.NoError(t, err)
	stop := c.tlsRefresh.start(nil)
	stop()
	time.Sleep(40 * time.Millisecond)

	require.NoError(t, os.WriteFile(certPath, secondPEM, 0o600))
	require.NoError(t, os.WriteFile(keyPath, secondKey, 0o600))
	time.Sleep(80 * time.Millisecond)

	got, err := c.Policy.TlsConfig.GetClientCertificate(nil)
	require.NoError(t, err)
	require.NotEmpty(t, got.Certificate)
	leaf, err := x509.ParseCertificate(got.Certificate[0])
	require.NoError(t, err)
	assert.Equal(t, 0, big.NewInt(1).Cmp(leaf.SerialNumber))
}

type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

func caCert(t *testing.T, cn string) (testCA, []byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return testCA{cert: cert, key: key}, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), marshalECKey(t, key)
}

func leafCert(t *testing.T, parent *x509.Certificate, parentKey *ecdsa.PrivateKey, cn string, serial int64, dns []string, client bool) (*x509.Certificate, []byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	usage := []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	if client {
		usage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: cn},
		DNSNames:     dns,
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  usage,
	}
	signer := parent
	signerKey := parentKey
	if signer == nil {
		signer = tmpl
		signerKey = key
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, signer, &key.PublicKey, signerKey)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return cert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), marshalECKey(t, key)
}

func marshalECKey(t *testing.T, key *ecdsa.PrivateKey) []byte {
	t.Helper()
	der, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der})
}

func writePair(t *testing.T, dir, certName, keyName string, certPEM, keyPEM []byte) (string, string) {
	t.Helper()
	certPath := filepath.Join(dir, certName)
	keyPath := filepath.Join(dir, keyName)
	require.NoError(t, os.WriteFile(certPath, certPEM, 0o600))
	require.NoError(t, os.WriteFile(keyPath, keyPEM, 0o600))
	return certPath, keyPath
}

func indentPEM(raw []byte, spaces int) string {
	pad := strings.Repeat(" ", spaces)
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	for i := range lines {
		lines[i] = pad + lines[i]
	}
	return strings.Join(lines, "\n")
}

func poolFromPEM(t *testing.T, pemBytes []byte) *x509.CertPool {
	t.Helper()
	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(pemBytes))
	return pool
}

func mustKeyPair(t *testing.T, certPEM, keyPEM []byte) tls.Certificate {
	t.Helper()
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	require.NoError(t, err)
	return cert
}

func echoServer(t *testing.T, cert tls.Certificate, auth tls.ClientAuthType, clientCAs *x509.CertPool) (string, <-chan *big.Int, func()) {
	t.Helper()
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   auth,
		ClientCAs:    clientCAs,
		MinVersion:   tls.VersionTLS12,
	})
	require.NoError(t, err)
	peers := make(chan *big.Int, 8)
	done := make(chan struct{})
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				close(done)
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				if tlsConn, ok := conn.(*tls.Conn); ok {
					if err := tlsConn.Handshake(); err != nil {
						return
					}
					if peerCerts := tlsConn.ConnectionState().PeerCertificates; len(peerCerts) > 0 {
						peers <- peerCerts[0].SerialNumber
					}
				}
				buf := make([]byte, 1)
				for {
					if _, err := io.ReadFull(conn, buf); err != nil {
						return
					}
					if _, err := conn.Write(buf); err != nil {
						return
					}
				}
			}(conn)
		}
	}()
	return ln.Addr().String(), peers, func() {
		_ = ln.Close()
		<-done
	}
}

func awaitSerial(t *testing.T, peers <-chan *big.Int) *big.Int {
	t.Helper()
	select {
	case serial := <-peers:
		return serial
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the client certificate")
		return nil
	}
}

func dialTLS(t *testing.T, addr string, base *tls.Config, serverName string) *tls.Conn {
	t.Helper()
	conn, err := dialRaw(t, addr, base, serverName)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func dialRaw(t *testing.T, addr string, base *tls.Config, serverName string) (*tls.Conn, error) {
	t.Helper()
	cfg := base.Clone()
	cfg.ServerName = serverName
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	return tls.DialWithDialer(dialer, "tcp", addr, cfg)
}

func exchange(t *testing.T, conn *tls.Conn) {
	t.Helper()
	require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))
	_, err := conn.Write([]byte{7})
	require.NoError(t, err)
	var buf [1]byte
	_, err = io.ReadFull(conn, buf[:])
	require.NoError(t, err)
	require.Equal(t, byte(7), buf[0])
}
