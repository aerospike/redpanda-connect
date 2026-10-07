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
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/youmark/pkcs8"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// tls field names owned by the shared Benthos TLS object.
const (
	fieldRootCAsFile = "root_cas_file"
	fieldClientCerts = "client_certs"
	fieldCertFile    = "cert_file"
	fieldKeyFile     = "key_file"
	fieldKeyPassword = "password"
)

// tlsRefresher re-reads the configured certificate files. The filenames stay
// fixed; a different path is a config change. New handshakes see the latest
// good material. Connections that already finished a handshake are left open.
type tlsRefresher struct {
	interval time.Duration

	rootFile string
	rootPEM  []byte

	certs []watchedCert

	roots       atomic.Pointer[x509.CertPool]
	clientCerts atomic.Pointer[[]tls.Certificate]

	mu   sync.Mutex
	refs int
	stop chan struct{}
}

type watchedCert struct {
	index    int
	certFile string
	keyFile  string
	password string
	certPEM  []byte
	keyPEM   []byte
}

// newTLSRefresher installs handshake callbacks when the TLS config names files
// to re-read. It returns nil when every certificate is inline PEM.
func newTLSRefresher(conf *service.ParsedConfig, tlsConf *tls.Config, hosts []*as.Host, clusterName string, interval time.Duration) (*tlsRefresher, error) {
	if tlsConf == nil || interval <= 0 {
		return nil, nil
	}

	ns := conf.Namespace(fieldTLS)
	rootFile, err := ns.FieldString(fieldRootCAsFile)
	if err != nil {
		return nil, err
	}
	entries, err := ns.FieldObjectList(fieldClientCerts)
	if err != nil {
		return nil, err
	}
	if len(entries) != len(tlsConf.Certificates) {
		return nil, fmt.Errorf("field '%v': parsed %d client certificate(s), expected %d", fieldClientCerts, len(tlsConf.Certificates), len(entries))
	}

	r := &tlsRefresher{interval: interval}
	if rootFile != "" && !tlsConf.InsecureSkipVerify {
		pemBytes, err := os.ReadFile(rootFile)
		if err != nil {
			return nil, fmt.Errorf("reading '%v' file %q: %w", fieldRootCAsFile, rootFile, err)
		}
		pool, err := certPoolFromPEM(pemBytes)
		if err != nil {
			return nil, fmt.Errorf("field '%v' file %q: %w", fieldRootCAsFile, rootFile, err)
		}
		r.rootFile = rootFile
		r.rootPEM = pemBytes
		r.roots.Store(pool)
		// crypto/tls verifies RootCAs before any callback, and Clone copies that
		// pointer. Swapping the pointer while a dial clones the config races.
		// Verification below reads the current pool at handshake time, including
		// the server name. InsecureSkipVerify makes the Aerospike client skip
		// its own name check and skip filling an empty TLS name, so empty names
		// are filled here first.
		fillTLSNames(hosts, clusterName)
		tlsConf.InsecureSkipVerify = true
		tlsConf.VerifyConnection = r.verifyServer
	}

	initial := append([]tls.Certificate(nil), tlsConf.Certificates...)
	for i, entry := range entries {
		certFile, err := entry.FieldString(fieldCertFile)
		if err != nil {
			return nil, err
		}
		keyFile, err := entry.FieldString(fieldKeyFile)
		if err != nil {
			return nil, err
		}
		if certFile == "" && keyFile == "" {
			continue
		}
		password, err := entry.FieldString(fieldKeyPassword)
		if err != nil {
			return nil, err
		}
		certPEM, keyPEM, cert, err := readKeyPair(certFile, keyFile, password)
		if err != nil {
			return nil, err
		}
		initial[i] = cert
		r.certs = append(r.certs, watchedCert{
			index:    i,
			certFile: certFile,
			keyFile:  keyFile,
			password: password,
			certPEM:  certPEM,
			keyPEM:   keyPEM,
		})
	}

	if r.rootFile == "" && len(r.certs) == 0 {
		return nil, nil
	}
	if len(r.certs) > 0 {
		r.clientCerts.Store(&initial)
		// GetClientCertificate replaces Certificates for every new handshake.
		tlsConf.Certificates = nil
		tlsConf.GetClientCertificate = r.getClientCertificate
	}
	return r, nil
}

func fillTLSNames(hosts []*as.Host, clusterName string) {
	for _, h := range hosts {
		if h == nil || h.TLSName != "" {
			continue
		}
		if clusterName != "" {
			h.TLSName = clusterName
		} else {
			h.TLSName = h.Name
		}
	}
}

// start runs one poller per live component. The returned function drops one
// reference and stops the poller when the last user is gone.
func (r *tlsRefresher) start(log *service.Logger) func() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.refs == 0 {
		stop := make(chan struct{})
		r.stop = stop
		go r.loop(log, stop)
	}
	r.refs++

	var once sync.Once
	return func() {
		once.Do(func() {
			r.mu.Lock()
			defer r.mu.Unlock()
			if r.refs == 0 {
				return
			}
			r.refs--
			if r.refs == 0 && r.stop != nil {
				close(r.stop)
				r.stop = nil
			}
		})
	}
}

func (r *tlsRefresher) loop(log *service.Logger, stop <-chan struct{}) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			r.reload(log)
		}
	}
}

func (r *tlsRefresher) reload(log *service.Logger) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reloadRoots(log)
	r.reloadCerts(log)
}

func (r *tlsRefresher) reloadRoots(log *service.Logger) {
	if r.rootFile == "" {
		return
	}
	pemBytes, err := os.ReadFile(r.rootFile)
	if err != nil {
		logTLS(log, true, "re-reading %s %q: %v", fieldRootCAsFile, r.rootFile, err)
		return
	}
	if bytes.Equal(pemBytes, r.rootPEM) {
		return
	}
	pool, err := certPoolFromPEM(pemBytes)
	if err != nil {
		logTLS(log, true, "parsing %s %q: %v", fieldRootCAsFile, r.rootFile, err)
		return
	}
	r.rootPEM = pemBytes
	r.roots.Store(pool)
	logTLS(log, false, "refreshed %s from %q", fieldRootCAsFile, r.rootFile)
}

func (r *tlsRefresher) reloadCerts(log *service.Logger) {
	if len(r.certs) == 0 {
		return
	}
	current := r.clientCerts.Load()
	if current == nil {
		return
	}
	next := append([]tls.Certificate(nil), (*current)...)
	changed := false
	for i := range r.certs {
		src := &r.certs[i]
		certPEM, keyPEM, cert, err := readKeyPair(src.certFile, src.keyFile, src.password)
		if err != nil {
			logTLS(log, true, "re-reading client certificate %q: %v", src.certFile, err)
			continue
		}
		if bytes.Equal(certPEM, src.certPEM) && bytes.Equal(keyPEM, src.keyPEM) {
			continue
		}
		next[src.index] = cert
		src.certPEM = certPEM
		src.keyPEM = keyPEM
		changed = true
		logTLS(log, false, "refreshed client certificate from %q", src.certFile)
	}
	if changed {
		r.clientCerts.Store(&next)
	}
}

func (r *tlsRefresher) verifyServer(cs tls.ConnectionState) error {
	if len(cs.PeerCertificates) == 0 {
		return errors.New("aerospike tls: server did not present a certificate")
	}
	opts := x509.VerifyOptions{
		Roots:         r.roots.Load(),
		DNSName:       cs.ServerName,
		Intermediates: x509.NewCertPool(),
	}
	for _, cert := range cs.PeerCertificates[1:] {
		opts.Intermediates.AddCert(cert)
	}
	if _, err := cs.PeerCertificates[0].Verify(opts); err != nil {
		return fmt.Errorf("aerospike tls: %w", err)
	}
	return nil
}

func (r *tlsRefresher) getClientCertificate(cri *tls.CertificateRequestInfo) (*tls.Certificate, error) {
	loaded := r.clientCerts.Load()
	if loaded == nil || len(*loaded) == 0 {
		return &tls.Certificate{}, nil
	}
	certs := *loaded
	if cri != nil {
		for i := range certs {
			cp := certs[i]
			if err := cri.SupportsCertificate(&cp); err == nil {
				return &cp, nil
			}
		}
	}
	cp := certs[0]
	return &cp, nil
}

func logTLS(log *service.Logger, warn bool, format string, args ...any) {
	if log == nil {
		return
	}
	if warn {
		log.Warnf(format, args...)
		return
	}
	log.Infof(format, args...)
}

func certPoolFromPEM(pemBytes []byte) (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		return nil, errors.New("no certificates found")
	}
	return pool, nil
}

func readKeyPair(certFile, keyFile, password string) (certPEM, keyPEM []byte, cert tls.Certificate, err error) {
	if certPEM, err = os.ReadFile(certFile); err != nil {
		return nil, nil, tls.Certificate{}, fmt.Errorf("reading cert_file %q: %w", certFile, err)
	}
	if keyPEM, err = os.ReadFile(keyFile); err != nil {
		return nil, nil, tls.Certificate{}, fmt.Errorf("reading key_file %q: %w", keyFile, err)
	}
	cert, err = loadKeyPair(certPEM, keyPEM, password)
	if err != nil {
		return nil, nil, tls.Certificate{}, fmt.Errorf("parsing client certificate %q: %w", certFile, err)
	}
	return certPEM, keyPEM, cert, nil
}

// loadKeyPair decrypts a PKCS#1 or PKCS#8 key with the configured password.
// The obsolete pbeWithMD5AndDES-CBC algorithm is not supported for PKCS#8,
// matching the shared TLS field.
func loadKeyPair(cert, key []byte, password string) (tls.Certificate, error) {
	keyPem, _ := pem.Decode(key)
	if keyPem == nil {
		return tls.Certificate{}, errors.New("decoding private key")
	}

	//nolint:staticcheck // SA1019: encrypted PKCS#1 keys still use these calls.
	if x509.IsEncryptedPEMBlock(keyPem) {
		if password == "" {
			return tls.Certificate{}, errors.New("missing password for PKCS#1 encrypted private key")
		}
		decryptedKey, err := x509.DecryptPEMBlock(keyPem, []byte(password)) //nolint:staticcheck // SA1019: encrypted PKCS#1 keys still use this call.
		if err != nil {
			return tls.Certificate{}, fmt.Errorf("parsing encrypted PKCS#1 private key: %w", err)
		}
		validKey := false
		if _, err = x509.ParsePKCS1PrivateKey(decryptedKey); err == nil {
			validKey = true
		}
		if _, err = x509.ParsePKCS8PrivateKey(decryptedKey); err == nil {
			validKey = true
		}
		if _, err = x509.ParseECPrivateKey(decryptedKey); err == nil {
			validKey = true
		}
		if !validKey {
			return tls.Certificate{}, fmt.Errorf("decrypting PKCS#1 key: %w", x509.IncorrectPasswordError)
		}
		return tls.X509KeyPair(cert, pem.EncodeToMemory(&pem.Block{Type: keyPem.Type, Bytes: decryptedKey}))
	}
	if keyPem.Type == "ENCRYPTED PRIVATE KEY" {
		if password == "" {
			return tls.Certificate{}, errors.New("missing password for PKCS#8 encrypted private key")
		}
		decryptedKey, err := pkcs8.ParsePKCS8PrivateKeyRSA(keyPem.Bytes, []byte(password))
		if err != nil {
			return tls.Certificate{}, fmt.Errorf("parsing encrypted PKCS#8 private key: %w", err)
		}
		return tls.X509KeyPair(cert, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(decryptedKey)}))
	}
	return tls.X509KeyPair(cert, key)
}
