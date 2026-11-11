package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// authority is a throwaway CA that writes certificates into a directory.
type authority struct {
	t    *testing.T
	dir  string
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

func newAuthority(t *testing.T, name string) *authority {
	t.Helper()
	a := &authority{t: t, dir: t.TempDir()}
	a.key, _ = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: name},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &a.key.PublicKey, a.key)
	if err != nil {
		t.Fatal(err)
	}
	a.cert, _ = x509.ParseCertificate(der)
	a.write(name+".pem", "CERTIFICATE", der)
	return a
}

func (a *authority) write(file, kind string, der []byte) string {
	a.t.Helper()
	path := filepath.Join(a.dir, file)
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: der}), 0o600); err != nil {
		a.t.Fatal(err)
	}
	return path
}

// issue writes a certificate and its key for name and returns their paths.
func (a *authority) issue(name string, serial int64) (cert, key string) {
	a.t.Helper()
	k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: name},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, a.cert, &k.PublicKey, a.key)
	if err != nil {
		a.t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(k)
	if err != nil {
		a.t.Fatal(err)
	}
	return a.write(name+".pem", "CERTIFICATE", der), a.write(name+"-key.pem", "EC PRIVATE KEY", keyDER)
}

func (a *authority) pool() *x509.CertPool {
	p := x509.NewCertPool()
	p.AddCert(a.cert)
	return p
}

// serve starts a gRPC server with the credentials of cfg and returns its address.
func serve(t *testing.T, cfg config) string {
	t.Helper()
	creds, err := transportCredentials(cfg)
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer(grpc.Creds(creds))
	healthpb.RegisterHealthServer(g, health.NewServer())
	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = g.Serve(lis) }()
	t.Cleanup(g.Stop)
	return lis.Addr().String()
}

// call makes one request with the given credentials.
func call(t *testing.T, addr string, creds credentials.TransportCredentials) error {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err = healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	return err
}

func TestTLS(t *testing.T) {
	ca := newAuthority(t, "ca")
	cert, key := ca.issue("server", 2)
	clientCert, clientKey := ca.issue("client", 3)
	client, err := tls.LoadX509KeyPair(clientCert, clientKey)
	if err != nil {
		t.Fatal(err)
	}
	stranger := newAuthority(t, "stranger")
	strangerCert, strangerKey := stranger.issue("client", 2)
	unknown, err := tls.LoadX509KeyPair(strangerCert, strangerKey)
	if err != nil {
		t.Fatal(err)
	}
	trusting := func(certs ...tls.Certificate) credentials.TransportCredentials {
		return credentials.NewTLS(&tls.Config{RootCAs: ca.pool(), Certificates: certs, MinVersion: tls.VersionTLS13})
	}

	// Server TLS only: a client that trusts the CA gets through, plaintext does not.
	addr := serve(t, config{tlsCert: cert, tlsKey: key})
	if err := call(t, addr, trusting()); err != nil {
		t.Fatalf("TLS: %v", err)
	}
	if err := call(t, addr, insecure.NewCredentials()); err == nil {
		t.Fatal("a plaintext client reached a TLS server")
	}

	// Mutual TLS: only a certificate of the client CA gets through.
	addr = serve(t, config{tlsCert: cert, tlsKey: key, tlsClientCA: filepath.Join(ca.dir, "ca.pem")})
	if err := call(t, addr, trusting(client)); err != nil {
		t.Fatalf("mutual TLS: %v", err)
	}
	if err := call(t, addr, trusting()); err == nil {
		t.Fatal("a client without a certificate got through")
	}
	if err := call(t, addr, trusting(unknown)); err == nil {
		t.Fatal("a client with a certificate of another CA got through")
	}

	// A renewed certificate is used without a restart.
	renewed, renewedKey := ca.issue("renewed", 4)
	for from, to := range map[string]string{renewed: cert, renewedKey: key} {
		data, err := os.ReadFile(from) //nolint:gosec // G304: a file this test wrote
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(to, data, 0o600); err != nil { //nolint:gosec // G703: a path in the test's own directory
			t.Fatal(err)
		}
	}
	var served string
	seen := credentials.NewTLS(&tls.Config{RootCAs: ca.pool(), Certificates: []tls.Certificate{client}, MinVersion: tls.VersionTLS13,
		VerifyConnection: func(cs tls.ConnectionState) error {
			served = cs.PeerCertificates[0].Subject.CommonName
			return nil
		}})
	if err := call(t, addr, seen); err != nil || served != "renewed" {
		t.Fatalf("after the renewal the server presented %q: %v", served, err)
	}
}

func TestTLSFlags(t *testing.T) {
	ca := newAuthority(t, "ca")
	cert, key := ca.issue("server", 2)
	if creds, err := transportCredentials(config{}); err != nil || creds != nil {
		t.Fatalf("no flags: %v %v", creds, err)
	}
	for name, cfg := range map[string]config{
		"a certificate without a key":  {tlsCert: cert},
		"a key without a certificate":  {tlsKey: key},
		"a client CA without TLS":      {tlsClientCA: filepath.Join(ca.dir, "ca.pem")},
		"a certificate that is absent": {tlsCert: filepath.Join(ca.dir, "none.pem"), tlsKey: key},
		"a client CA that is absent":   {tlsCert: cert, tlsKey: key, tlsClientCA: filepath.Join(ca.dir, "none.pem")},
		"a client CA that is no PEM":   {tlsCert: cert, tlsKey: key, tlsClientCA: key + ".txt"},
	} {
		if name == "a client CA that is no PEM" {
			if err := os.WriteFile(key+".txt", []byte("not a certificate"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := transportCredentials(cfg); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}
