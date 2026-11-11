package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"

	"google.golang.org/grpc/credentials"
)

// transportCredentials builds the server's TLS credentials from the flags, or returns
// nil when TLS is off.
//
// The certificate is read again for every handshake, so a renewed certificate is
// picked up without a restart. Connections are long-lived, so that costs nothing.
func transportCredentials(cfg config) (credentials.TransportCredentials, error) {
	if cfg.tlsCert == "" && cfg.tlsKey == "" {
		if cfg.tlsClientCA != "" {
			return nil, errors.New("-tls-client-ca needs -tls-cert and -tls-key")
		}
		return nil, nil //nolint:nilnil // no credentials is a valid answer: TLS is off
	}
	if cfg.tlsCert == "" || cfg.tlsKey == "" {
		return nil, errors.New("-tls-cert and -tls-key go together")
	}
	if _, err := tls.LoadX509KeyPair(cfg.tlsCert, cfg.tlsKey); err != nil {
		return nil, fmt.Errorf("the server certificate: %w", err)
	}
	conf := &tls.Config{
		MinVersion: tls.VersionTLS13,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			cert, err := tls.LoadX509KeyPair(cfg.tlsCert, cfg.tlsKey)
			if err != nil {
				return nil, err
			}
			return &cert, nil
		},
	}
	if cfg.tlsClientCA != "" {
		pem, err := os.ReadFile(cfg.tlsClientCA)
		if err != nil {
			return nil, fmt.Errorf("the client CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("the client CA: no certificate in %s", cfg.tlsClientCA)
		}
		conf.ClientCAs, conf.ClientAuth = pool, tls.RequireAndVerifyClientCert
	}
	return credentials.NewTLS(conf), nil
}
