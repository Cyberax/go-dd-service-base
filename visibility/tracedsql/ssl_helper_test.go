package tracedsql

import (
	"crypto/x509"
	"encoding/pem"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"testing"
)

func TestMakeCaCert(t *testing.T) {
	_, err := MakeCaCertFile(100)
	assert.Error(t, err)

	certFile, err := MakeCaCertFile(Rds2019)
	assert.NoError(t, err)

	data, err := ioutil.ReadFile(certFile)
	assert.NoError(t, err)

	block, _ := pem.Decode(data)
	certificate, err := x509.ParseCertificate(block.Bytes)
	assert.NoError(t, err)

	assert.Equal(t, "Amazon RDS Root 2019 CA", certificate.Issuer.CommonName)
}
