package main

import (
	"log"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"crypto/rand"
	_ "crypto/tls"
	"time"
	_ "github.com/davecgh/go-spew/spew"
	"encoding/asn1"
	"math/big"
	"encoding/pem"
	"os"
	"io"
)

func mustNoErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func fillTemplateCert() (template x509.Certificate) {
	now := time.Now()
	template.NotBefore = now.Add(-24*time.Hour)
	// interestingly, 100 years ahead doesn't work. Go complains
	// that this date cannot be represented by UTCTime in asn1
	template.NotAfter = now.Add(24*3650*time.Hour)
	template.Version = 2
	template.SerialNumber = big.NewInt(now.UnixNano())

	// this stuff is simply taken from openssl-generated certificate
	template.Issuer.CommonName = "*"
	template.Issuer.Names = []pkix.AttributeTypeAndValue{
		pkix.AttributeTypeAndValue{
			Type: asn1.ObjectIdentifier{2,5,4,3},
			Value: "*",
		},
	}
	template.Subject = template.Issuer

	return
}

func pemIfy(octets []byte, pemType string, out io.Writer) {
	pem.Encode(out, &pem.Block{
		Type: pemType,
		Bytes: octets,
	})
}

func main() {
	pkey, err := rsa.GenerateKey(rand.Reader, 2048)
	mustNoErr(err)
	template := fillTemplateCert()
	// log.Printf("template cert:\n%s", spew.Sdump(template))
	cert, err := x509.CreateCertificate(rand.Reader, &template, &template, &(pkey.PublicKey), pkey)
	mustNoErr(err)
	// log.Printf("cert:\n%s", spew.Sdump(x509.ParseCertificate(cert)))

	pkeyOctets := x509.MarshalPKCS1PrivateKey(pkey)
	pemIfy(cert, "CERTIFICATE", os.Stdout)
	pemIfy(pkeyOctets, "PRIVATE KEY", os.Stdout)
}
