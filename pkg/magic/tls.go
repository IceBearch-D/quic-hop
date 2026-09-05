package magic

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"time"

	"github.com/quic-go/quic-go"
)

func GenerateTLSConfig() *tls.Config { // 生成自签名 TLS 证书
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now(), NotAfter: time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	certDER, _ := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	tlsCert, _ := tls.X509KeyPair(certPEM, keyPEM)
	return &tls.Config{
		Certificates:       []tls.Certificate{tlsCert},
		NextProtos:         []string{"quic-poly-hop"},
		InsecureSkipVerify: true,
	}
}

type FixedBytesGenerator struct{ Len int } // 固定长度的 Connection ID 生成器

func (g *FixedBytesGenerator) GenerateConnectionID() (quic.ConnectionID, error) {
	b := make([]byte, g.Len)                  // 固定长度，返回的b是零值
	rand.Read(b)                              // 填充随机数据以增加不可预测性
	return quic.ConnectionIDFromBytes(b), nil // quic.ConnectionIDFromBytes 会复制一份，最后返回的值是
}
func (g *FixedBytesGenerator) ConnectionIDLen() int { return g.Len }
