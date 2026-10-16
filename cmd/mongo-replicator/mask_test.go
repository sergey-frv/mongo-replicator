package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMaskURI(t *testing.T) {
	cases := []struct{ name, in, want string }{
		{"с паролем", "mongodb://user:secret@h1:27017/db?replicaSet=rs8", "mongodb://user:xxxxx@h1:27017/db?replicaSet=rs8"},
		{"без userinfo", "mongodb://h1:27017/?replicaSet=rs8", "mongodb://h1:27017/?replicaSet=rs8"},
		{"только user", "mongodb://user@h1:27017/", "mongodb://user@h1:27017/"},
		{"multi-host", "mongodb://u:p@h1:27017,h2:27017/admin?replicaSet=x", "mongodb://u:xxxxx@h1:27017,h2:27017/admin?replicaSet=x"},
		{"multi-host без / перед ?", "mongodb://u:p@h1,h2?replicaSet=x", "mongodb://u:xxxxx@h1,h2?replicaSet=x"},
		{"srv", "mongodb+srv://u:p@cluster.example.com/?tls=true", "mongodb+srv://u:xxxxx@cluster.example.com/?tls=true"},
		{"percent-encoding", "mongodb://u%40x:p%40%3Ass@h1/", "mongodb://u%40x:xxxxx@h1/"},
		{"сырой @ в пароле", "mongodb://u:p@ss@h1/", "mongodb://u:xxxxx@h1/"},
		{"? в пароле", "mongodb://u:p?ss@h1/", "mongodb://u:xxxxx@h1/"},
		{"@ в query", "mongodb://h1/?authSource=a@b", "mongodb://h1/?authSource=a@b"},
		{"без хоста", "mongodb://u:p@", "mongodb://u:xxxxx@"},
		{":// не в начале", "mongodb:u:p@h1://x:y@h2/", "mongodb:u:p@h1://x:y@h2/"},
		{"схема с недопустимым символом", "mongo db://u:p@h1/", "mongo db://u:p@h1/"},
		{"не URI", "garbage", "garbage"},
		{"пусто", "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, maskURI(c.in))
		})
	}
}

func TestLogURI(t *testing.T) {
	const hidden = "(скрыт: в нём может быть пароль)"
	cases := []struct{ name, in, want string }{
		{"пароль маскируется", "mongodb://u:Secret@h1/", "mongodb://u:xxxxx@h1/"},
		{"только user", "mongodb://u@h1:27017/", "mongodb://u@h1:27017/"},
		{"не URI", "garbage", "garbage"},
		{"неэкранированный / в пароле", "mongodb://u:Secret/x@h1/", hidden},
		{"без ://", "mongodb:u:Secret@h1/", hidden},
		{":// после пароля", "mongodb:u:Secret@h1://bad", hidden},
		{":// после пароля и userinfo за ним", "mongodb:u:Secret@h1://x:y@h2/", hidden},
		// maskURI маскирует только начало пароля
		{"неэкранированные @ и / в пароле", "mongodb://u:prefix@x/Secret@h/?connectTimeoutMS=bad", hidden},
		{"неэкранированный @ в имени", "mongodb://u@x:Secret@h1/", hidden},
		{"@ в query без пароля", "mongodb://h1/?authSource=a@b", "mongodb://h1/?authSource=a@b"},
		{"пароль и секрет в query", "mongodb://u:Secret@h1/?tls=true&tlsCertificateKeyFilePassword=Secret",
			"mongodb://u:xxxxx@h1/?tls=true&tlsCertificateKeyFilePassword=xxxxx"},
		{"секрет в query без ://", "mongodb:h1/?tlsCertificateKeyFilePassword=Secret",
			"mongodb:h1/?tlsCertificateKeyFilePassword=xxxxx"},
		{"? в значении секрета", "mongodb://u:Secret@h1/?tlsCertificateKeyFilePassword=alpha?Secret",
			"mongodb://u:xxxxx@h1/?tlsCertificateKeyFilePassword=xxxxx"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, logURI(c.in))
		})
	}
}

func TestMaskOptions(t *testing.T) {
	cases := []struct{ name, in, want string }{
		{"tls", "mongodb://h1/?tlsCertificateKeyFilePassword=Secret&tls=true",
			"mongodb://h1/?tlsCertificateKeyFilePassword=xxxxx&tls=true"},
		{"ssl, регистр", "mongodb://h1/?SSLClientCertificateKeyPassword=Secret",
			"mongodb://h1/?SSLClientCertificateKeyPassword=xxxxx"},
		{"aws, разделитель ;", "mongodb://h1/?authMechanism=MONGODB-AWS;authMechanismProperties=AWS_SESSION_TOKEN:Secret",
			"mongodb://h1/?authMechanism=MONGODB-AWS;authMechanismProperties=xxxxx"},
		{"экранированный ключ", "mongodb://h1/?tlsCertificateKeyFilePassw%6Frd=Secret",
			"mongodb://h1/?tlsCertificateKeyFilePassw%6Frd=xxxxx"},
		{"неразбираемый ключ", "mongodb://h1/?tls%zz=Secret", "mongodb://h1/?tls%zz=xxxxx"},
		{"пустое значение", "mongodb://h1/?tlsCertificateKeyFilePassword=", "mongodb://h1/?tlsCertificateKeyFilePassword=xxxxx"},
		{"? в значении", "mongodb://h1/?tlsCertificateKeyFilePassword=alpha?Secret&tls=true",
			"mongodb://h1/?tlsCertificateKeyFilePassword=xxxxx&tls=true"},
		{"? в значении после ;", "mongodb://h1/?tls=true;authMechanismProperties=AWS_SESSION_TOKEN:a?Secret",
			"mongodb://h1/?tls=true;authMechanismProperties=xxxxx"},
		{"? в имени пользователя", "mongodb://us?er@h1/?tlsCertificateKeyFilePassword=a?Secret",
			"mongodb://us?er@h1/?tlsCertificateKeyFilePassword=xxxxx"},
		{"без секретов", "mongodb://h1/db?replicaSet=rs&authSource=a@b", "mongodb://h1/db?replicaSet=rs&authSource=a@b"},
		{"не URI", "garbage", "garbage"},
		{"пусто", "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, maskOptions(c.in))
		})
	}
}
