package goses

import (
	"testing"

	"github.com/ggarcia209/go-aws/goaws"
)

func TestListVerifiedIdentities(t *testing.T) {
	svc := InitSesh()
	err := ListVerifiedIdentities(svc)
	if err != nil {
		t.Errorf("FAIL: %v", err)
	}
}

func TestSendEmail(t *testing.T) {
	var tests = []struct {
		to       []string
		cc       []string
		replyTo  []string
		textBody string
		htmlBody string
	}{
		{to: []string{"danielgarcia95367@gmail.com"}, cc: []string{}, textBody: "Testing\nThis is a test", replyTo: []string{}, htmlBody: "<h1>Testing</h1><p>This is a test</p>"},
		{to: []string{"danielgarcia95367@gmail.com"}, cc: []string{}, textBody: "Testing\nThis is a test", replyTo: []string{}, htmlBody: ""}, // result: empty msg body - no text body output
	}
	subject := "SES TEST"
	from := "dg.dev.test510@gmail.com"
	svc := InitSesh()
	for _, test := range tests {
		err := SendEmail(svc, test.to, test.cc, test.replyTo, from, subject, test.textBody, test.htmlBody)
		if err != nil {
			t.Errorf("FAIL: %v", err)
		}
	}
}

func TestSendEmailWithSession(t *testing.T) {
	var tests = []struct {
		to       []string
		cc       []string
		replyTo  []string
		textBody string
		htmlBody string
	}{
		{to: []string{"danielgarcia95367@gmail.com"}, cc: []string{}, replyTo: []string{}, textBody: "Testing\nThis is a test", htmlBody: "<h1>Testing</h1><br><p>This is an HTML test</p>"},
		{to: []string{"danielgarcia95367@gmail.com"}, cc: []string{}, replyTo: []string{}, textBody: "Testing\nThis is a test", htmlBody: ""}, // result: empty msg body - no text body output
	}
	subject := "SES TEST"
	from := "dg.dev.test510@gmail.com"

	for _, test := range tests {
		sesh := goaws.NewDefaultSession()
		svc := NewSESClient(sesh)
		err := SendEmail(svc, test.to, test.cc, test.replyTo, from, subject, test.textBody, test.htmlBody)
		if err != nil {
			t.Errorf("FAIL: %v", err)
		}
	}
}
