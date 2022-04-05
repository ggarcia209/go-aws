package goaws

// TO DO: add error handling for credentials not found

import (
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
)

// Session contains an AWS Session for use with other AWS services in the go-aws package.
type Session struct {
	session *session.Session
}

// Retrieve AWS Session from Session object.
func (s *Session) GetSession() *session.Session {
	return s.session
}

func NewDefaultSession() Session {
	// Initialize a session that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials
	s := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	log.Printf("region: %v", aws.StringValue(s.Config.Region))

	sesh := Session{session: s}

	return sesh
}

// InitSesh initializes a new SES session.
func NewSessionWithProfile(profile string) Session {
	// Initialize a session that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials
	// matching the given profile
	s := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Profile:           profile,
	}))
	log.Printf("region: %v", aws.StringValue(s.Config.Region))

	sesh := Session{session: s}

	return sesh
}
