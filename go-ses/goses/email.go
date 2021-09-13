package goses

import (
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ses"
)

// CharSet repsents the charset type for email messages (UTF-8)
const CharSet = "UTF-8"

// InitSesh initializes a new SES session.
func InitSesh() interface{} {
	// Initialize a session that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials
	sesh := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	log.Printf("region: %v", aws.StringValue(sesh.Config.Region))

	// Create SNS client
	svc := ses.New(sesh)

	log.Println("SES client initialized")

	return svc
}

// ListVerifiedIdentities lists the SES verified email addresses for the account.
func ListVerifiedIdentities(svc interface{}) error {
	result, err := svc.(*ses.SES).ListIdentities(&ses.ListIdentitiesInput{IdentityType: aws.String("EmailAddress")})
	if err != nil {
		log.Printf("ListVerifiedIdentities failed: %v", err)
		return err
	}

	for _, email := range result.Identities {
		e := []*string{email}

		verified, err := svc.(*ses.SES).GetIdentityVerificationAttributes(&ses.GetIdentityVerificationAttributesInput{Identities: e})
		if err != nil {
			log.Printf("ListVerifiedIdentities failed: %v", err)
			return err
		}

		for _, va := range verified.VerificationAttributes {
			if *va.VerificationStatus == "Success" {
				log.Println(*email)
			}
		}
	}
	return nil
}

// SendEmail sends a new email message. To and CC addresses are passed as []string, all other fields as strings.
func SendEmail(svc interface{}, to, cc []string, from, subject, textBody, htmlBody string) error {
	ccAddr, toAddr := []*string{}, []*string{}
	for _, addr := range to {
		a := aws.String(addr)
		toAddr = append(toAddr, a)
	}
	for _, addr := range cc {
		a := aws.String(addr)
		ccAddr = append(ccAddr, a)
	}

	// Assemble the email.
	input := &ses.SendEmailInput{
		Destination: &ses.Destination{
			CcAddresses: ccAddr,
			ToAddresses: toAddr,
		},
		Message: &ses.Message{
			Body: &ses.Body{
				Html: &ses.Content{
					Charset: aws.String(CharSet),
					Data:    aws.String(htmlBody),
				},
				Text: &ses.Content{
					Charset: aws.String(CharSet),
					Data:    aws.String(textBody),
				},
			},
			Subject: &ses.Content{
				Charset: aws.String(CharSet),
				Data:    aws.String(subject),
			},
		},
		Source: aws.String(from),
		// Uncomment to use a configuration set
		//ConfigurationSetName: aws.String(ConfigurationSet),
	}

	// Attempt to send the email.
	result, err := svc.(*ses.SES).SendEmail(input)

	// Display error messages if they occur.
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case ses.ErrCodeMessageRejected:
				log.Printf("SendEmail failed: %v: %v", ses.ErrCodeMessageRejected, aerr.Error())
			case ses.ErrCodeMailFromDomainNotVerifiedException:
				log.Printf("SendEmail failed: %v: %v", ses.ErrCodeMailFromDomainNotVerifiedException, aerr.Error())
			case ses.ErrCodeConfigurationSetDoesNotExistException:
				log.Printf("SendEmail failed: %v: %v", ses.ErrCodeConfigurationSetDoesNotExistException, aerr.Error())
			default:
				log.Printf("SendEmail failed: %v", aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			log.Printf("SendEmail failed: %v", err.Error())
		}

		return err
	}
	log.Printf("result: %v", result) // test only
	return nil
}
