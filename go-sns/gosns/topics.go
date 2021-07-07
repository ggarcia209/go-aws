package gosns

import (
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"

	"fmt"
)

// InvalidSvcArgErr is returned when an interface object passed as the svc argument to
// the package methods is not the AWS *sns.SNS type.
const InvalidSvcArgErr = "INVALID_SVC_ARG_TYPE"

// InvliadProtocolErr is returned when an invalid value is passed to the Subscribe function.
const InvalidProtocolErr = "INVALID_SUBSCRIPTION_PROTOCOL"

// InitSesh intitializes a new SNS client session and returns the AWS *sns.SNS object
// as an interface type to maintain encapsulation of the AWS sns package. The *sns.SNS
// type is asserted by the methods used in this package, which return the InvalidSvcArgErr
// if the type is invalid.
func InitSesh() interface{} {
	// Initialize a session that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials
	sesh := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	log.Printf("region: %v", aws.StringValue(sesh.Config.Region))

	// Create SNS client
	svc := sns.New(sesh)

	log.Println("SQS client initialized")

	return svc
}

// ListTopics prints and returns a list of all SNS topics' ARNs in the AWS account.
func ListTopics(svc interface{}) ([]string, error) {
	arns := []string{}
	_, ok := svc.(*sns.SNS)
	if !ok {
		err := fmt.Errorf(InvalidSvcArgErr)
		log.Printf("ListTopics failed: %v", err)
		return arns, err
	}

	// get topics
	result, err := svc.(*sns.SNS).ListTopics(nil)
	if err != nil {
		log.Printf("ListTopics failed: %v", err)
		return arns, err
	}

	// print topic ARNs
	for _, t := range result.Topics {
		fmt.Println(*t.TopicArn)
		arns = append(arns, *t.TopicArn)
	}

	return arns, nil
}

// CreateTopic creates a new SNS topic with the given name.
func CreateTopic(svc interface{}, name string) (string, error) {
	_, ok := svc.(*sns.SNS)
	if !ok {
		err := fmt.Errorf(InvalidSvcArgErr)
		log.Printf("CreateTopic failed: %v", err)
		return "", err
	}

	result, err := svc.(*sns.SNS).CreateTopic(&sns.CreateTopicInput{
		Name: aws.String(name),
	})
	if err != nil {
		log.Printf("CreateTopics failed: %v", err)
		return "", err
	}

	return *result.TopicArn, nil
}

// Subscribe creates a new subscription for an endpoint.
func Subscribe(svc interface{}, endpoint, protocol, topicArn string) (string, error) {
	_, ok := svc.(*sns.SNS)
	if !ok {
		err := fmt.Errorf(InvalidSvcArgErr)
		log.Printf("Subscribe failed: %v", err)
		return "", err
	}
	validProtocols := map[string]bool{
		"http":        true,
		"https":       true,
		"email":       true,
		"email-json":  true,
		"sms":         true,
		"sqs":         true,
		"application": true,
		"lambda":      true,
		"firehose":    true,
	}
	if !validProtocols[protocol] {
		err := fmt.Errorf(InvalidProtocolErr)
		log.Printf("Subsrcibe failed: %v (%s)", err, protocol)
		return "", err
	}

	result, err := svc.(*sns.SNS).Subscribe(&sns.SubscribeInput{
		Endpoint:              aws.String(endpoint),
		Protocol:              aws.String(protocol),
		ReturnSubscriptionArn: aws.Bool(true), // Return the ARN, even if user has yet to confirm
		TopicArn:              aws.String(topicArn),
	})
	if err != nil {
		log.Printf("Subscribe failed: %v", err)
		return "", err
	}

	return *result.SubscriptionArn, nil
}

// Publish publishes a new message to a Topic and returns the message ID
// of the published message.
func Publish(svc interface{}, msgStr, topicArn string) (string, error) {
	_, ok := svc.(*sns.SNS)
	if !ok {
		err := fmt.Errorf(InvalidSvcArgErr)
		log.Printf("Publish failed: %v", err)
		return "", err
	}

	result, err := svc.(*sns.SNS).Publish(&sns.PublishInput{
		Message:  aws.String(msgStr),
		TopicArn: aws.String(topicArn),
	})
	if err != nil {
		log.Printf("Publish failed: %v", err)
		return "", err
	}

	return *result.MessageId, nil
}
