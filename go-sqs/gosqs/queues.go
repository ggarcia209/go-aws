package gosqs

import (
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// QueueDefault contains the default attribute values for new SQS Queue objects
var QueueDefault = QueueOptions{
	DelaySeconds:                  "0",
	MaximumMessageSize:            "262144",
	MessageRetentionPeriod:        "345600",
	Policy:                        "",
	ReceiveMessageWaitTimeSeconds: "0",
	RedrivePolicy:                 "",
	VisibilityTimeout:             "30",
	KmsMasterKeyId:                "",
	KmsDataKeyReusePeriodSeconds:  "300",
	FifoQueue:                     "false",
	ContentBasedDeduplication:     "false",
	// * high throughput preview *
	// only available in us-east-1, us-east-2, us-west-2, eu-west-1
	DeduplicationScope:  "queue",
	FifoThroughputLimit: "perQueue",
	// *  *
}

// QueueOptions contains struct fields for setting custom options when creating a new SQS queue
type QueueOptions struct {
	DelaySeconds                  string
	MaximumMessageSize            string
	MessageRetentionPeriod        string
	Policy                        string // IAM Policy
	ReceiveMessageWaitTimeSeconds string
	RedrivePolicy                 string
	VisibilityTimeout             string
	KmsMasterKeyId                string
	KmsDataKeyReusePeriodSeconds  string
	FifoQueue                     string
	ContentBasedDeduplication     string
	DeduplicationScope            string
	FifoThroughputLimit           string
}

// QueueTags is a map object that enables tags when creating a new queue with CreateQueue()
type QueueTags map[string]*string

// InitSesh initializes a new session with default config/credentials.
func InitSesh() *sqs.SQS {
	// Initialize a session that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials
	sesh := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	fmt.Println("session intialized")
	fmt.Println("region: ", aws.StringValue(sesh.Config.Region))

	// Create DynamoDB client
	svc := sqs.New(sesh)

	fmt.Println("SQS client initialized")
	fmt.Println()

	return svc
}

// CreateQueue creates a new SQS queue per the given name, options, & tags arguments and returns the url of the queue and/or error
func CreateQueue(svc *sqs.SQS, name string, options QueueOptions, tags map[string]*string) (string, error) {
	url := ""
	input := &sqs.CreateQueueInput{
		QueueName: &name,
		Attributes: map[string]*string{
			"DelaySeconds":                  aws.String(options.DelaySeconds),
			"MaximumMessageSize":            aws.String(options.MaximumMessageSize),
			"MessageRetentionPeriod":        aws.String(options.MessageRetentionPeriod),
			"Policy":                        aws.String(options.Policy),
			"ReceiveMessageWaitTimeSeconds": aws.String(options.ReceiveMessageWaitTimeSeconds),
			"RedrivePolicy":                 aws.String(options.RedrivePolicy),
			"VisibilityTimeout":             aws.String(options.VisibilityTimeout),
			"KmsMasterKeyId":                aws.String(options.KmsMasterKeyId),
			"KmsDataKeyReusePeriodSeconds":  aws.String(options.KmsDataKeyReusePeriodSeconds),
		},
	}
	// set FIFO Queue options
	if options.FifoQueue == "true" {
		input.Attributes["FifoQueue"] = aws.String("true")
		input.Attributes["ContentBasedDeduplication"] = aws.String(options.ContentBasedDeduplication)
		input.Attributes["DeduplicationScope"] = aws.String(options.DeduplicationScope)
		input.Attributes["FifoThroughputLimit"] = aws.String(options.FifoThroughputLimit)
	}
	// set tags
	if len(tags) > 0 {
		input.Tags = tags
	}
	result, err := svc.CreateQueue(input)
	if err != nil {
		log.Printf("CreateQueue failed: %v", err.Error())
		return url, err
	}

	url = *result.QueueUrl
	log.Print("CreateQueue succeeded: ", url)
	return url, nil
}

// GetQueueURL retrives the URL for the given queue name
func GetQueueURL(svc *sqs.SQS, name string) (string, error) {
	result, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: &name,
	})
	if err != nil {
		log.Printf("GetQueueURLfailed: %v", err.Error())
		return "", err
	}
	return *result.QueueUrl, nil
}

// DeleteQueue deletes the queue at the given URL
func DeleteQueue(svc *sqs.SQS, url string) error {
	_, err := svc.DeleteQueue(&sqs.DeleteQueueInput{
		QueueUrl: aws.String(url),
	})
	if err != nil {
		log.Printf("DeleteQueue failed: %v", err.Error())
		return err
	}

	return nil
}

// PurgeQueue purges the specified queue.
func PurgeQueue(svc *sqs.SQS, url string) error {
	_, err := svc.PurgeQueue(&sqs.PurgeQueueInput{
		QueueUrl: aws.String(url),
	})
	if err != nil {
		log.Printf("PurgeQueue failed: %v", err.Error())
		return err
	}

	return nil
}
