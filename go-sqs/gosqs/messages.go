package gosqs

import (
	"crypto/md5"
	"encoding/hex"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// SendMsgDefault contains the default options for the sqs.SendMessageInput object.
var SendMsgDefault = SendMsgOptions{
	DelaySeconds:            int64(0),
	MessageAttributes:       nil,
	MessageBody:             "",
	MessageDeduplicationId:  "",
	MessageGroupId:          "",
	MessageSystemAttributes: nil,
	QueueURL:                "",
}

// SendMsgOptions is used to pass send message options to the sqs.SendMessageInput object.
type SendMsgOptions struct {
	DelaySeconds            int64
	MessageAttributes       map[string]*sqs.MessageAttributeValue
	MessageBody             string
	MessageDeduplicationId  string
	MessageGroupId          string
	MessageSystemAttributes map[string]*sqs.MessageSystemAttributeValue
	QueueURL                string
}

// RecMsgDefault contains the default values for the sqs.ReceiveMessageInput object.
var RecMsgDefault = RecMsgOptions{
	AttributeNames:          []*string{aws.String("All")},
	MaxNumberOfMessages:     int64(1),
	MessageAttributeNames:   []*string{aws.String("All")},
	QueueURL:                "",
	ReceiveRequestAttemptId: "",
	VisibilityTimeout:       int64(30),
	WaitTimeSeconds:         int64(3),
}

// RecMsgOptions is used to pass receive message options to the sqs.ReceiveMessageInput object.
type RecMsgOptions struct {
	AttributeNames          []*string
	MaxNumberOfMessages     int64
	MessageAttributeNames   []*string
	QueueURL                string
	ReceiveRequestAttemptId string
	VisibilityTimeout       int64
	WaitTimeSeconds         int64
}

// MsgAV represents a single sqs.MessageAttributeValue or sqs.MessageSystemAttributeValue object.
// Limited to StringValue types; BinaryValue not supported.
type MsgAV struct {
	Key      string
	DataType string
	Value    string
}

// CreateMsgAttributes creates a MessageAttributeValue map from a list of MsgAV objects.
// Limited to StringValue types; BinaryValue not supported.
func CreateMsgAttributes(attributes []MsgAV) map[string]*sqs.MessageAttributeValue {
	msgAttr := make(map[string]*sqs.MessageAttributeValue)
	for _, av := range attributes {
		attribute := &sqs.MessageAttributeValue{
			DataType:    aws.String(av.DataType),
			StringValue: aws.String(av.Value),
		}
		msgAttr[av.Key] = attribute
	}
	return msgAttr
}

// CreateMsgSystemAttributes creates a MessageSystemAttributeValue map from a list of MsgAV objects
// Limited to StringValue types; BinaryValue not supported
func CreateMsgSystemAttributes(attributes []MsgAV) map[string]*sqs.MessageSystemAttributeValue {
	msgSysAttr := make(map[string]*sqs.MessageSystemAttributeValue)
	for _, av := range attributes {
		attribute := &sqs.MessageSystemAttributeValue{
			DataType:    aws.String(av.DataType),
			StringValue: aws.String(av.Value),
		}
		msgSysAttr[av.Key] = attribute
	}
	return msgSysAttr
}

// CreateMsgAttribute constructs a MsgAV object from the given parameters
func CreateMsgAttribute(key, dataType, value string) MsgAV {
	av := MsgAV{
		Key:      key,
		DataType: dataType,
		Value:    value,
	}
	return av
}

// SendMessage sends a new message to a queue per the options argument.
// Unique MD5 checksums are generated for the MessageDeduplicationID
// and MessageGroupID fields if not set for messages sent to FIFO Queues.
func SendMessage(svc *sqs.SQS, options SendMsgOptions) error {
	// ensure values are valid
	if options.DelaySeconds < 0 {
		options.DelaySeconds = 0
	}
	if options.DelaySeconds > 900 {
		options.DelaySeconds = 900
	}
	input := &sqs.SendMessageInput{
		DelaySeconds:            aws.Int64(options.DelaySeconds),
		MessageAttributes:       options.MessageAttributes,
		MessageBody:             aws.String(options.MessageBody),
		MessageSystemAttributes: options.MessageSystemAttributes,
		QueueUrl:                aws.String(options.QueueURL),
	}
	// set FIFO queue options
	if checkFifo(options.QueueURL) {
		if options.MessageDeduplicationId != "" {
			input.MessageDeduplicationId = aws.String(options.MessageDeduplicationId)
		} else {
			input.MessageDeduplicationId = aws.String(GenerateDedupeID(options.QueueURL))
		}
		if options.MessageGroupId != "" {
			input.MessageGroupId = aws.String(options.MessageGroupId)
		} else {
			input.MessageGroupId = aws.String(GenerateDedupeID(options.QueueURL))
		}
	}

	_, err := svc.SendMessage(input)
	if err != nil {
		log.Printf("SendMessage failed: %v", err.Error())
		return err
	}
	return nil
}

// ReceiveMessage receives a message from a queue per the options argument
func ReceiveMessage(svc *sqs.SQS, options RecMsgOptions) ([]*sqs.Message, error) {
	// ensure values are valid
	if options.MaxNumberOfMessages < 1 {
		options.MaxNumberOfMessages = 1
	}
	if options.MaxNumberOfMessages > 10 {
		options.MaxNumberOfMessages = 10
	}
	if options.VisibilityTimeout < 0 {
		options.VisibilityTimeout = 0
	}
	if options.VisibilityTimeout > 43200 {
		options.VisibilityTimeout = 43200
	}
	if options.WaitTimeSeconds < 1 {
		options.WaitTimeSeconds = 1
	}
	if options.WaitTimeSeconds > 20 {
		options.WaitTimeSeconds = 20
	}
	// set ReceiveRequestAttemptID for FIFO queues if not set
	if checkFifo(options.QueueURL) && options.ReceiveRequestAttemptId == "" {
		options.ReceiveRequestAttemptId = GenerateDedupeID(options.QueueURL)
	}

	msgResult, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
		AttributeNames:          options.AttributeNames,
		MaxNumberOfMessages:     aws.Int64(options.MaxNumberOfMessages),
		MessageAttributeNames:   options.MessageAttributeNames,
		QueueUrl:                aws.String(options.QueueURL),
		ReceiveRequestAttemptId: aws.String(options.ReceiveRequestAttemptId),
		VisibilityTimeout:       aws.Int64(options.VisibilityTimeout),
		WaitTimeSeconds:         aws.Int64(options.WaitTimeSeconds),
	})
	if err != nil {
		log.Printf("ReceiveMessage failed: %v", err.Error())
		return msgResult.Messages, err
	}
	return msgResult.Messages, nil
}

func checkFifo(url string) bool {
	spl := strings.Split(url, ".")
	if len(spl) > 1 {
		appendix := spl[len(spl)-1]
		if appendix == "fifo" {
			return true
		}
	}
	return false
}

func GenerateDedupeID(url string) string {
	timestamp := time.Now()
	hash := md5.Sum([]byte(url + timestamp.String()))
	hashStr := hex.EncodeToString(hash[:])
	return hashStr
}

// DeleteMessage deletes a message from the specified queue (by url) with the
// given handle.
func DeleteMessage(svc *sqs.SQS, url, handle string) error {
	_, err := svc.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      aws.String(url),
		ReceiptHandle: aws.String(handle),
	})
	if err != nil {
		log.Printf("DeleteMessage failed: %v", err.Error())
		return err
	}
	return nil
}
