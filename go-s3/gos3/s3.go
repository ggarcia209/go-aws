package gos3

import (
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const ErrNoSuchKey = "ITEM_NOT_FOUND"

// InitSesh initializes a new AWS sesions and S3 client
func InitSesh() interface{} {
	// Initialize a session that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials
	sesh := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	log.Printf("region: %v", aws.StringValue(sesh.Config.Region))

	// Create SNS client
	svc := s3.New(sesh)

	log.Println("S3 client initialized")

	return svc
}

// GetObject returns the S3 object at the given bucket/key as a byte slice.
func GetObject(svc interface{}, bucket, key string) ([]byte, error) {
	obj, err := svc.(*s3.S3).GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		log.Printf("GetObject failed: %v", err)
		if awsErr, ok := err.(awserr.Error); ok {
			log.Printf("code: %v", awsErr.Code())
			if awsErr.Code() == "NoSuchKey" {
				return []byte{}, fmt.Errorf(ErrNoSuchKey)
			}
		}
		return []byte{}, err
	}

	buf := new(strings.Builder)
	_, err = io.Copy(buf, obj.Body)
	if err != nil {
		log.Printf("GetObject failed: %v", err)
		return []byte{}, err
	}

	res := []byte(buf.String())

	return res, nil
}
