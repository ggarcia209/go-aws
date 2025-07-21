package gos3

// redesign notes:
// move top level functions to SVC object methods
// change return types to struct responses containing return values, errors, etc...
//   - improve backwards/forwards compatiblity by enabling additional return data to be added without affecting code structure
// implement customizable retry logic

import (
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/ggarcia209/go-aws/goaws"

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

func NewDownloader(sess goaws.Session) *s3manager.Downloader {
	return s3manager.NewDownloader(sess.GetSession())
}

func NewUploader(sess goaws.Session) *s3manager.Uploader {
	return s3manager.NewUploader(sess.GetSession())
}

func NewS3Client(session goaws.Session) interface{} {
	// Create SNS client
	svc := s3.New(session.GetSession())

	log.Println("S3 client initialized")

	return svc
}

func InitUploader(svc interface{}, partSize int64) interface{} {
	uploader := s3manager.NewUploaderWithClient(svc.(*s3.S3), func(u *s3manager.Uploader) {
		u.PartSize = partSize
	})
	log.Printf("uploader initialized...")
	return uploader
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

// UploadFile uploads a new file to the given S3 bucket.
func UploadFile(uploader interface{}, bucket, key string, file io.Reader, publicRead bool) (UploadFileResponse, error) {
	input := &s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   file,
	}
	if publicRead {
		input.ACL = aws.String("public-read")
	}
	result, err := uploader.(*s3manager.Uploader).Upload(input)
	if err != nil {
		log.Printf("UploadFile failed: %v", err)
		return UploadFileResponse{}, err
	}

	resp := UploadFileResponse{
		Location: result.Location,
		UploadID: result.UploadID,
	}
	if result.VersionID != nil {
		resp.VersionID = *result.VersionID
	}

	return resp, nil
}

// UploadFileResponse contains the data returned by the S3 Upload operation.
type UploadFileResponse struct {
	Location  string `json:"location"`
	VersionID string `json:"version_id"`
	UploadID  string `json:"upload_id"`
	ETag      string `json:"etag"`
}
