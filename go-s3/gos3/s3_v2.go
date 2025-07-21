package gos3

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/ggarcia209/go-aws/goaws"
)

type S3Logic interface {
	GetObject(bucket, key string) ([]byte, error)
	UploadFile(bucket, key string, file io.Reader, publicRead bool) (UploadFileResponse, error)
}

type S3v2 struct {
	svc        *s3.S3
	uploader   *s3manager.Uploader
	downloader *s3manager.Downloader
}

func NewS3v2(session goaws.Session) *S3v2 {
	svc := s3.New(session.GetSession())
	uploader := NewUploader(session)
	downloader := NewDownloader(session)

	return &S3v2{svc: svc, uploader: uploader, downloader: downloader}
}

func (s *S3v2) GetObject(bucket, key string) ([]byte, error) {
	obj, err := s.svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == "NoSuchKey" {
				return []byte{}, errors.New(ErrNoSuchKey)
			}
		}
		return []byte{}, fmt.Errorf("s.svc.GetObject: %w", err)
	}

	buf := new(strings.Builder)
	if _, err = io.Copy(buf, obj.Body); err != nil {
		return []byte{}, fmt.Errorf("io.Copy: %w", err)
	}

	res := []byte(buf.String())

	return res, nil
}

func (s *S3v2) UploadFile(bucket, key string, file io.Reader, publicRead bool) (UploadFileResponse, error) {
	input := &s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   file,
	}
	if publicRead {
		input.ACL = aws.String("public-read")
	}
	result, err := s.uploader.Upload(input)
	if err != nil {
		return UploadFileResponse{}, fmt.Errorf("s.uploader.Upload: %w", err)
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
