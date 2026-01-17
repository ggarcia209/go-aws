package gos3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/ggarcia209/go-aws/v2/goaws"
	"go.openly.dev/pointy"
)

//go:generate mockgen -destination=../mocks/gos3mock/s3.go -package=gos3mock . S3Logic
type S3Logic interface {
	GetObject(ctx context.Context, req GetFileRequest) ([]byte, error)
	HeadObject(ctx context.Context, req GetFileRequest) (*HeadObjectResponse, error)
	CheckIfObjectExists(ctx context.Context, req GetFileRequest) (bool, error)
	UploadFile(ctx context.Context, req UploadFileRequest) (*UploadFileResponse, error)
	DeleteFile(ctx context.Context, bucket, key string) error
	GetPresignedURL(ctx context.Context, req GetPresignedUrlRequest) (*GetPresignedUrlResponse, error)
}

type S3 struct {
	svc *s3.Client
}

func NewS3(config goaws.AwsConfig, partitionSize int64) *S3 {
	return &S3{
		svc: NewS3Client(config.Config),
	}
}

func NewS3Client(config aws.Config) *s3.Client {
	return s3.NewFromConfig(config)
}

// GetObject returns the S3 object at the given bucket/key as a byte slice.
// TODO: add options for checksum
func (s *S3) GetObject(ctx context.Context, req GetFileRequest) ([]byte, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(req.Bucket),
		Key:    aws.String(req.Key),
	}

	if req.UseChecksum {
		input.ChecksumMode = types.ChecksumModeEnabled
	}

	obj, err := s.svc.GetObject(ctx, input)
	if err != nil {
		var notExist *types.NoSuchKey
		var re *awshttp.ResponseError
		switch {
		case errors.As(err, &notExist):
			return nil, ErrItemNotFound
		case errors.As(err, &re):
			if re.ResponseError == nil {
				return nil, fmt.Errorf("s.svc.HeadObject: %w", re.Err)
			}
			switch re.ResponseError.HTTPStatusCode() {
			case http.StatusNotFound:
				return nil, ErrItemNotFound
			default:
				return nil, fmt.Errorf("s.svc.HeadObject: %w", re.Err)
			}
		default:
			return nil, fmt.Errorf("s.svc.GetObject: %w", err)
		}
	}

	buf := new(strings.Builder)
	if _, err = io.Copy(buf, obj.Body); err != nil {
		return nil, fmt.Errorf("io.Copy: %w", err)
	}

	res := []byte(buf.String())

	return res, nil
}

func (s *S3) HeadObject(ctx context.Context, req GetFileRequest) (*HeadObjectResponse, error) {
	input := &s3.HeadObjectInput{
		Bucket: aws.String(req.Bucket),
		Key:    aws.String(req.Key),
	}

	if req.UseChecksum {
		input.ChecksumMode = types.ChecksumModeEnabled
	}

	obj, err := s.svc.HeadObject(ctx, input)
	if err != nil {
		var notExist *types.NoSuchKey
		var re *awshttp.ResponseError
		switch {
		case errors.As(err, &notExist):
			return nil, ErrItemNotFound
		case errors.As(err, &re):
			if re.ResponseError == nil {
				return nil, fmt.Errorf("s.svc.HeadObject: %w", re.Err)
			}
			switch re.ResponseError.HTTPStatusCode() {
			case http.StatusNotFound:
				return nil, ErrItemNotFound
			default:
				return nil, fmt.Errorf("s.svc.HeadObject: %w", re.Err)
			}
		default:
			return nil, fmt.Errorf("s.svc.GetObject: %w", err)
		}
	}

	resp := &HeadObjectResponse{
		Metadata: obj.Metadata,
	}

	if obj.ContentType != nil {
		resp.ContentType = *obj.ContentType
	}

	if req.UseChecksum && obj.ChecksumSHA256 != nil {
		resp.Sha256Checksum = *obj.ChecksumSHA256
	}
	return resp, nil
}

// CheckIfObjectExists checks if a head object exists at bucket/key
func (s *S3) CheckIfObjectExists(ctx context.Context, req GetFileRequest) (bool, error) {
	if _, err := s.svc.HeadObject(
		ctx,
		&s3.HeadObjectInput{
			Bucket: aws.String(req.Bucket),
			Key:    aws.String(req.Key),
		},
	); err != nil {
		var notExist *types.NoSuchKey
		var re *awshttp.ResponseError
		switch {
		case errors.As(err, &notExist):
			return false, nil
		case errors.As(err, &re):
			if re.ResponseError == nil {
				return false, fmt.Errorf("s.svc.HeadObject: %w", re.Err)
			}
			switch re.ResponseError.HTTPStatusCode() {
			case http.StatusNotFound:
				return false, nil
			default:
				return false, fmt.Errorf("s.svc.HeadObject: %w", re.Err)
			}
		default:
			return false, fmt.Errorf("s.svc.HeadObject: %w", err)
		}
	}

	return true, nil
}

// UploadFile uploads a new file to the given S3 bucket.
func (s *S3) UploadFile(ctx context.Context, req UploadFileRequest) (*UploadFileResponse, error) {
	input := &s3.PutObjectInput{
		Bucket:   aws.String(req.Bucket),
		Key:      aws.String(req.Key),
		Body:     req.File,
		Metadata: req.Metadata,
	}

	if req.Checksum != nil {
		input.ChecksumAlgorithm = types.ChecksumAlgorithmSha256
		input.ChecksumSHA256 = pointy.String(string(*req.Checksum))
	}

	result, err := s.svc.PutObject(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("s.svc.PutObject: %w", err)
	}

	resp := &UploadFileResponse{}
	if result.VersionId != nil {
		resp.VersionID = *result.VersionId
	}

	return resp, nil
}

// DeleteFile deletes the the file at bucket/key
func (s *S3) DeleteFile(ctx context.Context, bucket, key string, versionId *string) error {
	input := &s3.DeleteObjectInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(key),
		VersionId: versionId,
	}

	if _, err := s.svc.DeleteObject(ctx, input); err != nil {
		return fmt.Errorf("s.svc.DeleteObject: %w", err)
	}

	return nil
}

// GetPresignedURL returns presigned URLs for put and get requests
func (s *S3) GetPresignedURL(ctx context.Context, req GetPresignedUrlRequest) (*GetPresignedUrlResponse, error) {
	var presignedUrl = new(GetPresignedUrlResponse)

	psCli := s3.NewPresignClient(s.svc)
	if req.Put != nil {
		input := &s3.PutObjectInput{
			Bucket:   aws.String(req.Put.Bucket),
			Key:      aws.String(req.Put.Key),
			Body:     req.Put.File,
			Metadata: req.Put.Metadata,
		}

		if req.Put.Checksum != nil {
			input.ChecksumAlgorithm = types.ChecksumAlgorithmSha256
			input.ChecksumSHA256 = pointy.String(string(*req.Put.Checksum))
		}

		resp, err := psCli.PresignPutObject(
			ctx,
			input,
			s3.WithPresignExpires(time.Second*time.Duration(req.ExpirySeconds)),
		)
		if err != nil {
			return nil, fmt.Errorf("psCli.PresignGetObject")
		}
		presignedUrl.PutUrl = resp.URL
	}

	if req.Get != nil {
		input := &s3.GetObjectInput{
			Bucket: aws.String(req.Get.Bucket),
			Key:    aws.String(req.Get.Key),
		}

		if req.Get.UseChecksum {
			input.ChecksumMode = types.ChecksumModeEnabled
		}

		resp, err := psCli.PresignGetObject(
			ctx,
			input,
			s3.WithPresignExpires(time.Second*time.Duration(req.ExpirySeconds)),
		)
		if err != nil {
			return nil, fmt.Errorf("psCli.PresignGetObject")
		}
		presignedUrl.GetUrl = resp.URL
	}

	return presignedUrl, nil
}
