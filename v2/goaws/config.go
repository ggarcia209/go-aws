package goaws

// TO DO: add error handling for credentials not found

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

type AwsConfig struct {
	Config aws.Config
}

func NewDefaultConfig(ctx context.Context) (*AwsConfig, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("config.LoadDefaultConfig: %w", err)
	}

	return &AwsConfig{Config: cfg}, nil
}

func NewConfigWithProfile(ctx context.Context, profile string) (*AwsConfig, error) {
	cfg, err := config.LoadDefaultConfig(
		ctx,
		config.WithSharedConfigProfile(profile),
	)
	if err != nil {
		return nil, fmt.Errorf("config.LoadDefaultConfig: %w", err)
	}

	return &AwsConfig{Config: cfg}, nil
}

func NewConfigFromEnv(
	ctx context.Context,
	accessKeyId,
	secretKey,
	stsToken string,
) (*AwsConfig, error) {
	cfg, err := config.LoadDefaultConfig(
		ctx,
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			accessKeyId, secretKey, stsToken,
		)),
	)
	if err != nil {
		return nil, fmt.Errorf("config.LoadDefaultConfig: %w", err)
	}

	return &AwsConfig{Config: cfg}, nil
}
