package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func parseS3Path(path string) (bucket string, prefix string) {
	if len(path) <= 5 || path[:5] != "s3://" {
		log.Fatal("Must start with `s3://`.")
	}

	bucketAndPrefix := path[5:]
	s := strings.SplitN(bucketAndPrefix, "/", 2)
	bucket = s[0]
	if len(s) > 1 {
		prefix = s[1]
	}
	return
}

func deleteObjects(s3client *s3.Client, output *s3.ListObjectsV2Output, bucket string) (errors []string) {
	deleteObjects := []types.ObjectIdentifier{}
	for _, o := range output.Contents {
		deleteObjects = append(deleteObjects, types.ObjectIdentifier{Key: o.Key})
	}
	currRes, err := s3client.DeleteObjects(context.TODO(), &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &types.Delete{
			Objects: deleteObjects,
		},
	})
	if err != nil {
		errors = append(errors, fmt.Sprintf("Exiting delete operation: %s", err))
	}

	for _, e := range currRes.Errors {
		errors = append(errors, fmt.Sprint(e))
	}
	return
}

func getKeys(s3client *s3.Client, bucket string, prefix string) {
	var continuationToken *string
	var wg sync.WaitGroup
	res := [][]string{}

	for {
		output, err := s3client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			log.Fatal(err)
		}

		if len(output.Contents) == 0 {
			log.Println("No objects to delete.")
			return
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			deleteErrors := deleteObjects(s3client, output, bucket)
			res = append(res, deleteErrors)
		}()

		if !*output.IsTruncated {
			break
		}
		continuationToken = output.NextContinuationToken
	}
	wg.Wait()
	log.Println("Errors:", res)
}

func main() {
	if len(os.Args) != 2 {
		log.Fatalf("Expected 1 arg, got %d.", len(os.Args)-1)
	}

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatal(err)
	}
	client := s3.NewFromConfig(cfg)
	bucket, prefix := parseS3Path(os.Args[1])

	getKeys(client, bucket, prefix)
}
