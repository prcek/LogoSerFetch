// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Sample run-helloworld is a minimal Cloud Run service.
package main

import (
	"context"
	"log"
	"os"

	"cloud.google.com/go/firestore"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"google.golang.org/api/iterator"

	"github.com/joho/godotenv"
)

func isRunningInContainer() bool {
	if _, err := os.Stat("/.inside"); err != nil {
		return false
	}
	return true
}

func S3Fetch() {
	aws_s3_bucket := os.Getenv("AWS_S3_BUCKET")
	log.Print("AWS_S3_BUCKET is ", aws_s3_bucket)
	aws_s3_region := os.Getenv("AWS_S3_REGION")
	log.Print("AWS_S3_REGION is ", aws_s3_region)
	aws_access_key_id := os.Getenv("AWS_ACCESS_KEY_ID")
	log.Print("AWS_ACCESS_KEY_ID is ", aws_access_key_id)

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(os.Getenv("AWS_S3_REGION"))},
	)
	if err != nil {
		log.Fatal("Error new sessiong", err)
	}
	svc := s3.New(sess)

	bucket := os.Getenv("AWS_S3_BUCKET") //"dateio-logoser"

	delim := ""
	prefix := "logos_to_process/logos_2022-09"
	maxkey := int64(1000)

	input := s3.ListObjectsV2Input{
		Bucket: &bucket,

		Delimiter: &delim,

		MaxKeys: &maxkey,
		Prefix:  &prefix,
	}
	page := 1

	err = svc.ListObjectsV2Pages(&input, func(objs *s3.ListObjectsV2Output, b bool) bool {
		log.Print("page loaded - ", page)

		/*
			for _, item := range objs.Contents {

				fmt.Println("Name:          ", *item.Key)
				//root.Insert(*item.Key, *item.Size)
				//fmt.Println("Last modified: ", *item.LastModified)
				//fmt.Println("Size:          ", *item.Size)
				//fmt.Println("Storage class: ", *item.StorageClass)
				//fmt.Println("")
			}*/
		page++
		return true
	})
	if err != nil {
		log.Fatal("Error ListObjects", err)
	}
}

func createFireStoreClient(ctx context.Context) *firestore.Client {
	// Sets your Google Cloud Platform project ID.
	projectID := os.Getenv("GCP_PROJECT")
	log.Print("GCP_PROJECT is ", projectID)
	client, err := firestore.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	// Close client when done with
	// defer client.Close()
	return client
}

func FireStoreTest() {

	ctx := context.Background()
	client := createFireStoreClient(ctx)
	defer client.Close()

	iter := client.Collection("users").Documents(ctx)
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatalf("Failed to iterate: %v", err)
		}
		log.Println(doc.Data())
	}

}

func main() {
	log.Print("LogoSerFetch 0.1")

	if !isRunningInContainer() {
		log.Print("Not running in container. Loading '.env'.")
		err := godotenv.Load()

		if err != nil {
			log.Fatal("Error loading .env file", err)
		}
	} else {
		log.Print("Running in container. Using env.")
	}

	FireStoreTest()

	log.Print("Done.")
}
