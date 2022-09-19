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
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/joho/godotenv"
)

func isRunningInContainer() bool {
	if _, err := os.Stat("/.inside"); err != nil {
		return false
	}
	return true
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
	log.Print("Done.")
}
