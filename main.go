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
	"strconv"
	"strings"
	"time"

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

type FileElem struct {
	name       string
	s3key      string
	merchantId int64
	suffix     string
	hash       string
	size       int64
}

type BatchElem struct {
	name          string
	metafileS3key string
	timestamp     time.Time
	key           string
	files         *map[string]*FileElem
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
	//prefix := "logos_to_process/"
	prefix := "logos_to_process/logos_2022-09-07"
	maxkey := int64(1000)

	input := s3.ListObjectsV2Input{
		Bucket: &bucket,

		Delimiter: &delim,

		MaxKeys: &maxkey,
		Prefix:  &prefix,
	}
	page := 1

	batches := make(map[string]*BatchElem)

	err = svc.ListObjectsV2Pages(&input, func(objs *s3.ListObjectsV2Output, b bool) bool {
		log.Print("s3 page loaded - ", page)

		for _, item := range objs.Contents {

			//fmt.Println("Name:          ", *item.Key)
			parts := strings.Split(*item.Key, "/")
			if len(parts) > 1 {
				batch_id := parts[1]
				be := batches[batch_id]
				if be == nil {
					log.Print("batch ", batch_id)
					be = new(BatchElem)
					fl := make(map[string]*FileElem)
					be.files = &fl
					be.key = batch_id

					if !strings.HasPrefix(batch_id, "logos_") {
						log.Panic("batch dir missing prefix 'logos_'")
						return false
					}
					timestr := strings.TrimPrefix(batch_id, "logos_")
					timeval, err := time.Parse(time.RFC3339, timestr)
					if err != nil {
						log.Panic("batch name parse time problem ", err)
					}

					be.name = batch_id
					be.timestamp = timeval

					batches[batch_id] = be
				}
				if len(parts) > 2 {
					file_id := parts[2]
					if strings.HasPrefix(file_id, "data_") && strings.HasSuffix(file_id, ".json") {

						metatime, err := time.Parse(time.RFC3339, strings.TrimSuffix(strings.TrimPrefix(file_id, "data_"), ".json"))
						if err != nil {
							log.Panic("meta name parse time problem ", err)
						}
						if !metatime.Equal(be.timestamp) {
							log.Panic("metafile timestamp diffs batch timestamp")
						}
						//log.Print("metafile detected ", *item.Key)
						be.metafileS3key = *item.Key
					} else {
						//log.Print("file ", file_id)
						fe := (*be.files)[file_id]
						if fe == nil {
							fe = new(FileElem)

							fe.s3key = *item.Key
							fe.name = file_id
							fe.size = *item.Size

							s1 := strings.Split(file_id, "_")
							if len(s1) != 2 {
								log.Panic("file name too many '_'", file_id)
							}
							i64, err := strconv.ParseInt(s1[0], 10, 64)
							if err != nil {
								log.Panic("file name cannot parse merchantid ", file_id)
							}
							fe.merchantId = i64

							s2 := strings.Split(s1[1], ".")
							if len(s2) != 2 {
								log.Panic("file name cannot parse file suffix ", file_id)
							}
							fe.suffix = s2[1]

							(*be.files)[file_id] = fe
							//log.Print(fe)
						} else {
							log.Panic("duplicate file_id")
						}
					}
				}
			}

			//root.Insert(*item.Key, *item.Size)
			//fmt.Println("Last modified: ", *item.LastModified)
			//fmt.Println("Size:          ", *item.Size)
			//fmt.Println("Storage class: ", *item.StorageClass)
			//fmt.Println("")
		}
		page++
		return true
	})
	if err != nil {
		log.Fatal("Error ListObjects", err)
	}
	log.Print("reading metafiles")

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

	_, err := client.Collection("logo_batches").Doc("batch_id").Set(ctx, map[string]interface{}{
		"name":        "logobatchname",
		"dateExample": time.Now(),
	})
	if err != nil {
		log.Fatalf("Failed adding alovelace: %v", err)
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
	S3Fetch()
	//FireStoreTest()
	//parseBatch("logos_2022-09-06T16:11:40Z")
	log.Print("Done.")
}
