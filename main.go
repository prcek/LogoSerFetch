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
	"bytes"
	"context"
	"encoding/json"
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

type LogoMeta struct {
	Merchant_id  int64
	SeqNo        int
	Scrape_time  string
	Old_filename string
	New_filename string
	Origin_url   string
	Color        string
	Note         string
}

type LogoExtendMeta struct {
	Tag            string
	S3KeySrc       string
	S3KeyCandidate string
	SeqNo          int
	Merchant_id    int64
	Color          string
	Disable        bool
	Keep           bool
	Space          bool
	Zoom           bool
	HardCrop       bool
}

type FileElem struct {
	name        string
	s3key       string
	merchantId  int64
	suffix      string
	hash        string
	size        int64
	hasMeta     bool
	mNote       string
	mColor      string
	mOriginUrl  string
	mScape_time time.Time
}

type BatchElem struct {
	err           string
	name          string
	metafileS3key string
	timestamp     time.Time
	files         *map[string]*FileElem
}

type FileOptionElem struct {
	Merchant_id    int64
	srcS3Key       string
	candidateS3Key string
	color          string
	disable        bool
	keep           bool
	space          bool
	zoom           bool
	hardcrop       bool
}

type BatchOptionElem struct {
	err           string
	name          string
	metafileS3key string
	fileOpts      *map[int64]*FileOptionElem
}

func S3FileRead(svc *s3.S3, s3key string) ([]byte, error) {
	requestInput := s3.GetObjectInput{
		Bucket: aws.String(os.Getenv("AWS_S3_BUCKET")),
		Key:    aws.String(s3key),
	}

	result, err := svc.GetObject(&requestInput)
	if err != nil {
		log.Print(err)
		return nil, err
	}
	defer result.Body.Close()

	buf := new(bytes.Buffer)
	buf.ReadFrom(result.Body)
	return buf.Bytes(), nil
}

func S3Fetch(svc *s3.S3) map[string]*BatchElem {

	bucket := os.Getenv("AWS_S3_BUCKET") //"dateio-logoser"

	delim := ""
	//prefix := "logos_to_process/"
	prefix := "logos_to_process/logos_2022-09"
	maxkey := int64(1000)

	input := s3.ListObjectsV2Input{
		Bucket: &bucket,

		Delimiter: &delim,

		MaxKeys: &maxkey,
		Prefix:  &prefix,
	}
	page := 1

	batches := make(map[string]*BatchElem)

	err := svc.ListObjectsV2Pages(&input, func(objs *s3.ListObjectsV2Output, b bool) bool {
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
							fe.hash = s2[0]

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
	for bk, b := range batches {

		if b.metafileS3key == "" {
			log.Print("batch without metafile ", b.name)
			batches[bk].err = "no metafile"
			continue
		}
		log.Print(b.metafileS3key)
		data, err := S3FileRead(svc, b.metafileS3key)
		if err != nil {
			log.Panic("can't fetch metafile ", b.metafileS3key)
		}
		var lms []LogoMeta
		err = json.Unmarshal(data, &lms)
		if err != nil {
			log.Panic("can't parse json", b.metafileS3key)
		}

		for _, lm := range lms {
			//fmt.Println(lm.New_filename)
			fe := (*b.files)[lm.New_filename]
			if fe == nil {
				log.Panic("missing ", lm.New_filename)
			}
			if fe.hasMeta {
				log.Print("duplicate meta - last meta used -", lm.New_filename)
			}
			(*b.files)[lm.New_filename].hasMeta = true
			if fe.merchantId != lm.Merchant_id {
				log.Panic("Merchant_id diff meta vs. files")
			}
			(*b.files)[lm.New_filename].mColor = lm.Color
			(*b.files)[lm.New_filename].mNote = lm.Note
			(*b.files)[lm.New_filename].mOriginUrl = lm.Origin_url

			scrapetime, err := time.Parse(time.RFC3339, lm.Scrape_time)
			if err != nil {
				log.Panic("can't parse scrape time")

			}
			(*b.files)[lm.New_filename].mScape_time = scrapetime
		}
		for _, f := range *b.files {
			if !f.hasMeta {
				log.Panic("file without meta ", f.s3key)
			}
		}
	}
	return batches
}

func S3FetchOptions(svc *s3.S3) map[string]*BatchOptionElem {

	bucket := os.Getenv("AWS_S3_BUCKET") //"dateio-logoser"

	delim := ""
	prefix := "logos_options/logos_2022-09"
	//prefix := "logos_options/logos_2022-09-07"
	maxkey := int64(1000)

	input := s3.ListObjectsV2Input{
		Bucket: &bucket,

		Delimiter: &delim,

		MaxKeys: &maxkey,
		Prefix:  &prefix,
	}
	page := 1

	batches := make(map[string]*BatchOptionElem)

	err := svc.ListObjectsV2Pages(&input, func(objs *s3.ListObjectsV2Output, b bool) bool {
		log.Print("s3 page loaded - ", page)

		for _, item := range objs.Contents {
			//fmt.Println("key size", *item.Key, *item.Size)
			parts := strings.Split(*item.Key, "/")
			if len(parts) > 2 {
				batch_id := parts[1]

				be := batches[batch_id]
				if be == nil {
					be = new(BatchOptionElem)
					fl := make(map[int64]*FileOptionElem)
					be.fileOpts = &fl
					be.name = batch_id
					batches[batch_id] = be
				}

				file_name := parts[2]
				if file_name == "meta_v1.json" {
					be.metafileS3key = *item.Key
					data, err := S3FileRead(svc, be.metafileS3key)
					if err != nil {
						log.Panic("can't fetch options metafile ", be.metafileS3key)
					}
					var lms []LogoExtendMeta
					err = json.Unmarshal(data, &lms)
					if err != nil {
						log.Panic("can't parse json", be.metafileS3key)
					}
					for _, lm := range lms {
						//fmt.Println(lm.New_filename)
						fe := (*be.fileOpts)[lm.Merchant_id]
						if fe != nil {
							if !fe.disable { // not disabled => do not overrire
								log.Print("duplicate opt for ", lm.Merchant_id, " keep current")
								continue
							}
							log.Print("duplicate opt for ", lm.Merchant_id, " override")
						} else {
							fe = new(FileOptionElem)
							(*be.fileOpts)[lm.Merchant_id] = fe
						}
						fe.color = lm.Color
						fe.disable = lm.Disable
						fe.candidateS3Key = lm.S3KeyCandidate
						fe.srcS3Key = lm.S3KeySrc
						fe.hardcrop = lm.HardCrop
						fe.keep = lm.Keep
						fe.space = lm.Space
						fe.zoom = lm.Zoom

					}

				}

			}
		}

		page++
		return true
	})
	if err != nil {
		log.Fatal("Error ListObjects", err)
	}
	return batches
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

func FireStoreUpdate(client *firestore.Client, ctx context.Context, batches map[string]*BatchElem) {

	iter := client.Collection("logo_batches").Documents(ctx)
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatalf("Failed to iterate: %v", err)
		}
		bid := doc.Ref.ID
		batch := batches[bid]
		if batch == nil {
			log.Print("missing S3 batch ", bid)
		}
	}
	for bkey, batch := range batches {
		_, err := client.Collection("logo_batches").Doc(bkey).Set(ctx, map[string]interface{}{
			"name":          batch.name,
			"timestamp":     batch.timestamp,
			"err":           batch.err,
			"metafileS3key": batch.metafileS3key,
			"files":         len(*batch.files),
		})
		if err != nil {
			log.Fatalf("Failed adding batch: %v", err)
		}

		iter := client.Collection("logo_batches").Doc(bkey).Collection("logo_files").Documents(ctx)
		for {
			doc, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				log.Fatalf("Failed to iterate: %v", err)
			}
			fid := doc.Ref.ID
			file := (*batch.files)[fid]
			if file == nil {
				log.Print("missing S3 file ", fid)
			}
		}

		for fkey, file := range *batch.files {
			_, err := client.Collection("logo_batches").Doc(bkey).Collection("logo_files").Doc(fkey).Set(ctx, map[string]interface{}{
				"name":       file.name,
				"s3key":      file.s3key,
				"merchantId": file.merchantId,

				"suffix":      file.suffix,
				"hash":        file.hash,
				"size":        file.size,
				"hasMeta":     file.hasMeta,
				"mNote":       file.mNote,
				"mColor":      file.mColor,
				"mOriginUrl":  file.mOriginUrl,
				"mScape_time": file.mScape_time,
			})
			if err != nil {
				log.Fatalf("Failed adding file: %v", err)
			}
		}
	}

}

func FireStoreUpdateOpts(client *firestore.Client, ctx context.Context, batches map[string]*BatchOptionElem) {
	iter := client.Collection("logo_batches").Documents(ctx)
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatalf("Failed to iterate: %v", err)
		}
		bid := doc.Ref.ID
		batch := batches[bid]
		if batch == nil {
			log.Print("missing S3 batch ", bid)
		}
	}

	for bkey, batch := range batches {

		iter := client.Collection("logo_batches").Doc(bkey).Collection("logo_files").Documents(ctx)
		for {
			doc, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				log.Fatalf("Failed to iterate: %v", err)
			}
			fid := doc.Ref.ID
			type FileE struct {
				MerchantId int64 `firestore:"merchantId"`
			}

			var fex FileE
			err = doc.DataTo(&fex)
			if err != nil {
				log.Panic("can't read logo_files document")
			}
			fo := (*batch.fileOpts)[fex.MerchantId]
			if fo == nil {
				log.Print("missing S3 fileopt ", fex.MerchantId, " ", fid)
				continue
			}

			_, err = client.Collection("logo_batches").Doc(bkey).Collection("logo_opts").Doc(fid).Set(ctx, map[string]interface{}{
				"merchantId":     fex.MerchantId,
				"color":          fo.color,
				"disable":        fo.disable,
				"keep":           fo.keep,
				"space":          fo.space,
				"zoom":           fo.zoom,
				"hardcrop":       fo.hardcrop,
				"candidateS3Key": fo.candidateS3Key,
				"srcS3Key":       fo.srcS3Key,
			})
			if err != nil {
				log.Fatalf("Failed adding file: %v", err)
			}

		}

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

	ctx := context.Background()
	fsClient := createFireStoreClient(ctx)
	defer fsClient.Close()

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

	batches := S3Fetch(svc)

	FireStoreUpdate(fsClient, ctx, batches)

	batchesOpt := S3FetchOptions(svc)
	FireStoreUpdateOpts(fsClient, ctx, batchesOpt)

	log.Print("Done.")
}
