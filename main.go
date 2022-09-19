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

	"github.com/joho/godotenv"
)

func isRunningInContainer() bool {
	if _, err := os.Stat("/.dockerenv"); err != nil {
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
}
