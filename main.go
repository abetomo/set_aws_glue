package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/glue"
	"io/ioutil"
	"os"
)

type CreateCrawlerParams struct {
	DatabaseName string
	CrawlerName  string
	CrawlerRole  string
	S3Path       string
}

func createDatabaseIfNotExists(service *glue.Glue, name string) {
	getParams := glue.GetDatabaseInput{}
	getParams.SetName(name)
	if _, err := service.GetDatabase(&getParams); err == nil {
		// Database already exists
		return
	}

	createParams := glue.CreateDatabaseInput{}
	createParams.SetDatabaseInput(&glue.DatabaseInput{Name: aws.String(name)})
	if _, err := service.CreateDatabase(&createParams); err != nil {
		fmt.Println(err)
	}
}

func main() {
	configJsonPath := flag.String("config", "", "Path of config JSON file")
	flag.Parse()
	if *configJsonPath == "" {
		flag.Usage()
		os.Exit(1)
	}

	// Load configuration file
	var config CreateCrawlerParams
	bytes, _ := ioutil.ReadFile(*configJsonPath)
	if err := json.Unmarshal(bytes, &config); err != nil {
		flag.Usage()
		os.Exit(1)
	}

	sess, sessErr := session.NewSession()
	if sessErr != nil {
		fmt.Println(sessErr)
		os.Exit(1)
	}

	service := glue.New(sess)
	createDatabaseIfNotExists(service, config.DatabaseName)

	targets := glue.CrawlerTargets{}
	targets.SetS3Targets([]*glue.S3Target{
		&glue.S3Target{Path: aws.String(config.S3Path)},
	})

	params := glue.CreateCrawlerInput{
		DatabaseName: aws.String(config.DatabaseName),
		Name:         aws.String(config.CrawlerName),
		Role:         aws.String(config.CrawlerRole),
		Targets:      &targets,
	}
	if _, err := service.CreateCrawler(&params); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Println("Added a glue crawler")
	fmt.Println(string(bytes))
}
