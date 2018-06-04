package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/glue"
)

func main() {
	sess, _ := session.NewSession()
	// TODO: Error handling

	service := glue.New(sess)

	// TODO: Make the value of the example a genuine version
	targets := glue.CrawlerTargets{}
	targets.SetS3Targets([]*glue.S3Target{
		&glue.S3Target{Path: aws.String("S3Path")},
	})

	params := glue.CreateCrawlerInput{
		DatabaseName: aws.String("DatabaseName"),
		Name:         aws.String("CrawlerName"),
		Role:         aws.String("CrawlerRole"),
		Targets:      &targets,
	}
	response, err := service.CreateCrawler(&params)
	fmt.Println(response)
	// TODO: Error handling
	if err != nil {
		fmt.Println(err)
	}
}
