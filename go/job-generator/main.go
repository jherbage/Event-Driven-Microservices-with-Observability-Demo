package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	joblib "github.com/jherbage/Event-Driven-Microservices-with-Observability-Demo/go/job"
)

func main() {
	// Define a flag for the runtime duration in minutes
	runMinutes := flag.Int("minutes", 0, "Number of minutes to run the job generator")
	flag.Parse()

	endTime := time.Time{}
	// Calculate the end time
	if *runMinutes > 0 {
		endTime = time.Now().Add(time.Duration(*runMinutes) * time.Minute)
		log.Printf("Job generator will run for %d minutes (until %s)", *runMinutes, endTime.Format(time.RFC3339))
	} else {
		log.Printf("Job generator will run indefinitely. Use Ctrl+C to stop.")
	}

	// Load AWS configuration
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithEndpointResolver(aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
			if service == eventbridge.ServiceID {
				return aws.Endpoint{URL: "http://localhost:4566"}, nil
			}
			return aws.Endpoint{}, fmt.Errorf("unknown endpoint requested")
		})),
	)
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	// Create EventBridge client
	client := eventbridge.NewFromConfig(cfg)

	// Read the messages from the good JSON file
	goodMessages, err := readMessages("good_jobs.json")
	if err != nil {
		log.Fatalf("failed to read good messages: %v", err)
	}

	// Read the messages from the good JSON file
	badMessages, err := readMessages("bad_jobs.json")
	if err != nil {
		log.Fatalf("failed to read bad messages: %v", err)
	}

	// Process messages until the runtime duration ends
	for {
		// Check if the current time has exceeded the end time
		if !endTime.IsZero() && time.Now().After(endTime) {
			log.Println("Job generator has completed its runtime.")
			break
		}

		// Process each message
		for _, jobMessage := range goodMessages {
			// Marshal the job message to JSON
			eventJSON, err := json.Marshal(jobMessage)
			if err != nil {
				log.Printf("failed to marshal job message: %v", err)
				continue
			}

			randomiseMessageParameters(&jobMessage)

			// Randomly pick a good or bad message
			if rand.Intn(5) == 0 { // 20% chance to pick a bad message
				randomIndex := rand.Intn(len(badMessages))
				eventJSON, err = json.Marshal(badMessages[randomIndex])
				log.Printf("Sending a bad message: %v", badMessages[randomIndex])
			} else {
				eventJSON, err = json.Marshal(jobMessage)
				log.Printf("Sending a good message: %v", jobMessage)
			}

			// Send the message to EventBridge
			err = sendToEventBridge(client, eventJSON)
			if err != nil {
				log.Printf("failed to send job message to EventBridge: %v", err)
			}

			// Sleep for a random interval between 2 and 10 seconds
			sleepDuration := time.Duration(rand.Intn(9)+2) * time.Second
			log.Printf("Sleeping for %v before sending the next message...", sleepDuration)
			time.Sleep(sleepDuration)
		}
	}

}

func readMessages(filename string) ([]joblib.JobMessage, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	var messages []joblib.JobMessage
	err = json.Unmarshal(data, &messages)
	if err != nil {
		return nil, err
	}

	return messages, nil
}

func sendToEventBridge(client *eventbridge.Client, eventJSON []byte) error {
	output, err := client.PutEvents(context.TODO(), &eventbridge.PutEventsInput{
		Entries: []types.PutEventsRequestEntry{
			{
				Source:       aws.String("jobs"),
				DetailType:   aws.String("JobEvent"),
				Detail:       aws.String(string(eventJSON)),
				EventBusName: aws.String("default"), // Use "default" for the default event bus
			},
		},
	})
	if err != nil {
		return err
	}

	// Log the result
	for _, entry := range output.Entries {
		if entry.EventId != nil {
			log.Printf("Event sent successfully with ID: %s", *entry.EventId)
		} else {
			log.Printf("Failed to send event: %v", entry.ErrorMessage)
		}
	}

	return nil
}

func randomiseMessageParameters(jobMessage *joblib.JobMessage) {
	// Seed the random number generator
	rand.Seed(time.Now().UnixNano())

	var messageMap map[string]interface{}
	if err := json.Unmarshal(jobMessage.Message, &messageMap); err != nil {
		log.Printf("failed to unmarshal job message for randomization: %v", err)
		return
	}

	// Randomly modify one of the fields in the message
	switch jobMessage.JobType {
	case "report_generation":
		if _, ok := messageMap["report_name"]; ok {
			messageMap["report_name"] = fmt.Sprintf("Random Report %d", rand.Intn(100))
		}
		if _, ok := messageMap["filters"]; ok {
			messageMap["filters"] = fmt.Sprintf("region=%d", rand.Intn(100))
		}
	case "data_cleanup":
		if _, ok := messageMap["target_table"]; ok {
			messageMap["target_table"] = fmt.Sprintf("table_%d", rand.Intn(100))
		}
		if _, ok := messageMap["retention"]; ok {
			messageMap["retention"] = rand.Intn(365) // Random retention between 0 and 365 days
		}
	case "user_onboarding":
		if _, ok := messageMap["user_id"]; ok {
			messageMap["user_id"] = fmt.Sprintf("user_%d", rand.Intn(10000))
		}
		if _, ok := messageMap["user_name"]; ok {
			messageMap["user_name"] = fmt.Sprintf("Random User %d", rand.Intn(100))
		}
	case "long_running_job":
		if _, ok := messageMap["task_name"]; ok {
			messageMap["task_name"] = fmt.Sprintf("Task %d", rand.Intn(100))
		}
		if _, ok := messageMap["timeout"]; ok {
			messageMap["timeout"] = rand.Intn(600) + 1 // Random timeout
		}
	default:
		log.Printf("Unknown job type: %s", jobMessage.JobType)
		return
	}

	// Marshal the modified message back into JSON
	modifiedMessage, err := json.Marshal(messageMap)
	if err != nil {
		log.Printf("failed to marshal modified job message: %v", err)
		return
	}

	// Update the jobMessage with the modified message
	jobMessage.Message = json.RawMessage(modifiedMessage)
}
