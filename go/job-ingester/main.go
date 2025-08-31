package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	joblib "github.com/jherbage/Event-Driven-Microservices-with-Observability-Demo/go/job"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

var (
	tracer        trace.Tracer
	sqsClient     *sqs.Client
	jobsTodoURL   string
	deadletterURL string
	snsClient     *sns.Client
	snsTopicArn   string
)

func initTracer() func() {
	// Create OTLP HTTP exporter
	exporter, err := otlptracehttp.New(context.Background(),
		otlptracehttp.WithEndpoint("otel_collector:4318"),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		log.Fatalf("failed to create OTLP exporter: %v", err)
	}

	// Create trace provider
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(resource.NewSchemaless(
			attribute.String("service.name", "job-ingester"),
		)),
	)

	otel.SetTracerProvider(tp)
	tracer = otel.Tracer("jobs")

	return func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			log.Printf("failed to shut down tracer provider: %v", err)
		}
	}
}

func init() {
	// Load AWS configuration
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithEndpointResolver(aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
			if service == sqs.ServiceID {
				return aws.Endpoint{URL: "http://localstack:4566"}, nil // LocalStack endpoint
			}
			return aws.Endpoint{}, &aws.EndpointNotFoundError{}
		})),
	)
	if err != nil {
		log.Fatalf("unable to load AWS SDK config: %v", err)
	}

	// Create SQS client
	sqsClient = sqs.NewFromConfig(cfg)

	// Set the jobs-todo queue URL
	jobsTodoURL = "http://localstack:4566/000000000000/jobs-todo"

	// Dead letter for post mortem analysis
	deadletterURL = "http://localstack:4566/000000000000/dead-letter-queue"

	// Initialize SNS client
	snsClient = sns.NewFromConfig(cfg)

	// Set the SNS topic ARN for LocalStack
	snsTopicArn = "arn:aws:sns:us-east-1:000000000000:job-end-state-topic"
}

func handler(ctx context.Context, sqsEvent events.SQSEvent) error {
	ctx, span := tracer.Start(ctx, "ProcessSQSEvent")
	defer span.End()

	for _, message := range sqsEvent.Records {
		processMessage(ctx, message)
	}

	return nil
}

func sendToDeadLetterQueue(ctx context.Context, messageBody string) {
	_, err := sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(deadletterURL),
		MessageBody: aws.String(messageBody),
	})
	if err != nil {
		log.Printf("failed to send message to dead-letter queue: %v", err)
	}
}

func processMessage(ctx context.Context, message events.SQSMessage) {
	ctx, span := tracer.Start(ctx, "ProcessMessage", trace.WithAttributes(
		attribute.String("message.id", message.MessageId),
	))
	defer span.End()

	// Parse the EventBridge message
	var eventBridgeMessage struct {
		Detail json.RawMessage `json:"detail"`
	}
	if err := json.Unmarshal([]byte(message.Body), &eventBridgeMessage); err != nil {
		span.RecordError(err)
		log.Printf("failed to parse EventBridge message: %v", err)
		publishToSNS(snsClient, snsTopicArn, fmt.Sprintf("failed to parse EventBridge message: %s", string(eventBridgeMessage.Detail)))
		sendToDeadLetterQueue(ctx, string(eventBridgeMessage.Detail))
		return
	}

	// Parse the message into a Job
	job, _, _, err := joblib.ParseJob(eventBridgeMessage.Detail)
	if err != nil {
		span.RecordError(err)
		log.Printf("failed to parse or validate job: %v", err)
		publishToSNS(snsClient, snsTopicArn, fmt.Sprintf("failed to parse or validate job: %s", string(eventBridgeMessage.Detail)))
		sendToDeadLetterQueue(ctx, string(eventBridgeMessage.Detail))
		return
	}

	enrichedPayload := joblib.EnrichedPayload{
		OriginalMessage: []byte(eventBridgeMessage.Detail),
		ID:              message.MessageId, // propogate the SQS message ID
		Timestamp:       time.Now().Format(time.RFC3339),
		Status:          joblib.StatusNew,
		TraceContext:    fmt.Sprintf("00-%s-%s-01", span.SpanContext().TraceID(), span.SpanContext().SpanID()),
	}

	// Marshal the enriched payload to JSON
	enrichedPayloadJSON, err := json.Marshal(enrichedPayload)
	if err != nil {
		span.RecordError(err)
		log.Printf("failed to marshal enriched payload: %v", err)
		publishToSNS(snsClient, snsTopicArn, fmt.Sprintf("failed to marshal enriched payload: %s", string(eventBridgeMessage.Detail)))
		sendToDeadLetterQueue(ctx, string(eventBridgeMessage.Detail))
		return
	}

	// Send the enriched payload to the jobs-todo SQS queue
	_, err = sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(jobsTodoURL),
		MessageBody: aws.String(string(enrichedPayloadJSON)),
	})
	if err != nil {

		span.RecordError(err)
		log.Printf("failed to send message to jobs-todo queue: %v, message was %s", err, string(enrichedPayloadJSON))
		publishToSNS(snsClient, snsTopicArn, fmt.Sprintf("failed to send message to jobs-todo queue: %v, message was %s", err, string(enrichedPayloadJSON)))
		sendToDeadLetterQueue(ctx, string(eventBridgeMessage.Detail))
		return
	}

	span.AddEvent("Message sent to jobs-todo queue", trace.WithAttributes(
		attribute.String("message.id", enrichedPayload.ID),
		attribute.String("message.timestamp", enrichedPayload.Timestamp),
	))

	// Log the enriched payload
	log.Printf("Successfully processed job: %+v", job)
	log.Printf("Enriched Payload: %+v", enrichedPayload)
}

func publishToSNS(snsClient *sns.Client, topicArn string, message string) error {
	input := &sns.PublishInput{
		Message:  aws.String(message),
		TopicArn: aws.String(topicArn),
	}
	_, err := snsClient.Publish(context.TODO(), input)
	return err
}

func main() {
	// Initialize OpenTelemetry Tracer
	shutdown := initTracer()
	defer shutdown()

	// Start the Lambda handler
	lambda.Start(handler)
}
