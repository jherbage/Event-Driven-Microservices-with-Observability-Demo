package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

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
	tracer        = otel.Tracer("jobs")
	sqsClient     *sqs.Client
	jobsTodoURL   string
	deadletterURL string
	snsClient     *sns.Client
	snsTopicArn   string
)

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
	jobsTodoURL = "http://localstack:4566/000000000000/jobs-todo" // Replace with the actual queue URL

	// Dead letter for post mortem analysis
	deadletterURL = "http://localstack:4566/000000000000/dead-letter-queue"

	// Initialize SNS client
	snsClient = sns.NewFromConfig(cfg)

	// Set the SNS topic ARN for LocalStack
	snsTopicArn = "arn:aws:sns:us-east-1:000000000000:job-end-state-topic"
}

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
			attribute.String("service.name", "job-processor"),
		)),
	)

	otel.SetTracerProvider(tp)
	tracer = otel.Tracer("job-processor")

	return func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			log.Printf("failed to shut down tracer provider: %v", err)
		}
	}
}

func handler(ctx context.Context, sqsEvent events.SQSEvent) error {

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

	log.Printf("Processing SQS message: %s", message.Body)

	// Parse the SQS message into a Job
	var job joblib.EnrichedPayload
	if err := json.Unmarshal([]byte(message.Body), &job); err != nil {
		log.Printf("failed to parse job message: %s, err: %s", message.Body, err)
		publishToSNS(snsClient, snsTopicArn, fmt.Sprintf("failed to parse job message: %s, err: %v", message.Body, err))
		sendToDeadLetterQueue(ctx, message.Body)
		return
	}

	// Extract the propagated trace context
	traceparent := job.TraceContext
	var executeCtx context.Context

	if traceparent == "" {
		log.Printf("No trace context found in the job message")
	} else {
		// Validate the traceparent format
		if len(traceparent) != 55 || traceparent[:3] != "00-" {
			log.Printf("Invalid traceparent format: %s", traceparent)
		} else {
			// Parse the traceID from the traceparent string
			traceID, err := trace.TraceIDFromHex(traceparent[3:35]) // Extract the trace ID part
			if err != nil {
				log.Printf("Failed to parse trace ID from traceparent: %s, err: %v", traceparent, err)
				return
			}
			// Parse the spanID from the traceparent string
			spanID, err := trace.SpanIDFromHex(traceparent[36:52]) // Extract the span ID part
			if err != nil {
				log.Printf("Failed to parse span ID from traceparent: %s, err: %v", traceparent, err)
				return
			}
			// Create a new SpanContext
			spanContext := trace.NewSpanContext(trace.SpanContextConfig{
				TraceID:    traceID,
				SpanID:     spanID,
				TraceFlags: trace.FlagsSampled,
				Remote:     true, // Mark this as a remote span
			})

			// Inject the SpanContext into the context
			executeCtx = trace.ContextWithSpanContext(ctx, spanContext)
			log.Printf("Manually added trace context: traceID=%s, spanID=%s", traceID.String(), spanID.String())
		}
	}

	// Parse the job from the JobMessage
	parsedJob, _, jobType, err := joblib.ParseJob(job.OriginalMessage)
	if err != nil {
		log.Printf("failed to parse job: %s, err: %s", job.OriginalMessage, err)
		publishToSNS(snsClient, snsTopicArn, fmt.Sprintf("failed to parse job: %s, err: %s", job.OriginalMessage, err))
		sendToDeadLetterQueue(ctx, message.Body)
		return
	}

	// Execute the job
	_, jobSpan := tracer.Start(executeCtx, "ExecuteJob", trace.WithAttributes(
		attribute.String("job.type", *jobType),
		attribute.String("message.id", job.ID),
		attribute.String("sqs.message.id", message.MessageId),
	))

	defer func() {
		log.Println("Ending ExecuteJob span")
		jobSpan.End()
	}()

	if err := parsedJob.Execute(); err != nil {
		jobSpan.RecordError(err)
		job.Status = joblib.StatusExecuteFailed
		log.Printf("failed to execute job: %v, err: %s", job, err)
		jobSpan.AddEvent("job failed to execute", trace.WithAttributes(
			attribute.String("message.id", job.ID),
			attribute.String("job.type", *jobType),
		))
		publishToSNS(snsClient, snsTopicArn, fmt.Sprintf("failed to execute job: %v, err: %s", job, err))
		// add to dead letter for retry
		sendToDeadLetterQueue(executeCtx, message.Body)
		return
	}

	jobSpan.AddEvent("job executed successfully", trace.WithAttributes(
		attribute.String("message.id", job.ID),
		attribute.String("job.type", *jobType),
	))
	job.Status = joblib.StatusCompleted
	log.Printf("successfully executed job: %v", job)
	publishToSNS(snsClient, snsTopicArn, fmt.Sprintf("successfully executed job: %v", job))

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
	// Initialize the tracer
	shutdown := initTracer()
	defer shutdown()
	// Start the Lambda handler
	lambda.Start(handler)
}
