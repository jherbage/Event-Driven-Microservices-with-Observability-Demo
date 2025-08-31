package job

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"
)

// input schema for users
type JobMessage struct {
	JobType string          `json:"job_type"`
	Message json.RawMessage `json:"message"`
}

func (jm JobMessage) String() string {
	// Marshal the JobMessage into a JSON string
	data, err := json.Marshal(jm)
	if err != nil {
		return fmt.Sprintf("JobMessage{JobType: %s, Message: <invalid JSON>}", jm.JobType)
	}
	return string(data)
}

// schema to share between job ingester and worker
type EnrichedPayload struct {
	OriginalMessage json.RawMessage `json:"originalmessage"`
	ID              string          `json:"id"`
	Timestamp       string          `json:"timestamp"`
	Status          string          `json:"status"`
	TraceContext    string          `json:"trace_context"`
}

// Job is the interface that all job types must implement.
type Job interface {
	Validate() error // Validate ensures the job payload is well-formed.
	Execute() error
}

// JobType represents the type of the job
type JobType string

const (
	ReportGeneration JobType = "report_generation"
	DataCleanup      JobType = "data_cleanup"
	UserOnboarding   JobType = "user_onboarding"
	LongRunning      JobType = "long_running_job"
)

// job statuses
const (
	StatusNew           = "NEW"
	StatusCompleted     = "COMPLETED"
	StatusExecuteFailed = "EXECUTE_FAILED"
)

// ReportGenerationJob represents the payload for a "report_generation" job.
type ReportGenerationJob struct {
	ReportName string `json:"report_name"`
	Filters    string `json:"filters"`
}

// DataCleanupJob represents the payload for a "data_cleanup" job.
type DataCleanupJob struct {
	TargetTable string `json:"target_table"`
	Retention   int    `json:"retention"` // Retention in days
}

// UserOnboardingJob represents the payload for a "user_onboarding" job.
type UserOnboardingJob struct {
	UserID   string `json:"user_id"`
	UserName string `json:"user_name"`
}

// LongRunningJob represents the payload for a "long_running_job".
type LongRunningJob struct {
	TaskName string `json:"task_name"`
	Timeout  int    `json:"timeout"` // Timeout in seconds
}

// Validate methods for each job type.

func (j ReportGenerationJob) Validate() error {
	if j.ReportName == "" {
		return errors.New("report_name is required")
	}
	if j.Filters == "" {
		return errors.New("filters are required")
	}
	return nil
}

func (j ReportGenerationJob) Execute() error {
	log.Printf("Generating report %s with filters %s\n", j.ReportName, j.Filters)
	return nil
}

func (j DataCleanupJob) Validate() error {
	if j.TargetTable == "" {
		return errors.New("target_table is required")
	}
	if j.Retention <= 0 {
		return errors.New("retention must be greater than 0")
	}
	return nil
}

func (j DataCleanupJob) Execute() error {
	log.Printf("Executing data cleanup on table %s with retention %d days\n", j.TargetTable, j.Retention)
	return nil
}

func (j UserOnboardingJob) Validate() error {
	if j.UserID == "" {
		return errors.New("user_id is required")
	}
	if j.UserName == "" {
		return errors.New("user_name is required")
	}
	return nil
}

func (j UserOnboardingJob) Execute() error {
	log.Printf("Onboarding user %s with ID %s\n", j.UserName, j.UserID)
	return nil
}

func (j LongRunningJob) Validate() error {
	if j.TaskName == "" {
		return errors.New("task_name is required")
	}
	if j.Timeout <= 0 {
		return errors.New("timeout must be greater than 0")
	}
	return nil
}

func (j LongRunningJob) Execute() error {
	log.Printf("Starting long-running task %s with timeout %d seconds\n", j.TaskName, j.Timeout)
	time.Sleep(time.Duration(j.Timeout) * time.Second)

	return nil
}

// ParseJob parses a JSON message into the appropriate job type and validates it.
func ParseJob(message []byte) (Job, json.RawMessage, *string, error) {
	// Parse the top-level JobMessage
	var jobMessage JobMessage

	if err := json.Unmarshal(message, &jobMessage); err != nil {
		return nil, nil, nil, fmt.Errorf("failed to parse job message: %w", err)
	}

	// Determine the job type and parse the message field into the correct schema
	var job Job
	switch JobType(jobMessage.JobType) {
	case ReportGeneration:
		var reportJob ReportGenerationJob
		if err := json.Unmarshal(jobMessage.Message, &reportJob); err != nil {
			return nil, json.RawMessage(message), stringPtr(string(ReportGeneration)), fmt.Errorf("failed to parse report_generation job: %w", err)
		}
		job = reportJob
	case DataCleanup:
		var cleanupJob DataCleanupJob
		if err := json.Unmarshal(jobMessage.Message, &cleanupJob); err != nil {
			return nil, json.RawMessage(message), stringPtr(string(DataCleanup)), fmt.Errorf("failed to parse data_cleanup job: %w", err)
		}
		job = cleanupJob
	case UserOnboarding:
		var onboardingJob UserOnboardingJob
		if err := json.Unmarshal(jobMessage.Message, &onboardingJob); err != nil {
			return nil, json.RawMessage(message), stringPtr(string(UserOnboarding)), fmt.Errorf("failed to parse user_onboarding job: %w", err)
		}
		job = onboardingJob
	case LongRunning:
		var longJob LongRunningJob
		if err := json.Unmarshal(jobMessage.Message, &longJob); err != nil {
			return nil, json.RawMessage(message), stringPtr(string(LongRunning)), fmt.Errorf("failed to parse long_running_job: %w", err)
		}
		job = longJob
	default:
		return nil, nil, nil, fmt.Errorf("unknown job type: %s, raw message %s", jobMessage.JobType, string(message))
	}

	// Validate the job
	if err := job.Validate(); err != nil {
		return nil, nil, stringPtr(string(jobMessage.JobType)), fmt.Errorf("job validation failed: %w", err)
	}

	return job, json.RawMessage(message), stringPtr(string(jobMessage.JobType)), nil
}

func ParseEnrichedPayload(message []byte) (Job, *EnrichedPayload, *string, error) {
	// Parse the top-level EnrichedPayload
	var enrichedPayload EnrichedPayload
	if err := json.Unmarshal(message, &enrichedPayload); err != nil {
		return nil, nil, nil, fmt.Errorf("failed to parse enriched payload: %w", err)
	}

	// Use ParseJob to extract the job details
	job, _, jobType, err := ParseJob(enrichedPayload.OriginalMessage)
	if err != nil {
		return nil, nil, jobType, fmt.Errorf("failed to parse job from enriched payload: %w", err)
	}

	return job, &enrichedPayload, jobType, nil
}

func stringPtr(s string) *string {
	return &s
}
