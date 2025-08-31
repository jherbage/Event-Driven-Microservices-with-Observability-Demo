package job

import (
	"encoding/json"
	"testing"
)

func TestParseJob(t *testing.T) {
	tests := []struct {
		name            string
		input           string
		expectError     bool
		expectedJob     Job
		expectedJobType *string
	}{

		{
			name:        "Valid ReportGeneration Job",
			input:       `{"job_type":"report_generation","message":{"report_name":"Sales Report","filters":"region=US"}}`,
			expectError: false,
			expectedJob: ReportGenerationJob{
				ReportName: "Sales Report",
				Filters:    "region=US",
			},
			expectedJobType: stringPtr(string(ReportGeneration)),
		},
		{
			name: "Valid DataCleanup Job",
			input: `{
                "job_type": "data_cleanup",
                "message": {
                    "target_table": "users",
                    "retention": 30
                }
            }`,
			expectError: false,
			expectedJob: DataCleanupJob{
				TargetTable: "users",
				Retention:   30,
			},
			expectedJobType: stringPtr(string(DataCleanup)),
		},
		{
			name: "Invalid DataCleanup Job (missing target_table)",
			input: `{
                "job_type": "data_cleanup",
                "message": {
                    "retention": 30
                }
            }`,
			expectError: true,
		},
		{
			name: "Valid UserOnboarding Job",
			input: `{
                "job_type": "user_onboarding",
                "message": {
                    "user_id": "user-001",
                    "user_name": "John Doe"
                }
            }`,
			expectError: false,
			expectedJob: UserOnboardingJob{
				UserID:   "user-001",
				UserName: "John Doe",
			},
			expectedJobType: stringPtr(string(UserOnboarding)),
		},
		{
			name: "Unknown Job Type",
			input: `{
                "job_type": "unknown_job",
                "message": {}
            }`,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job, originalMessage, jobType, err := ParseJob([]byte(tt.input))
			if tt.expectError {
				if err == nil {
					t.Errorf("expected an error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			var actualMessage, expectedMessage map[string]interface{}
			if err := json.Unmarshal([]byte(tt.input), &expectedMessage); err != nil {
				t.Errorf("failed to unmarshal input message: %v", err)
			}
			if err := json.Unmarshal(originalMessage, &actualMessage); err != nil {
				t.Errorf("failed to unmarshal original message: %v", err)
			}
			if !equalJSONObjects(actualMessage, expectedMessage) {
				t.Errorf("expected Message %+v, got %+v", expectedMessage, actualMessage)
			}

			// validate type
			if tt.expectedJobType != nil && jobType != nil {
				if *tt.expectedJobType != *jobType {
					t.Errorf("expected jobType %s, got %s", *tt.expectedJobType, *jobType)
				}
			} else if tt.expectedJobType != jobType { // one is nil, the other is not
				t.Errorf("expected jobType %v, got %v", tt.expectedJobType, jobType)
			}

			// Validate the Job
			switch expected := tt.expectedJob.(type) {
			case ReportGenerationJob:
				actual, ok := job.(ReportGenerationJob)
				if !ok {
					t.Errorf("expected ReportGenerationJob, got %T", job)
				}
				if actual.ReportName != expected.ReportName || actual.Filters != expected.Filters {
					t.Errorf("expected job %+v, got %+v", expected, actual)
				}
			case DataCleanupJob:
				actual, ok := job.(DataCleanupJob)
				if !ok {
					t.Errorf("expected DataCleanupJob, got %T", job)
				}
				if actual.TargetTable != expected.TargetTable || actual.Retention != expected.Retention {
					t.Errorf("expected job %+v, got %+v", expected, actual)
				}
			case UserOnboardingJob:
				actual, ok := job.(UserOnboardingJob)
				if !ok {
					t.Errorf("expected UserOnboardingJob, got %T", job)
				}
				if actual.UserID != expected.UserID || actual.UserName != expected.UserName {
					t.Errorf("expected job %+v, got %+v", expected, actual)
				}
			default:
				t.Errorf("unexpected job type: %T", job)
			}
		})
	}
}

func TestParseEnrichedPayload(t *testing.T) {
	tests := []struct {
		name            string
		input           string
		expectError     bool
		expectedJob     Job
		expectedJobType *string
		expectedPayload EnrichedPayload
	}{
		{
			name: "Valid EnrichedPayload with ReportGeneration Job",
			input: `{
				"originalmessage": {
					"job_type": "report_generation",
					"message": {
						"report_name": "Sales Report",
						"filters": "region=US"
					}
				},
				"id": "12345",
				"timestamp": "2025-08-30T12:00:00Z",
				"status": "NEW"
			}`,
			expectError: false,
			expectedJob: ReportGenerationJob{
				ReportName: "Sales Report",
				Filters:    "region=US",
			},
			expectedJobType: stringPtr(string(ReportGeneration)),
			expectedPayload: EnrichedPayload{
				OriginalMessage: json.RawMessage(`{
					"job_type": "report_generation",
					"message": {
						"report_name": "Sales Report",
						"filters": "region=US"
					}
				}`),
				ID:        "12345",
				Timestamp: "2025-08-30T12:00:00Z",
				Status:    StatusNew,
			},
		},
		{
			name: "Valid EnrichedPayload with DataCleanup Job",
			input: `{
				"originalmessage": {
					"job_type": "data_cleanup",
					"message": {
						"target_table": "users",
						"retention": 30
					}
				},
                "id": "67890",
                "timestamp": "2025-08-30T12:30:00Z",
                "status": "IN_PROGRESS"
            }`,
			expectError: false,
			expectedJob: DataCleanupJob{
				TargetTable: "users",
				Retention:   30,
			},
			expectedJobType: stringPtr(string(DataCleanup)),
			expectedPayload: EnrichedPayload{
				OriginalMessage: json.RawMessage(`{
                    "job_type": "data_cleanup",
                    "message": {
                        "target_table": "users",
                        "retention": 30
                    }
                }`),
				ID:        "67890",
				Timestamp: "2025-08-30T12:30:00Z",
				Status:    "IN_PROGRESS",
			},
		},
		{
			name: "Invalid EnrichedPayload (unknown job type)",
			input: `{
                "job_type": "unknown_job",
                "message": {},
                "id": "99999",
                "timestamp": "2025-08-30T13:00:00Z",
                "status": "FAILED"
            }`,
			expectError: true,
		},
		{
			name: "Invalid EnrichedPayload (missing fields)",
			input: `{
                "job_type": "report_generation",
                "message": {}
            }`,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job, enrichedPayload, jobType, err := ParseEnrichedPayload([]byte(tt.input))
			if tt.expectError {
				if err == nil {
					t.Errorf("expected an error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			// Validate the EnrichedPayload
			if enrichedPayload.ID != tt.expectedPayload.ID {
				t.Errorf("expected ID %s, got %s", tt.expectedPayload.ID, enrichedPayload.ID)
			}
			if enrichedPayload.Timestamp != tt.expectedPayload.Timestamp {
				t.Errorf("expected Timestamp %s, got %s", tt.expectedPayload.Timestamp, enrichedPayload.Timestamp)
			}
			if enrichedPayload.Status != tt.expectedPayload.Status {
				t.Errorf("expected Status %s, got %s", tt.expectedPayload.Status, enrichedPayload.Status)
			}

			// Validate the OriginalMessage
			var actualMessage, expectedMessage map[string]interface{}
			if err := json.Unmarshal(enrichedPayload.OriginalMessage, &actualMessage); err != nil {
				t.Errorf("failed to unmarshal actual OriginalMessage: %v", err)
			}
			if err := json.Unmarshal(tt.expectedPayload.OriginalMessage, &expectedMessage); err != nil {
				t.Errorf("failed to unmarshal expected OriginalMessage: %v", err)
			}
			if !equalJSONObjects(actualMessage, expectedMessage) {
				t.Errorf("expected OriginalMessage %+v, got %+v", expectedMessage, actualMessage)
			}

			// validate jobType
			if tt.expectedJobType != nil && jobType != nil {
				if *tt.expectedJobType != *jobType {
					t.Errorf("expected jobType %s, got %s", *tt.expectedJobType, *jobType)
				}
			} else if tt.expectedJobType != jobType { // one is nil, the other is not
				t.Errorf("expected jobType %v, got %v", tt.expectedJobType, jobType)
			}

			// Validate the Job
			switch expected := tt.expectedJob.(type) {
			case ReportGenerationJob:
				actual, ok := job.(ReportGenerationJob)
				if !ok {
					t.Errorf("expected ReportGenerationJob, got %T", job)
				}
				if actual.ReportName != expected.ReportName || actual.Filters != expected.Filters {
					t.Errorf("expected job %+v, got %+v", expected, actual)
				}
			case DataCleanupJob:
				actual, ok := job.(DataCleanupJob)
				if !ok {
					t.Errorf("expected DataCleanupJob, got %T", job)
				}
				if actual.TargetTable != expected.TargetTable || actual.Retention != expected.Retention {
					t.Errorf("expected job %+v, got %+v", expected, actual)
				}
			default:
				t.Errorf("unexpected job type: %T", job)
			}
		})
	}
}

func equalJSONObjects(a, b map[string]interface{}) bool {
	aJSON, err := json.Marshal(a)
	if err != nil {
		return false
	}

	bJSON, err := json.Marshal(b)
	if err != nil {
		return false
	}

	return string(aJSON) == string(bJSON)
}
