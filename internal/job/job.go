// Package job holds the domain types for the scheduler: the Job record, its status
// state machine, and retry/backoff policy. It performs no I/O so it is trivially testable.
package job

import (
	"encoding/json"
	"time"
)

// Status is the lifecycle state of a job. See docs/DESIGN.md section 3.
type Status string

const (
	StatusPending   Status = "pending"   // waiting for run_at
	StatusRunning   Status = "running"   // claimed; holds a lease
	StatusSucceeded Status = "succeeded" // terminal
	StatusFailed    Status = "failed"    // terminal: permanent (non-retryable) error
	StatusRetrying  Status = "retrying"  // failed an attempt, waiting for backoff
	StatusDead      Status = "dead"      // terminal: retries exhausted (dead-letter)
)

// transitions is the full set of legal edges. The SQL enforces these via fenced UPDATEs;
// this table exists so the rules are explicit, documented, and unit-tested.
var transitions = map[Status][]Status{
	StatusPending:   {StatusRunning},
	StatusRetrying:  {StatusRunning},
	StatusRunning:   {StatusSucceeded, StatusRetrying, StatusDead, StatusFailed, StatusPending}, // pending: released on shutdown
	StatusFailed:    {StatusPending},                                                            // replay
	StatusDead:      {StatusPending},                                                            // replay
	StatusSucceeded: {},
}

// CanTransition reports whether from -> to is a legal state change.
func CanTransition(from, to Status) bool {
	for _, t := range transitions[from] {
		if t == to {
			return true
		}
	}
	return false
}

// Valid reports whether s is a known status.
func (s Status) Valid() bool {
	_, ok := transitions[s]
	return ok
}

// Terminal reports whether no automatic transition leaves this state.
func (s Status) Terminal() bool {
	return s == StatusSucceeded || s == StatusFailed || s == StatusDead
}

// Job is a row of the jobs table.
type Job struct {
	ID             string          `json:"id"`
	TenantID       string          `json:"tenant_id"`
	Queue          string          `json:"queue"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Status         Status          `json:"status"`
	Priority       int16           `json:"priority"`
	RunAt          time.Time       `json:"run_at"`
	Attempts       int             `json:"attempts"`
	MaxAttempts    int             `json:"max_attempts"`
	TimeoutSeconds *int            `json:"timeout_seconds,omitempty"`
	LockedBy       *string         `json:"locked_by,omitempty"`
	LockedUntil    *time.Time      `json:"locked_until,omitempty"`
	IdempotencyKey *string         `json:"idempotency_key,omitempty"`
	LastError      *string         `json:"last_error,omitempty"`
	Traceparent    string          `json:"-"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	FinishedAt     *time.Time      `json:"finished_at,omitempty"`

	// PickupLatency is how long the job waited past its run_at before being claimed,
	// measured on the database clock. Only populated by Claim.
	PickupLatency time.Duration `json:"-"`
}

// Ref identifies one specific claim of a job. It is the fencing token: every write that
// ends or extends a claim must present it, and is rejected if the claim is no longer
// current (lease expired and the job was re-claimed, bumping attempts).
type Ref struct {
	ID       string
	WorkerID string
	Attempts int
}

// Ref returns the fencing token for a freshly claimed job.
func (j Job) Ref() Ref {
	w := ""
	if j.LockedBy != nil {
		w = *j.LockedBy
	}
	return Ref{ID: j.ID, WorkerID: w, Attempts: j.Attempts}
}

// NewJob is the input to Enqueue.
type NewJob struct {
	TenantID       string
	Queue          string
	Type           string
	Payload        json.RawMessage
	Priority       int16
	RunAt          time.Time     // absolute start time (client clock); zero means "now"
	Delay          time.Duration // relative delay added to RunAt/now; resolved on the database clock
	MaxAttempts    int           // zero means default
	TimeoutSeconds *int
	IdempotencyKey *string
	Traceparent    string
}

const (
	DefaultQueue       = "default"
	DefaultMaxAttempts = 5
)
