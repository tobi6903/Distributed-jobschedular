package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/vasistamagam/distributed-jobscheduler/internal/scheduler"
)

// ScheduleStore is the schedule persistence the API needs.
type ScheduleStore interface {
	Create(ctx context.Context, n scheduler.NewSchedule) (scheduler.Schedule, error)
	Get(ctx context.Context, tenantID, id string) (scheduler.Schedule, error)
	List(ctx context.Context, tenantID string) ([]scheduler.Schedule, error)
	Delete(ctx context.Context, tenantID, id string) error
	SetEnabled(ctx context.Context, tenantID, id string, enabled bool) (scheduler.Schedule, error)
}

func (s *Server) registerScheduleRoutes() {
	s.route("POST /schedules", s.authed(s.createSchedule))
	s.route("GET /schedules", s.authed(s.listSchedules))
	s.route("GET /schedules/{id}", s.authed(s.getSchedule))
	s.route("PATCH /schedules/{id}", s.authed(s.patchSchedule))
	s.route("DELETE /schedules/{id}", s.authed(s.deleteSchedule))
}

type scheduleRequest struct {
	Name           string          `json:"name"`
	CronExpr       string          `json:"cron_expr"`
	Timezone       string          `json:"timezone"`
	MisfirePolicy  string          `json:"misfire_policy"`
	Queue          string          `json:"queue"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int             `json:"priority"`
	MaxAttempts    int             `json:"max_attempts"`
	TimeoutSeconds *int            `json:"timeout_seconds"`
}

func (s *Server) createSchedule(w http.ResponseWriter, r *http.Request) {
	var req scheduleRequest
	if err := decode(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	switch {
	case !nameRE.MatchString(req.Type):
		writeError(w, http.StatusBadRequest, "type is required (1-128 chars: letters, digits, _ . : -)")
		return
	case req.Queue != "" && !nameRE.MatchString(req.Queue):
		writeError(w, http.StatusBadRequest, "queue must be 1-128 chars: letters, digits, _ . : -")
		return
	case req.Priority < -100 || req.Priority > 100:
		writeError(w, http.StatusBadRequest, "priority must be between -100 and 100")
		return
	case req.MaxAttempts < 0 || req.MaxAttempts > 100:
		writeError(w, http.StatusBadRequest, "max_attempts must be between 1 and 100")
		return
	case req.TimeoutSeconds != nil && (*req.TimeoutSeconds < 1 || *req.TimeoutSeconds > 86400):
		writeError(w, http.StatusBadRequest, "timeout_seconds must be between 1 and 86400")
		return
	}
	payload := req.Payload
	if string(payload) == "null" {
		payload = nil
	}
	sc, err := s.d.Schedules.Create(r.Context(), scheduler.NewSchedule{
		TenantID: tenantFrom(r.Context()), Name: req.Name, CronExpr: req.CronExpr, Timezone: req.Timezone,
		MisfirePolicy: scheduler.MisfirePolicy(req.MisfirePolicy), Queue: req.Queue, Type: req.Type,
		Payload: payload, Priority: int16(req.Priority), MaxAttempts: req.MaxAttempts, TimeoutSeconds: req.TimeoutSeconds,
	})
	s.respondSchedule(w, sc, err, http.StatusCreated)
}

func (s *Server) listSchedules(w http.ResponseWriter, r *http.Request) {
	list, err := s.d.Schedules.List(r.Context(), tenantFrom(r.Context()))
	if err != nil {
		s.d.Logger.Error("list schedules failed", "err", err)
		writeError(w, http.StatusServiceUnavailable, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"schedules": list})
}

func (s *Server) getSchedule(w http.ResponseWriter, r *http.Request) {
	sc, err := s.d.Schedules.Get(r.Context(), tenantFrom(r.Context()), r.PathValue("id"))
	s.respondSchedule(w, sc, err, http.StatusOK)
}

func (s *Server) patchSchedule(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Enabled *bool `json:"enabled"`
	}
	if err := decode(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.Enabled == nil {
		writeError(w, http.StatusBadRequest, "enabled is required")
		return
	}
	sc, err := s.d.Schedules.SetEnabled(r.Context(), tenantFrom(r.Context()), r.PathValue("id"), *req.Enabled)
	s.respondSchedule(w, sc, err, http.StatusOK)
}

func (s *Server) deleteSchedule(w http.ResponseWriter, r *http.Request) {
	err := s.d.Schedules.Delete(r.Context(), tenantFrom(r.Context()), r.PathValue("id"))
	switch {
	case errors.Is(err, scheduler.ErrNotFound):
		writeError(w, http.StatusNotFound, "schedule not found")
	case err != nil:
		s.d.Logger.Error("delete schedule failed", "err", err)
		writeError(w, http.StatusServiceUnavailable, "internal error")
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}

func (s *Server) respondSchedule(w http.ResponseWriter, sc scheduler.Schedule, err error, okStatus int) {
	switch {
	case errors.Is(err, scheduler.ErrInvalid):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, scheduler.ErrDuplicateName):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, scheduler.ErrNotFound):
		writeError(w, http.StatusNotFound, "schedule not found")
	case err != nil:
		s.d.Logger.Error("schedule request failed", "err", err)
		writeError(w, http.StatusServiceUnavailable, "internal error")
	default:
		writeJSON(w, okStatus, sc)
	}
}
