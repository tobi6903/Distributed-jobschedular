package api_test

import (
	"net/http"
	"testing"
)

func TestScheduleLifecycle(t *testing.T) {
	f := setup(t)
	st, sc := f.do(t, "POST", "/schedules", f.keyA,
		`{"name":"nightly","cron_expr":"0 2 * * *","timezone":"America/New_York","misfire_policy":"skip","type":"report","payload":{"x":1}}`)
	if st != http.StatusCreated || sc["timezone"] != "America/New_York" || sc["misfire_policy"] != "skip" || sc["enabled"] != true {
		t.Fatalf("create: %d %v", st, sc)
	}
	id := sc["id"].(string)

	if st, got := f.do(t, "GET", "/schedules/"+id, f.keyA, ""); st != 200 || got["name"] != "nightly" {
		t.Fatalf("get: %d %v", st, got)
	}
	if st, list := f.do(t, "GET", "/schedules", f.keyA, ""); st != 200 || len(list["schedules"].([]any)) != 1 {
		t.Fatalf("list: %d %v", st, list)
	}
	if st, got := f.do(t, "PATCH", "/schedules/"+id, f.keyA, `{"enabled":false}`); st != 200 || got["enabled"] != false {
		t.Fatalf("pause: %d %v", st, got)
	}
	if st, _ := f.do(t, "PATCH", "/schedules/"+id, f.keyA, `{}`); st != http.StatusBadRequest {
		t.Fatalf("patch without enabled: %d", st)
	}
	if st, _ := f.do(t, "DELETE", "/schedules/"+id, f.keyA, ""); st != http.StatusNoContent {
		t.Fatalf("delete: %d", st)
	}
	if st, _ := f.do(t, "GET", "/schedules/"+id, f.keyA, ""); st != http.StatusNotFound {
		t.Fatalf("get after delete: %d", st)
	}
}

func TestScheduleValidationAndIsolation(t *testing.T) {
	f := setup(t)
	tests := []struct {
		name, body string
		want       int
	}{
		{"bad cron", `{"name":"a","cron_expr":"nope","type":"x"}`, 400},
		{"bad timezone", `{"name":"a","cron_expr":"* * * * *","timezone":"Mars/Base","type":"x"}`, 400},
		{"bad policy", `{"name":"a","cron_expr":"* * * * *","misfire_policy":"yolo","type":"x"}`, 400},
		{"missing type", `{"name":"a","cron_expr":"* * * * *"}`, 400},
		{"missing name", `{"cron_expr":"* * * * *","type":"x"}`, 400},
		{"unknown field", `{"name":"a","cron_expr":"* * * * *","type":"x","tenant_id":"evil"}`, 400},
		{"ok", `{"name":"ok","cron_expr":"* * * * *","type":"x"}`, 201},
		{"duplicate name", `{"name":"ok","cron_expr":"* * * * *","type":"x"}`, 409},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if st, body := f.do(t, "POST", "/schedules", f.keyA, tt.body); st != tt.want {
				t.Fatalf("status %d want %d body %v", st, tt.want, body)
			}
		})
	}
	_, list := f.do(t, "GET", "/schedules", f.keyA, "")
	id := list["schedules"].([]any)[0].(map[string]any)["id"].(string)
	if st, _ := f.do(t, "GET", "/schedules/"+id, f.keyB, ""); st != http.StatusNotFound {
		t.Fatalf("other tenant get: %d", st)
	}
	if st, _ := f.do(t, "DELETE", "/schedules/"+id, f.keyB, ""); st != http.StatusNotFound {
		t.Fatalf("other tenant delete: %d", st)
	}
	if st, _ := f.do(t, "POST", "/schedules", "", `{}`); st != http.StatusUnauthorized {
		t.Fatalf("unauthenticated: %d", st)
	}
}
