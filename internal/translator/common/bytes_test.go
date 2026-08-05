package common

import "testing"

func TestSSEEventDataFramesCompleteEvent(t *testing.T) {
	got := SSEEventData("response.created", []byte(`{"type":"response.created"}`))
	want := "event: response.created\ndata: {\"type\":\"response.created\"}\n\n"
	if string(got) != want {
		t.Fatalf("SSEEventData() = %q, want %q", got, want)
	}
}
