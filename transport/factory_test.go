package transport

import "testing"

func TestNewPollerSupportsBackendStd(t *testing.T) {
	p, err := NewPoller(Config{Backend: BackendStd})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if p.Backend() != BackendStd || p.Model() != PollerReadiness {
		t.Fatalf("poller backend=%v model=%v", p.Backend(), p.Model())
	}
}
