package conn

import "testing"

func TestAdvertisedSettings_Defaulted_FillsRFCDefaults(t *testing.T) {
	s := AdvertisedSettings{}.defaulted()
	if s.HeaderTableSize != 4096 {
		t.Fatalf("HeaderTableSize = %d, want 4096", s.HeaderTableSize)
	}
	if s.MaxConcurrentStreams != 1 {
		t.Fatalf("MaxConcurrentStreams = %d, want 1 (B.1 cap)", s.MaxConcurrentStreams)
	}
	if s.InitialWindowSize != 65535 {
		t.Fatalf("InitialWindowSize = %d, want 65535", s.InitialWindowSize)
	}
	if s.MaxFrameSize != 16384 {
		t.Fatalf("MaxFrameSize = %d, want 16384", s.MaxFrameSize)
	}
}

func TestAdvertisedSettings_Defaulted_PreservesNonZero(t *testing.T) {
	s := AdvertisedSettings{HeaderTableSize: 8192}.defaulted()
	if s.HeaderTableSize != 8192 {
		t.Fatalf("HeaderTableSize = %d, want 8192", s.HeaderTableSize)
	}
}

func TestAdvertisedSettings_Defaulted_AlwaysCapsConcurrent(t *testing.T) {
	s := AdvertisedSettings{MaxConcurrentStreams: 1000}.defaulted()
	if s.MaxConcurrentStreams != 1 {
		t.Fatalf("B.1 must cap to 1 even if caller asks for more, got %d", s.MaxConcurrentStreams)
	}
}

func TestConnOptions_Defaulted_FillsAllFields(t *testing.T) {
	o := ConnOptions{}.defaulted()
	if o.StreamEventBuffer != 8 {
		t.Fatalf("StreamEventBuffer = %d, want 8", o.StreamEventBuffer)
	}
	if o.Settings.MaxConcurrentStreams != 1 {
		t.Fatalf("nested settings cap not applied")
	}
	if o.Dialer == nil {
		t.Fatalf("Dialer not defaulted")
	}
}
