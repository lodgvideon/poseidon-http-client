package hpack

import (
	"fmt"
	"os"
	"testing"
)

var dynamicEncoderSink *Encoder

func BenchmarkEncoder_DynamicConstructor(b *testing.B) {
	if os.Getenv("POSEIDON_BENCH_DYNAMIC") == "" {
		b.Skip("set POSEIDON_BENCH_DYNAMIC=1 to measure encoder construction")
	}
	b.ReportAllocs()
	for range b.N {
		dynamicEncoderSink = NewEncoder()
	}
}

func BenchmarkEncoder_DynamicLookup(b *testing.B) {
	for _, count := range []int{8, 64, 512} {
		for _, match := range []string{"newest", "oldest", "name-only", "miss"} {
			b.Run(fmt.Sprintf("entries=%d/match=%s", count, match), func(b *testing.B) {
				enc := NewEncoder()
				enc.SetMaxDynamicTableSize(uint32(count * 64))
				enc.SetMaxDynamicTableSizeLimit(uint32(count * 64))
				for i := range count {
					enc.dt.add([]byte(fmt.Sprintf("x-%04d", i)), []byte("value"))
				}
				name, value := []byte(fmt.Sprintf("x-%04d", count-1)), []byte("value")
				switch match {
				case "oldest":
					name = []byte("x-0000")
				case "name-only":
					value = []byte("missing")
				case "miss":
					name = []byte("x-miss")
				}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					enc.dynamicLookup(name, value)
				}
			})
		}
	}
}

func BenchmarkEncoder_DynamicChurn(b *testing.B) {
	if os.Getenv("POSEIDON_BENCH_DYNAMIC") == "" {
		b.Skip("set POSEIDON_BENCH_DYNAMIC=1 to measure allocating table churn")
	}
	enc := NewEncoder()
	fields := make([]HeaderField, 256)
	for i := range fields {
		fields[i] = HeaderField{Name: []byte(fmt.Sprintf("x-%04d", i)), Value: []byte("value")}
	}
	dst := make([]byte, 0, 64)
	for _, field := range fields {
		dst = enc.WriteField(dst[:0], field.Name, field.Value, field.Indexing)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		field := fields[i%len(fields)]
		dst = enc.WriteField(dst[:0], field.Name, field.Value, field.Indexing)
	}
}

func BenchmarkEncoder_DynamicRepeatedName(b *testing.B) {
	for _, count := range []int{8, 64, 512} {
		b.Run(fmt.Sprintf("entries=%d", count), func(b *testing.B) {
			enc := NewEncoder()
			enc.SetMaxDynamicTableSize(uint32(count * 64))
			enc.SetMaxDynamicTableSizeLimit(uint32(count * 64))
			name := []byte("x-shared")
			for i := range count {
				enc.dt.add(name, []byte(fmt.Sprintf("v-%04d", i)))
			}
			value := []byte("v-0000")
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				enc.dynamicLookup(name, value)
			}
		})
	}
}
