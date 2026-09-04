package config

import (
	"fmt"
	"io"

	"gopkg.in/yaml.v3"
)

const redactedSecret = "[REDACTED]"

// Secret stores a pre-shared key while preventing accidental disclosure via
// fmt, Go-syntax formatting, or YAML marshaling. Bytes returns an explicit copy
// for authentication code that intentionally needs the key.
type Secret struct {
	value []byte
}

// NewSecret copies value into a protected Secret value.
func NewSecret(value string) Secret {
	return Secret{value: []byte(value)}
}

// Bytes returns a copy of the secret bytes.
func (s Secret) Bytes() []byte {
	return append([]byte(nil), s.value...)
}

// Len returns the number of bytes in the secret.
func (s Secret) Len() int {
	return len(s.value)
}

// String always returns a redaction marker.
func (Secret) String() string {
	return redactedSecret
}

// GoString always returns a redaction marker.
func (Secret) GoString() string {
	return redactedSecret
}

// Format redacts every fmt verb, including numeric and byte-oriented verbs
// that would otherwise recursively format Secret's private representation.
func (Secret) Format(state fmt.State, _ rune) {
	_, _ = io.WriteString(state, redactedSecret)
}

// MarshalYAML prevents configuration dumps from exposing the PSK.
func (Secret) MarshalYAML() (any, error) {
	return redactedSecret, nil
}

// UnmarshalYAML accepts only a YAML string. Validation separately rejects an
// empty value and values above the documented resource limit.
func (s *Secret) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode || node.Tag != "!!str" {
		return invalidf("psk must be a YAML string")
	}
	s.value = append(s.value[:0], node.Value...)
	return nil
}

func (s Secret) clone() Secret {
	return Secret{value: s.Bytes()}
}
