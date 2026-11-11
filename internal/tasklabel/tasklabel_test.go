package tasklabel

import (
	"strings"
	"testing"
)

func TestValid(t *testing.T) {
	for _, label := range []string{"a", "Category-1", "news/world", "a.b:c_d", strings.Repeat("x", 64)} {
		if !Valid(label) {
			t.Errorf("%q must be valid", label)
		}
	}
	for _, label := range []string{"", "has space", "new\nline", "näme", "a,b", strings.Repeat("x", 65)} {
		if Valid(label) {
			t.Errorf("%q must be invalid", label)
		}
	}
}
