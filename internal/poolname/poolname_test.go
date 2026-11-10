package poolname

import "testing"

func TestValid(t *testing.T) {
	for _, name := range []string{Default, "browser", "gpu_2", "a"} {
		if !Valid(name) {
			t.Errorf("%q must be valid", name)
		}
	}
	for _, name := range []string{"", "Browser", "a-b", "has space", "this_pool_name_is_longer_than_32_chars"} {
		if Valid(name) {
			t.Errorf("%q must be invalid", name)
		}
	}
}
