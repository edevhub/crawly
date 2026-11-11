// Package tasklabel holds the rules for task labels, which the Frontier enforces.
package tasklabel

import "regexp"

// Pattern is what a task label must match.
const Pattern = `[A-Za-z0-9_.:/-]{1,64}`

var valid = regexp.MustCompile(`^` + Pattern + `$`)

// Valid reports whether label is a legal task label. The empty string is not a label:
// it stands for none.
func Valid(label string) bool { return valid.MatchString(label) }
