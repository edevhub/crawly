// Package poolname holds the rules for pool names, which the worker SDK and the
// Frontier must agree on.
package poolname

import "regexp"

// Default is the pool of a task or handler that names none.
const Default = "default"

// Pattern is what a pool name must match.
const Pattern = `[a-z0-9_]{1,32}`

var valid = regexp.MustCompile(`^` + Pattern + `$`)

// Valid reports whether name is a legal pool name.
func Valid(name string) bool { return valid.MatchString(name) }
