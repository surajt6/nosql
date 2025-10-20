//go:build nopgx
// +build nopgx

package postgresql

import "github.com/surajt6/nosqlatabase"

type DB = database.NotSupportedDB
