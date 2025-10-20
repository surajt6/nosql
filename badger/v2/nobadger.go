//go:build nobadger || nobadgerv2
// +build nobadger nobadgerv2

package badger

import "github.com/surajt6/nosql/database"

type DB = database.NotSupportedDB
