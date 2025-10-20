//go:build nobadger || nobadgerv1
// +build nobadger nobadgerv1

package badger

import "github.com/surajt6/nosql/database"

type DB = database.NotSupportedDB
