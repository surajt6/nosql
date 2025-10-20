//go:build nomysql
// +build nomysql

package mysql

import "github.com/surajt6/nosql/database"

type DB = database.NotSupportedDB
