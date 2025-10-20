package nosql

import (
	"strings"

	"github.com/pkg/errors"
	badgerV1 "github.com/surajt6/nosql/badger/v1"
	badgerV2 "github.com/surajt6/nosql/badger/v2"
	"github.com/surajt6/nosql/bolt"
	"github.com/surajt6/nosql/database"
	"github.com/surajt6/nosql/mysql"
	"github.com/surajt6/nosql/postgresql"
	"github.com/surajt6/nosql/sqlite"
)

// Option is just a wrapper over database.Option.
type Option = database.Option

// DB is just a wrapper over database.DB.
type DB = database.DB

// Compactor in an interface implemented by those databases that can run a value
// log garbage collector like badger.
type Compactor interface {
	Compact(discardRatio float64) error
}

var (
	// WithValueDir is a wrapper over database.WithValueDir.
	WithValueDir = database.WithValueDir
	// WithDatabase is a wrapper over database.WithDatabase.
	WithDatabase = database.WithDatabase
	// WithBadgerFileLoadingMode is a wrapper over database.WithBadgerFileLoadingMode.
	WithBadgerFileLoadingMode = database.WithBadgerFileLoadingMode
	// IsErrNotFound is a wrapper over database.IsErrNotFound.
	IsErrNotFound = database.IsErrNotFound
	// IsErrOpNotSupported is a wrapper over database.IsErrOpNotSupported.
	IsErrOpNotSupported = database.IsErrOpNotSupported

	// Available db driver types. //

	// BadgerDriver indicates the default Badger database - currently Badger V1.
	BadgerDriver = "badger"
	// BadgerV1Driver explicitly selects the Badger V1 driver.
	BadgerV1Driver = "badgerv1"
	// BadgerV2Driver explicitly selects the Badger V2 driver.
	BadgerV2Driver = "badgerv2"
	// BBoltDriver indicates the default BBolt database.
	BBoltDriver = "bbolt"
	// MySQLDriver indicates the default MySQL database.
	MySQLDriver = "mysql"
	// PostgreSQLDriver indicates the default PostgreSQL database.
	PostgreSQLDriver = "postgresql"
	// SQLiteDriver indicates the default SQLite database.
	SQLiteDriver = "sqlite"

	// Badger FileLoadingMode

	// BadgerMemoryMap indicates the MemoryMap FileLoadingMode option.
	BadgerMemoryMap = database.BadgerMemoryMap
	// BadgerFileIO indicates the FileIO FileLoadingMode option.
	BadgerFileIO = database.BadgerFileIO
)

// New returns a database with the given driver.
func New(driver, dataSourceName string, opt ...Option) (db database.DB, err error) {
	switch strings.ToLower(driver) {
	case BadgerDriver, BadgerV1Driver:
		db = &badgerV1.DB{}
	case BadgerV2Driver:
		db = &badgerV2.DB{}
	case BBoltDriver:
		db = &bolt.DB{}
	case MySQLDriver:
		db = &mysql.DB{}
	case PostgreSQLDriver:
		db = &postgresql.DB{}
	case SQLiteDriver:
		db = &sqlite.DB{}
	default:
		return nil, errors.Errorf("%s database not supported", driver)
	}
	err = db.Open(dataSourceName, opt...)
	return
}
