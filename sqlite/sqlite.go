package sqlite

import (
	"bytes"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	"github.com/surajt6/nosql/database"
)

// DB is a wrapper over *sql.DB,
type DB struct {
	db *sql.DB
}

// Close implements database.DB.
func (db *DB) Close() error {
	return errors.WithStack(db.db.Close())
}

// CmpAndSwap implements database.DB.
func (db *DB) CmpAndSwap(bucket []byte, key []byte, oldValue []byte, newValue []byte) ([]byte, bool, error) {
	sqlTx, err := db.db.Begin()
	if err != nil {
		return nil, false, errors.WithStack(err)
	}

	val, swapped, err := cmpAndSwap(sqlTx, bucket, key, oldValue, newValue)
	switch {
	case err != nil:
		if err := sqlTx.Rollback(); err != nil {
			return nil, false, errors.Wrapf(err, "failed to execute CmpAndSwap transaction on %s/%s and failed to rollback transaction", bucket, key)
		}
		return nil, false, err
	case swapped:
		if err := sqlTx.Commit(); err != nil {
			return nil, false, errors.Wrapf(err, "failed to commit MySQL transaction")
		}
		return val, swapped, nil
	default:
		if err := sqlTx.Rollback(); err != nil {
			return nil, false, errors.Wrapf(err, "failed to rollback read-only CmpAndSwap transaction on %s/%s", bucket, key)
		}
		return val, swapped, err
	}
}

func cmpAndSwap(sqlTx *sql.Tx, bucket, key, oldValue, newValue []byte) ([]byte, bool, error) {
	var current []byte
	err := sqlTx.QueryRow(getQryForUpdate(bucket), key).Scan(&current)

	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, false, err
	}
	if !bytes.Equal(current, oldValue) {
		return current, false, nil
	}

	if _, err = sqlTx.Exec(insertUpdateQry(bucket), key, newValue, newValue); err != nil {
		return nil, false, errors.Wrapf(err, "failed to set %s/%s", bucket, key)
	}
	return newValue, true, nil
}

// CreateTable implements database.DB.
func (db *DB) CreateTable(bucket []byte) error {
	_, err := db.db.Exec(createTableQry(bucket))
	if err != nil {
		println(err.Error())
		return errors.Wrapf(err, "failed to create table %s", bucket)
	}
	return nil
}

// Del implements database.DB.
func (db *DB) Del(bucket []byte, key []byte) error {
	_, err := db.db.Exec(delQry(bucket), key)
	return errors.Wrapf(err, "failed to delete %s/%s", bucket, key)
}

// DeleteTable implements database.DB.
func (db *DB) DeleteTable(bucket []byte) error {
	_, err := db.db.Exec(deleteTableQry(bucket))
	if err != nil {
		estr := err.Error()
		if strings.HasPrefix(estr, "no such table") {
			return errors.Wrapf(database.ErrNotFound, estr)
		}
		return errors.Wrapf(err, "failed to delete table %s", bucket)
	}
	return nil
}

// Get implements database.DB.
func (db *DB) Get(bucket []byte, key []byte) (ret []byte, err error) {
	var val string
	err = db.db.QueryRow(getQry(bucket), key).Scan(&val)
	switch {
	case err == sql.ErrNoRows:
		return nil, errors.Wrapf(database.ErrNotFound, "%s/%s not found", bucket, key)
	case err != nil:
		return nil, errors.Wrapf(err, "failed to get %s/%s", bucket, key)
	default:
		return []byte(val), nil
	}
}

// List implements database.DB.
func (db *DB) List(bucket []byte) ([]*database.Entry, error) {
	rows, err := db.db.Query(fmt.Sprintf("SELECT * FROM `%s`", bucket))
	if err != nil {
		estr := err.Error()
		if strings.HasPrefix(estr, "no such table") {
			return nil, errors.Wrapf(database.ErrNotFound, estr)
		}
		return nil, errors.Wrapf(err, "error querying table %s", bucket)
	}
	defer rows.Close()
	var (
		key, value string
		entries    []*database.Entry
	)
	for rows.Next() {
		err := rows.Scan(&key, &value)
		if err != nil {
			return nil, errors.Wrap(err, "error getting key and value from row")
		}
		entries = append(entries, &database.Entry{
			Bucket: bucket,
			Key:    []byte(key),
			Value:  []byte(value),
		})
	}
	err = rows.Err()
	if err != nil {
		return nil, errors.Wrap(err, "error accessing row")
	}
	return entries, nil
}

// Open implements database.DB.
func (db *DB) Open(dataSourceName string, opt ...database.Option) error {
	opts := &database.Options{}
	for _, o := range opt {
		if err := o(opts); err != nil {
			return err
		}
	}

	// Database name in DSN is ignored if explicitly set
	// if opts.Database == "" {
	// 	opts.Database = parsedDSN.DBName
	// }

	_db, err := sql.Open("sqlite3", dataSourceName)

	if err != nil {
		return errors.Wrap(err, "error connecting to sqlite3")
	}

	// _, err = _db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", opts.Database))
	// if err != nil {
	// 	return errors.Wrapf(err, "error creating database %s (if not exists)", opts.Database)
	// }

	// parsedDSN.DBName = opts.Database
	// db.db, err = sql.Open("mysql", parsedDSN.FormatDSN())
	db.db = _db
	// if err != nil {
	// 	return errors.Wrapf(err, "error connecting to mysql database")
	// }

	return nil
}

// Set implements database.DB.
func (db *DB) Set(bucket []byte, key []byte, value []byte) error {
	_, err := db.db.Exec(insertUpdateQry(bucket), key, value, value)
	if err != nil {
		return errors.Wrapf(err, "failed to set %s/%s", bucket, key)
	}
	return nil
}

// Update implements database.DB.
func (db *DB) Update(tx *database.Tx) error {
	sqlTx, err := db.db.Begin()
	if err != nil {
		return errors.WithStack(err)
	}
	rollback := func(err error) error {
		if rollbackErr := sqlTx.Rollback(); rollbackErr != nil {
			return errors.Wrap(err, "UPDATE failed, unable to rollback transaction")
		}
		return errors.Wrap(err, "UPDATE failed")
	}
	for _, q := range tx.Operations {
		// create or delete buckets
		switch q.Cmd {
		case database.CreateTable:
			_, err := sqlTx.Exec(createTableQry(q.Bucket))
			if err != nil {
				return rollback(errors.Wrapf(err, "failed to create table %s", q.Bucket))
			}
		case database.DeleteTable:
			_, err := sqlTx.Exec(deleteTableQry(q.Bucket))
			if err != nil {
				estr := err.Error()
				if strings.HasPrefix(err.Error(), "no such table") {
					return errors.Wrapf(database.ErrNotFound, estr)
				}
				return errors.Wrapf(err, "failed to delete table %s", q.Bucket)
			}
		case database.Get:
			var val string
			err := sqlTx.QueryRow(getQry(q.Bucket), q.Key).Scan(&val)
			switch {
			case err == sql.ErrNoRows:
				return rollback(errors.Wrapf(database.ErrNotFound, "%s/%s not found", q.Bucket, q.Key))
			case err != nil:
				return rollback(errors.Wrapf(err, "failed to get %s/%s", q.Bucket, q.Key))
			default:
				q.Result = []byte(val)
			}
		case database.Set:
			if _, err = sqlTx.Exec(insertUpdateQry(q.Bucket), q.Key, q.Value, q.Value); err != nil {
				return rollback(errors.Wrapf(err, "failed to set %s/%s", q.Bucket, q.Key))
			}
		case database.Delete:
			if _, err = sqlTx.Exec(delQry(q.Bucket), q.Key); err != nil {
				return rollback(errors.Wrapf(err, "failed to delete %s/%s", q.Bucket, q.Key))
			}
		case database.CmpAndSwap:
			q.Result, q.Swapped, err = cmpAndSwap(sqlTx, q.Bucket, q.Key, q.CmpValue, q.Value)
			if err != nil {
				return rollback(errors.Wrapf(err, "failed to load-or-store %s/%s", q.Bucket, q.Key))
			}
		case database.CmpOrRollback:
			return database.ErrOpNotSupported
		default:
			return database.ErrOpNotSupported
		}
	}

	if err = errors.WithStack(sqlTx.Commit()); err != nil {
		return rollback(err)
	}
	return nil
}

func createTableQry(bucket []byte) string {
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`(nkey BLOB(255) PRIMARY KEY, nvalue BLOB);", bucket)
}

func deleteTableQry(bucket []byte) string {
	return fmt.Sprintf("DROP TABLE `%s`", bucket)
}

func getQry(bucket []byte) string {
	return fmt.Sprintf("SELECT nvalue FROM `%s` WHERE nkey = ?", bucket)
}

func delQry(bucket []byte) string {
	return fmt.Sprintf("DELETE FROM `%s` WHERE nkey = ?", bucket)
}

func insertUpdateQry(bucket []byte) string {
	return fmt.Sprintf("INSERT INTO `%s`(nkey, nvalue) VALUES(?,?) ON CONFLICT(nkey) DO UPDATE SET nvalue = ?", bucket)
}

// TODO: check this
func getQryForUpdate(bucket []byte) string {
	return fmt.Sprintf("SELECT nvalue FROM `%s` WHERE nkey = ?", bucket)
}
