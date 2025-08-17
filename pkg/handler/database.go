package handler

import (
	"fmt"
	"strings"
	"sync"

	"github.com/quantumwake/alethic-ism-core-go/pkg/data/models"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var (
	db   *gorm.DB
	once sync.Once
)

// GetDB returns a singleton database connection
func GetDB() (*gorm.DB, error) {
	var err error
	once.Do(func() {
		if dsn == "" {
			err = fmt.Errorf("DSN environment variable not set")
			return
		}
		db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Silent),
		})
	})
	return db, err
}

// FormatTableName creates a valid table name from processor ID and optional config name
func FormatTableName(processorID string, configName string) string {
	//sanitized := strings.ReplaceAll(processorID, "-", "_")
	if configName != "" {
		return fmt.Sprintf("%s_%s", processorID, configName)
	}
	return processorID
}

// CreateTableFromMap creates a table with TEXT columns based on the keys in the first record
func CreateTableFromMap(tableName string, firstRecord models.Data) error {
	db, err := GetDB()
	if err != nil {
		return err
	}

	var columns []string
	for key := range firstRecord {
		// Quote column names to handle special characters
		columnDef := fmt.Sprintf(`"%s" TEXT`, key)
		columns = append(columns, columnDef)
	}

	// Quote table name to handle names starting with numbers
	createSQL := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS "%s" (%s)`,
		tableName,
		strings.Join(columns, ", "),
	)

	return db.Exec(createSQL).Error
}

// InsertRecord inserts a single record into the specified table
func InsertRecord(tableName string, record models.Data) error {
	db, err := GetDB()
	if err != nil {
		return err
	}

	var keys []string
	var placeholders []string
	var values []interface{}

	i := 1
	for key, value := range record {
		// Quote column names
		keys = append(keys, fmt.Sprintf(`"%s"`, key))
		placeholders = append(placeholders, fmt.Sprintf("$%d", i))
		values = append(values, value)
		i++
	}

	// Quote table name to handle names starting with numbers
	insertSQL := fmt.Sprintf(
		`INSERT INTO "%s" (%s) VALUES (%s)`,
		tableName,
		strings.Join(keys, ", "),
		strings.Join(placeholders, ", "),
	)

	return db.Exec(insertSQL, values...).Error
}
