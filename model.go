package pgbuffer

import (
	"database/sql"
	"time"

	"github.com/sirupsen/logrus"
)

type Buffer struct {
	MaxTime   time.Duration
	Limit     int
	data      map[string]*BufferedData
	db        *sql.DB
	writeChan chan *writePayload
	logger    *logrus.Logger
	workers   int
}

type BufferedData struct {
	LastExit  time.Time
	LastWrite time.Time
	Columns   []string
	Data      [][]interface{}
}

type Config struct {
	MaxTime time.Duration  `yaml:"max-time"`
	Limit   int            `yaml:"limit"`
	Tables  []*TableConfig `yaml:"tables"`
	Workers int            `yaml:"workers"`
}

type TableConfig struct {
	Table   string   `yaml:"table"`
	Columns []string `yaml:"columns"`
}

type writePayload struct {
	table string
	rows  []interface{}
}
