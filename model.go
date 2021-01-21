package pgbuffer

import (
	"database/sql"
	"time"

	"github.com/sirupsen/logrus"
)

type Buffer struct {
	data        map[string]*BufferedData
	db          *sql.DB
	writeChan   chan *writePayload
	logger      *logrus.Logger
	cfg         *Config
	stopSignal  chan struct{}
	flushSignal chan struct{}
}

type Config struct {
	Limit   int64           `yaml:"limit"`
	Tables  []*BufferedData `yaml:"tables"`
	Workers int             `yaml:"workers"`
	Logger *logrus.Logger
}

type BufferedData struct {
	LastExit  time.Time
	LastWrite time.Time
	data      [][]interface{}
	Table     string   `yaml:"table"`
	Columns   []string `yaml:"columns"`
	Limit     int64    `yaml:"limit"`
	position  int64
}

type writePayload struct {
	table string
	rows  []interface{}
}
