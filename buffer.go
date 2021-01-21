package pgbuffer

import (
	"database/sql"
	"errors"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"sync"
	"time"

	"github.com/lib/pq"
)

func NewBuffer(db *sql.DB, cfg *Config) (*Buffer,error) {
	if cfg.Logger == nil {
		cfg.Logger = logrus.New()
		cfg.Logger.SetOutput(ioutil.Discard)
	}
	b := &Buffer{
		db:        db,
		writeChan: make(chan *writePayload, 1000),
		logger:    cfg.Logger,
		cfg:       cfg,
		stopSignal:      make(chan struct{}, 2),
		flushSignal:     make(chan struct{}, 2),
	}
	if err := b.validate(); err != nil {
		return nil,err
	}

	//Setup some defaults
	if cfg.Limit == 0 {
		cfg.Limit = 1000
	}
	if cfg.Workers == 0 {
		cfg.Workers = 1
	}
	b.data = make(map[string]*BufferedData)
	for _, t := range cfg.Tables {
		if t.Limit == 0 { // if its not set use the global limit
			t.Limit = cfg.Limit
		}
		t.data = make([][]interface{}, t.Limit)
		t.position = 0
		t.LastWrite = time.Now()
		b.data[t.Table] = t
	}
	return b,nil
}

func (b *Buffer) validate() error {
	if len(b.cfg.Tables) == 0 {
		return errors.New("no table config provided")
	}
	for _,t := range b.cfg.Tables {
		if err := b.validateTable(t); err != nil {
			return err
		}
	}
	return nil
}

func (b *Buffer) validateTable(t *BufferedData) error {
	tx, err := b.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	cpin := pq.CopyIn(t.Table, t.Columns...)
	stmt, err := tx.Prepare(cpin)
	if err != nil {
		return err
	}
	stmt.Close()
	return nil
}

func (b *Buffer) Stop() {
	b.stopSignal <- struct{}{}
}

func (b *Buffer) FlushAll() {
	b.flushSignal <- struct{}{}
}

func (b *Buffer) Run() {
	//Control loop
	//TODO: refactor
	CONTROL:
	for {
		select {
		case data := <-b.writeChan:
			if buff, ok := b.data[data.table]; ok {
				if buff.position == buff.Limit {
					data := buff.data
					buff.data = make([][]interface{}, buff.Limit)
					buff.position = 0
					if len(data) > 0 {
						go b.Flush(buff, data)
					}
				}
				buff.data[buff.position] = data.rows
				buff.position++
			}
		case <-b.stopSignal:
			for _, buff := range b.data {
				buff.LastWrite = time.Now()
				b.Flush(buff, buff.data)
			}
			break CONTROL
		case <-b.flushSignal:
			for _, buff := range b.data {
				buff.LastWrite = time.Now()
				data := buff.data
				buff.data = make([][]interface{}, buff.Limit)
				buff.position = 0
				if len(data) > 0 {
					go b.Flush(buff, data)
				}

			}
		}
	}
}

func (b *Buffer) Flush(buff *BufferedData, data [][]interface{}) {
	st := time.Now()
	var wg sync.WaitGroup
	if len(data) <= b.cfg.Workers {
		wg.Add(1)
		go b.flushBatch(&wg, buff.Table, buff.Columns, data)
	} else {

		var chunks = make([][][]interface{}, 0)
		chunkSize := len(data) / b.cfg.Workers
		for i := 0; i < b.cfg.Workers; i++ {
			temp := make([][]interface{}, 0)
			idx := i * chunkSize
			end := i*chunkSize + chunkSize
			for x := range data[idx:end] {
				temp = append(temp, data[x])
			}
			if i == b.cfg.Workers {
				for x := range data[idx:] {
					temp = append(temp, data[x])
				}
			}
			chunks = append(chunks, temp)
		}
		if b.logger != nil {
			b.logger.Infof("workers %v, chunks %v\n", b.cfg.Workers, len(chunks))
		}
		for i := 0; i < b.cfg.Workers; i++ {
			wg.Add(1)
			if b.logger != nil {
				b.logger.Infof("flushing %v %v records\n", i, len(chunks[i]))
			}
			go b.flushBatch(&wg, buff.Table, buff.Columns, chunks[i])
		}
	}
	wg.Wait()
	if b.logger != nil {
		b.logger.Infof("flushed %d records in %v", len(data), time.Since(st))
	}
}

func (b *Buffer) flushBatch(wg *sync.WaitGroup, table string, columns []string, data [][]interface{}) {
	defer wg.Done()
	tx, err := b.db.Begin()
	if err != nil {
		if b.logger != nil {
			b.logger.Error(err)
		}
		return
	}

	defer tx.Rollback()
	cpin := pq.CopyIn(table, columns...)
	stmt, err := tx.Prepare(cpin)
	if err != nil {
		if b.logger != nil {
			b.logger.Error(err)
		}
		return
	}

	for _, row := range data {
		_, err := stmt.Exec(row...)
		if err != nil {
			if b.logger != nil {
				b.logger.Error(err)
			}
		}
	}
	_, err = stmt.Exec()
	if err != nil {
		if b.logger != nil {
			b.logger.Error(err)
		}
		return
	}
	err = stmt.Close()
	if err != nil {
		if b.logger != nil {
			b.logger.Error(err)
		}
		return
	}
	err = tx.Commit()
	if err != nil {
		if b.logger != nil {
			b.logger.Error(err)
		}
		return
	}
}

func (b *Buffer) Write(table string, rows ...interface{}) {
	p := &writePayload{
		table: table,
		rows:  rows,
	}
	b.writeChan <- p
}
