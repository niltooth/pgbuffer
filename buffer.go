package pgbuffer

import (
	"database/sql"
	"sync"
	"time"

	"github.com/lib/pq"
	"github.com/sirupsen/logrus"
)

func NewBuffer(db *sql.DB, cfg *Config, logger *logrus.Logger) *Buffer {
	b := &Buffer{
		db:        db,
		writeChan: make(chan *writePayload, 1000),
		logger:    logger,
	}
	b.MaxTime = cfg.MaxTime
	b.workers = cfg.Workers
	//set some default
	if b.MaxTime == 0 {
		b.MaxTime = time.Minute
	}
	if b.workers == 0 {
		b.workers = 1
	}
	b.Limit = cfg.Limit
	data := make(map[string]*BufferedData)

	for _, t := range cfg.Tables {
		buff := &BufferedData{
			Columns:   t.Columns,
			LastWrite: time.Now(),
		}
		data[t.Table] = buff
	}

	b.data = data

	return b
}

func (b *Buffer) Run() {
	flushTicker := time.NewTicker(time.Second * 5)
	defer flushTicker.Stop()
	//Control loop
	for {
		select {
		case data := <-b.writeChan:
			if buff, ok := b.data[data.table]; ok {
				buff.Data = append(buff.Data, data.rows)
			}
		case <-flushTicker.C:
			for table, buff := range b.data {
				if (time.Since(buff.LastWrite) > b.MaxTime) || (b.Limit > 0 && len(buff.Data) >= b.Limit) {
					data := buff.Data // copy it
					buff.Data = make([][]interface{}, 0)
					buff.LastWrite = time.Now()
					if len(data) > 0 {
						go b.Flush(table, buff, data)
					}
				}
			}
		}
	}
}

func (b *Buffer) Flush(table string, buff *BufferedData, data [][]interface{}) {
	st := time.Now()
	var wg sync.WaitGroup
	if len(data) <= b.workers {
		wg.Add(1)
		go b.flushBatch(&wg, table, buff.Columns, data)
	} else {

		var chunks = make([][][]interface{}, 0)
		chunkSize := len(data) / b.workers
		for i := 0; i < b.workers; i++ {
			temp := make([][]interface{}, 0)
			idx := i * chunkSize
			end := i*chunkSize + chunkSize
			for x := range data[idx:end] {
				temp = append(temp, data[x])
			}
			if i == b.workers {
				for x := range data[idx:] {
					temp = append(temp, data[x])
				}
			}
			chunks = append(chunks, temp)
		}
		if b.logger != nil {
			b.logger.Infof("workers %v, chunks %v\n", b.workers, len(chunks))
		}
		for i := 0; i < b.workers; i++ {
			wg.Add(1)
			if b.logger != nil {
				b.logger.Infof("flushing %v %v records\n", i, len(chunks[i]))
			}
			go b.flushBatch(&wg, table, buff.Columns, chunks[i])
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
