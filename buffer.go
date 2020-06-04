package pgbuffer

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/lib/pq"
)

func NewBuffer(db *sql.DB, cfg *Config) *Buffer {
	b := &Buffer{
		db:        db,
		writeChan: make(chan *writePayload, 1000),
	}
	b.MaxTime = cfg.MaxTime
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
				if time.Since(buff.LastWrite) > b.MaxTime || len(buff.Data) >= b.Limit {
					data := buff.Data // copy it
					buff.Data = make([][]interface{}, 0)
					if len(data) > 0 {
						go b.Flush(table, buff, data)
					}
				}
			}
		}
	}
}

func (b *Buffer) Flush(table string, buff *BufferedData, data [][]interface{}) {
	tx, err := b.db.Begin()
	if err != nil {
		//TODO LOG
		return
	}

	defer tx.Rollback()
	cpin := pq.CopyIn(table, buff.Columns...)
	stmt, err := tx.Prepare(cpin)
	if err != nil {
		//TODO LOG
		fmt.Println(err)
		return
	}

	for _, row := range data {
		_, err := stmt.Exec(row...)
		if err != nil {
			fmt.Println(err)
			//TODO LOG
		}
	}
	_, err = stmt.Exec()
	if err != nil {
		fmt.Println(err)
		//TODO LOG
		return
	}
	err = stmt.Close()
	if err != nil {
		fmt.Println(err)
		//TODO LOG
		return
	}
	err = tx.Commit()
	if err != nil {
		fmt.Println(err)
		//TODO LOG
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
