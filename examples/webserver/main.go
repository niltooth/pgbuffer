package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"github.com/dev-mull/pgbuffer"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var buff *pgbuffer.Buffer

//Table should be as follows
//CREATE TABLE test (
//	time timestamp with time zone not null,
//	data jsonb not null
//);

func main() {

	logger := logrus.New()
	logger.SetOutput(os.Stdin)

	cfg := &pgbuffer.Config{
		Limit:   1000,
		Workers: 2,
		Logger:  logger,
		Tables: []*pgbuffer.BufferedData{
			{
				Table:   "test",
				Columns: []string{"time", "data"},
			},
		},
	}
	//Connect to the db
	db, err := sql.Open("postgres", os.Getenv("DB_URL"))
	if err != nil {
		log.Fatal(err)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(2)

	//Initialize the buffer
	buff, err = pgbuffer.NewBuffer(db, cfg)
	if err != nil {
		log.Fatal(err)
	}
	http.HandleFunc("/", handler)

	wg := &sync.WaitGroup{}
	httpServer := &http.Server{Addr: ":8080"}

	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		buff.Run()
	}(wg)

	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		httpServer.ListenAndServe()
	}(wg)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		httpServer.Shutdown(context.Background())
		buff.Stop()

	}()
	wg.Wait()
	db.Close()
}

func handler(w http.ResponseWriter, r *http.Request) {
	if r.Body != nil {
		defer r.Body.Close()
	} else {
		http.Error(w, "missing body", http.StatusBadRequest)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "404 not found", http.StatusNotFound)
		return
	}
	b, _ := ioutil.ReadAll(r.Body)
	if !json.Valid(b) {
		http.Error(w, "missing body", http.StatusBadRequest)
		return
	}
	buff.Write("test", time.Now(), string(b))

}
