package main

import (
	"context"
	"flag"
	_ "github.com/jackc/pgx/v4/stdlib"
	_ "github.com/lib/pq"
	"github.com/pressly/goose/v3"
	handlers "github.com/rusMatryoska/yandex-practicum-go-developer-sprint-4/internal/handlers"
	middleware "github.com/rusMatryoska/yandex-practicum-go-developer-sprint-4/internal/middleware"
	storage "github.com/rusMatryoska/yandex-practicum-go-developer-sprint-4/internal/storage"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
)

const (
	command    = "up"
	dir        = "internal/migrations"
	bufferChan = 1000
)

func main() {
	var (
		st       storage.Storage
		err      error
		server   = flag.String("a", os.Getenv("SERVER_ADDRESS"), "server address")
		baseURL  = flag.String("b", os.Getenv("BASE_URL"), "base URL")
		filePath = flag.String("f", os.Getenv("FILE_STORAGE_PATH"), "file location")
		connStr  = flag.String("d", os.Getenv("DATABASE_DSN"), "connection url for DB")
	)
	flag.Parse()

	if *server == "" {
		*server = "localhost:8080"
	}

	if *baseURL == "" {
		*baseURL = "http://localhost:8080/"
	}

	if len(strings.Split(*server, ":")) != 2 {
		log.Fatal("Need address in a form host:port")
	}

	if bu := *baseURL; bu[len(bu)-1:] != "/" {
		*baseURL = *baseURL + "/"
	}

	mwItem := &middleware.MiddlewareStruct{
		SecretKey: middleware.SecretKey,
		BaseURL:   *baseURL,
		Server:    *server,
		CH:        make(chan middleware.ItemDelete, bufferChan),
		WG:        &sync.WaitGroup{},
	}

	if *connStr != "" {
		log.Println("WARNING: saving will be done through DataBase.")

		DBItem := &storage.Database{
			BaseURL:   *baseURL,
			DBConnURL: *connStr,
		}
		var dbErrorConnect error

		ctx := context.Background()
		pool, err := DBItem.GetDBConnection(ctx)

		if err != nil {
			log.Println(err)
			dbErrorConnect = err
		}

		defer pool.Close()

		DBItem.ConnPool = pool
		DBItem.DBErrorConnect = dbErrorConnect

		if DBItem.DBErrorConnect == nil {
			db, err := goose.OpenDBWithDriver("pgx", *connStr)
			if err != nil {
				log.Fatalf("failed to open DB: %v\n", err)
			}

			defer func() {
				if err := db.Close(); err != nil {
					log.Fatalf("failed to close DB: %v\n", err)
				}
			}()

			if err := goose.Run(command, db, dir); err != nil {
				log.Fatalf("goose %v: %v", command, err)
			} else {
				log.Println("Success migration!")
			}
		}
		st = storage.Storage(DBItem)

	} else if *connStr == "" && *filePath != "" {
		log.Println("WARNING: saving will be done through file.")

		fileItem := &storage.File{
			BaseURL:  *baseURL,
			Filepath: *filePath,
			ID:       0,
			URLID:    make(map[string]int),
			IDURL:    make(map[int]string),
			UserURLs: make(map[string][]int),
		}

		if _, err := os.Stat(*filePath); os.IsNotExist(err) {
			middleware.CreateFile(*filePath)
		} else {
			targets := middleware.InitMapByJSON(*filePath)
			fileItem.NewFromFile(*baseURL, targets)
		}
		st = storage.Storage(fileItem)

	} else if *connStr == "" && *filePath == "" {
		log.Println("WARNING: saving will be done through memory.")
		memoryItem := &storage.Memory{
			BaseURL:  *baseURL,
			ID:       0,
			URLID:    make(map[string]int),
			IDURL:    make(map[int]string),
			UserURLs: make(map[string][]int),
		}

		st = storage.Storage(memoryItem)
	}

	mwItem.WG.Add(1)
	go st.DeleteForUser(context.Background(), *mwItem.WG, mwItem.CH)

	go func() {
		mwItem.WG.Wait()
		close(mwItem.CH)
	}()

	srv := handlers.NewRouter(st, mwItem)
	if err = http.ListenAndServe(":"+strings.Split(*server, ":")[1], &srv); err != http.ErrServerClosed {
		log.Fatalf("HTTP server ListenAndServe Error: %v", err)
	}

}
