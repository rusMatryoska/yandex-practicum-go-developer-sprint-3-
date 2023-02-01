package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4/pgxpool"
	middleware "github.com/rusMatryoska/yandex-practicum-go-developer-sprint-4/internal/middleware"
	"log"
	"os"
	"strconv"
	"sync"
)

type Storage interface {
	AddURL(ctx context.Context, url string, user string) (string, error)
	SearchURL(ctx context.Context, id int) (string, error)
	GetAllURLForUser(ctx context.Context, user string) ([]middleware.JSONStructForAuth, error)
	Ping(ctx context.Context) error
	DeleteForUser(ctx context.Context, inputCh chan middleware.ItemDelete)
}

//MEMORY PART//

type Memory struct {
	BaseURL  string
	mu       sync.Mutex
	ID       int
	URLID    map[string]int
	IDURL    map[int]string
	UserURLs map[string][]int
}

func (m *Memory) AddURL(_ context.Context, url string, user string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, found := m.URLID[url]; !found {
		m.ID = m.ID + 1
		m.URLID[url] = m.ID
		m.IDURL[m.ID] = url
		m.UserURLs[user] = append(m.UserURLs[user], m.ID)
	}
	_, found := m.URLID[url]
	if m.URLID[url] != m.ID || m.IDURL[m.ID] != url || !found {
		return "", errors.New("error while adding new URL")
	} else {
		log.Println("url", url, "added to storage, you can get access by shorten:",
			m.BaseURL+strconv.Itoa(m.URLID[url]))
		return m.BaseURL + strconv.Itoa(m.URLID[url]), nil
	}
}

func (m *Memory) SearchURL(_ context.Context, id int) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.IDURL[id] != "" {
		return m.IDURL[id], nil
	} else {
		return "", errors.New("no URL with this ID")
	}

}

func (m *Memory) GetAllURLForUser(ctx context.Context, user string) ([]middleware.JSONStructForAuth, error) {

	var (
		JSONStructList []middleware.JSONStructForAuth
		JSONStruct     middleware.JSONStructForAuth
	)

	m.mu.Lock()
	defer m.mu.Unlock()

	URLs := make([]int, len(m.UserURLs[user]))
	copy(URLs, m.UserURLs[user])

	if len(m.UserURLs[user]) == 0 {
		return JSONStructList, middleware.ErrNoContent
	} else {
		for i := range URLs {
			JSONStruct.ShortURL = m.BaseURL + strconv.Itoa(m.UserURLs[user][i])

			if m.IDURL[m.UserURLs[user][i]] != "" {
				JSONStruct.OriginalURL = m.IDURL[m.UserURLs[user][i]]
			} else {
				JSONStruct.OriginalURL = ""
			}

			JSONStructList = append(JSONStructList, JSONStruct)
		}
		return JSONStructList, nil
	}

}

func (m *Memory) Ping(_ context.Context) error {
	return errors.New("there is no connection to DB")
}

func (m *Memory) DeleteForUser(_ context.Context, _ chan middleware.ItemDelete) {
}

//FILE PART//

type File struct {
	BaseURL        string
	Filepath       string
	mu             sync.Mutex
	ID             int
	URLID          map[string]int
	IDURL          map[int]string
	UserURLs       map[string][]int
	URLSToWrite    middleware.JSONStruct
	JSONStructList []middleware.JSONStruct
}

func (f *File) NewFromFile(baseURL string, targets []middleware.JSONStruct) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.JSONStructList = targets

	for _, t := range targets {
		f.URLID[t.FullURL] = t.ShortenURL
		f.IDURL[t.ShortenURL] = t.FullURL
		f.UserURLs[t.User] = append(f.UserURLs[t.User], t.ShortenURL)
		f.ID = t.ShortenURL
		log.Println("url", t.FullURL, "added to storage, you can get access by shorten:", baseURL+strconv.Itoa(t.ShortenURL))
	}
}

func (f *File) AddURL(_ context.Context, url string, user string) (string, error) {

	f.mu.Lock()
	defer f.mu.Unlock()

	if _, found := f.URLID[url]; !found {

		f.ID = f.ID + 1
		f.URLID[url] = f.ID
		f.IDURL[f.ID] = url
		f.UserURLs[user] = append(f.UserURLs[user], f.ID)

		f.URLSToWrite.FullURL = url
		f.URLSToWrite.ShortenURL = f.URLID[url]
		f.URLSToWrite.User = user

		f.JSONStructList = append(f.JSONStructList, f.URLSToWrite)
		jsonString, err := json.Marshal(f.JSONStructList)
		if err != nil {
			return "", err
		}
		os.WriteFile(f.Filepath, jsonString, 0644)
	}

	_, found := f.URLID[url]
	if f.URLID[url] != f.ID || f.IDURL[f.ID] != url || !found {
		return "", errors.New("error while adding new URL")
	} else {
		log.Println("url", url, "added to storage, you can get access by shorten:",
			f.BaseURL+strconv.Itoa(f.URLID[url]))
		return f.BaseURL + strconv.Itoa(f.URLID[url]), nil
	}
}

func (f *File) SearchURL(_ context.Context, id int) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.IDURL[id], nil
}

func (f *File) GetAllURLForUser(ctx context.Context, user string) ([]middleware.JSONStructForAuth, error) {
	var (
		JSONStructList []middleware.JSONStructForAuth
		JSONStruct     middleware.JSONStructForAuth
	)

	f.mu.Lock()
	defer f.mu.Unlock()

	if len(f.UserURLs[user]) == 0 {
		return JSONStructList, middleware.ErrNoContent
	} else {
		for i := range f.UserURLs[user] {
			JSONStruct.ShortURL = f.BaseURL + strconv.Itoa(f.UserURLs[user][i])
			JSONStruct.OriginalURL = f.IDURL[f.UserURLs[user][i]]
			JSONStructList = append(JSONStructList, JSONStruct)

		}
		return JSONStructList, nil
	}
}

func (f *File) Ping(_ context.Context) error {
	return errors.New("there is no connection to DB")
}

func (f *File) DeleteForUser(_ context.Context, _ chan middleware.ItemDelete) {
}

//DATABASE PART//

type Database struct {
	BaseURL        string
	DBConnURL      string
	ConnPool       *pgxpool.Pool
	DBErrorConnect error
}

func (db *Database) Exec(ctx context.Context, query string) (pgconn.CommandTag, error) {
	res, err := db.ConnPool.Exec(ctx, query)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (db *Database) GetDBConnection(ctx context.Context) (*pgxpool.Pool, error) {
	pool, err := pgxpool.Connect(ctx, db.DBConnURL)
	if err != nil {
		return nil, err
	} else {
		return pool, nil
	}
}

func (db *Database) Ping(ctx context.Context) error {
	if db.DBErrorConnect != nil {
		return db.DBErrorConnect
	} else {
		err := db.ConnPool.Ping(ctx)
		return err
	}
}

func (db *Database) AddURL(ctx context.Context, url string, user string) (string, error) {
	var newID int64

	row := db.ConnPool.QueryRow(ctx,
		"INSERT INTO public.storage (full_url, user_id, actual) VALUES ($1, $2, $3) RETURNING id", url, user, true)
	if err := row.Scan(&newID); err != nil {
		id, err := db.SearchID(ctx, url)
		if err == nil {
			return db.BaseURL + strconv.Itoa(id), middleware.ErrConflict
		} else {
			return "", middleware.ErrConflict
		}
	} else {
		return db.BaseURL + strconv.FormatInt(newID, 10), nil
	}
}

func (db *Database) SearchURL(ctx context.Context, id int) (string, error) {
	var (
		url    string
		actual bool
	)

	row, err := db.ConnPool.Query(ctx, "select full_url, actual from public.storage where id = $1", id)

	if err != nil {
		return "", err
	}
	defer row.Close()

	for row.Next() {
		value, err := row.Values()
		if err != nil {
			return "", err
		}

		if value[0] == nil {
			url = ""
		} else {
			url = value[0].(string)
		}

		if value[1] == nil {
			actual = true
		} else {
			actual = value[1].(bool)
		}
	}

	if err := row.Err(); err != nil {
		return "", err
	}

	if !actual {
		return url, middleware.ErrGone
	} else {
		return url, nil
	}
}

func (db *Database) GetAllURLForUser(ctx context.Context, user string) ([]middleware.JSONStructForAuth, error) {
	var (
		JSONStructList []middleware.JSONStructForAuth
		JSONStruct     middleware.JSONStructForAuth
		returnErr      error
	)

	row, err := db.ConnPool.Query(ctx, "select id, full_url from public.storage where user_id = $1", user)

	if err != nil {
		return nil, err
	}
	defer row.Close()

	if !row.Next() {
		returnErr = middleware.ErrNoContent
	} else {
		value, err := row.Values()
		if err != nil {
			return nil, err
		}

		JSONStruct.ShortURL = db.BaseURL + strconv.FormatInt(int64(value[0].(int32)), 10)
		JSONStruct.OriginalURL = value[1].(string)
		JSONStructList = append(JSONStructList, JSONStruct)
	}

	for row.Next() {
		value, err := row.Values()
		if err != nil {
			return nil, err
		}

		JSONStruct.ShortURL = db.BaseURL + strconv.FormatInt(int64(value[0].(int32)), 10)
		JSONStruct.OriginalURL = value[1].(string)
		JSONStructList = append(JSONStructList, JSONStruct)
	}

	if err := row.Err(); err != nil {
		return nil, err
	}

	fmt.Sprintln(JSONStructList)
	return JSONStructList, returnErr
}

func (db *Database) SearchID(ctx context.Context, url string) (int, error) {
	var id int

	row, err := db.ConnPool.Query(ctx, "select id from public.storage where full_url = $1", url)

	if err != nil {
		return 0, err
	}
	defer row.Close()

	for row.Next() {
		value, err := row.Values()
		if err != nil {
			return 0, err
		}

		if value[0] == nil {
			id = 0
		} else {
			id = int(value[0].(int32))
		}
	}

	if err := row.Err(); err != nil {
		return 0, err
	}

	return id, nil

}

func (db *Database) DeleteForUser(ctx context.Context, inputCh chan middleware.ItemDelete) {

	sql := ""
	size := 0
	for {
		select {
		case <-ctx.Done():
			close(inputCh)
			return
		case item, ok := <-inputCh:
			if !ok {

				_, err := db.ConnPool.Exec(ctx, sql)
				if err != nil {
					log.Println(err)
				}
				close(inputCh)
				return
			}

			if size < middleware.BatchSize {
				sql = sql + "UPDATE public.storage SET actual=false WHERE user_id ='" + item.User + "' and id in " + item.ListID + ";"
				size = size + 1
			} else {
				_, err := db.ConnPool.Exec(ctx, sql)
				if err != nil {
					log.Println(err)
				}
				sql = "UPDATE public.storage SET actual=false WHERE user_id ='" + item.User + "' and id in " + item.ListID + ";"
				size = 0
			}

		}
	}
}
