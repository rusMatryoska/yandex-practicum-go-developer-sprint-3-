package handlers

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"github.com/gorilla/mux"
	m "github.com/rusMatryoska/yandex-practicum-go-developer-sprint-4/internal/middleware"
	s "github.com/rusMatryoska/yandex-practicum-go-developer-sprint-4/internal/storage"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
)

type StorageHandlers struct {
	storage s.Storage
	mw      m.MiddlewareStruct
}

func ReadBody(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	var reader io.Reader

	if strings.Contains(r.Header.Get("Content-Encoding"), "gzip") {
		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return nil, err
		}
		reader = gz
		defer gz.Close()
	} else {
		reader = r.Body
		defer r.Body.Close()
	}

	body, err := io.ReadAll(reader)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return nil, err
	}

	return body, nil
}

func (sh StorageHandlers) PingDB(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	err := sh.storage.Ping(ctx)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else {
		w.WriteHeader(http.StatusOK)
	}
}

func (sh StorageHandlers) PostAddURLHandler(w http.ResponseWriter, r *http.Request) {

	ctx := r.Context()

	urlBytes, err := ReadBody(w, r)
	if err != nil {
		log.Printf("failed read request: %v", err)
		http.Error(w, "failed read request", http.StatusInternalServerError)
		return
	}
	url := string(urlBytes)
	user := r.Context().Value(m.UserIDKey{}).(string)
	if user == "" {
		user = m.GetCookie(r, m.CookieUserID)
	}

	fullShortenURL, err := sh.storage.AddURL(ctx, url, user)
	w.Header().Set("Content-Type", "text/html")

	if err != nil {
		log.Println("unable to add url", err)
		if errors.Is(m.NewStorageError(m.ErrConflict, "409"), err) {
			w.WriteHeader(http.StatusConflict)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
	} else {
		w.WriteHeader(http.StatusCreated)
	}
	w.Write([]byte(fullShortenURL))

}

func (sh StorageHandlers) ShortenBatchHandler(w http.ResponseWriter, r *http.Request) {
	var (
		batchRequestList  []m.JSONBatchRequest
		batchResponseList []m.JSONBatchResponse
	)
	user := r.Context().Value(m.UserIDKey{}).(string)
	if user == "" {
		user = m.GetCookie(r, m.CookieUserID)
	}

	urlBytes, err := ReadBody(w, r)
	if err != nil {
		log.Printf("failed read request: %v", err)
		http.Error(w, "failed read request", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	err = json.Unmarshal([]byte(urlBytes), &batchRequestList)
	if err != nil {
		log.Printf("error in JSON: %v", err)
		http.Error(w, "error in JSON", http.StatusInternalServerError)
		return
	}
	for i := range batchRequestList {
		ctx := r.Context()
		fullShortenURL, err := sh.storage.AddURL(ctx, batchRequestList[i].OriginalURL, user)
		if errors.Is(m.NewStorageError(m.ErrConflict, "409"), err) {
			w.WriteHeader(http.StatusConflict)
			return
		} else if err != nil {
			log.Printf("error wile add URL to storage: %v", err)
			http.Error(w, "error wile add URL to storage", http.StatusInternalServerError)
			return
		} else {
			w.WriteHeader(http.StatusCreated)
		}

		batch := &m.JSONBatchResponse{
			CorrelationID: batchRequestList[i].CorrelationID,
			ShortenURL:    fullShortenURL,
		}
		batchResponseList = append(batchResponseList, *batch)
	}
	json.NewEncoder(w).Encode(batchResponseList)
}

func (sh StorageHandlers) ShortenHandler(w http.ResponseWriter, r *http.Request) {
	var (
		newURLFull    m.URLFull
		newURLShorten m.URLShorten
	)

	urlBytes, err := ReadBody(w, r)
	if err != nil {
		log.Printf("failed read request: %v", err)
		http.Error(w, "failed read request", http.StatusInternalServerError)
		return
	}
	user := r.Context().Value(m.UserIDKey{}).(string)
	if user == "" {
		user = m.GetCookie(r, m.CookieUserID)
	}

	err = json.Unmarshal(urlBytes, &newURLFull)
	if err != nil {
		http.Error(w, "unmarshall failed", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	ctx := r.Context()
	fullShortenURL, err := sh.storage.AddURL(ctx, newURLFull.URLFull, user)
	if err != nil {
		if errors.Is(m.NewStorageError(m.ErrConflict, "409"), err) {
			w.WriteHeader(http.StatusConflict)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
	} else {
		w.WriteHeader(http.StatusCreated)
	}

	newURLShorten.URLShorten = fullShortenURL
	json.NewEncoder(w).Encode(newURLShorten)
}

func (sh StorageHandlers) GetURLHandler(w http.ResponseWriter, r *http.Request) {

	params := mux.Vars(r)
	id, err := strconv.Atoi(params["id"])

	if err != nil {
		http.Error(w, "ID parameter must be Integer type", http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	url, err := sh.storage.SearchURL(ctx, id)
	if err != nil {
		http.Error(w, "There is no URL with this ID", http.StatusNotFound)
		return
	} else {
		w.Header().Set("Location", url)
		w.WriteHeader(http.StatusTemporaryRedirect)
		w.Write([]byte(url))
	}

}

func (sh StorageHandlers) GetAllURLsHandler(w http.ResponseWriter, r *http.Request) {
	user := r.Context().Value(m.UserIDKey{}).(string)
	if user == "" {
		user = m.GetCookie(r, m.CookieUserID)
	}

	ctx := r.Context()
	JSONStructList, err := sh.storage.GetAllURLForUser(ctx, user)

	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		if errors.Is(m.NewStorageError(m.ErrNoContent, "204"), err) {
			w.WriteHeader(http.StatusNoContent)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
	} else {
		json.NewEncoder(w).Encode(JSONStructList)
		w.WriteHeader(http.StatusOK)
	}

}

func (sh *StorageHandlers) DeleteHandler(w http.ResponseWriter, r *http.Request) {
	sh.mw.MU.Lock()
	defer sh.mw.MU.Unlock()

	user := r.Context().Value(m.UserIDKey{}).(string)
	if user == "" {
		user = m.GetCookie(r, m.CookieUserID)
	}

	urls, err := io.ReadAll(r.Body)

	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		w.WriteHeader(http.StatusAccepted)
	}

	st := m.ChanDelete{User: user, URLS: string(urls)}
	sh.mw.CH <- st

	//sh.storage.DeleteForUser(string(urls), user)
}

func NewRouter(storage s.Storage, mw m.MiddlewareStruct) *mux.Router {

	router := mux.NewRouter()
	router.Use(mw.CheckAuth)

	handlers := StorageHandlers{
		storage: storage,
		mw:      mw,
	}

	router.HandleFunc("/", handlers.PostAddURLHandler).Methods("POST")
	router.HandleFunc("/api/shorten", handlers.ShortenHandler).Methods("POST")
	router.HandleFunc("/api/shorten/batch", handlers.ShortenBatchHandler).Methods("POST")

	router.HandleFunc("/ping", handlers.PingDB).Methods("GET")
	router.HandleFunc("/{id}", handlers.GetURLHandler).Methods("GET")
	router.HandleFunc("/api/user/urls", handlers.GetAllURLsHandler).Methods("GET")

	router.HandleFunc("/api/user/urls", handlers.DeleteHandler).Methods("DELETE")

	return router
}
