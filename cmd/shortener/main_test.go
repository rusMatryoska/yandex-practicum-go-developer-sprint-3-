package main

import (
	h "github.com/rusMatryoska/yandex-practicum-go-developer-sprint-3/internal/handlers"
	m "github.com/rusMatryoska/yandex-practicum-go-developer-sprint-3/internal/middleware"
	s "github.com/rusMatryoska/yandex-practicum-go-developer-sprint-3/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func testRequest(t *testing.T, ts *httptest.Server, method, path string, body string) (int, string) {
	t.Helper()
	r := strings.NewReader(body)
	req, err := http.NewRequest(method, ts.URL+path, r)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	defer resp.Body.Close()

	return resp.StatusCode, string(respBody)
}

//DBConnURL: "postgresql://pguser:pgpwd@127.0.0.1:5432/db",
func TestRouter(t *testing.T) {
	storageItem := &s.Memory{
		BaseURL:  "http://localhost:8080/",
		ID:       0,
		URLID:    make(map[string]int),
		IDURL:    make(map[int]string),
		UserURLs: make(map[string][]int),
	}
	mwItem := &m.MiddlewareStruct{
		SecretKey: m.GenerateRandom(16),
		BaseURL:   "http://localhost:8080/",
		Server:    "localhost:8080",
	}

	r := h.NewRouter(s.Storage(storageItem), *mwItem)

	ts := httptest.NewServer(r)
	defer ts.Close()

	status, body := testRequest(t, ts, http.MethodGet, "/1", "")
	assert.Equal(t, http.StatusNotFound, status)
	assert.Equal(t, "There is no URL with this ID\n", body)

	status, body = testRequest(t, ts, http.MethodGet, "/a", "")
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "ID parameter must be Integer type\n", body)

	status, body = testRequest(t, ts, http.MethodGet, "/api/user/urls", "")
	assert.Equal(t, http.StatusNoContent, status)
	assert.Equal(t, "", body)

	status, body = testRequest(t, ts, http.MethodPost, "/", "https://github.com/")
	assert.Equal(t, http.StatusCreated, status)
	assert.Equal(t, "http://localhost:8080/1", body)

	status, _ = testRequest(t, ts, http.MethodGet, "/1", "")
	assert.Equal(t, http.StatusOK, status)

	status, body = testRequest(t, ts, http.MethodPost, "/api/shorten", "{\"url\":\"https://www.google.ru/\"}")
	assert.Equal(t, http.StatusCreated, status)
	assert.Equal(t, "{\"result\":\"http://localhost:8080/2\"}\n", body)

	status, _ = testRequest(t, ts, http.MethodGet, "/2", "")
	assert.Equal(t, http.StatusOK, status)

	status, body = testRequest(t, ts, http.MethodGet, "/ping", "")
	assert.Equal(t, http.StatusInternalServerError, status)
	assert.Equal(t, "there is no connection to DB\n", body)

}
