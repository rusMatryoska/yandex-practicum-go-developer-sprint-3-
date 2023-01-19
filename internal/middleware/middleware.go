package middleware

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gofrs/uuid"
	"log"
	"net/http"
	"os"
	"sync"
)

const (
	CookieUserID   = "UserID"
	CookieUserSign = "UserSigned"
)

var (
	ErrConflict  = errors.New(`409 Conflict`)
	ErrNoContent = errors.New(`204 No Content`)
	ErrGone      = errors.New(`410 Gone`)
	SecretKey    = GenerateRandom(16)
)

type UserIDKey struct{}

type SignInStruct struct {
	UserID string `json:"user_id"`
}

type MiddlewareStruct struct {
	SecretKey []byte
	BaseURL   string
	Server    string
	MU        sync.Mutex
	CH        []chan ChanDelete
}

type JSONStructForAuth struct {
	ShortURL    string `json:"short_url"`
	OriginalURL string `json:"original_url"`
}

type JSONStruct struct {
	FullURL    string `json:"fullURL"`
	ShortenURL int    `json:"shortenURL"`
	User       string `json:"user"`
}

type URLFull struct {
	URLFull string `json:"url"`
}

type URLShorten struct {
	URLShorten string `json:"result"`
}

type JSONBatchRequest struct {
	CorrelationID string `json:"correlation_id"`
	OriginalURL   string `json:"original_url"`
}

type JSONBatchResponse struct {
	CorrelationID string `json:"correlation_id"`
	ShortenURL    string `json:"short_url"`
}

type ChanDelete struct {
	User string
	URLS string
}

func GenerateRandom(size int) []byte {
	b := make([]byte, size)
	_, err := rand.Read(b)
	if err != nil {
		log.Fatal(err)
	}

	return b
}

func GetCookie(r *http.Request, nameCookie string) string {
	if cookie, err := r.Cookie(nameCookie); err == nil {
		return cookie.Value
	} else {
		return ""
	}
}

func SetSign(id string, key []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(id))
	return h.Sum(nil)
}

func CreateFile(filePath string) {
	f, err := os.Create(filePath)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
}

func InitMapByJSON(filePath string) []JSONStruct {
	jsonString, err := os.ReadFile(filePath)
	if err != nil {
		log.Fatal(err)
	}

	targets := []JSONStruct{}

	err = json.Unmarshal(jsonString, &targets)
	if err != nil {
		log.Fatal(err)
	}
	return targets

}

type StorageError struct {
	Label string
	Err   error
}

func (se *StorageError) Unwrap() error {
	return se.Err
}

func (se *StorageError) Error() string {
	return fmt.Sprintf("[%s] %v", se.Label, se.Err)
}

func NewStorageError(err error, label string) error {
	return &StorageError{Err: err, Label: label}
}

func (s *MiddlewareStruct) CheckAuth(next http.Handler) http.Handler {

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		reqUserID := GetCookie(r, CookieUserID)
		reqUserSign := GetCookie(r, CookieUserSign)

		var (
			UserID   string
			UserSign []byte
		)

		if reqUserID == "" || reqUserSign == "" ||
			(reqUserID != "" && reqUserSign != "" &&
				reqUserSign != fmt.Sprintf("%x", SetSign(reqUserID, SecretKey))) {
			u, _ := uuid.NewV4()
			UserID = u.String()
			UserSign = SetSign(UserID, SecretKey)

			cookieSign := &http.Cookie{
				Name:  CookieUserSign,
				Value: fmt.Sprintf("%x", UserSign),
				Path:  "/",
			}
			cookieUserID := &http.Cookie{
				Name:  CookieUserID,
				Value: UserID,
				Path:  "/",
			}
			http.SetCookie(w, cookieSign)
			http.SetCookie(w, cookieUserID)
		}

		ctx := context.WithValue(r.Context(), UserIDKey{}, UserID)

		r = r.WithContext(ctx)
		next.ServeHTTP(w, r)
	})
}
