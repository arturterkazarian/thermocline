// Minimal in-memory REST server for thermocline benchmarks.
//
// Serves the dataset dumped by seed.py from RAM so the HTTP backend is
// never the bottleneck. Endpoints mirror the HttpxSource contract:
//
//	GET /products/{id}            -> product JSON | 404
//	GET /products/{id}/hash       -> hash as a JSON string | 404
//	GET /products/changed?updated_at=&key=&limit=&offset= -> {"items": [...]}
package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
)

type Product struct {
	ID          int     `json:"id"`
	Title       string  `json:"title"`
	Price       int     `json:"price"`
	ContentHash string  `json:"content_hash"`
	UpdatedAt   string  `json:"updated_at"`
	DeletedAt   *string `json:"deleted_at"`
}

var (
	byID    = map[int]*Product{}
	ordered []*Product // sorted by (updated_at, id)
)

func main() {
	raw, err := os.ReadFile("/data/products.json")
	if err != nil {
		log.Fatalf("load dump: %v", err)
	}
	var rows []*Product
	if err := json.Unmarshal(raw, &rows); err != nil {
		log.Fatalf("parse dump: %v", err)
	}
	for _, p := range rows {
		byID[p.ID] = p
	}
	ordered = rows
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].UpdatedAt != ordered[j].UpdatedAt {
			return ordered[i].UpdatedAt < ordered[j].UpdatedAt
		}
		return ordered[i].ID < ordered[j].ID
	})

	mux := http.NewServeMux()
	mux.HandleFunc("GET /products/{id}", getOne)
	mux.HandleFunc("GET /products/{id}/hash", getHash)
	mux.HandleFunc("GET /products/changed", getChanged)
	log.Printf("serving %d products on :8077", len(byID))
	log.Fatal(http.ListenAndServe(":8077", mux))
}

func alive(id int) *Product {
	p := byID[id]
	if p == nil || p.DeletedAt != nil {
		return nil
	}
	return p
}

func getOne(w http.ResponseWriter, r *http.Request) {
	id, _ := strconv.Atoi(r.PathValue("id"))
	p := alive(id)
	if p == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	writeJSON(w, p)
}

func getHash(w http.ResponseWriter, r *http.Request) {
	id, _ := strconv.Atoi(r.PathValue("id"))
	p := alive(id)
	if p == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	writeJSON(w, p.ContentHash)
}

func getChanged(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	offset, _ := strconv.Atoi(q.Get("offset"))
	rows := ordered
	if since := q.Get("updated_at"); since != "" {
		key, _ := strconv.Atoi(q.Get("key"))
		i := sort.Search(len(rows), func(i int) bool {
			if rows[i].UpdatedAt != since {
				return rows[i].UpdatedAt > since
			}
			return rows[i].ID > key
		})
		rows = rows[i:]
	}
	if offset < len(rows) {
		rows = rows[offset:]
	} else {
		rows = nil
	}
	if limit > 0 && limit < len(rows) {
		rows = rows[:limit]
	}
	if rows == nil {
		rows = []*Product{} // an empty page must be [], not null
	}
	writeJSON(w, map[string]any{"items": rows})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
