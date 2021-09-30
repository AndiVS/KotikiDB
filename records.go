package main

import (
	"context"
	"encoding/json"
	"github.com/gorilla/mux"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	log "github.com/sirupsen/logrus"
	"net/http"
	"strconv"
)

type Record struct {
	Id   int    `json:"id" xml:"id" form:"id" query:"id"`
	Name string `json:"name" xml:"name" form:"name" query:"name"`
	Type string `json:"type" xml:"type" form:"type" query:"type"`
}

//curl localhost:8080/records
func SelectAll(p *pgxpool.Pool, w http.ResponseWriter, r *http.Request) {
	conn, err := p.Acquire(context.Background())
	if err != nil {
		log.Errorf("Unable to acquire a database connection: %v\n", err)
		w.WriteHeader(500)
		return
	}
	defer conn.Release()

	row, err := conn.Query(context.Background(),
		"SELECT * FROM catsbase")

	rec := []Record{}
	for row.Next() {
		var rc Record
		err = row.Scan(&rc.Id, &rc.Name, &rc.Type)
		if err == pgx.ErrNoRows {
			w.WriteHeader(404)
			return
		}
		if err != nil {
			log.Errorf("Unable to SELECT: %v", err)
			w.WriteHeader(500)
			return
		}
		rec = append(rec, rc)
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	err = json.NewEncoder(w).Encode(rec)
	if err != nil {
		log.Errorf("Unable to encode json: %v", err)
		w.WriteHeader(500)
		return
	}

}

//curl localhost:8080/records/6
func Select(p *pgxpool.Pool, w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id, err := strconv.ParseUint(vars["id"], 10, 64)
	if err != nil { // bad request
		w.WriteHeader(400)
		return
	}

	conn, err := p.Acquire(context.Background())
	if err != nil {
		log.Errorf("Unable to acquire a database connection: %v\n", err)
		w.WriteHeader(500)
		return
	}
	defer conn.Release()

	row := conn.QueryRow(context.Background(),
		"SELECT id, name, type FROM catsbase WHERE id = $1", id)

	var rec Record
	err = row.Scan(&rec.Id, &rec.Name, &rec.Type)
	if err == pgx.ErrNoRows {
		w.WriteHeader(404)
		return
	}
	if err != nil {
		log.Errorf("Unable to SELECT: %v", err)
		w.WriteHeader(500)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	err = json.NewEncoder(w).Encode(rec)
	if err != nil {
		log.Errorf("Unable to encode json: %v", err)
		w.WriteHeader(500)
		return
	}
}

//curl -d '{"name":"A","type":"B"}'  localhost:8080/records
func Insert(p *pgxpool.Pool, w http.ResponseWriter, r *http.Request) {
	var rec Record
	err := json.NewDecoder(r.Body).Decode(&rec)
	if err != nil { // bad request
		w.WriteHeader(400)
		return
	}

	conn, err := p.Acquire(context.Background())
	if err != nil {
		log.Errorf("Unable to acquire a database connection: %v", err)
		w.WriteHeader(500)
		return
	}
	defer conn.Release()

	row := conn.QueryRow(context.Background(),
		"INSERT INTO catsbase (name, type) VALUES ($1, $2) RETURNING id", rec.Name, rec.Type)

	var id uint64
	err = row.Scan(&id)
	if err != nil {
		log.Errorf("Unable to INSERT: %v", err)
		w.WriteHeader(500)
		return
	}

	resp := make(map[string]string, 1)
	resp["id"] = strconv.FormatUint(id, 10)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	err = json.NewEncoder(w).Encode(resp)
	if err != nil {
		log.Errorf("Unable to encode json: %v", err)
		w.WriteHeader(500)
		return
	}
}

//curl -XPUT  -d '{"name":"AAA","type":"BBB"}'  localhost:8080/records/10
func Update(p *pgxpool.Pool, w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id, err := strconv.ParseUint(vars["id"], 10, 64)
	if err != nil { // bad request
		w.WriteHeader(400)
		return
	}

	var rec Record
	err = json.NewDecoder(r.Body).Decode(&rec)
	if err != nil { // bad request
		w.WriteHeader(400)
		return
	}

	conn, err := p.Acquire(context.Background())
	if err != nil {
		log.Errorf("Unable to acquire a database connection: %v", err)
		w.WriteHeader(500)
		return
	}
	defer conn.Release()

	ct, err := conn.Exec(context.Background(),
		"UPDATE catsbase SET name = $2, type = $3 WHERE id = $1", id, rec.Name, rec.Type)

	if err != nil {
		log.Errorf("Unable to UPDATE: %v\n", err)
		w.WriteHeader(500)
		return
	}

	if ct.RowsAffected() == 0 {
		w.WriteHeader(404)
		return
	}

	w.WriteHeader(200)
}

//curl -XDELETE  localhost:8080/records/9
func Delete(p *pgxpool.Pool, w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id, err := strconv.ParseUint(vars["id"], 10, 64)
	if err != nil { // bad request
		w.WriteHeader(400)
		return
	}

	conn, err := p.Acquire(context.Background())
	if err != nil {
		log.Errorf("Unable to acquire a database connection: %v", err)
		w.WriteHeader(500)
		return
	}
	defer conn.Release()

	ct, err := conn.Exec(context.Background(), "DELETE FROM catsbase WHERE id = $1", id)
	if err != nil {
		log.Errorf("Unable to DELETE: %v", err)
		w.WriteHeader(500)
		return
	}

	if ct.RowsAffected() == 0 {
		w.WriteHeader(404)
		return
	}

	w.WriteHeader(200)
}
