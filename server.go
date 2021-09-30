package main

import (
	"context"
	"github.com/gorilla/mux"
	"github.com/jackc/pgx/v4/pgxpool"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"net/http"
	"strings"
)

func initViper(configPath string) {
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.SetEnvPrefix("restexample")

	viper.SetDefault("loglevel", "debug")
	viper.SetDefault("listen", "localhost:8080")
	viper.SetDefault("db.url", "postgres://postgres:s3cr3t@localhost:5432/cats")

	if configPath != "" {
		log.Infof("Parsing config: %s", configPath)
		viper.SetConfigFile(configPath)
		err := viper.ReadInConfig()
		if err != nil {
			log.Fatalf("Unable to read config file: %s", err)
		}
	} else {
		log.Infof("Config file is not specified.")
	}
}

func initHandlers(pool *pgxpool.Pool) http.Handler {
	r := mux.NewRouter()
	r.HandleFunc("/records",
		func(w http.ResponseWriter, r *http.Request) {
			SelectAll(pool, w, r)
		}).Methods("GET")

	r.HandleFunc("/records/{id:[0-9]+}",
		func(w http.ResponseWriter, r *http.Request) {
			Select(pool, w, r)
		}).Methods("GET")

	r.HandleFunc("/records",
		func(w http.ResponseWriter, r *http.Request) {
			Insert(pool, w, r)
		}).Methods("POST")

	r.HandleFunc("/records/{id:[0-9]+}",
		func(w http.ResponseWriter, r *http.Request) {
			Update(pool, w, r)
		}).Methods("PUT")

	r.HandleFunc("/records/{id:[0-9]+}",
		func(w http.ResponseWriter, r *http.Request) {
			Delete(pool, w, r)
		}).Methods("DELETE")
	return r
}

func run(configPath string) {
	customFormatter := new(log.TextFormatter)
	customFormatter.TimestampFormat = "2006-01-02 15:04:05"
	customFormatter.FullTimestamp = true
	log.SetFormatter(customFormatter)

	initViper(configPath)

	logLevelString := viper.GetString("loglevel")
	logLevel, err := log.ParseLevel(logLevelString)
	if err != nil {
		log.Fatalf("Unable to parse loglevel: %s", logLevelString)
	}

	log.SetLevel(logLevel)

	dbURL := viper.GetString("db.url")
	log.Infof("Using DB URL: %s", dbURL)

	pool, err := pgxpool.Connect(context.Background(), dbURL)
	if err != nil {
		log.Fatalf("Unable to connection to database: %v", err)
	}
	defer pool.Close()
	log.Infof("Connected!")

	listenAddr := viper.GetString("listen")
	log.Infof("Starting HTTP server at %s...", listenAddr)
	http.Handle("/", initHandlers(pool))
	err = http.ListenAndServe(listenAddr, nil)
	if err != nil {
		log.Fatalf("http.ListenAndServe: %v", err)
	}

	log.Info("HTTP server terminated")
}

func main() {
	var configPath string

	rootCmd := cobra.Command{
		Use:     "rest-service-example",
		Version: "v1.0",
		Run: func(cmd *cobra.Command, args []string) {
			run(configPath)
		},
	}

	err := rootCmd.Execute()
	if err != nil {
		// Required arguments are missing, etc
		return
	}
}
