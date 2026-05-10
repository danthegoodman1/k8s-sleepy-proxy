package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	host := flag.String("host", getenv("PGHOST", "127.0.0.1"), "Postgres host")
	port := flag.String("port", getenv("PGPORT", "5432"), "Postgres port")
	user := flag.String("user", getenv("PGUSER", "postgres"), "Postgres user")
	password := flag.String("password", getenv("PGPASSWORD", ""), "Postgres password")
	database := flag.String("database", getenv("PGDATABASE", "postgres"), "Postgres database")
	timeout := flag.Duration("timeout", 60*time.Second, "overall timeout")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	dsn := postgresURL(*host, *port, *user, *password, *database)
	started := time.Now()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Fatalf("open postgres: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	conn, err := db.Conn(ctx)
	if err != nil {
		log.Fatalf("connect postgres: %v", err)
	}
	defer conn.Close()

	if err := conn.PingContext(ctx); err != nil {
		log.Fatalf("ping postgres: %v", err)
	}
	connected := time.Now()

	if _, err := conn.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS sleepy_counter (id integer PRIMARY KEY, n integer NOT NULL)`); err != nil {
		log.Fatalf("create table: %v", err)
	}

	var count int
	if err := conn.QueryRowContext(ctx, `INSERT INTO sleepy_counter (id, n) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET n = sleepy_counter.n + 1 RETURNING n`).Scan(&count); err != nil {
		log.Fatalf("increment counter: %v", err)
	}
	finished := time.Now()

	fmt.Printf(
		"count=%d connect_ms=%.3f query_ms=%.3f total_ms=%.3f\n",
		count,
		ms(connected.Sub(started)),
		ms(finished.Sub(connected)),
		ms(finished.Sub(started)),
	)
}

func postgresURL(host, port, user, password, database string) string {
	u := &url.URL{
		Scheme: "postgres",
		Host:   net.JoinHostPort(host, port),
		Path:   "/" + database,
	}
	if password == "" {
		u.User = url.User(user)
	} else {
		u.User = url.UserPassword(user, password)
	}
	q := u.Query()
	q.Set("sslmode", "disable")
	u.RawQuery = q.Encode()
	return u.String()
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func ms(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}
