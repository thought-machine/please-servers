package redis

import (
	"crypto/tls"
	"crypto/x509"
	"log"
	"os"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/peterebden/go-cli-init/v4/flags"
)

// Flags is a collection of flags used to set up a redis client.
// It supports single node redis as well as primary and read replicas.
type Flags struct {
	URL             string         `long:"url" env:"REDIS_URL" description:"host:port of Redis server"`
	ReadURL         string         `long:"read_url" env:"REDIS_READ_URL" description:"host:port of a Redis read replica, if set any read operation will be routed to it"`
	Password        string         `long:"password" description:"AUTH password"`
	PasswordFile    string         `long:"password_file" env:"REDIS_PASSWORD_FILE" description:"File containing AUTH password"`
	PoolSize        int            `long:"pool_size" env:"REDIS_POOL_SIZE" default:"10" description:"Size of connection pool on primary redis client"`
	ReadPoolSize    int            `long:"read_pool_size" env:"REDIS_READ_POOL_SIZE" default:"10" description:"Size of connection pool on reading redis client"`
	PoolTimeout     flags.Duration `long:"pool_timeout" env:"REDIS_POOL_TIMEOUT" default:"5s" description:"Timeout waiting for free connection to primary redis"`
	ReadPoolTimeout flags.Duration `long:"read_pool_timeout" env:"REDIS_READ_POOL_TIMEOUT" default:"5s" description:"Timeout waiting for free connection to read replicas"`
	ReadTimeout     flags.Duration `long:"read_timeout" env:"REDIS_READ_TIMEOUT" default:"1s" description:"Timeout on network read (not read commands)"`
	WriteTimeout    flags.Duration `long:"write_timeout" env:"REDIS_WRITE_TIMEOUT" default:"1m" description:"Timeout on network write (not write commands)"`
	CAFile          string         `long:"ca_file" env:"REDIS_CA_FILE" description:"File containing the Redis instance CA cert"`
	TLS             bool           `long:"tls" description:"Use TLS for connecting to Redis"`
}

// Clients sets up clients to both primary and read replicas. If no read URL
// has been set up in the flags, it will return the primary as read client as
// well, to save the caller from doing any unnecessary.
// At the moment, any error raised while initialising the clients (ie failed
// to read TLS cert or password) will cause the program to exit.
// If `URL` is empty, no client is returned. This might change in the future.
func (r Flags) Clients() (primary, read *redis.Client) {
	if r.URL == "" {
		return nil, nil
	}

	password := r.readPassword()
	tlsConfig := r.readTLSConfig()

	primary = redis.NewClient(&redis.Options{
		Addr:         r.URL,
		Password:     password,
		TLSConfig:    tlsConfig,
		PoolSize:     r.PoolSize,
		ReadTimeout:  time.Duration(r.ReadTimeout),
		WriteTimeout: time.Duration(r.WriteTimeout),
		PoolTimeout:  time.Duration(r.PoolTimeout),
	})
	if r.ReadURL != "" {
		read = redis.NewClient(&redis.Options{
			Addr:         r.ReadURL,
			Password:     password,
			TLSConfig:    tlsConfig,
			PoolSize:     r.ReadPoolSize,
			ReadTimeout:  time.Duration(r.ReadTimeout),
			WriteTimeout: time.Duration(r.WriteTimeout),
			PoolTimeout:  time.Duration(r.ReadPoolTimeout),
		})
	} else {
		read = primary
	}
	return
}

func (r Flags) readPassword() string {
	if r.Password != "" {
		return r.Password
	} else if r.PasswordFile == "" {
		return ""
	}
	b, err := os.ReadFile(r.PasswordFile)
	if err != nil {
		log.Fatalf("Failed to read Redis password file: %s", err)
	}
	return strings.TrimSpace(string(b))
}

func (r Flags) readTLSConfig() *tls.Config {
	if !r.TLS {
		return nil
	}
	caCert, err := os.ReadFile(r.CAFile)
	if err != nil {
		log.Fatalf("Failed to read CA file at %s or load TLS config for Redis: %v", r.CAFile, err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	return &tls.Config{
		RootCAs: caCertPool,
	}
}
