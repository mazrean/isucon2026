package isureal

import (
	"net"
	"net/url"
	"os"
	"strconv"
)

type Config struct {
	Region     string
	DBHost     string
	DBPort     string
	DBUser     string
	DBPassword string
	DBName     string
	LogLevel   string
	ReadHost   string
	NodeName   string
	DBMaxOpen  int
}

func ConfigFromEnv() *Config {
	return &Config{
		Region:     envOrDefault("ISUREAL_REGION", "asia"),
		DBHost:     envOrDefault("ISUREAL_DB_HOST", "127.0.0.1"),
		DBPort:     envOrDefault("ISUREAL_DB_PORT", "5432"),
		DBUser:     envOrDefault("ISUREAL_DB_USER", "isureal"),
		DBPassword: envOrDefault("ISUREAL_DB_PASSWORD", "isureal"),
		DBName:     envOrDefault("ISUREAL_DB_NAME", "isureal"),
		LogLevel:   envOrDefault("ISUREAL_LOG_LEVEL", "debug"),
		ReadHost:   os.Getenv("ISUREAL_DB_READ_HOST"),
		NodeName:   os.Getenv("ISUREAL_NODE_NAME"),
		DBMaxOpen:  envIntOrDefault("ISUREAL_DB_MAX_OPEN", 30),
	}
}

func (c *Config) DatabaseDSN() string {
	return c.databaseDSNForHost(c.DBHost)
}

// ReadReplicaEnabled は、primary とは別の読み取り用ホストが指定されているかを返す。
func (c *Config) ReadReplicaEnabled() bool {
	return c.ReadHost != "" && c.ReadHost != c.DBHost
}

func (c *Config) databaseDSNForHost(host string) string {
	u := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(c.DBUser, c.DBPassword),
		Host:   net.JoinHostPort(host, c.DBPort),
		Path:   "/" + c.DBName,
	}
	return u.String()
}

func envOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func envIntOrDefault(key string, defaultValue int) int {
	if value, err := strconv.Atoi(os.Getenv(key)); err == nil && value > 0 {
		return value
	}
	return defaultValue
}
