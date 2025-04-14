package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

// Log is the global logger instance
var Log zerolog.Logger

// InitLogger initializes the structured logger with logfmt format
func InitLogger() {
	// Set global level for development
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	// Configure logfmt output
	logfmtWriter := &LogfmtWriter{Out: os.Stdout}

	// Create the root logger with logfmt formatter
	Log = zerolog.New(logfmtWriter).With().Timestamp().Logger()

	// Create a standard logger that forwards to zerolog
	stdLogger := log.New(StdLogAdapter{log: Log}, "", 0)
	Logger = stdLogger
}

// LogfmtWriter implements a writer that formats in logfmt style
type LogfmtWriter struct {
	Out io.Writer
}

// Write implements the io.Writer interface and formats log events in logfmt format
func (w LogfmtWriter) Write(p []byte) (n int, err error) {
	// Parse the JSON log entry
	var entry map[string]interface{}
	if err := json.Unmarshal(p, &entry); err != nil {
		return 0, err
	}
	
	// Extract the timestamp, level and message
	timestamp, _ := entry["time"].(string)
	if timestamp == "" {
		timestamp = time.Now().Format(time.RFC3339)
	}
	
	level, _ := entry["level"].(string)
	if level == "" {
		level = "info"
	}
	
	msg, _ := entry["message"].(string)
	
	// Start building logfmt
	parts := []string{fmt.Sprintf("time=\"%s\"" , timestamp)}
	parts = append(parts, fmt.Sprintf("level=%s", level))
	
	if msg != "" {
		parts = append(parts, fmt.Sprintf("msg=\"%s\"", msg))
	}
	
	// Add all other fields
	for k, v := range entry {
		// Skip fields we've already processed
		if k == "time" || k == "level" || k == "message" {
			continue
		}
		
		// Format the value based on its type and content
		valStr := formatValue(v)
		
		// Add the field to our parts list
		parts = append(parts, fmt.Sprintf("%s=%s", k, valStr))
	}
	
	// Join all parts with spaces and add a newline
	logline := strings.Join(parts, " ") + "\n"
	
	// Write the formatted log line
	return w.Out.Write([]byte(logline))
}

// StdLogAdapter adapts zerolog for use with the standard log package
type StdLogAdapter struct {
	log zerolog.Logger
}

// Write implements io.Writer to redirect standard log output to zerolog
func (w StdLogAdapter) Write(p []byte) (n int, err error) {
	w.log.Info().Msg(string(p))
	return len(p), nil
}

// LogEvent creates a structured log event with standardized fields and consistent ordering
func LogEvent(probeName string) *zerolog.Event {
	// Always use the same field ordering across all log messages
	// This makes logs easier to read and parse
	return Log.Info().Str("probe", probeName)
}

// formatValue formats a value for logfmt output
func formatValue(v interface{}) string {
	switch val := v.(type) {
	case string:
		// Quote strings that contain spaces
		if strings.Contains(val, " ") || val == "" {
			return fmt.Sprintf("\"%s\"", val)
		}
		return val
	case []interface{}:
		// Format arrays nicely
		var items []string
		for _, item := range val {
			items = append(items, fmt.Sprintf("%v", item))
		}
		return fmt.Sprintf("[%s]", strings.Join(items, ","))
	case bool:
		return fmt.Sprintf("%t", val)
	case nil:
		return "null"
	default:
		return fmt.Sprintf("%v", val)
	}
}

// LogWarnEvent creates a structured warning log event with standardized fields
func LogWarnEvent(probeName string) *zerolog.Event {
	return Log.Warn().Str("probe", probeName)
}

// LogErrorEvent creates a structured error log event with standardized fields
func LogErrorEvent(probeName string, err error) *zerolog.Event {
	// Log error message as a simple string to avoid potential marshalling issues
	return Log.Error().Str("probe", probeName).Str("error", err.Error())
}

// FormatArgs converts variadic arguments to a string for compatibility with legacy logging
func FormatArgs(v ...interface{}) string {
	return fmt.Sprint(v...)
}
