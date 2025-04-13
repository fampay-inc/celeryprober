// Package utils provides common utility functions for the celery-monitor application
package utils

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// ExtractDBNumberFromRedisURL extracts the database number from a Redis URL
// Redis URLs have the format: redis://host:port/dbNumber
// If no database is specified, it defaults to 0
func ExtractDBNumberFromRedisURL(redisURL string) string {
	// Default database number
	defaultDB := "0"

	// If URL is empty, return default
	if redisURL == "" {
		return defaultDB
	}

	// Parse the URL
	parsedURL, err := url.Parse(redisURL)
	if err != nil {
		fmt.Printf("Failed to parse Redis URL: %s - %v\n", redisURL, err)
		return defaultDB
	}

	// Extract DB number from path
	path := parsedURL.Path
	if path == "" || path == "/" {
		return defaultDB
	}

	// Remove leading slash if present
	path = strings.TrimPrefix(path, "/")
	
	// Validate that it's a number
	_, err = strconv.Atoi(path)
	if err != nil {
		return defaultDB
	}

	return path
}

// GenerateTaskEventChannels creates the standard Celery event channel names for a Redis URL
func GenerateTaskEventChannels(redisURL string) []string {
	// Extract the DB number from the Redis URL
	dbNumber := ExtractDBNumberFromRedisURL(redisURL)

	// Define standard Celery event types
	eventTypes := []string{
		"task.sent",
		"task.received",
		"task.started",
		"task.succeeded",
		"task.failed",
	}

	// Generate the channel names with the extracted DB number
	channels := make([]string, len(eventTypes))
	for i, eventType := range eventTypes {
		channels[i] = fmt.Sprintf("/%s.celeryev/%s", dbNumber, eventType)
	}

	return channels
}
