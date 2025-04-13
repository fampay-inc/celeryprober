package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
	"time"
)

func GenerateEventObject(channel_name string) TaskEvent {
	if strings.HasSuffix(channel_name, "sent") {
		return &TaskSent{}
	} else if strings.HasSuffix(channel_name, "received") {
		return &TaskReceived{}
	} else if strings.HasSuffix(channel_name, "started") {
		return &TaskStarted{}
	} else if strings.HasSuffix(channel_name, "succeeded") {
		return &TaskSucceeded{}
	} else if strings.HasSuffix(channel_name, "failed") {
		return &TaskFailed{}
	} else {
		return nil
	}
}

func CheckIfEventAlreadyProcessed(timestamp float64, processed_timestamps []float64) bool {
	for _, t := range processed_timestamps {
		if t == timestamp {
			return true
		}
	}
	return false
}

func SendFileToSlackChannel(message, file_name string, file_content []byte) (status_code int, body []byte, err error) {
	url := "https://slack.com/api/files.upload"
	payload := &bytes.Buffer{}
	writer := multipart.NewWriter(payload)
	defer writer.Close()

	writer.WriteField("token", Config.SlackAccessToken)
	writer.WriteField("channels", Config.SlackChannelId)
	writer.WriteField("initial_comment", message)

	fileWriter, err := writer.CreateFormFile("file", "task-drop-report.json")
	if err != nil {
		Logger.Println("Error creating form field:", err)
		return
	}

	fileWriter.Write(file_content)

	request, err := http.NewRequest("POST", url, payload)
	if err != nil {
		Logger.Println("Error creating request:", err)
		return
	}

	request.Header.Set("Content-Type", writer.FormDataContentType())
	client := &http.Client{
		Timeout: 30 * time.Second,
	}
	response, err := client.Do(request)
	if err != nil {
		Logger.Println("Error sending request:", err)
		return
	}
	defer response.Body.Close()

	body, err = io.ReadAll(response.Body)
	if err != nil {
		Logger.Println("Error parsing response body:", err)
		return
	}
	status_code = response.StatusCode

	return
}

type SlackSendMessageAPIPayload struct {
	Channel string `json:"channel"`
	Text    string `json:"text"`
}

func SendMessageToSlackChannel(message string) (status_code int, body []byte, err error) {
	url := "https://slack.com/api/chat.postMessage"
	payload := &SlackSendMessageAPIPayload{
		Channel: Config.SlackChannelId,
		Text:    message,
	}
	payload_json, _ := json.Marshal(payload)

	request, err := http.NewRequest("POST", url, bytes.NewBuffer(payload_json))
	if err != nil {
		Logger.Println("Error creating request:", err)
		return
	}

	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", fmt.Sprintf("Bearer %s", Config.SlackAccessToken))
	client := &http.Client{
		Timeout: 30 * time.Second,
	}
	response, err := client.Do(request)
	if err != nil {
		Logger.Println("Error sending request:", err)
		return
	}
	defer response.Body.Close()

	body, err = io.ReadAll(response.Body)
	if err != nil {
		Logger.Println("Error parsing response body:", err)
		return
	}
	status_code = response.StatusCode

	return
}
