package main

import (
	"bytes"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
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

func SendFileToSlackChannel(message, file_name string, file_content []byte) (status_code int, body []byte) {
	url := "https://slack.com/api/files.upload"
	payload := &bytes.Buffer{}
	writer := multipart.NewWriter(payload)
	defer writer.Close()

	writer.WriteField("token", Config.SlackAccessToken)
	writer.WriteField("channels", Config.SlackChannelId)
	writer.WriteField("initial_comment", message)

	fileWriter, err := writer.CreateFormFile("file", "task-drop-report.json")
	if err != nil {
		Logger.Fatalln("Error creating form field:", err)
	}

	fileWriter.Write(file_content)

	request, err := http.NewRequest("POST", url, payload)
	if err != nil {
		Logger.Fatalln("Error creating request:", err)
	}

	request.Header.Set("Content-Type", writer.FormDataContentType())
	client := http.Client{}
	response, err := client.Do(request)
	if err != nil {
		Logger.Fatalln("Error sending request:", err)
	}

	body, err = io.ReadAll(response.Body)
	if err != nil {
		Logger.Fatalln("Error parsing response body:", err)
	}
	status_code = response.StatusCode

	return
}

func SendMessageToSlackChannel(message string) (status_code int, body []byte) {
	url := "https://slack.com/api/chat.postMessage"
	payload := strings.NewReader(fmt.Sprintf(`{
		"channel": "%s",
		"text": ""
	}`, message))

	request, err := http.NewRequest("POST", url, payload)
	if err != nil {
		Logger.Fatalln("Error creating request:", err)
	}

	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", fmt.Sprintf("Bearer %s", Config.SlackAccessToken))
	client := http.Client{}
	response, err := client.Do(request)
	if err != nil {
		Logger.Fatalln("Error sending request:", err)
	}

	body, err = io.ReadAll(response.Body)
	if err != nil {
		Logger.Fatalln("Error parsing response body:", err)
	}
	status_code = response.StatusCode

	return
}
