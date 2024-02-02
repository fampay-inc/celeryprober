package main

import "strings"

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
