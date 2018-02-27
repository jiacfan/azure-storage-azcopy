package handlers

import (
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	"io/ioutil"
	"net/http"
)

// handles the cancel command
// dispatches the cancel Job order to the storage engine
func HandleCancelCommand(jobIdString string) {
	url := "http://localhost:1337"

	// parsing the given JobId to validate its format correctness
	jobId, err := common.ParseUUID(jobIdString)
	if err != nil {
		// If parsing gives an error, hence it is not a valid JobId format
		fmt.Println("invalid jobId string passed. Failed while parsing string to jobId")
		return
	}

	httpClient := common.NewHttpClient(url)

	resp := httpClient.Send("cancel", jobId)
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	// If the request is not valid or it is not processed by transfer engine, it does not returns Http StatusAccepted
	if resp.StatusCode != http.StatusAccepted {
		errorMessage := fmt.Sprintf("request failed with status %s and message %s", resp.Status, string(body))
		fmt.Println(errorMessage)
		return
	}
	fmt.Println(fmt.Sprintf("Job %s cancelled successfully", jobId))
}

// handles the pause command
// dispatches the pause Job order to the storage engine
func HandlePauseCommand(jobIdString string) {
	url := "http://localhost:1337"
	client := common.NewHttpClient(url)

	// parsing the given JobId to validate its format correctness
	jobId, err := common.ParseUUID(jobIdString)
	if err != nil {
		// If parsing gives an error, hence it is not a valid JobId format
		fmt.Println("invalid jobId string passed. Failed while parsing string to jobId")
		return
	}

	resp := client.Send("resume", jobId)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	// If the request is not valid or it is not processed by transfer engine, it does not returns Http StatusAccepted
	if resp.StatusCode != http.StatusAccepted {
		errorMessage := fmt.Sprintf("request failed with status %s and message %s", resp.Status, string(body))
		fmt.Println(errorMessage)
		return
	}
	fmt.Println(fmt.Sprintf("Job %s paused successfully", jobId))
}

// handles the resume command
// dispatches the resume Job order to the storage engine
func HandleResumeCommand(jobIdString string) {
	url := "http://localhost:1337"
	client := common.NewHttpClient(url)

	// parsing the given JobId to validate its format correctness
	jobId, err := common.ParseUUID(jobIdString)
	if err != nil {
		// If parsing gives an error, hence it is not a valid JobId format
		fmt.Println("invalid jobId string passed. Failed while parsing string to jobId")
		return
	}

	resp := client.Send("resume", jobId)

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	// If the request is not valid or it is not processed by transfer engine, it does not returns Http StatusAccepted
	if resp.StatusCode != http.StatusAccepted {
		errorMessage := fmt.Sprintf("request failed with status %s and message %s", resp.Status, string(body))
		fmt.Println(errorMessage)
		return
	}
	fmt.Println(fmt.Sprintf("Job %s resume successfully", jobId))
}
