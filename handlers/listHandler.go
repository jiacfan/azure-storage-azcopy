// Copyright © 2017 Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package handlers

import (
	"github.com/Azure/azure-storage-azcopy/common"
	"net/http"
	"fmt"
	"io/ioutil"
	"encoding/json"
	"errors"
	"math"
)

// handles the list command
// dispatches the list order to the storage engine
func HandleListCommand(commandLineInput common.ListCmdArgsAndFlags) {
	listOrder := common.ListJobPartsTransfers{}
	listOrder.JobId =  common.JobID(commandLineInput.JobId)
	// if the expected status is given by User, then it is converted to the respective Transfer status code
	if commandLineInput.TransferStatus != ""{
		listOrder.ExpectedTransferStatus = common.TransferStatusStringToStatusCode(commandLineInput.TransferStatus)
	}else {
		// if the expected status is not given by user, it is set to 255
		listOrder.ExpectedTransferStatus = math.MaxUint8
	}
	// converted the list order command to json byte array
	commandSerialized, err := json.Marshal(listOrder)
	if err != nil{
		panic(err)
	}
	url := "http://localhost:1337"
	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil{
		panic(err)
	}
	q := req.URL.Query()
	// Type defines the type of GET request processed by the transfer engine
	q.Add("Type", "list")
	// command defines the actual list command serialized to byte array
	q.Add("command", string(commandSerialized))
	req.URL.RawQuery = q.Encode()
	resp, err := client.Do(req)
	if err != nil{
		panic(err)
	}
	// If the request is not valid or it is not processed by transfer engine, it does not returns Http StatusAccepted
	if resp.StatusCode != http.StatusAccepted {
		fmt.Println("request failed with status ", resp.Status)
		panic(errors.New(fmt.Sprintf("request failed with status %s", resp.Status)))
	}

	defer resp.Body.Close()
	body, err:= ioutil.ReadAll(resp.Body)
	if err != nil{
		panic(err)
	}
	// list Order command requested the list of existing jobs
	if listOrder.JobId == ""{
		PrintExistingJobIds(body)
	}else if commandLineInput.TransferStatus == "" { //list Order command requested the progress summary of an existing job
		PrintJobProgressSummary(body, commandLineInput.JobId)
	}else{ //list Order command requested the list of specific transfer of an existing job
		PrintJobTransfers(body, commandLineInput.JobId)
	}
}

// PrintExistingJobIds prints the response of listOrder command when listOrder command requested the list of existing jobs
func PrintExistingJobIds(data []byte){
	var jobs common.ExistingJobDetails
	err := json.Unmarshal(data, &jobs)
	if err != nil{
		panic(err)
	}
	fmt.Println("Existing Jobs ")
	for index := 0; index < len(jobs.JobIds); index++{
		fmt.Println(jobs.JobIds[index])
	}
}

// PrintJobTransfers prints the response of listOrder command when list Order command requested the list of specific transfer of an existing job
func PrintJobTransfers(data []byte, jobId string){
	var transfers common.TransfersStatus
	err := json.Unmarshal(data, &transfers)
	if err != nil{
		panic(err)
	}
	fmt.Println(fmt.Sprintf("----------- Transfers for JobId %s -----------", jobId))
	for index := 0; index < len(transfers.Status); index++{
		fmt.Println(fmt.Sprintf("transfer--> source: %s destination: %s status %s", transfers.Status[index].Src, transfers.Status[index].Dst,
																common.TransferStatusCodeToString(transfers.Status[index].TransferStatus)))
	}
}

// PrintJobProgressSummary prints the response of listOrder command when listOrder command requested the progress summary of an existing job
func PrintJobProgressSummary(summaryData []byte, jobId string) (status common.Status){
	var summary common.JobProgressSummary
	err := json.Unmarshal(summaryData, &summary)
	if err != nil{
		panic(errors.New(fmt.Sprintf("error unmarshaling the progress summary. Failed with error %s", err.Error())))
		return
	}
	fmt.Println(fmt.Sprintf("--------------- Progress Summary for Job %s ---------------", jobId))
	fmt.Println("Total Number of Transfer ", summary.TotalNumberOfTransfer)
	fmt.Println("Total Number of Transfer Completed ", summary.TotalNumberofTransferCompleted)
	fmt.Println("Total Number of Transfer Failed ", summary.TotalNumberofFailedTransfer)
	fmt.Println("Has the final part been ordered ", summary.CompleteJobOrdered)
	fmt.Println("Progress of Job in terms of Perecentage ", summary.PercentageProgress)
	for index := 0; index < len(summary.FailedTransfers); index++ {
		message := fmt.Sprintf("transfer-%d	source: %s	destination: %s", index, summary.FailedTransfers[index].Src, summary.FailedTransfers[index].Dst)
		fmt.Println(message)
	}
	return summary.JobStatus
}