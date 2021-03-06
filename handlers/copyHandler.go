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
	"os"
	"fmt"
	"io/ioutil"
	"path"
	"net/url"
	"strings"
	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob"
	"log"
	"context"
	"crypto/rand"
	"io"
	"time"
)

const (
	NumOfFilesPerUploadJobPart = 2
)

// handles the copy command
// dispatches the job order (in parts) to the storage engine
func HandleCopyCommand(commandLineInput common.CopyCmdArgsAndFlags) string {
	jobPartOrder := common.CopyJobPartOrder{}
	ApplyFlags(&commandLineInput, &jobPartOrder)

	// generate job id
	uuid, err := newUUID()
	if err != nil {
		panic("Failed to generate job id")
	}
	jobPartOrder.ID = common.JobID(uuid)

	coordinatorScheduleFunc := generateCoordinatorScheduleFunc()
	if commandLineInput.SourceType == common.Local && commandLineInput.DestinationType == common.Blob {
		HandleUploadFromLocalToWastore(&commandLineInput, &jobPartOrder, coordinatorScheduleFunc)
	} else if commandLineInput.SourceType == common.Blob && commandLineInput.DestinationType == common.Local {
		HandleDownloadFromWastoreToLocal(&commandLineInput, &jobPartOrder, coordinatorScheduleFunc)
	}

	fmt.Println("Job with id", uuid, "has started.")
	if commandLineInput.IsaBackgroundOp {
		return uuid
	}
	for jobStatus := fetchJobStatus(uuid); jobStatus != common.StatusCompleted; jobStatus = fetchJobStatus(uuid){
		time.Sleep(time.Second)
	}
	return uuid
}

func HandleUploadFromLocalToWastore(commandLineInput *common.CopyCmdArgsAndFlags,
	jobPartOrderToFill *common.CopyJobPartOrder,
	dispatchJobPartOrderFunc func(jobPartOrder *common.CopyJobPartOrder))  {

	// set the source and destination type
	jobPartOrderToFill.SourceType = common.Local
	jobPartOrderToFill.DestinationType = common.Blob

	sourceFileInfo, err := os.Stat(commandLineInput.Source)

	// since source was already validated, it would be surprising if file/directory cannot be accessed at this point
	if err != nil {
		panic("cannot access source, not a valid local file system path")
	}

	// attempt to parse the destination url
	destinationUrl, err := url.Parse(commandLineInput.Destination)
	if err != nil {
		panic(err)
	}

	// TODO add source id = last modified time
	// uploading entire directory to Azure Storage
	// listing needs to be performed
	if sourceFileInfo.IsDir() {
		files, err := ioutil.ReadDir(commandLineInput.Source)

		// since source was already validated, it would be surprising if file/directory cannot be accessed at this point
		if err != nil {
			panic("cannot access source, not a valid local file system path")
		}

		// make sure this is a container url
		if strings.Contains(destinationUrl.Path[1:], "/"){
			panic("destination is not a valid container url")
		}

		// temporarily save the path of the container
		cleanContainerPath := destinationUrl.Path
		var Transfers []common.CopyTransfer
		numInTransfers := 0
		partNumber := 0

		for _, f := range files {
			if !f.IsDir() {
				destinationUrl.Path = fmt.Sprintf("%s/%s", cleanContainerPath, f.Name())
				Transfers = append(Transfers, common.CopyTransfer{
					Source:           path.Join(commandLineInput.Source, f.Name()),
					Destination:      destinationUrl.String(),
					LastModifiedTime: f.ModTime(),
					SourceSize:      f.Size(),
				})
				numInTransfers += 1

				if numInTransfers == NumOfFilesPerUploadJobPart {
					jobPartOrderToFill.Transfers = Transfers //TODO make truth, more defensive, consider channel
					jobPartOrderToFill.PartNum = common.PartNumber(partNumber)
					partNumber += 1
					dispatchJobPartOrderFunc(jobPartOrderToFill)
					Transfers = []common.CopyTransfer{}
					numInTransfers = 0
				}
			}
		}

		if numInTransfers != 0 {
			jobPartOrderToFill.Transfers = Transfers
		} else {
			jobPartOrderToFill.Transfers = []common.CopyTransfer{}
		}
		jobPartOrderToFill.PartNum = common.PartNumber(partNumber)
		jobPartOrderToFill.IsFinalPart = true
		dispatchJobPartOrderFunc(jobPartOrderToFill)

	} else { // upload single file

		// if a container url is given, must append file name to it
		if !strings.Contains(destinationUrl.Path[1:], "/") {
			destinationUrl.Path = fmt.Sprintf("%s/%s", destinationUrl.Path, sourceFileInfo.Name())
		}
		//fmt.Println("Upload", path.Join(commandLineInput.Source), "to", destinationUrl.String(), "with size", sourceFileInfo.Size())
		singleTask := common.CopyTransfer{
			Source:           commandLineInput.Source,
			Destination:      destinationUrl.String(),
			LastModifiedTime: sourceFileInfo.ModTime(),
			SourceSize:      sourceFileInfo.Size(),
		}
		jobPartOrderToFill.Transfers = []common.CopyTransfer{singleTask}
		jobPartOrderToFill.PartNum = 0
		jobPartOrderToFill.IsFinalPart = true
		dispatchJobPartOrderFunc(jobPartOrderToFill)
	}
}

func HandleDownloadFromWastoreToLocal(
	commandLineInput *common.CopyCmdArgsAndFlags,
	jobPartOrderToFill *common.CopyJobPartOrder,
	dispatchJobPartOrderFunc func(jobPartOrder *common.CopyJobPartOrder))  {
	// set the source and destination type
	jobPartOrderToFill.SourceType = common.Blob
	jobPartOrderToFill.DestinationType = common.Local

	// attempt to parse the container/blob url
	sourceUrl, err := url.Parse(commandLineInput.Source)
	if err != nil {
		panic(err)
	}
	sourcePathParts := strings.Split(sourceUrl.Path[1:], "/")

	destinationFileInfo, err := os.Stat(commandLineInput.Destination)
	// something is wrong with the destination, handle if it does not exist, else throw
	if err != nil {

		// create the destination if it does not exist
		if os.IsNotExist(err) {
			if len(sourcePathParts) <  2 { // create the directory if the source is a container
				err = os.MkdirAll(commandLineInput.Destination, os.ModePerm)
				if err != nil {
					panic("failed to create the destination on the local file system")
				}
			}

			destinationFileInfo, err = os.Stat(commandLineInput.Destination)
		} else {
			panic("cannot access destination, not a valid local file system path")
		}
	}

	// source is a single blob
	if len(sourcePathParts) > 1 {
		p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})
		blobUrl := azblob.NewBlobURL(*sourceUrl, p)
		blobProperties, err := blobUrl.GetPropertiesAndMetadata(context.Background(), azblob.BlobAccessConditions{})
		if err != nil {
			panic("Cannot get blob properties")
		}

		//TODO figure out what to do when destination is dir for a single blob download
		//unless file info tells us, it is impossible to know whether the destination is a dir
		//if destinationFileInfo.IsDir() { // destination is dir, therefore the file name needs to be generated
		//	blobName := sourcePathParts[1]
		//	commandLineInput.Destination = path.Join(commandLineInput.Destination, blobName)
		//}

		singleTask := common.CopyTransfer{
			Source: sourceUrl.String(),
			Destination: commandLineInput.Destination,
			LastModifiedTime:blobProperties.LastModified(),
			SourceSize:blobProperties.ContentLength(),
		}
		jobPartOrderToFill.Transfers = []common.CopyTransfer{singleTask}
		jobPartOrderToFill.IsFinalPart = true
		jobPartOrderToFill.PartNum = 0
		dispatchJobPartOrderFunc(jobPartOrderToFill)
	} else { // source is a container
		if !destinationFileInfo.IsDir() {
			panic("destination should be a directory")
		}

		p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})
		containerUrl := azblob.NewContainerURL(*sourceUrl, p)
		// temporarily save the path of the container
		cleanContainerPath := sourceUrl.Path
		var Transfers []common.CopyTransfer
		partNumber := 0

		// iterate over the container
		for marker := (azblob.Marker{}); marker.NotDone(); {
			// Get a result segment starting with the blob indicated by the current Marker.
			listBlob, err := containerUrl.ListBlobs(context.Background(), marker, azblob.ListBlobsOptions{})
			if err != nil {
				log.Fatal(err)
			}
			marker = listBlob.NextMarker

			// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
			for _, blobInfo := range listBlob.Blobs.Blob {
				sourceUrl.Path = cleanContainerPath + "/" + blobInfo.Name
				Transfers = append(Transfers, common.CopyTransfer{Source: sourceUrl.String(), Destination: path.Join(commandLineInput.Destination, blobInfo.Name), LastModifiedTime:blobInfo.Properties.LastModified, SourceSize:*blobInfo.Properties.ContentLength})
			}
			jobPartOrderToFill.Transfers = Transfers
			jobPartOrderToFill.PartNum = common.PartNumber(partNumber)
			partNumber += 1
			if !marker.NotDone() { // if there is no more segment
				jobPartOrderToFill.IsFinalPart = true
			}
			dispatchJobPartOrderFunc(jobPartOrderToFill)
		}
	}

	// erase the blob type, as it does not matter
	commandLineInput.BlobType = ""
}

func ApplyFlags(commandLineInput *common.CopyCmdArgsAndFlags, jobPartOrderToFill *common.CopyJobPartOrder)  {
	optionalAttributes := common.BlobTransferAttributes{
		BlockSizeinBytes: commandLineInput.BlockSize,
		ContentType: commandLineInput.ContentType,
		ContentEncoding: commandLineInput.ContentEncoding,
		Metadata: commandLineInput.Metadata,
		NoGuessMimeType: commandLineInput.NoGuessMimeType,
		PreserveLastModifiedTime: commandLineInput.PreserveLastModifiedTime,
	}

	jobPartOrderToFill.OptionalAttributes = optionalAttributes
	jobPartOrderToFill.LogVerbosity = common.LogSeverity(commandLineInput.LogVerbosity)
	jobPartOrderToFill.IsaBackgroundOp = commandLineInput.IsaBackgroundOp
	//jobPartOrderToFill.DestinationBlobType = commandLineInput.BlobType
	//jobPartOrderToFill.Acl = commandLineInput.Acl
	//jobPartOrderToFill.BlobTier = commandLineInput.BlobTier
}

// newUUID generates a random UUID according to RFC 4122
func newUUID() (string, error) {
	uuid := make([]byte, 16)
	n, err := io.ReadFull(rand.Reader, uuid)
	if n != len(uuid) || err != nil {
		return "", err
	}
	// variant bits; see section 4.1.1
	uuid[8] = uuid[8]&^0xc0 | 0x80
	// version 4 (pseudo-random); see section 4.1.3
	uuid[6] = uuid[6]&^0xf0 | 0x40
	return fmt.Sprintf("%x%x%x%x%x", uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:]), nil
}

