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

package ste

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob"
	"github.com/edsrzf/mmap-go"
	"net/url"
)

type localToBlockBlob struct {}

// this function performs the setup for each transfer and schedules the corresponding chunkMsgs into the chunkChannel
func (localToBlockBlob localToBlockBlob) prologue(transfer TransferMsg, chunkChannel chan<- ChunkMsg) {

	// step 1: create pipeline for the destination blob
	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{
		Retry: azblob.RetryOptions{
			Policy:        azblob.RetryPolicyExponential,
			MaxTries:      UploadMaxTries,
			TryTimeout:    UploadTryTimeout,
			RetryDelay:    UploadRetryDelay,
			MaxRetryDelay: UploadMaxRetryDelay,
		},
		Log: pipeline.LogOptions{
			Log: func(l pipeline.LogLevel, msg string) {
				transfer.Log(common.LogLevel(l), msg)
			},
			MinimumLevelToLog: func() pipeline.LogLevel {
				return pipeline.LogLevel(transfer.MinimumLogLevel)
			},
		},
	})

	u, _ := url.Parse(transfer.Destination)
	blobUrl := azblob.NewBlobURL(*u, p)

	// step 2: get size info from transfer
	blobSize := int64(transfer.SourceSize)
	chunkSize := int64(transfer.BlockSize)
	numOfBlocks := transfer.NumChunks

	// step 3: map in the file to upload before transferring chunks
	memoryMappedFile := executionEngineHelper{}.openAndMemoryMapFile(transfer.Source)

	// step 4.a: if blob size is smaller than chunk size, we should do a put blob instead of chunk up the file
	if blobSize <= chunkSize {
		fmt.Println("PUT BLOB TRIGGERED for", transfer.Source)
		localToBlockBlob.putBlob(transfer, blobUrl, memoryMappedFile)
		return
	}

	// step 4.b: get the number of blocks and create a slice to hold the blockIDs of each chunk
	blocksIds := make([]string, numOfBlocks)
	blockIdCount := int32(0)

	// step 5: go through the file and schedule chunk messages to upload each chunk
	for startIndex := int64(0); startIndex < blobSize; startIndex += chunkSize {
		adjustedChunkSize := chunkSize

		// compute actual size of the chunk
		if startIndex+chunkSize > blobSize {
			adjustedChunkSize = blobSize - startIndex
		}

		// schedule the chunk job/msg
		chunkChannel <- ChunkMsg{
			doTransfer: localToBlockBlob.generateUploadFunc(
				transfer,
				blockIdCount, // this is the index of the chunk
				uint32(numOfBlocks),
				adjustedChunkSize,
				startIndex,
				blobUrl,
				memoryMappedFile,
				&blocksIds),
		}
		blockIdCount += 1
	}
}

// this generates a function which performs the uploading of a single chunk
func (localToBlockBlob) generateUploadFunc(t TransferMsg, chunkId int32, totalNumOfChunks uint32, chunkSize int64, startIndex int64, blobURL azblob.BlobURL,
	memoryMappedFile mmap.MMap, blockIds *[]string) chunkFunc {
	return func(workerId int) {

		// TODO consider encapsulating this check operation on transferMsg
		if t.TransferContext.Err() != nil {
			t.Log(common.LogInfo, fmt.Sprintf("is cancelled. Hence not picking up chunkId %d", chunkId))
			if t.ChunksDone() == totalNumOfChunks {
				t.Log(common.LogInfo,
					fmt.Sprintf("has worker %d which is finalizing cancellation of transfer", workerId))
				t.TransferDone()
			}
		} else {
			// step 1: generate block ID
			blockId := common.NewUUID().String()
			encodedBlockId := base64.StdEncoding.EncodeToString([]byte(blockId))

			// step 2: save the block ID into the list of block IDs
			(*blockIds)[chunkId] = encodedBlockId

			// step 3: perform put block
			blockBlobUrl := blobURL.ToBlockBlobURL()

			body := newRequestBodyPacer(bytes.NewReader(memoryMappedFile[startIndex:startIndex+chunkSize]), pc)
			putBlockResponse, err := blockBlobUrl.PutBlock(t.TransferContext, encodedBlockId, body, azblob.LeaseAccessConditions{})
			// TODO consider encapsulating cancel operation on transferMsg
			if err != nil {
				// cancel entire transfer because this chunk has failed
				t.TransferCancelFunc()
				t.Log(common.LogInfo,
					fmt.Sprintf("has worker %d which is canceling transfer because upload of chunkId %d because startIndex of %d has failed",
						workerId, chunkId, startIndex))

				//updateChunkInfo(jobId, partNum, transferId, uint16(chunkId), ChunkTransferStatusFailed, jobsInfoMap)
				t.TransferStatus(common.TransferFailed)

				if t.ChunksDone() == totalNumOfChunks {
					t.Log(common.LogInfo,
						fmt.Sprintf("has worker %d is finalizing cancellation of transfer", workerId))
					t.TransferDone()

					err := memoryMappedFile.Unmap()
					if err != nil {
						t.Log(common.LogError,
							fmt.Sprintf("has worker %v which failed to conclude transfer after processing chunkId %v",
								workerId, chunkId))
					}

				}
				return
			}

			if putBlockResponse != nil {
				putBlockResponse.Response().Body.Close()
			}

			// TODO this should be 1 counter per job
			realTimeThroughputCounter.updateCurrentBytes(chunkSize)

			// step 4: check if this is the last chunk
			if t.ChunksDone() == totalNumOfChunks {
				// If the transfer gets cancelled before the putblock list
				if t.TransferContext.Err() != nil {
					t.TransferDone()
					return
				}
				// step 5: this is the last block, perform EPILOGUE
				t.Log(common.LogInfo,
					fmt.Sprintf("has worker %d which is concluding download transfer after processing chunkId %d with blocklist %s",
						workerId, chunkId, *blockIds))

				// fetching the blob http headers with content-type, content-encoding attributes
				// fetching the metadata passed with the JobPartOrder
				blobHttpHeader, metaData := t.blobHttpHeaderandMetaData(memoryMappedFile)

				putBlockListResponse, err := blockBlobUrl.PutBlockList(t.TransferContext, *blockIds, blobHttpHeader, metaData, azblob.BlobAccessConditions{})
				if err != nil {
					t.Log(common.LogError,
						fmt.Sprintf("has worker %d which failed to conclude Transfer after processing chunkId %d due to error %s",
							workerId, chunkId, string(err.Error())))
					t.TransferStatus(common.TransferFailed)
					t.TransferDone()
					return
				}

				if putBlockListResponse != nil {
					putBlockListResponse.Response().Body.Close()
				}

				t.Log(common.LogInfo, "completed successfully")
				t.TransferStatus(common.TransferComplete)
				t.TransferDone()

				err = memoryMappedFile.Unmap()
				if err != nil {
					t.Log(common.LogError,
						fmt.Sprintf("has worker %v which failed to conclude Transfer after processing chunkId %v",
							workerId, chunkId))
				}
			}
		}
	}
}

func (localToBlockBlob localToBlockBlob) putBlob(t TransferMsg, blobURL azblob.BlobURL, memoryMappedFile mmap.MMap) {

	// transform blobURL and perform put blob operation
	blockBlobUrl := blobURL.ToBlockBlobURL()
	blobHttpHeader, metaData := t.blobHttpHeaderandMetaData(memoryMappedFile)
	body := newRequestBodyPacer(bytes.NewReader(memoryMappedFile), pc)
	putBlobResp, err := blockBlobUrl.PutBlob(t.TransferContext, body, blobHttpHeader, metaData, azblob.BlobAccessConditions{})

	// if the put blob is a failure, updating the transfer status to failed
	if err != nil {
		t.Log(common.LogInfo, " put blob failed and so cancelling the transfer")
		t.TransferStatus(common.TransferFailed)
	} else {
		// if the put blob is a success, updating the transfer status to success
		t.Log(common.LogInfo,
			fmt.Sprintf("put blob successful"))
		t.TransferStatus(common.TransferComplete)
	}

	// updating number of transfers done for job part order
	t.TransferDone()

	// closing the put blob response body
	if putBlobResp != nil {
		putBlobResp.Response().Body.Close()
	}

	err = memoryMappedFile.Unmap()
	if err != nil {
		t.Log(common.LogError, " has error mapping the memory map file")
	}
}
