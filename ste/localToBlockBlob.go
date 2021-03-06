package ste

import (
	"context"
	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob"
	"net/url"
	"os"
	"time"
	"github.com/edsrzf/mmap-go"
	"encoding/base64"
	"bytes"
	"sync/atomic"
	"github.com/Azure/azure-storage-azcopy/common"
	"fmt"
)

type localToBlockBlob struct {
	// count the number of chunks that are done
	count uint32
}

// this function performs the setup for each transfer and schedules the corresponding chunkMsgs into the chunkChannel
func (localToBlockBlob localToBlockBlob) prologue(transfer TransferMsgDetail, chunkChannel chan<- ChunkMsg) {
	// step 1: create pipeline for the destination blob
	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{
		Retry: azblob.RetryOptions{
			Policy:        azblob.RetryPolicyExponential,
			MaxTries:      3,
			TryTimeout:    time.Second * 60,
			RetryDelay:    time.Second * 1,
			MaxRetryDelay: time.Second * 3,
		},
	})
	u, _ := url.Parse(transfer.Destination)
	blobUrl := azblob.NewBlobURL(*u, p)

	// step 2: get the file size
	fi, _ := os.Stat(transfer.Source)
	blobSize := fi.Size()

	// step 3: map in the file to upload before transferring chunks
	memoryMappedFile := openAndMemoryMapFile(transfer.Source)

	// step 4: compute the number of blocks and create a slice to hold the blockIDs of each chunk
	downloadChunkSize := int64(transfer.ChunkSize)

	numOfBlocks := computeNumOfChunks(blobSize, downloadChunkSize)
	blocksIds := make([]string, numOfBlocks)
	blockIdCount := int32(0)

	// step 5: go through the file and schedule chunk messages to upload each chunk
	for startIndex := int64(0); startIndex < blobSize; startIndex += downloadChunkSize {
		adjustedChunkSize := downloadChunkSize

		// compute actual size of the chunk
		if startIndex+downloadChunkSize > blobSize {
			adjustedChunkSize = blobSize - startIndex
		}

		// schedule the chunk job/msg
		chunkChannel <- ChunkMsg{
			doTransfer: generateUploadFunc(
				transfer.JobId,
				transfer.PartNumber,
				transfer.TransferId,
				blockIdCount, // this is the index of the chunk
				numOfBlocks,
				adjustedChunkSize,
				startIndex,
				blobUrl,
				memoryMappedFile,
				transfer.TransferCtx,
				transfer.TransferCancelFunc,
				&localToBlockBlob.count,
				&blocksIds, transfer.JobHandlerMap),
		}
		blockIdCount += 1
	}
}

// this generates a function which performs the uploading of a single chunk
func generateUploadFunc(jobId common.JobID, partNum common.PartNumber, transferId uint32, chunkId int32, totalNumOfChunks uint32, chunkSize int64, startIndex int64, blobURL azblob.BlobURL,
	memoryMappedFile mmap.MMap, ctx context.Context, cancelTransfer func(), progressCount *uint32, blockIds *[]string, jPartPlanInfoMap *JobPartPlanInfoMap) chunkFunc {
	return func(workerId int) {
		logger := getLoggerFromJobPartPlanInfo(jobId, partNum, jPartPlanInfoMap)
		transferIdentifierStr := fmt.Sprintf("jobId %s and partNum %d and transferId %d", jobId, partNum, transferId)

		// step 1: generate block ID
		blockId, _ := common.NewUUID()
		encodedBlockId := base64.StdEncoding.EncodeToString([]byte(blockId))

		// step 2: save the block ID into the list of block IDs
		(*blockIds)[chunkId] = encodedBlockId
		//fmt.Println("Worker", workerId, "is processing upload CHUNK job with", transferIdentifierStr, "and chunkID", chunkId, "and blockID", encodedBlockId)

		// step 3: perform put block
		blockBlobUrl := blobURL.ToBlockBlobURL()
		_, err := blockBlobUrl.PutBlock(ctx, encodedBlockId, bytes.NewReader(memoryMappedFile[startIndex: startIndex + chunkSize]), azblob.LeaseAccessConditions{})
		if err != nil {
			// cancel entire transfer because this chunk has failed
			cancelTransfer()
			logger.Debug("worker %d is canceling Chunk job with %s and chunkId %d because startIndex of %d has failed", workerId, transferIdentifierStr, chunkId, startIndex)
			//fmt.Println("Worker", workerId, "is canceling CHUNK job with", transferIdentifierStr, "and chunkID", chunkId, "because startIndex of", startIndex, "has failed due to err", err)
			updateChunkInfo(jobId, partNum, transferId, uint16(chunkId), ChunkTransferStatusFailed, jPartPlanInfoMap)
			updateTransferStatus(jobId, partNum, transferId, common.TransferStatusFailed, jPartPlanInfoMap)
			return
		}

		updateChunkInfo(jobId, partNum, transferId, uint16(chunkId), ChunkTransferStatusComplete, jPartPlanInfoMap)
		updateThroughputCounter(chunkSize)

		// step 4: check if this is the last chunk
		if atomic.AddUint32(progressCount, 1) == totalNumOfChunks {
			// step 5: this is the last block, perform EPILOGUE
			logger.Debug("worker %d is concluding download Transfer job with %s after processing chunkId %d with blocklist %s", workerId, transferIdentifierStr, chunkId, *blockIds)
			//fmt.Println("Worker", workerId, "is concluding upload TRANSFER job with", transferIdentifierStr, "after processing chunkId", chunkId, "with blocklist", *blockIds)

			_, err = blockBlobUrl.PutBlockList(ctx, *blockIds, azblob.Metadata{}, azblob.BlobHTTPHeaders{}, azblob.BlobAccessConditions{})
			if err != nil {
				logger.Error("Worker %d failed to conclude Transfer job with %s after processing chunkId %d due to error %s", workerId, transferIdentifierStr, chunkId, string(err.Error()))
				//fmt.Println("Worker", workerId, "failed to conclude TRANSFER job with", transferIdentifierStr, "after processing chunkId", chunkId, "due to err", err)
				updateTransferStatus(jobId, partNum, transferId, common.TransferStatusFailed, jPartPlanInfoMap)
			}

			updateTransferStatus(jobId, partNum, transferId, common.TransferStatusComplete, jPartPlanInfoMap)

			err := memoryMappedFile.Unmap()
			if err != nil {
				logger.Error("worker %v failed to conclude Transfer job with %v after processing chunkId %v", workerId, transferIdentifierStr, chunkId)
				//fmt.Println("Worker", workerId, "failed to conclude TRANSFER job with", transferIdentifierStr, "after processing chunkId", chunkId, "due to err", err)
			}

		}
	}
}