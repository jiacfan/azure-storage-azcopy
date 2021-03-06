package ste

import (
	"github.com/Azure/azure-storage-azcopy/common"
)

//These constant defines the various types of source and destination of the transfers

const dataSchemaVersion = 0 // To be Incremented every time when we release azcopy with changed dataschema

// JobPartPlan represent the header of Job Part's Memory Map File
type JobPartPlanHeader struct {
	Version uint32 // represent the version of data schema format of header
	Id [128 / 8] byte
	PartNum uint32
	IsFinalPart bool
	Priority uint8
	TTLAfterCompletion uint32
	SrcLocationType common.LocationType
	DstLocationType common.LocationType
	NumTransfers uint32
	//Status uint8
	BlobData JobPartPlanBlobData
}

// JobPartPlan represent the header of Job Part's Optional Attributes in Memory Map File
type JobPartPlanBlobData struct {
	ContentTypeLength     uint8
	ContentType           [256]byte
	ContentEncodingLength uint8
	ContentEncoding       [256]byte
	MetaDataLength        uint16
	MetaData              [1000]byte
	BlockSize             uint64
}

// JobPartPlan represent the header of Job Part's Transfer in Memory Map File
type JobPartPlanTransfer struct {
	Offset         uint64
	SrcLength      uint16
	DstLength      uint16
	ChunkNum       uint16
	ModifiedTime   uint32
	Status         common.Status
	SourceSize     uint64
	CompletionTime uint64
}


type JobPartPlanTransferChunk struct {
	BlockId [128 / 8]byte
	Status uint8
}

const (
	ChunkTransferStatusInactive = 0
	ChunkTransferStatusActive = 1
	ChunkTransferStatusProgress = 2
	ChunkTransferStatusComplete = 3
	ChunkTransferStatusFailed = 4
)



const (
	HighJobPriority = 0
	MediumJobPriority = 1
	LowJobPriority = 2
	DefaultJobPriority = HighJobPriority
)

const (
	MAX_SIZE_CONTENT_TYPE = 256
	MAX_SIZE_CONTENT_ENCODING = 256
	MAX_SIZE_META_DATA = 1000
)