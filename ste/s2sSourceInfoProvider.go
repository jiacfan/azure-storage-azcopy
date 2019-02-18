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
	"net/url"

	"github.com/jiacfan/azure-storage-blob-go/azblob"
)

// Abstraction of the methods needed to prepare copy source
type s2sSourceInfoProvider interface {
	// SourceURL returns source's URL.
	PreSignedSourceURL() (*url.URL, error)

	// Properties returns source's properties.
	Properties() (*S2SSrcProperties, error)

	// SourceSize returns size of source
	SourceSize() int64

	// RawSource returns raw source
	RawSource() string

	// This can be further extended, e.g. add DownloadSourceRange, which can be used to implement download+upload fashion S2S copy.
}

type s2sBlobSourceInfoProvider interface {
	s2sSourceInfoProvider

	// BlobTier returns source's blob tier.
	BlobTier() azblob.AccessTierType

	// BlobType returns source's blob type.
	BlobType() azblob.BlobType
}

type s2sSourceInfoProviderFactory func(jptm IJobPartTransferMgr) (s2sSourceInfoProvider, error)

func newDefaultSourceInfoProvider(jptm IJobPartTransferMgr) (s2sSourceInfoProvider, error) {
	return &defaultSourceInfoProvider{jptm: jptm, transferInfo: jptm.Info()}, nil
}

type defaultSourceInfoProvider struct {
	jptm         IJobPartTransferMgr
	transferInfo TransferInfo
}

func (p *defaultSourceInfoProvider) PreSignedSourceURL() (*url.URL, error) {
	srcURL, err := url.Parse(p.transferInfo.Source)
	if err != nil {
		return nil, err
	}

	return srcURL, nil
}

func (p *defaultSourceInfoProvider) Properties() (*S2SSrcProperties, error) {
	return &S2SSrcProperties{
		SrcHTTPHeaders: p.transferInfo.SrcHTTPHeaders,
		SrcMetadata:    p.transferInfo.SrcMetadata,
	}, nil
}

func (p *defaultSourceInfoProvider) SourceSize() int64 {
	return p.transferInfo.SourceSize
}

func (p *defaultSourceInfoProvider) RawSource() string {
	return p.transferInfo.Source
}