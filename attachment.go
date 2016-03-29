package sgreplicate

import (
	"io/ioutil"
	"mime/multipart"

	"github.com/couchbaselabs/logg"
)

type Attachment struct {
	Headers map[string]string
	Data    []byte
}

func NewAttachment(part *multipart.Part) (*Attachment, error) {

	attachment := &Attachment{
		Headers: make(map[string]string),
	}

	// copy headers into Headers
	contentType := part.Header["Content-Type"][0]
	contentDisposition := part.Header["Content-Disposition"][0]
	logg.LogTo("Replicate", "attachment contentType: %v", contentType)
	logg.LogTo("Replicate", "attachment contentDisposition: %v", contentDisposition)
	attachment.Headers["Content-Type"] = contentType
	attachment.Headers["Content-Disposition"] = contentDisposition

	// read part body into Data
	data, err := ioutil.ReadAll(part)
	if err != nil {
		logg.LogTo("Replicate", "error reading attachment body: %v", err)
		return nil, err
	}
	attachment.Data = data

	return attachment, nil
}
