package sgreplicate

import (
	"io/ioutil"
	"mime/multipart"
)

type Attachment struct {
	Headers map[string]string
	Data    []byte
}

func NewAttachment(part *multipart.Part, logger loggerFunction) (*Attachment, error) {

	attachment := &Attachment{
		Headers: make(map[string]string),
	}

	// copy headers into Headers
	contentTypes := part.Header["Content-Type"]
	if contentTypes != nil {
		contentType := contentTypes[0]
		logger("Replicate", "attachment contentType: %v", contentType)
		attachment.Headers["Content-Type"] = contentType
	}
	
	contentDisposition := part.Header["Content-Disposition"][0]
	logger("Replicate", "attachment contentDisposition: %v", contentDisposition)

	attachment.Headers["Content-Disposition"] = contentDisposition

	// read part body into Data
	data, err := ioutil.ReadAll(part)
	if err != nil {
		logger("Replicate", "error reading attachment body: %v", err)
		return nil, err
	}
	attachment.Data = data

	return attachment, nil
}
