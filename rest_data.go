package sgreplicate

import (
	"fmt"

	"github.com/couchbase/clog"
)

type Checkpoint struct {
	LastSequence string `json:"lastSequence"`
	Revision     string `json:"_rev"`
	Id           string `json:"_id"`
}

type PushCheckpointResponse struct {
	Id string `json:"id"`
	Ok bool   `json:"ok"`
}

type ReplicationError struct {
	ReplicationEventSignal ReplicationEventSignal
}

func NewReplicationError(eventSignal ReplicationEventSignal) *ReplicationError {
	return &ReplicationError{
		ReplicationEventSignal: eventSignal,
	}
}

func (error ReplicationError) Error() string {
	return fmt.Sprintf("%v", error.ReplicationEventSignal)
}

func (checkpoint Checkpoint) IsEmpty() bool {
	return len(checkpoint.Id) == 0
}

type ChangedRev struct {
	Revision string `json:"rev"`
}

type Change struct {
	Sequence    interface{}  `json:"seq"`
	Id          string       `json:"id"`
	ChangedRevs []ChangedRev `json:"changes"`
	Deleted     bool         `json:"deleted"`
}

type Changes struct {
	Results      []Change    `json:"results"`
	LastSequence interface{} `json:"last_seq"`
}

type RevsDiffDocumentResponse struct {
	Missing []string `json:"missing"`
}

type RevsDiffResponseMap map[string]RevsDiffDocumentResponse

func (change Change) getRevs() []string {
	revs := []string{}
	for _, changedRev := range change.ChangedRevs {
		revs = append(revs, changedRev.Revision)
	}
	return revs
}

type RevsDiffQueryMap map[string][]string

type DocumentBody map[string]interface{}

type Document struct {
	Body        DocumentBody
	Attachments []*Attachment
}

func subsetDocsWithoutAttachemnts(docs []Document) []Document {
	docsWithoutAttachemnts := []Document{}
	for _, doc := range docs {
		if len(doc.Attachments) == 0 {
			docsWithoutAttachemnts = append(docsWithoutAttachemnts, doc)
		}
	}
	return docsWithoutAttachemnts
}

func subsetDocsWithAttachemnts(docs []Document) []Document {
	docsWithAttachemnts := []Document{}
	for _, doc := range docs {
		if len(doc.Attachments) > 0 {
			docsWithAttachemnts = append(docsWithAttachemnts, doc)
		}
	}
	return docsWithAttachemnts
}

func numDocsWithoutAttachments(docs []Document) int {
	return len(subsetDocsWithoutAttachemnts(docs))
}

func numDocsWithAttachments(docs []Document) int {
	return len(subsetDocsWithAttachemnts(docs))
}

func generateRevsDiffMap(changes Changes) RevsDiffQueryMap {

	revsDiffMap := RevsDiffQueryMap{}
	for _, change := range changes.Results {
		revsDiffMap[change.Id] = change.getRevs()
	}
	return revsDiffMap
}

type DocumentRevisionPair struct {
	Id       string `json:"id"`
	Revision string `json:"rev"`
}

type BulkGetRequest struct {
	Docs []DocumentRevisionPair `json:"docs"`
}

type BulkDocsRequest struct {
	NewEdits       bool           `json:"new_edits"`
	DocumentBodies []DocumentBody `json:"docs"`
}

type PushCheckpointRequest struct {
	LastSequence string `json:"lastSequence"`
	Revision     string `json:"_rev"`
}

func generateBulkGetRequest(revsDiff RevsDiffResponseMap) BulkGetRequest {
	bulkDocsRequest := BulkGetRequest{}
	docs := []DocumentRevisionPair{}
	for docid, docResponse := range revsDiff {
		for _, missingRev := range docResponse.Missing {
			docRevPair := DocumentRevisionPair{}
			docRevPair.Id = docid
			docRevPair.Revision = missingRev
			docs = append(docs, docRevPair)
		}
	}
	bulkDocsRequest.Docs = docs
	return bulkDocsRequest
}

func generateBulkDocsRequest(documents []Document) BulkDocsRequest {
	documentBodies := []DocumentBody{}
	// we can only send the documents _without_ attachments in _bulk_docs
	for _, document := range documents {
		if len(document.Attachments) == 0 {
			documentBodies = append(documentBodies, document.Body)
		} else {
			for _, attachment := range document.Attachments {
				clog.To("Replicate", "attachment.Headers :%v", attachment.Headers)
			}
		}
	}
	return BulkDocsRequest{
		NewEdits:       false,
		DocumentBodies: documentBodies,
	}
}
