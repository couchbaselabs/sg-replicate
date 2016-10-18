package sgreplicate

import (
	"fmt"
	"strconv"
	"strings"
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

func (d DocumentBody) ChannelNames() []string {
	channelNames := []string{}
	channelNamesIface, ok := d["channels"]
	if !ok {
		return channelNames
	}
	channelNamesIfaceSlice := channelNamesIface.([]interface{})
	for _, channelNameIface := range channelNamesIfaceSlice {
		channelNameStr := channelNameIface.(string)
		channelNames = append(channelNames, channelNameStr)
	}
	return channelNames
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
		// Ignore document ids beginning with underscore
		if !strings.HasPrefix(change.Id, "_") {
			revsDiffMap[change.Id] = change.getRevs()
		}
	}
	return revsDiffMap
}

type DocumentRevisionPair struct {
	Id       string `json:"id"`
	Revision string `json:"rev"`
	Error    string `json:"error,omitempty"`
	Reason   string `json:"reason,omitempty"`
}

func (drp DocumentRevisionPair) GetGeneration() (int, error) {
	revParts := strings.Split(drp.Revision, "-")
	generation, err := strconv.Atoi(revParts[0])
	if err != nil {
		return 0, err
	}
	return generation, nil
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

func generateBulkDocsRequest(r Replication, documents []Document) BulkDocsRequest {
	documentBodies := []DocumentBody{}
	// we can only send the documents _without_ attachments in _bulk_docs
	for _, document := range documents {
		if len(document.Attachments) == 0 {
			documentBodies = append(documentBodies, document.Body)
		} else {
			for _, attachment := range document.Attachments {
				r.LogTo("Replicate", "attachment.Headers :%v", attachment.Headers)
			}
		}
	}
	return BulkDocsRequest{
		NewEdits:       false,
		DocumentBodies: documentBodies,
	}
}

func bulkDocsHaveErrors(docRevPairs []DocumentRevisionPair) bool {
	for _, docRevPair := range docRevPairs {
		if docRevPair.Error != "" {
			return true
		}
	}
	return false
}
