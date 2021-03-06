package sgreplicate

import (
	"fmt"
	"strconv"
	"strings"

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
	Status   int    `json:"status,omitempty"`
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
				r.log(clog.LevelDebug, "attachment.Headers :%v", attachment.Headers)
			}
		}
	}
	return BulkDocsRequest{
		NewEdits:       false,
		DocumentBodies: documentBodies,
	}
}

// Find out if any of the bulk docs have recoverable errors that should be retried.
// Loosely based on couchbase lite ios code:
// https://github.com/couchbase/couchbase-lite-ios/blob/9a8903b9e851d5584abeec069ead4d2637b3bb91/Source/CBLMisc.m#L381
// But it does not consider 500 and 502 errors as transient / retryable.
func bulkDocsHaveRecoverableErrors(docRevPairs []DocumentRevisionPair) bool {
	for _, docRevPair := range docRevPairs {
		if docRevPair.Status == 503 || docRevPair.Status == 504 {
			return true
		}
	}
	return false
}

// A function that takes a Document as a parameter and returns a boolean value.
// If keep is true, it will retain the doc.  Otherwise, discard the doc
type KeepDocFilter func(doc Document) (keep bool)

// Filter out any documents that has the "_removed":true property
func filterRemovedDocs(docs []Document) []Document {
	// This function filters out all the docs that have the _removed:true property
	filterOutRemovedDocs := func(doc Document) (keep bool) {
		removedProperty, ok := doc.Body["_removed"]
		if !ok {
			// doesn't have _removed property, keep it
			return true
		}
		removedPropertyBool, ok := removedProperty.(bool)
		if !ok {
			// this should never happen, the doc has a _removed property, but
			// it's not a boolean type
			panic(fmt.Sprintf("Doc has _removed, but not boolean type: %+v", doc))
		}
		return removedPropertyBool == false
	}
	return filterDocs(docs, filterOutRemovedDocs)

}

func filterDocs(docs []Document, keepDocFilter KeepDocFilter) []Document {
	filtered := []Document{}
	for _, doc := range docs {
		if !keepDocFilter(doc) {
			continue
		}
		filtered = append(filtered, doc)
	}
	return filtered
}
