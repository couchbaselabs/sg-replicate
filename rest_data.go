package synctube

import (
	"fmt"
	"github.com/couchbaselabs/logg"
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

func (checkpoint Checkpoint) LastCheckpointNumeric() (i int, err error) {
	i, err = strconv.Atoi(checkpoint.LastSequence)
	if err != nil {
		logg.LogError(err)
	}
	return
}

type ChangedRev struct {
	Revision string `json:"rev"`
}

type Change struct {
	Sequence    int          `json:"seq"`
	Id          string       `json:"id"`
	ChangedRevs []ChangedRev `json:"changes"`
}

type Changes struct {
	Results      []Change `json:"results"`
	LastSequence int      `json:"last_seq"`
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

func generateBulkDocsRequest(documentBodies []DocumentBody) BulkDocsRequest {
	return BulkDocsRequest{
		NewEdits:       false,
		DocumentBodies: documentBodies,
	}
}

// Given "0-1", return "0-2"
func incrementLocalRevision(previousRevision string) string {
	if len(previousRevision) == 0 {
		return "0-1"
	}
	components := strings.Split(previousRevision, "-")
	secondComponent := components[1]
	secondComponentInt, err := strconv.Atoi(secondComponent)
	if err != nil {
		logg.LogPanic("Error incrementing: %v error: %v", previousRevision, err)
	}

	// introduce error so that unit tests can be enhanced to catch it
	// return fmt.Sprintf("%v-%v", components[0], secondComponentInt+1)
	return fmt.Sprintf("%v-%v", components[0], secondComponentInt)
}
