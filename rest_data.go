package synctube

import (
	"fmt"
	"github.com/couchbaselabs/logg"
	"strconv"
)

type Checkpoint struct {
	LastSequence string `json:"lastSequence"`
	Id           string `json:"_id"`
}

type PushCheckpointResponse struct {
	Id string `json:"id"`
	Ok bool   `json:"ok"`
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

func generatePushCheckpointRequest(changes Changes) PushCheckpointRequest {
	return PushCheckpointRequest{
		LastSequence: fmt.Sprintf("%v", changes.LastSequence),
	}
}
