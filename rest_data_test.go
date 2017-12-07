package sgreplicate

import (
	"encoding/json"
	"testing"

	"github.com/couchbase/clog"
	"github.com/couchbaselabs/go.assert"
)

func init() {
	clog.EnableKey("TEST")
	clog.EnableKey("Replicate")
}

func TestGenerateRevsMap(t *testing.T) {
	lastSequence := "3"
	fakeChangesFeed := fakeChangesFeed(lastSequence)
	changes := Changes{}
	err := json.Unmarshal([]byte(fakeChangesFeed), &changes)
	assert.True(t, err == nil)
	revsDiffMap := generateRevsDiffMap(changes)
	assert.Equals(t, len(revsDiffMap), 2)
	assert.Equals(t, len(revsDiffMap["doc2"]), 1)
	assert.Equals(t, len(revsDiffMap["doc3"]), 1)
	clog.To("TEST", "revsDiffMap: %v", revsDiffMap)
}

func TestFilterRemovedDocs(t *testing.T) {

	doc1 := Document{
		Body: map[string]interface{}{},
	}
	doc1.Body["_id"] = "1"
	doc1.Body["_removed"] = true
	doc2 := Document{
		Body: map[string]interface{}{},
	}
	doc2.Body["_id"] = "2"
	doc2.Body["_removed"] = false
	doc3 := Document{
		Body: map[string]interface{}{},
	}
	doc3.Body["_id"] = "3"
	docs := []Document{
		doc1,
		doc2,
		doc3,
	}
	filtered := filterRemovedDocs(docs)
	assert.Equals(t, len(filtered), 2)
	foundDoc1 := false
	for _, doc := range filtered {
		if doc.Body["_id"] == "1" {
			foundDoc1 = true
		}
	}
	assert.True(t, !foundDoc1)

}

func TestBulkDocsHaveRecoverableErrors(t *testing.T) {

	docRevPairsWithRecoverableErrors := []DocumentRevisionPair{
		{
			Id:       "foo",
			Revision: "1-324234",
			Error:    "temporary failure",
			Status:   503,
		},
		{
			Id:       "foo2",
			Revision: "3-3234",
		},
	}

	assert.True(t, bulkDocsHaveRecoverableErrors(docRevPairsWithRecoverableErrors))

	docRevPairsWithNonRecoverableErrors := []DocumentRevisionPair{
		{
			Id:       "foo",
			Revision: "1-324234",
			Error:    "internal error",
			Status:   500,
		},
		{
			Id:       "foo2",
			Revision: "3-3234",
		},
	}

	assert.False(t, bulkDocsHaveRecoverableErrors(docRevPairsWithNonRecoverableErrors))

	docRevPairsWithNoErrors := []DocumentRevisionPair{
		{
			Id:       "foo2",
			Revision: "3-3234",
		},
	}

	assert.False(t, bulkDocsHaveRecoverableErrors(docRevPairsWithNoErrors))

}
