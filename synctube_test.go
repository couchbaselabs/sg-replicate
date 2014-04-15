package synctube

import (
	"encoding/json"
	"fmt"
	"github.com/couchbaselabs/go.assert"
	"github.com/couchbaselabs/logg"
	"github.com/tleyden/fakehttp"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

func init() {
	logg.LogKeys["TEST"] = true
	logg.LogKeys["SYNCTUBE"] = true
}

func replicationParams(sourceServerUrl *url.URL, targetServerUrl *url.URL) ReplicationParameters {
	params := ReplicationParameters{}
	params.Source = sourceServerUrl
	params.SourceDb = "db"
	params.Target = targetServerUrl
	params.TargetDb = "db"
	params.ChangesFeedLimit = 2
	return params
}

func fakeServers(sourcePort, targetPort int) (source *fakehttp.HTTPServer, target *fakehttp.HTTPServer) {

	source = fakehttp.NewHTTPServerWithPort(sourcePort)
	source.Start()

	target = fakehttp.NewHTTPServerWithPort(targetPort)
	target.Start()

	return
}

func TestOneShotReplicationCancelImmediately(t *testing.T) {

	sourceServer, targetServer := fakeServers(5977, 5976)

	params := replicationParams(sourceServer.URL, targetServer.URL)

	notificationChan := make(chan ReplicationNotification)

	replication := NewReplication(params, notificationChan)

	lastSequence := 1
	jsonResponse := fakeCheckpointResponse(replication.targetCheckpointAddress(), lastSequence)
	targetServer.Response(200, jsonHeaders(), jsonResponse)

	// fake response to changes feed
	lastSequenceChanges := 3
	sourceServer.Response(200, jsonHeaders(), fakeChangesFeed(lastSequenceChanges))

	err := replication.Start()
	assert.True(t, err == nil)

	waitForNotification(replication, REPLICATION_FETCHED_CHECKPOINT)

	// get the fetched checkpoint and make sure it matches
	lastSequenceFetched, err := replication.FetchedTargetCheckpoint.LastCheckpointNumeric()
	assert.True(t, err == nil)
	assert.Equals(t, lastSequence, lastSequenceFetched)

	err = replication.Stop()
	assert.True(t, err == nil)

	waitForNotification(replication, REPLICATION_CANCELLED)

	assertNotificationChannelClosed(notificationChan)

	// if we try to call Start() or Stop(), it should give an error
	err = replication.Start()
	assert.True(t, err != nil)
	err = replication.Stop()
	assert.True(t, err != nil)

}

func TestOneShotReplicationGetCheckpointFailed(t *testing.T) {

	// the simulated sync gateway source only returns a _local doc
	// with a checkpoint.  after that, the request to the _changes
	// feed returns invalid responses and the replication
	// stops and goes into an error state

	sourceServer, targetServer := fakeServers(5981, 5980)

	// setup fake response on target server
	targetServer.Response(500, jsonHeaders(), bogusJson())

	params := replicationParams(sourceServer.URL, targetServer.URL)

	notificationChan := make(chan ReplicationNotification)

	replication := NewReplication(params, notificationChan)
	replication.Start()

	// expect to get a replication active event
	replicationNotification := <-notificationChan
	assert.Equals(t, replicationNotification.Status, REPLICATION_ACTIVE)

	// since the attempt to get the checkpoint from the target
	// server will fail, expect to get a replication stopped event
	replicationNotification = <-notificationChan

	assert.Equals(t, replicationNotification.Status, REPLICATION_ABORTED)
	assert.True(t, replicationNotification.Error != nil)

	assertNotificationChannelClosed(notificationChan)

}

func TestOneShotReplicationGetCheckpointHappypath(t *testing.T) {

	sourceServer, targetServer := fakeServers(5989, 5988)

	params := replicationParams(sourceServer.URL, targetServer.URL)

	notificationChan := make(chan ReplicationNotification)

	replication := NewReplication(params, notificationChan)

	lastSequence := 1
	jsonResponse := fakeCheckpointResponse(replication.targetCheckpointAddress(), 1)
	targetServer.Response(200, jsonHeaders(), jsonResponse)

	replication.Start()

	waitForNotification(replication, REPLICATION_FETCHED_CHECKPOINT)

	// get the fetched checkpoint and make sure it matches
	lastSequenceFetched, err := replication.FetchedTargetCheckpoint.LastCheckpointNumeric()
	assert.True(t, err == nil)
	assert.Equals(t, lastSequence, lastSequenceFetched)

	replication.Stop()
	waitForNotification(replication, REPLICATION_CANCELLED)

	assertNotificationChannelClosed(notificationChan)

}

func TestOneShotReplicationGetChangesFeedFailed(t *testing.T) {

	sourceServer, targetServer := fakeServers(5987, 5986)

	params := replicationParams(sourceServer.URL, targetServer.URL)

	notificationChan := make(chan ReplicationNotification)

	replication := NewReplication(params, notificationChan)

	targetServer.Response(200, jsonHeaders(), fakeCheckpointResponse(replication.targetCheckpointAddress(), 1))

	// fake response to changes feed
	sourceServer.Response(500, jsonHeaders(), "{\"error\": true}")

	replication.Start()

	// expect to get a replication active event
	replicationNotification := <-notificationChan
	assert.Equals(t, replicationNotification.Status, REPLICATION_ACTIVE)

	waitForNotification(replication, REPLICATION_FETCHED_CHECKPOINT)

	waitForNotification(replication, REPLICATION_ABORTED)

	assertNotificationChannelClosed(notificationChan)

}

func TestOneShotReplicationGetChangesFeedHappyPath(t *testing.T) {

	sourceServer, targetServer := fakeServers(5991, 5990)

	params := replicationParams(sourceServer.URL, targetServer.URL)

	notificationChan := make(chan ReplicationNotification)

	replication := NewReplication(params, notificationChan)

	targetServer.Response(200, jsonHeaders(), fakeCheckpointResponse(replication.targetCheckpointAddress(), 1))

	// response to changes feed
	lastSequenceChanges := 3
	sourceServer.Response(200, jsonHeaders(), fakeChangesFeed(lastSequenceChanges))

	replication.Start()

	// expect to get a replication active event
	replicationNotification := <-notificationChan
	assert.Equals(t, replicationNotification.Status, REPLICATION_ACTIVE)

	waitForNotification(replication, REPLICATION_FETCHED_CHECKPOINT)

	waitForNotification(replication, REPLICATION_FETCHED_CHANGES_FEED)

	changes := replication.Changes
	assert.Equals(t, len(changes.Results), 2)

	replication.Stop()

	waitForNotification(replication, REPLICATION_CANCELLED)

	assertNotificationChannelClosed(notificationChan)

}

func TestOneShotReplicationGetChangesFeedEmpty(t *testing.T) {

	sourceServer, targetServer := fakeServers(5993, 5992)

	params := replicationParams(sourceServer.URL, targetServer.URL)

	notificationChan := make(chan ReplicationNotification)

	replication := NewReplication(params, notificationChan)

	targetServer.Response(200, jsonHeaders(), fakeCheckpointResponse(replication.targetCheckpointAddress(), 1))

	// response to changes feed
	sourceServer.Response(200, jsonHeaders(), fakeEmptyChangesFeed())

	replication.Start()

	// expect to get a replication active event
	replicationNotification := <-notificationChan
	assert.Equals(t, replicationNotification.Status, REPLICATION_ACTIVE)

	waitForNotification(replication, REPLICATION_FETCHED_CHECKPOINT)

	waitForNotification(replication, REPLICATION_FETCHED_CHANGES_FEED)

	changes := replication.Changes
	assert.Equals(t, len(changes.Results), 0)

	waitForNotification(replication, REPLICATION_STOPPED)

	assertNotificationChannelClosed(notificationChan)

}

func TestOneShotReplicationGetRevsDiffFailed(t *testing.T) {

	sourceServer, targetServer := fakeServers(5995, 5994)

	params := replicationParams(sourceServer.URL, targetServer.URL)

	notificationChan := make(chan ReplicationNotification)

	// create a new replication and start it
	replication := NewReplication(params, notificationChan)

	targetServer.Response(200, jsonHeaders(), fakeCheckpointResponse(replication.targetCheckpointAddress(), 1))

	// fake response to changes feed
	lastSequence := 3
	sourceServer.Response(200, jsonHeaders(), fakeChangesFeed(lastSequence))

	// fake response to revs_diff
	targetServer.Response(500, jsonHeaders(), "{\"error\": true}")

	replication.Start()

	// expect to get a replication active event
	replicationNotification := <-notificationChan
	assert.Equals(t, replicationNotification.Status, REPLICATION_ACTIVE)

	waitForNotification(replication, REPLICATION_FETCHED_CHECKPOINT)

	waitForNotification(replication, REPLICATION_ABORTED)
	assertNotificationChannelClosed(notificationChan)

}

func TestOneShotReplicationGetRevsDiffHappyPath(t *testing.T) {

	sourceServer, targetServer := fakeServers(5997, 5996)

	lastSequence := 1

	params := replicationParams(sourceServer.URL, targetServer.URL)

	notificationChan := make(chan ReplicationNotification)

	// create a new replication and start it
	replication := NewReplication(params, notificationChan)

	targetServer.Response(200, jsonHeaders(), fakeCheckpointResponse(replication.targetCheckpointAddress(), lastSequence))

	// fake response to changes feed
	lastSequenceChanges := 3
	sourceServer.Response(200, jsonHeaders(), fakeChangesFeed(lastSequenceChanges))

	// fake response to revs_diff
	targetServer.Response(200, jsonHeaders(), fakeRevsDiff())

	replication.Start()

	// expect to get a replication active event
	replicationNotification := <-notificationChan
	assert.Equals(t, replicationNotification.Status, REPLICATION_ACTIVE)

	waitForNotification(replication, REPLICATION_FETCHED_CHECKPOINT)

	waitForNotification(replication, REPLICATION_FETCHED_REVS_DIFF)

	replication.Stop()

	waitForNotification(replication, REPLICATION_CANCELLED)

	assertNotificationChannelClosed(notificationChan)

	for _, savedReq := range sourceServer.SavedRequests {
		path := savedReq.Request.URL.Path
		if strings.Contains(path, "/db/_changes") {

			params, err := url.ParseQuery(savedReq.Request.URL.RawQuery)
			logg.LogTo("TEST", "params: %v", params)
			assert.True(t, err == nil)
			assert.Equals(t, params["since"][0], strconv.Itoa(lastSequence))
		}

	}

}

func TestOneShotReplicationBulkGetFailed(t *testing.T) {
	sourceServer, targetServer := fakeServers(5999, 5998)

	params := replicationParams(sourceServer.URL, targetServer.URL)

	notificationChan := make(chan ReplicationNotification)

	// create a new replication and start it
	replication := NewReplication(params, notificationChan)

	targetServer.Response(200, jsonHeaders(), fakeCheckpointResponse(replication.targetCheckpointAddress(), 1))

	// fake response to changes feed
	lastSequence := 3
	sourceServer.Response(200, jsonHeaders(), fakeChangesFeed(lastSequence))

	// fake response to bulk get
	sourceServer.Response(500, jsonHeaders(), bogusJson())

	// fake response to revs_diff
	targetServer.Response(200, jsonHeaders(), fakeRevsDiff())

	replication.Start()

	// expect to get a replication active event
	replicationNotification := <-notificationChan
	assert.Equals(t, replicationNotification.Status, REPLICATION_ACTIVE)

	waitForNotification(replication, REPLICATION_FETCHED_CHECKPOINT)

	waitForNotification(replication, REPLICATION_FETCHED_REVS_DIFF)

	waitForNotification(replication, REPLICATION_ABORTED)

	assertNotificationChannelClosed(notificationChan)

}

func TestOneShotReplicationBulkGetHappyPath(t *testing.T) {

	sourceServer, targetServer := fakeServers(6001, 6000)

	params := replicationParams(sourceServer.URL, targetServer.URL)

	notificationChan := make(chan ReplicationNotification)

	// create a new replication and start it
	replication := NewReplication(params, notificationChan)

	targetServer.Response(200, jsonHeaders(), fakeCheckpointResponse(replication.targetCheckpointAddress(), 1))

	// fake response to changes feed
	lastSequence := 3
	sourceServer.Response(200, jsonHeaders(), fakeChangesFeed(lastSequence))

	// fake response to bulk get
	boundary := fakeBoundary()
	sourceServer.Response(200, jsonHeadersMultipart(boundary), fakeBulkGetResponse(boundary))

	// fake response to revs_diff
	targetServer.Response(200, jsonHeaders(), fakeRevsDiff())

	replication.Start()

	// expect to get a replication active event
	replicationNotification := <-notificationChan
	assert.Equals(t, replicationNotification.Status, REPLICATION_ACTIVE)

	waitForNotification(replication, REPLICATION_FETCHED_CHECKPOINT)

	waitForNotification(replication, REPLICATION_FETCHED_REVS_DIFF)

	waitForNotification(replication, REPLICATION_FETCHED_BULK_GET)

	assert.Equals(t, len(replication.Documents), 1)
	documentBody := replication.Documents[0].Body
	assert.Equals(t, documentBody["_id"], "doc2")

	replication.Stop()

	waitForNotification(replication, REPLICATION_CANCELLED)

	assertNotificationChannelClosed(notificationChan)

}

func TestOneShotReplicationBulkDocsFailed(t *testing.T) {

	sourceServer, targetServer := fakeServers(6003, 6002)

	params := replicationParams(sourceServer.URL, targetServer.URL)

	notificationChan := make(chan ReplicationNotification)

	// create a new replication and start it
	replication := NewReplication(params, notificationChan)

	targetServer.Response(200, jsonHeaders(), fakeCheckpointResponse(replication.targetCheckpointAddress(), 1))

	// fake response to changes feed
	lastSequence := 3
	sourceServer.Response(200, jsonHeaders(), fakeChangesFeed(lastSequence))

	// fake response to bulk get
	boundary := fakeBoundary()
	sourceServer.Response(200, jsonHeadersMultipart(boundary), fakeBulkGetResponse(boundary))

	// fake response to revs_diff
	targetServer.Response(200, jsonHeaders(), fakeRevsDiff())

	// failed response to bulk docs
	targetServer.Response(500, jsonHeaders(), bogusJson())

	replication.Start()

	// expect to get a replication active event
	replicationNotification := <-notificationChan
	assert.Equals(t, replicationNotification.Status, REPLICATION_ACTIVE)

	waitForNotification(replication, REPLICATION_FETCHED_CHECKPOINT)

	waitForNotification(replication, REPLICATION_FETCHED_REVS_DIFF)

	waitForNotification(replication, REPLICATION_FETCHED_BULK_GET)

	waitForNotification(replication, REPLICATION_ABORTED)

	assertNotificationChannelClosed(notificationChan)

}

func TestOneShotReplicationBulkDocsHappyPath(t *testing.T) {

	sourceServer, targetServer := fakeServers(6005, 6004)

	params := replicationParams(sourceServer.URL, targetServer.URL)

	notificationChan := make(chan ReplicationNotification)

	// create a new replication and start it
	replication := NewReplication(params, notificationChan)

	targetServer.Response(200, jsonHeaders(), fakeCheckpointResponse(replication.targetCheckpointAddress(), 1))

	// fake response to changes feed
	lastSequence := 3
	sourceServer.Response(200, jsonHeaders(), fakeChangesFeed(lastSequence))

	// fake response to bulk get
	boundary := fakeBoundary()
	sourceServer.Response(200, jsonHeadersMultipart(boundary), fakeBulkGetResponse(boundary))

	// fake response to revs_diff
	targetServer.Response(200, jsonHeaders(), fakeRevsDiff())

	// fake response to bulk docs
	targetServer.Response(200, jsonHeaders(), fakeBulkDocsResponse())

	replication.Start()

	// expect to get a replication active event
	replicationNotification := <-notificationChan
	assert.Equals(t, replicationNotification.Status, REPLICATION_ACTIVE)

	waitForNotification(replication, REPLICATION_FETCHED_CHECKPOINT)

	assert.Equals(t, replication.FetchedTargetCheckpoint.Revision, "0-1")

	waitForNotification(replication, REPLICATION_FETCHED_REVS_DIFF)

	waitForNotification(replication, REPLICATION_FETCHED_BULK_GET)

	waitForNotification(replication, REPLICATION_PUSHED_BULK_DOCS)

	replication.Stop()

	waitForNotification(replication, REPLICATION_CANCELLED)

	assertNotificationChannelClosed(notificationChan)

}

func TestOneShotReplicationPushCheckpointFailed(t *testing.T) {

	sourceServer, targetServer := fakeServers(6007, 6006)

	params := replicationParams(sourceServer.URL, targetServer.URL)

	notificationChan := make(chan ReplicationNotification)

	// create a new replication and start it
	replication := NewReplication(params, notificationChan)

	targetServer.Response(200, jsonHeaders(), fakeCheckpointResponse(replication.targetCheckpointAddress(), 1))

	// fake response to changes feed
	lastSequence := 3
	sourceServer.Response(200, jsonHeaders(), fakeChangesFeed(lastSequence))

	// fake response to bulk get
	boundary := fakeBoundary()
	sourceServer.Response(200, jsonHeadersMultipart(boundary), fakeBulkGetResponse(boundary))

	// fake response to revs_diff
	targetServer.Response(200, jsonHeaders(), fakeRevsDiff())

	// fake response to bulk docs
	targetServer.Response(200, jsonHeaders(), fakeBulkDocsResponse())

	// failed response to push checkpoint
	targetServer.Response(500, jsonHeaders(), bogusJson())

	replication.Start()

	// expect to get a replication active event
	replicationNotification := <-notificationChan
	assert.Equals(t, replicationNotification.Status, REPLICATION_ACTIVE)

	waitForNotification(replication, REPLICATION_FETCHED_CHECKPOINT)

	waitForNotification(replication, REPLICATION_FETCHED_REVS_DIFF)

	waitForNotification(replication, REPLICATION_FETCHED_BULK_GET)

	waitForNotification(replication, REPLICATION_PUSHED_BULK_DOCS)

	waitForNotification(replication, REPLICATION_ABORTED)

	assertNotificationChannelClosed(notificationChan)

}

func TestOneShotReplicationPushCheckpointSucceeded(t *testing.T) {

	sourceServer, targetServer := fakeServers(6009, 6008)

	params := replicationParams(sourceServer.URL, targetServer.URL)

	notificationChan := make(chan ReplicationNotification)

	// create a new replication and start it
	replication := NewReplication(params, notificationChan)

	// fake response to get checkpoint
	targetServer.Response(404, jsonHeaders(), bogusJson())

	// fake response to changes feed
	lastSequence := 3
	sourceServer.Response(200, jsonHeaders(), fakeChangesFeed(lastSequence))

	// fake response to bulk get
	boundary := fakeBoundary()
	sourceServer.Response(200, jsonHeadersMultipart(boundary), fakeBulkGetResponse(boundary))

	// fake response to revs_diff
	targetServer.Response(200, jsonHeaders(), fakeRevsDiff())

	// fake response to bulk docs
	targetServer.Response(200, jsonHeaders(), fakeBulkDocsResponse())

	// fake response to push checkpoint
	targetServer.Response(200, jsonHeaders(), fakePushCheckpointResponse(replication.targetCheckpointAddress()))

	// TODO: the fake server should return the last pushed checkpoint in this case

	// fake second call to get checkpoint
	targetServer.Response(200, jsonHeaders(), fakeCheckpointResponse(replication.targetCheckpointAddress(), 3))

	// fake second response to changes feed
	sourceServer.Response(200, jsonHeaders(), `{"results":[],"last_seq":3}`)

	replication.Start()

	// expect to get a replication active event
	replicationNotification := <-notificationChan
	assert.Equals(t, replicationNotification.Status, REPLICATION_ACTIVE)

	waitForNotification(replication, REPLICATION_FETCHED_CHECKPOINT)

	waitForNotification(replication, REPLICATION_FETCHED_REVS_DIFF)

	waitForNotification(replication, REPLICATION_FETCHED_BULK_GET)

	waitForNotification(replication, REPLICATION_PUSHED_BULK_DOCS)

	waitForNotification(replication, REPLICATION_PUSHED_CHECKPOINT)

	remoteCheckpoint := waitForReplicationStoppedNotification(replication)
	assert.Equals(t, remoteCheckpoint, 3)

	assertNotificationChannelClosed(notificationChan)

	for _, savedReq := range targetServer.SavedRequests {
		path := savedReq.Request.URL.Path
		if strings.Contains(path, "/db/_local") {
			if savedReq.Request.Method == "PUT" {
				// since the checkpoint response above was a 404,
				// when we push a checkpoint there should be no
				// revision field.
				pushCheckpointRequest := PushCheckpointRequest{}
				err := json.Unmarshal(savedReq.Data, &pushCheckpointRequest)
				assert.True(t, err == nil)
				assert.True(t, len(pushCheckpointRequest.Revision) == 0)

			}
		}

	}

}

func TestOneShotReplicationHappyPath(t *testing.T) {

	sourceServer, targetServer := fakeServers(6011, 6010)

	params := replicationParams(sourceServer.URL, targetServer.URL)

	notificationChan := make(chan ReplicationNotification)

	// create a new replication and start it
	replication := NewReplication(params, notificationChan)

	// fake response to get checkpoint
	targetServer.Response(404, jsonHeaders(), bogusJson())

	// fake response to changes feed
	lastSequence := 3
	sourceServer.Response(200, jsonHeaders(), fakeChangesFeed(lastSequence))

	// fake response to bulk get
	boundary1 := fakeBoundary()
	boundary2 := fakeBoundary2()
	sourceServer.Response(200, jsonHeadersMultipart(boundary1), fakeBulkGetResponseWithTextAttachment(boundary1, boundary2))

	// fake response to revs_diff
	targetServer.Response(200, jsonHeaders(), fakeRevsDiff())

	// fake response to bulk docs
	targetServer.Response(200, jsonHeaders(), fakeBulkDocsResponse())

	// fake response to put doc w/ attachment
	targetServer.Response(200, jsonHeaders(), fakePutDocAttachmentResponse())

	// fake response to push checkpoint
	targetServer.Response(200, jsonHeaders(), fakePushCheckpointResponse(replication.targetCheckpointAddress()))

	// TODO: the fake server should return the last pushed checkpoint in this case
	// rather than hardcoding to 3

	// fake second call to get checkpoint
	targetServer.Response(200, jsonHeaders(), fakeCheckpointResponse(replication.targetCheckpointAddress(), 3))

	// fake second response to changes feed
	sourceServer.Response(200, jsonHeaders(), fakeChangesFeed2())

	// fake second reponse to bulk get
	sourceServer.Response(200, jsonHeadersMultipart(boundary1), fakeBulkGetResponse2(boundary1))

	// fake second response to revs_diff
	targetServer.Response(200, jsonHeaders(), fakeRevsDiff2())

	// fake second response to bulk docs
	targetServer.Response(200, jsonHeaders(), fakeBulkDocsResponse2())

	// fake second response to push checkpoint
	targetServer.Response(200, jsonHeaders(), fakePushCheckpointResponse(replication.targetCheckpointAddress()))

	// fake third response to get checkpoint
	lastSequence = 4
	targetServer.Response(200, jsonHeaders(), fakeCheckpointResponse(replication.targetCheckpointAddress(), lastSequence))

	// fake third response to changes feed
	sourceServer.Response(200, jsonHeaders(), fakeChangesFeedEmpty(lastSequence))

	replication.Start()

	// expect to get a replication active event
	replicationNotification := <-notificationChan
	assert.Equals(t, replicationNotification.Status, REPLICATION_ACTIVE)

	waitForNotification(replication, REPLICATION_FETCHED_CHECKPOINT)

	waitForNotification(replication, REPLICATION_FETCHED_REVS_DIFF)

	waitForNotification(replication, REPLICATION_FETCHED_BULK_GET)

	waitForNotification(replication, REPLICATION_PUSHED_BULK_DOCS)

	waitForNotification(replication, REPLICATION_PUSHED_ATTACHMENT_DOCS)

	waitForNotification(replication, REPLICATION_PUSHED_CHECKPOINT)

	waitForNotification(replication, REPLICATION_STOPPED)

	assertNotificationChannelClosed(notificationChan)

	putCheckpointRequestIndex := 0
	for _, savedReq := range targetServer.SavedRequests {

		path := savedReq.Request.URL.Path
		if strings.Contains(path, "/db/_local") {
			if savedReq.Request.Method == "PUT" {

				pushCheckpointRequest := PushCheckpointRequest{}
				err := json.Unmarshal(savedReq.Data, &pushCheckpointRequest)
				assert.True(t, err == nil)

				if putCheckpointRequestIndex == 0 {
					// since the checkpoint response above was a 404,
					// the first time we push a checkpoint there should be no
					// revision field.
					assert.True(t, len(pushCheckpointRequest.Revision) == 0)

				} else if putCheckpointRequestIndex == 1 {
					// since second fake checkpoint is "0-1", expect
					// to push with "0-1" as rev
					assert.True(t, pushCheckpointRequest.Revision == "0-1")

				}
				putCheckpointRequestIndex += 1

			}
		}

	}

	getChangesRequestIndex := 0
	for _, savedReq := range sourceServer.SavedRequests {
		path := savedReq.Request.URL.Path
		if strings.Contains(path, "/db/_changes") {
			params, err := url.ParseQuery(savedReq.Request.URL.RawQuery)
			assert.True(t, err == nil)
			if getChangesRequestIndex > 0 {
				assert.True(t, len(params["since"][0]) > 0)
			}
			getChangesRequestIndex += 1
		}
	}

	// we should expect to see _bulk_doc requests for these docs
	assertBulkDocsDocIds(t, targetServer.SavedRequests, []string{"doc1", "doc4"})

	// and individual PUT requests for these docs that contain attachments
	assertPutAttachDocsDocIds(t, targetServer.SavedRequests, []string{"doc2"})

}

func assertBulkDocsDocIds(t *testing.T, reqs []fakehttp.SavedRequest, docIds []string) {
	assertDocIds(t, "/db/_bulk_docs", reqs, docIds)
}

func assertPutAttachDocsDocIds(t *testing.T, reqs []fakehttp.SavedRequest, docIds []string) {

	for _, docId := range docIds {

		urlPath := fmt.Sprintf("/db/%s", docId)
		assertDocIds(t, urlPath, reqs, []string{docId})

		// make sure all requests to PUT docs w/ attachments have
		// content-type of multipart/related
		for _, savedReq := range reqs {
			path := savedReq.Request.URL.Path
			if strings.Contains(path, urlPath) {
				contentType := savedReq.Request.Header.Get("Content-Type")
				assert.True(t, strings.Contains(contentType, "multipart/related"))
			}
		}

	}

}

func assertDocIds(t *testing.T, urlPath string, reqs []fakehttp.SavedRequest, docIds []string) {
	foundDocIds := []string{}
	for _, savedReq := range reqs {
		path := savedReq.Request.URL.Path
		dataStr := string(savedReq.Data)
		if strings.Contains(path, urlPath) {
			for _, seekingDocId := range docIds {
				// rather than parsing the request (which might be
				// multipart), just do a string search for that docid
				if strings.Contains(dataStr, seekingDocId) {
					foundDocIds = append(foundDocIds, seekingDocId)
				}
			}

		}
	}

	assert.Equals(t, len(docIds), len(foundDocIds))
	for i, seekingDocId := range docIds {
		assert.Equals(t, seekingDocId, foundDocIds[i])
	}

}

// Test against mock source server that has no changes, nothing to sync.
func TestOneShotReplicationNoOp(t *testing.T) {

	sourceServer, targetServer := fakeServers(6013, 6012)

	params := replicationParams(sourceServer.URL, targetServer.URL)

	notificationChan := make(chan ReplicationNotification)

	// create a new replication and start it
	replication := NewReplication(params, notificationChan)

	// fake response to get checkpoint
	targetServer.Response(404, jsonHeaders(), bogusJson())

	// fake response to changes feed
	lastSequence := 3
	sourceServer.Response(200, jsonHeaders(), fakeChangesFeedEmpty(lastSequence))

	replication.Start()

	// expect to get a replication active event
	replicationNotification := <-notificationChan
	assert.Equals(t, replicationNotification.Status, REPLICATION_ACTIVE)

	waitForNotification(replication, REPLICATION_FETCHED_CHECKPOINT)

	waitForNotification(replication, REPLICATION_FETCHED_CHANGES_FEED)

	remoteCheckpoint := waitForReplicationStoppedNotification(replication)
	assert.Equals(t, remoteCheckpoint, lastSequence)

}

// Integration test.  Not fully automated; should be commented out.
// After adding attachment support its failing
func DISTestOneShotIntegrationReplication(t *testing.T) {

	sourceServerUrlStr := "http://localhost:4984"
	targetServerUrlStr := "http://localhost:4986"

	sourceServerUrl, err := url.Parse(sourceServerUrlStr)
	if err != nil {
		logg.LogPanic("could not parse url: %v", sourceServerUrlStr)
	}

	targetServerUrl, err := url.Parse(targetServerUrlStr)
	if err != nil {
		logg.LogPanic("could not parse url: %v", targetServerUrlStr)
	}
	params := replicationParams(sourceServerUrl, targetServerUrl)

	notificationChan := make(chan ReplicationNotification)

	replication := NewReplication(params, notificationChan)
	replication.Start()

	for {
		select {
		case replicationNotification := <-notificationChan:
			logg.LogTo("TEST", "Got notification %v", replicationNotification)
			if replicationNotification.Status == REPLICATION_ABORTED {
				logg.LogPanic("Got REPLICATION_ABORTED")
				return
			}
			if replicationNotification.Status == REPLICATION_STOPPED {
				logg.LogTo("TEST", "Replication stopped")
				return
			}
		case <-time.After(time.Second * 10):
			logg.LogPanic("Timeout waiting for a notification")
		}
	}

}

func fakePushCheckpointResponse(checkpointAddress string) string {
	return fmt.Sprintf(`{"id":"_local/%s","ok":true,"rev":"0-1"}`, checkpointAddress)
}

func fakeCheckpointResponse(checkpointAddress string, lastSequence int) string {
	return fmt.Sprintf(`{"_id":"_local/%s","ok":true,"_rev":"0-1","lastSequence":"%v"}`, checkpointAddress, lastSequence)

}

func jsonHeaders() map[string]string {
	return map[string]string{"Content-Type": "application/json"}
}

func jsonHeadersMultipart(boundary string) map[string]string {
	contentType := fmt.Sprintf(`multipart/mixed; boundary="%s"`, boundary)
	return map[string]string{"Content-Type": contentType}
}

func assertNotificationChannelClosed(notificationChan chan ReplicationNotification) {
	_, ok := <-notificationChan
	if ok {
		logg.LogPanic("notificationChan was not closed")
	}
}

func fakeChangesFeed(lastSequence int) string {
	return fmt.Sprintf(`{"results":[{"seq":2,"id":"doc2","changes":[{"rev":"1-5e38"}]},{"seq":3,"id":"doc3","changes":[{"rev":"1-563b"}]}],"last_seq":%v}`, lastSequence)
}

func fakeChangesFeed2() string {
	return `{"results":[{"seq":4,"id":"doc4","changes":[{"rev":"1-786e"}]}],"last_seq":4}`
}

func fakeChangesFeedEmpty(lastSequence int) string {
	return fmt.Sprintf(`{"results":[],"last_seq":%v}`, lastSequence)
}

func fakeRevsDiff() string {
	return `{"doc2":{"missing":["1-5e38"]}}`
}

func fakeRevsDiff2() string {
	return `{"doc4":{"missing":["1-786e"]}}`
}

func fakeBoundary() string {
	return "882fbb2ef17c452b4a30362990eaed6bc53d5ed71b27ad32f9b50f7616aa"
}

func fakeBoundary2() string {
	return "2a9349ad8e2c52ba0a58d3cddb60b2a55f23ceadcf03ef8f160260d0275d"
}

func fakeBulkGetResponse(boundary string) string {
	return fmt.Sprintf(`--%s
Content-Type: application/json

{"_id":"doc2","_rev":"1-5e38","_revisions":{"ids":["5e38"],"start":1},"fakefield1":false,"fakefield2":1, "fakefield3":"blah"}
--%s--
`, boundary, boundary)
}

func fakeBulkGetResponseWithTextAttachment(boundary1, boundary2 string) string {

	response := fmt.Sprintf(`--%s
Content-Type: application/json

{"_id":"doc1","_rev":"1-6b5f","_revisions":{"ids":["6b5f"],"start":1},"fakefield1":true,"fakefield2":2}
--%s
X-Doc-Id: doc2
X-Rev-Id: 1-5e38
Content-Type: multipart/related; boundary="%s"

--%s
Content-Type: application/json

{"_attachments":{"attachment.txt":{"content_type":"text/plain","digest":"sha1-3a30948f8cd5655fede389d73b5fecd91251df4a","follows":true,"length":10,"revpos":1}},"_id":"doc2","_rev":"1-5e38","_revisions":{"ids":["5e38"],"start":1},"fakefield1":false,"fakefield2":1, "fakefield3":"blah"}
--%s
Content-Type: text/plain
Content-Disposition: attachment; filename="attachment.txt"

0123456789
--%s--
--%s--
`, boundary1, boundary1, boundary2, boundary2, boundary2, boundary2, boundary1)
	return response
}

func fakeBulkGetResponse2(boundary string) string {
	return fmt.Sprintf(`--%s
Content-Type: application/json

{"_id":"doc4","_rev":"1-786e","_revisions":{"ids":["786e"],"start":1},"fakefield1":true,"fakefield2":3, "fakefield3":"woof"}
--%s--
`, boundary, boundary)
}

func fakePutDocAttachmentResponse() string {
	return `[{"id":"doc2","rev":"1-5e38", "ok":true}]`
}

func fakeBulkDocsResponse() string {
	return `[{"id":"doc1","rev":"1-6b5f"}]`
}

func fakeBulkDocsResponse2() string {
	return `[{"id":"doc4","rev":"1-786e"}]`
}

func fakeEmptyChangesFeed() string {
	return `{"results":[],"last_seq":4}`
}

func waitForNotificationAndStop(replication *Replication, expected ReplicationStatus) {
	logg.LogTo("TEST", "Waiting for %v", expected)
	notificationChan := replication.NotificationChan

	for {
		select {
		case replicationNotification := <-notificationChan:
			if replicationNotification.Status == expected {
				logg.LogTo("TEST", "Got %v", expected)
				replication.Stop()
				return
			} else {
				logg.LogTo("TEST", "Waiting for %v but got %v, igoring", expected, replicationNotification.Status)
			}
		case <-time.After(time.Second * 10):
			logg.LogPanic("Timeout waiting for %v", expected)
		}
	}

}

func waitForReplicationStoppedNotification(replication *Replication) (remoteCheckpoint int) {

	for {
		select {
		case replicationNotification, ok := <-replication.NotificationChan:
			if !ok {
				logg.LogPanic("notifictionChan appears to be closed")
				return
			}
			if replicationNotification.Status == REPLICATION_STOPPED {
				remoteCheckpoint = replicationNotification.Data.(int)
				return
			}
		case <-time.After(time.Second * 10):
			logg.LogPanic("Timeout")
		}
	}

}

func waitForNotification(replication *Replication, expected ReplicationStatus) {
	logg.LogTo("TEST", "Waiting for %v", expected)
	notificationChan := replication.NotificationChan

	for {
		select {
		case replicationNotification, ok := <-notificationChan:
			if !ok {
				logg.LogPanic("TEST", "notifictionChan appears to be closed")
				return
			}
			if replicationNotification.Status == REPLICATION_ABORTED {

				if replicationNotification.Error == nil {
					logg.LogPanic("TEST", "expected replicationNotification.Error != nil")
				}
			}
			if replicationNotification.Status == expected {
				logg.LogTo("TEST", "Got %v", expected)
				return
			} else {
				logg.LogTo("TEST", "Waiting for %v but got %v, igoring", expected, replicationNotification.Status)
			}

		case <-time.After(time.Second * 10):
			logg.LogPanic("Timeout waiting for %v", expected)
		}
	}

}

func bogusJson() string {
	return `{"bogus": true}`
}

func TestGetTargetCheckpoint(t *testing.T) {

	targetServer := fakehttp.NewHTTPServerWithPort(5986)

	params := ReplicationParameters{}
	params.Target = targetServer.URL
	replication := NewReplication(params, nil)
	targetCheckpoint := replication.targetCheckpointAddress()
	logg.LogTo("TEST", "checkpoint: %v", targetCheckpoint)

}

func TestGeneratePushCheckpointRequest(t *testing.T) {

	// codereview: how to get the pushCheckpointRequest to not have the
	// rev field at all, as opposed to having an empty string

	// with an empty fetchedtargetcheckpoint, the pushCheckpointRequest
	// should _not_ have a _rev version
	replication := NewReplication(ReplicationParameters{}, nil)
	pushCheckpointRequest := replication.generatePushCheckpointRequest()
	logg.LogTo("TEST", "pushCheckpointRequest: %v", pushCheckpointRequest)
	assert.True(t, len(pushCheckpointRequest.Revision) == 0)

}
