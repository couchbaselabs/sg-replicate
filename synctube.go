package synctube

import (
	"bytes"
	"crypto"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/couchbaselabs/logg"
	"github.com/mreiferson/go-httpclient"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"time"
)

type Replication struct {
	Parameters              ReplicationParameters
	EventChan               chan ReplicationEvent
	NotificationChan        chan ReplicationNotification
	FetchedTargetCheckpoint Checkpoint
	Changes                 Changes
	RevsDiff                RevsDiffResponseMap
	DocumentBodies          []DocumentBody
	PushedBulkDocs          []DocumentRevisionPair
	LastSequencePushed      int
}

func NewReplication(params ReplicationParameters, notificationChan chan ReplicationNotification) *Replication {

	eventChan := make(chan ReplicationEvent)

	replication := &Replication{
		Parameters:         params,
		EventChan:          eventChan,
		NotificationChan:   notificationChan,
		LastSequencePushed: -1,
	}

	// spawn a go-routine that reads from event channel and acts on events
	go replication.processEvents()

	return replication

}

// Start this replication
func (r Replication) Start() error {
	return r.sendEventWithTimeout(NewReplicationEvent(REPLICATION_START))
}

// Stop this replication
func (r *Replication) Stop() error {
	return r.sendEventWithTimeout(NewReplicationEvent(REPLICATION_STOP))
}

func (r *Replication) processEvents() {

	// nil out the EventChan after the event loop has finished.
	// originally this closed the channel, but any outstanding
	// goroutines running an http request would then try to
	// write their result to the closed channel and cause a panic
	defer func() { r.EventChan = nil }()

	defer close(r.NotificationChan) // No more notifications

	for state := stateFnPreStarted; state != nil; {
		state = state(r)
		logg.LogTo("SYNCTUBE", "new state: %v", state)
	}
	logg.LogTo("SYNCTUBE", "processEvents() is done")

}

func (r Replication) targetCheckpointAddress() string {

	// TODO: this needs to take into account other aspects
	// of replication (filters, filterparams, etc)

	targetUrlString := r.Parameters.getTargetDbUrl()
	hash := crypto.SHA1.New()
	hash.Sum([]byte(targetUrlString))
	return hex.EncodeToString(hash.Sum(nil))
}

func (r Replication) sendEventWithTimeout(event *ReplicationEvent) error {

	// if the event channel has already been nil'd out, no point
	// in even trying
	if r.EventChan == nil {
		return NewReplicationError(REPLICATION_STOP)
	}

	select {
	case r.EventChan <- *event:
		// event was sent
		return nil
	case <-time.After(10 * time.Second):
		// timed out ..
	}
	return NewReplicationError(REPLICATION_STOP)

}

func (r Replication) fetchTargetCheckpoint() {

	destUrl := r.getCheckpointUrl()

	transport := r.getTransport()
	defer transport.Close()

	client := &http.Client{Transport: transport}
	req, _ := http.NewRequest("GET", destUrl, nil)
	resp, err := client.Do(req)
	logg.LogTo("SYNCTUBE", "resp: %v, err: %v", resp, err)

	if err != nil {
		logg.LogTo("SYNCTUBE", "Error getting checkpoint: %v", err)
		event := NewReplicationEvent(FETCH_CHECKPOINT_FAILED)
		r.sendEventWithTimeout(event)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		// valid response, continue with empty remote checkpoint
		logg.LogTo("SYNCTUBE", "404 trying to get checkpoint, continue..")
		event := NewReplicationEvent(FETCH_CHECKPOINT_SUCCEEDED)
		checkpoint := Checkpoint{LastSequence: "0"}
		event.Data = checkpoint
		r.sendEventWithTimeout(event)
	} else if resp.StatusCode >= 400 {
		// we got an error, lets abort
		logg.LogTo("SYNCTUBE", "4xx error(not 404) getting checkpoint")
		event := NewReplicationEvent(FETCH_CHECKPOINT_FAILED)
		r.sendEventWithTimeout(event)
	} else if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		// looks like we got a valid checkpoint
		logg.LogTo("SYNCTUBE", "valid checkpoint")

		bodyText, _ := ioutil.ReadAll(resp.Body)
		logg.LogTo("SYNCTUBE", "body: %v", string(bodyText))
		checkpoint := Checkpoint{}
		err = json.Unmarshal(bodyText, &checkpoint)
		if err != nil {
			logg.LogTo("SYNCTUBE", "Error unmarshalling checkpoint")
			logg.LogError(err)
			event := NewReplicationEvent(FETCH_CHECKPOINT_FAILED)
			r.sendEventWithTimeout(event)
			return
		}
		if len(checkpoint.LastSequence) == 0 {
			logg.LogTo("SYNCTUBE", "Invalid checkpoint, no lastsequence")
			event := NewReplicationEvent(FETCH_CHECKPOINT_FAILED)
			r.sendEventWithTimeout(event)
			return
		}
		expectedId := fmt.Sprintf("_local/%s", r.targetCheckpointAddress())
		if checkpoint.Id != expectedId {
			logg.LogTo("SYNCTUBE", "Got %s, expected %s", checkpoint.Id, expectedId)
			event := NewReplicationEvent(FETCH_CHECKPOINT_FAILED)
			r.sendEventWithTimeout(event)
			return
		}
		logg.LogTo("SYNCTUBE", "checkpoint: %v", checkpoint.LastSequence)
		event := NewReplicationEvent(FETCH_CHECKPOINT_SUCCEEDED)
		event.Data = checkpoint
		logg.LogTo("SYNCTUBE", "event: %v", event)

		r.sendEventWithTimeout(event)

	} else {
		// unexpected http status, abort
		logg.LogTo("SYNCTUBE", "unexpected http status %v", resp.StatusCode)
		event := NewReplicationEvent(FETCH_CHECKPOINT_FAILED)
		r.sendEventWithTimeout(event)
	}

}

func (r Replication) fetchChangesFeed() {

	destUrl := r.getChangesFeedUrl()

	transport := r.getTransport()
	defer transport.Close()

	client := &http.Client{Transport: transport}
	req, _ := http.NewRequest("GET", destUrl, nil)
	resp, err := client.Do(req)
	logg.LogTo("SYNCTUBE", "changes feed resp: %v, err: %v", resp, err)
	if err != nil {
		logg.LogTo("SYNCTUBE", "Error getting changes feed: %v", err)
		event := NewReplicationEvent(FETCH_CHANGES_FEED_FAILED)
		r.sendEventWithTimeout(event)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		logg.LogTo("SYNCTUBE", "Error getting changes feed.  Resp: %v", resp)
		event := NewReplicationEvent(FETCH_CHANGES_FEED_FAILED)
		logg.LogTo("SYNCTUBE", "channel: %v", r.EventChan)
		r.sendEventWithTimeout(event)
		return
	}

	bodyText, _ := ioutil.ReadAll(resp.Body)
	changes := Changes{}
	err = json.Unmarshal(bodyText, &changes)
	if err != nil {
		logg.LogTo("SYNCTUBE", "Error unmarshalling change")
		logg.LogError(err)
		event := NewReplicationEvent(FETCH_CHANGES_FEED_FAILED)
		r.sendEventWithTimeout(event)
	}
	event := NewReplicationEvent(FETCH_CHANGES_FEED_SUCCEEDED)
	event.Data = changes
	logg.LogTo("SYNCTUBE", "event: %v", event)
	r.sendEventWithTimeout(event)

}

func (r Replication) fetchRevsDiff() {

	transport := r.getTransport()
	defer transport.Close()

	revsDiffUrl := r.getRevsDiffUrl()
	revsDiffMap := generateRevsDiffMap(r.Changes)
	revsDiffMapJson, err := json.Marshal(revsDiffMap)
	if err != nil {
		logg.LogTo("SYNCTUBE", "Error marshaling %v", revsDiffMap)
		logg.LogError(err)
		event := NewReplicationEvent(FETCH_REVS_DIFF_FAILED)
		r.sendEventWithTimeout(event)
		return
	}

	req, err := http.NewRequest("POST", revsDiffUrl, bytes.NewReader(revsDiffMapJson))
	if err != nil {
		logg.LogTo("SYNCTUBE", "Error creating request %v", revsDiffMapJson)
		logg.LogError(err)
		event := NewReplicationEvent(FETCH_REVS_DIFF_FAILED)
		r.sendEventWithTimeout(event)
		return
	}

	client := &http.Client{Transport: transport}

	resp, err := client.Do(req)
	logg.LogTo("SYNCTUBE", "revs diff resp: %v, err: %v", resp, err)
	if err != nil {
		logg.LogTo("SYNCTUBE", "Error getting revs diff: %v", err)
		event := NewReplicationEvent(FETCH_REVS_DIFF_FAILED)
		r.sendEventWithTimeout(event)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		logg.LogTo("SYNCTUBE", "Unexpected response getting revs diff: %v", resp)
		event := NewReplicationEvent(FETCH_REVS_DIFF_FAILED)
		r.sendEventWithTimeout(event)
		return
	}

	bodyText, _ := ioutil.ReadAll(resp.Body)
	revsDiffJson := RevsDiffResponseMap{}
	err = json.Unmarshal(bodyText, &revsDiffJson)
	if err != nil {
		logg.LogTo("SYNCTUBE", "Error unmarshalling json")
		logg.LogError(err)
		event := NewReplicationEvent(FETCH_REVS_DIFF_FAILED)
		r.sendEventWithTimeout(event)
		return
	}
	event := NewReplicationEvent(FETCH_REVS_DIFF_SUCCEEDED)
	event.Data = revsDiffJson
	logg.LogTo("SYNCTUBE", "event: %v", event)
	r.sendEventWithTimeout(event)

}

func (r Replication) fetchBulkGet() {
	transport := r.getTransport()
	defer transport.Close()

	bulkGetUrl := r.getBulkGetUrl()
	logg.LogTo("SYNCTUBE", "bulkGetUrl %v", bulkGetUrl)
	bulkGetRequest := generateBulkGetRequest(r.RevsDiff)

	bulkGetRequestJson, err := json.Marshal(bulkGetRequest)
	if err != nil {
		logg.LogTo("SYNCTUBE", "Error marshaling %v", bulkGetRequest)
		logg.LogError(err)
		event := NewReplicationEvent(FETCH_BULK_GET_FAILED)
		r.sendEventWithTimeout(event)
		return
	}

	req, err := http.NewRequest("POST", bulkGetUrl, bytes.NewReader(bulkGetRequestJson))
	logg.LogTo("SYNCTUBE", "bulkGet req %v", req)
	if err != nil {
		logg.LogTo("SYNCTUBE", "Error creating request %v", bulkGetRequestJson)
		logg.LogError(err)
		event := NewReplicationEvent(FETCH_BULK_GET_FAILED)
		r.sendEventWithTimeout(event)
		return
	}

	client := &http.Client{Transport: transport}

	resp, err := client.Do(req)
	logg.LogTo("SYNCTUBE", "bulk get resp: %v, err: %v", resp, err)
	if err != nil {
		logg.LogTo("SYNCTUBE", "Error getting bulk get: %v", err)
		event := NewReplicationEvent(FETCH_BULK_GET_FAILED)
		r.sendEventWithTimeout(event)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		logg.LogTo("SYNCTUBE", "Unexpected response getting bulk get: %v", resp)
		event := NewReplicationEvent(FETCH_BULK_GET_FAILED)
		r.sendEventWithTimeout(event)
		return
	}

	contentType := resp.Header.Get("Content-Type")

	mediaType, attrs, _ := mime.ParseMediaType(contentType)
	boundary := attrs["boundary"]

	if mediaType != "multipart/mixed" {
		logg.LogPanic("unexpected mediaType: %v", mediaType)
	}

	reader := multipart.NewReader(resp.Body, boundary)
	documentBodies := []DocumentBody{}

	for {
		mainPart, err := reader.NextPart()
		if err != nil {
			break
		}
		documentBody := DocumentBody{}
		decoder := json.NewDecoder(mainPart)

		if err = decoder.Decode(&documentBody); err != nil {
			logg.LogTo("SYNCTUBE", "Error decoding part: %v", err)
			event := NewReplicationEvent(FETCH_BULK_GET_FAILED)
			r.sendEventWithTimeout(event)
			return
		}
		documentBodies = append(documentBodies, documentBody)
		mainPart.Close()

	}

	event := NewReplicationEvent(FETCH_BULK_GET_SUCCEEDED)
	event.Data = documentBodies
	r.sendEventWithTimeout(event)

}

func (r Replication) pushBulkDocs() {
	transport := r.getTransport()
	defer transport.Close()

	bulkDocsUrl := r.getBulkDocsUrl()
	bulkDocsRequest := generateBulkDocsRequest(r.DocumentBodies)

	bulkDocsRequestJson, err := json.Marshal(bulkDocsRequest)
	if err != nil {
		logg.LogTo("SYNCTUBE", "Error marshaling %v", bulkDocsRequest)
		logg.LogError(err)
		event := NewReplicationEvent(PUSH_BULK_DOCS_FAILED)
		r.sendEventWithTimeout(event)
		return
	}

	req, err := http.NewRequest("POST", bulkDocsUrl, bytes.NewReader(bulkDocsRequestJson))
	if err != nil {
		logg.LogTo("SYNCTUBE", "Error creating request %v", bulkDocsRequestJson)
		logg.LogError(err)
		event := NewReplicationEvent(PUSH_BULK_DOCS_FAILED)
		r.sendEventWithTimeout(event)
		return
	}

	client := &http.Client{Transport: transport}

	resp, err := client.Do(req)
	logg.LogTo("SYNCTUBE", "bulk get resp: %v, err: %v", resp, err)
	if err != nil {
		logg.LogTo("SYNCTUBE", "Error getting bulk get: %v", err)
		event := NewReplicationEvent(PUSH_BULK_DOCS_FAILED)
		r.sendEventWithTimeout(event)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		logg.LogTo("SYNCTUBE", "Unexpected response getting bulk get: %v", resp)
		event := NewReplicationEvent(PUSH_BULK_DOCS_FAILED)
		r.sendEventWithTimeout(event)
		return
	}

	bulkDocsResponse := []DocumentRevisionPair{}
	decoder := json.NewDecoder(resp.Body)
	if err = decoder.Decode(&bulkDocsResponse); err != nil {
		logg.LogTo("SYNCTUBE", "Error decoding json: %v", err)
		event := NewReplicationEvent(PUSH_BULK_DOCS_FAILED)
		r.sendEventWithTimeout(event)
		return
	}

	event := NewReplicationEvent(PUSH_BULK_DOCS_SUCCEEDED)
	event.Data = bulkDocsResponse
	r.sendEventWithTimeout(event)

}

func (r Replication) generatePushCheckpointRequest() PushCheckpointRequest {

	pushCheckpointRequest := PushCheckpointRequest{
		LastSequence: fmt.Sprintf("%v", r.Changes.LastSequence),
	}

	if !r.FetchedTargetCheckpoint.IsEmpty() {
		pushCheckpointRequest.Revision = r.FetchedTargetCheckpoint.Revision
	}
	return pushCheckpointRequest
}

func (r Replication) pushCheckpoint() {

	transport := r.getTransport()
	defer transport.Close()

	checkpointUrl := r.getCheckpointUrl()
	logg.LogTo("SYNCTUBE", "calling pushCheckpointRequest. r.FetchedTargetCheckpoint: %v", r.FetchedTargetCheckpoint)
	pushCheckpointRequest := r.generatePushCheckpointRequest()
	logg.LogTo("SYNCTUBE", "pushCheckpointRequest %v", pushCheckpointRequest)
	logg.LogTo("SYNCTUBE", "r.Changes %v", r.Changes)
	logg.LogTo("SYNCTUBE", "r.Changes.LastSequence %v", r.Changes.LastSequence)

	requestJson, err := json.Marshal(pushCheckpointRequest)
	if err != nil {
		logg.LogTo("SYNCTUBE", "Error marshaling %v", pushCheckpointRequest)
		logg.LogError(err)
		event := NewReplicationEvent(PUSH_CHECKPOINT_FAILED)
		r.sendEventWithTimeout(event)
		return
	}

	req, err := http.NewRequest("PUT", checkpointUrl, bytes.NewReader(requestJson))
	if err != nil {
		logg.LogTo("SYNCTUBE", "Error creating request %v", requestJson)
		logg.LogError(err)
		event := NewReplicationEvent(PUSH_CHECKPOINT_FAILED)
		r.sendEventWithTimeout(event)
		return
	}

	client := &http.Client{Transport: transport}

	resp, err := client.Do(req)
	logg.LogTo("SYNCTUBE", "push checkpoint resp: %v, err: %v", resp, err)
	if err != nil {
		logg.LogTo("SYNCTUBE", "Error pushing checkpoint: %v", err)
		event := NewReplicationEvent(PUSH_CHECKPOINT_FAILED)
		r.sendEventWithTimeout(event)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		logg.LogTo("SYNCTUBE", "Unexpected response pushing checkpoint: %v", resp)
		event := NewReplicationEvent(PUSH_CHECKPOINT_FAILED)
		r.sendEventWithTimeout(event)
		return
	}

	checkpointResponse := PushCheckpointResponse{}
	decoder := json.NewDecoder(resp.Body)
	if err = decoder.Decode(&checkpointResponse); err != nil {
		logg.LogTo("SYNCTUBE", "Error decoding json: %v", err)
		event := NewReplicationEvent(PUSH_CHECKPOINT_FAILED)
		r.sendEventWithTimeout(event)
		return
	}

	if checkpointResponse.Ok != true {
		logg.LogTo("SYNCTUBE", "Error, checkpoint response !ok")
		event := NewReplicationEvent(PUSH_CHECKPOINT_FAILED)
		r.sendEventWithTimeout(event)
		return
	}

	event := NewReplicationEvent(PUSH_CHECKPOINT_SUCCEEDED)
	r.sendEventWithTimeout(event)

}

func (r Replication) getCheckpointUrl() string {
	checkpointAddress := r.targetCheckpointAddress()
	return fmt.Sprintf("%s/_local/%s", r.Parameters.getTargetDbUrl(), checkpointAddress)
}

func (r Replication) getChangesFeedUrl() string {

	changesFeedParams := NewChangesFeedParams()
	changesFeedUrl := r.Parameters.getSourceChangesFeedUrl(*changesFeedParams)

	checkpoint, err := r.FetchedTargetCheckpoint.LastCheckpointNumeric()
	if err != nil {
		logg.LogPanic("got non-numeric checkpoint: %v", r.FetchedTargetCheckpoint)
	}

	if !r.FetchedTargetCheckpoint.IsEmpty() {
		changesFeedUrl = fmt.Sprintf(
			"%s&since=%v",
			changesFeedUrl,
			checkpoint,
		)
	}

	return changesFeedUrl

}

func (r Replication) getRevsDiffUrl() string {
	dbUrl := r.Parameters.getTargetDbUrl()
	return fmt.Sprintf(
		"%s/_revs_diff",
		dbUrl)

}

func (r Replication) getBulkDocsUrl() string {
	dbUrl := r.Parameters.getTargetDbUrl()
	return fmt.Sprintf(
		"%s/_bulk_docs",
		dbUrl)
}

func (r Replication) getBulkGetUrl() string {
	dbUrl := r.Parameters.getSourceDbUrl()
	return fmt.Sprintf(
		"%s/_bulk_get?revs=true&attachments=true",
		dbUrl)

}

func (r Replication) getTransport() *httpclient.Transport {
	return &httpclient.Transport{
		ConnectTimeout:        60 * time.Second,
		RequestTimeout:        60 * time.Second,
		ResponseHeaderTimeout: 60 * time.Second,
	}
}
