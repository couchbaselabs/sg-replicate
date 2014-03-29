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
}

func NewReplication(params ReplicationParameters, notificationChan chan ReplicationNotification) *Replication {

	eventChan := make(chan ReplicationEvent)

	replication := &Replication{
		Parameters:       params,
		EventChan:        eventChan,
		NotificationChan: notificationChan,
	}

	// spawn a go-routine that reads from event channel and acts on events
	go replication.processEvents()

	return replication

}

// Start this replication
func (r *Replication) Start() {
	event := NewReplicationEvent(REPLICATION_START)
	r.EventChan <- *event
}

// Stop this replication
func (r *Replication) Stop() {
	event := NewReplicationEvent(REPLICATION_STOP)
	r.EventChan <- *event
}

func (r *Replication) processEvents() {

	defer close(r.EventChan)        // No more events
	defer close(r.NotificationChan) // No more notifications

	for state := stateFnPreStarted; state != nil; {
		state = state(r)
	}

	logg.LogTo("SYNC", "processEvents() is done")

}

func (r Replication) targetCheckpointAddress() string {
	targetUrlString := r.Parameters.getTargetDbUrl()
	hash := crypto.SHA1.New()
	hash.Sum([]byte(targetUrlString))
	return hex.EncodeToString(hash.Sum(nil))
}

func (r Replication) fetchTargetCheckpoint() {

	checkpoint := r.targetCheckpointAddress()
	destUrl := fmt.Sprintf("%s/_local/%s", r.Parameters.getTargetDbUrl(), checkpoint)

	transport := r.getTransport()
	defer transport.Close()

	client := &http.Client{Transport: transport}
	req, _ := http.NewRequest("GET", destUrl, nil)
	resp, err := client.Do(req)
	logg.LogTo("SYNCTUBE", "resp: %v, err: %v", resp, err)
	if err != nil {
		logg.LogTo("SYNCTUBE", "Error getting checkpoint: %v", err)
		event := NewReplicationEvent(FETCH_CHECKPOINT_FAILED)
		r.EventChan <- *event
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		// valid response, continue with empty remote checkpoint
		logg.LogTo("SYNCTUBE", "404 trying to get checkpoint, continue..")
		event := NewReplicationEvent(FETCH_CHECKPOINT_SUCCEEDED)
		checkpoint := Checkpoint{LastSequence: "0"}
		event.Data = checkpoint.LastSequence
		r.EventChan <- *event
	} else if resp.StatusCode >= 400 {
		// we got an error, lets abort
		logg.LogTo("SYNCTUBE", "4xx error(not 404) getting checkpoint")
		event := NewReplicationEvent(FETCH_CHECKPOINT_FAILED)
		r.EventChan <- *event
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
			r.EventChan <- *event
			return
		}
		if len(checkpoint.LastSequence) == 0 {
			logg.LogTo("SYNCTUBE", "Invalid checkpoint, no lastsequence")
			event := NewReplicationEvent(FETCH_CHECKPOINT_FAILED)
			r.EventChan <- *event
			return
		}
		expectedId := fmt.Sprintf("_local/%s", r.targetCheckpointAddress())
		if checkpoint.Id != expectedId {
			logg.LogTo("SYNCTUBE", "Got %s, expected %s", checkpoint.Id, expectedId)
			event := NewReplicationEvent(FETCH_CHECKPOINT_FAILED)
			r.EventChan <- *event
			return
		}
		logg.LogTo("SYNCTUBE", "checkpoint: %v", checkpoint.LastSequence)
		event := NewReplicationEvent(FETCH_CHECKPOINT_SUCCEEDED)
		event.Data = checkpoint.LastSequence
		logg.LogTo("SYNCTUBE", "event: %v", event)
		r.EventChan <- *event

	} else {
		// unexpected http status, abort
		logg.LogTo("SYNCTUBE", "unexpected http status %v", resp.StatusCode)
		event := NewReplicationEvent(FETCH_CHECKPOINT_FAILED)
		r.EventChan <- *event
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
		r.EventChan <- *event
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		logg.LogTo("SYNCTUBE", "Error getting changes feed.  Resp: %v", resp)
		event := NewReplicationEvent(FETCH_CHANGES_FEED_FAILED)
		r.EventChan <- *event
		return
	}

	bodyText, _ := ioutil.ReadAll(resp.Body)
	changes := Changes{}
	err = json.Unmarshal(bodyText, &changes)
	if err != nil {
		logg.LogTo("SYNCTUBE", "Error unmarshalling change")
		logg.LogError(err)
		event := NewReplicationEvent(FETCH_CHANGES_FEED_FAILED)
		r.EventChan <- *event
	}
	event := NewReplicationEvent(FETCH_CHANGES_FEED_SUCCEEDED)
	event.Data = changes
	logg.LogTo("SYNCTUBE", "event: %v", event)
	r.EventChan <- *event

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
		r.EventChan <- *event
		return
	}

	req, err := http.NewRequest("POST", revsDiffUrl, bytes.NewReader(revsDiffMapJson))
	if err != nil {
		logg.LogTo("SYNCTUBE", "Error creating request %v", revsDiffMapJson)
		logg.LogError(err)
		event := NewReplicationEvent(FETCH_REVS_DIFF_FAILED)
		r.EventChan <- *event
		return
	}

	client := &http.Client{Transport: transport}

	resp, err := client.Do(req)
	logg.LogTo("SYNCTUBE", "revs diff resp: %v, err: %v", resp, err)
	if err != nil {
		logg.LogTo("SYNCTUBE", "Error getting revs diff: %v", err)
		event := NewReplicationEvent(FETCH_REVS_DIFF_FAILED)
		r.EventChan <- *event
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		logg.LogTo("SYNCTUBE", "Unexpected response getting revs diff: %v", resp)
		event := NewReplicationEvent(FETCH_REVS_DIFF_FAILED)
		r.EventChan <- *event
		return
	}

	bodyText, _ := ioutil.ReadAll(resp.Body)
	revsDiffJson := RevsDiffResponseMap{}
	err = json.Unmarshal(bodyText, &revsDiffJson)
	if err != nil {
		logg.LogTo("SYNCTUBE", "Error unmarshalling json")
		logg.LogError(err)
		event := NewReplicationEvent(FETCH_REVS_DIFF_FAILED)
		r.EventChan <- *event
		return
	}
	event := NewReplicationEvent(FETCH_REVS_DIFF_SUCCEEDED)
	event.Data = revsDiffJson
	logg.LogTo("SYNCTUBE", "event: %v", event)
	r.EventChan <- *event

}

func (r Replication) fetchBulkGet() {
	transport := r.getTransport()
	defer transport.Close()

	bulkGetUrl := r.getBulkGetUrl()
	bulkGetRequest := generateBulkGetRequest(r.RevsDiff)

	bulkGetRequestJson, err := json.Marshal(bulkGetRequest)
	if err != nil {
		logg.LogTo("SYNCTUBE", "Error marshaling %v", bulkGetRequest)
		logg.LogError(err)
		event := NewReplicationEvent(FETCH_BULK_GET_FAILED)
		r.EventChan <- *event
		return
	}

	req, err := http.NewRequest("POST", bulkGetUrl, bytes.NewReader(bulkGetRequestJson))
	if err != nil {
		logg.LogTo("SYNCTUBE", "Error creating request %v", bulkGetRequestJson)
		logg.LogError(err)
		event := NewReplicationEvent(FETCH_BULK_GET_FAILED)
		r.EventChan <- *event
		return
	}

	client := &http.Client{Transport: transport}

	resp, err := client.Do(req)
	logg.LogTo("SYNCTUBE", "bulk get resp: %v, err: %v", resp, err)
	if err != nil {
		logg.LogTo("SYNCTUBE", "Error getting bulk get: %v", err)
		event := NewReplicationEvent(FETCH_BULK_GET_FAILED)
		r.EventChan <- *event
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		logg.LogTo("SYNCTUBE", "Unexpected response getting bulk get: %v", resp)
		event := NewReplicationEvent(FETCH_BULK_GET_FAILED)
		r.EventChan <- *event
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
			r.EventChan <- *event
			return
		}
		documentBodies = append(documentBodies, documentBody)
		mainPart.Close()

	}

	event := NewReplicationEvent(FETCH_BULK_GET_SUCCEEDED)
	event.Data = documentBodies
	r.EventChan <- *event

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
		r.EventChan <- *event
		return
	}

	req, err := http.NewRequest("POST", bulkDocsUrl, bytes.NewReader(bulkDocsRequestJson))
	if err != nil {
		logg.LogTo("SYNCTUBE", "Error creating request %v", bulkDocsRequestJson)
		logg.LogError(err)
		event := NewReplicationEvent(PUSH_BULK_DOCS_FAILED)
		r.EventChan <- *event
		return
	}

	client := &http.Client{Transport: transport}

	resp, err := client.Do(req)
	logg.LogTo("SYNCTUBE", "bulk get resp: %v, err: %v", resp, err)
	if err != nil {
		logg.LogTo("SYNCTUBE", "Error getting bulk get: %v", err)
		event := NewReplicationEvent(PUSH_BULK_DOCS_FAILED)
		r.EventChan <- *event
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		logg.LogTo("SYNCTUBE", "Unexpected response getting bulk get: %v", resp)
		event := NewReplicationEvent(PUSH_BULK_DOCS_FAILED)
		r.EventChan <- *event
		return
	}

	bulkDocsResponse := []DocumentRevisionPair{}
	decoder := json.NewDecoder(resp.Body)
	if err = decoder.Decode(&bulkDocsResponse); err != nil {
		logg.LogTo("SYNCTUBE", "Error decoding json: %v", err)
		event := NewReplicationEvent(PUSH_BULK_DOCS_FAILED)
		r.EventChan <- *event
		return
	}

	event := NewReplicationEvent(PUSH_BULK_DOCS_SUCCEEDED)
	event.Data = bulkDocsResponse
	r.EventChan <- *event

}

func (r Replication) getChangesFeedUrl() string {
	// TODO: add since param

	dbUrl := r.Parameters.getSourceDbUrl()
	return fmt.Sprintf(
		"%s/_changes?feed=%s&limit=%s&heartbeat=%s&style=%s",
		dbUrl,
		r.Parameters.getChangesFeedType(),
		r.Parameters.getChangesFeedLimit(),
		r.Parameters.getChangesFeedHeartbeat(),
		r.Parameters.getChangesFeedStyle())

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
