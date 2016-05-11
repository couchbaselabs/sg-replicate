package sgreplicate

import (
	"bytes"
	"crypto"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"time"

	"github.com/couchbase/clog"
	"github.com/mreiferson/go-httpclient"
)

var globalClient *http.Client

// Interface for interacting with either Replication or ContinuousReplication
type SGReplication interface {
	// GetParameters() ReplicationParameters
	GetStats() ReplicationStats
	Stop() error
}

type ReplicationStats struct {
	DocsRead         uint32
	DocsWritten      uint32
	DocWriteFailures uint32
	StartLastSeq     uint32
	EndLastSeq       interface{}
}

type Replication struct {
	LoggingReplication
	Stats                   ReplicationStats
	EventChan               chan ReplicationEvent
	NotificationChan        chan ReplicationNotification
	FetchedTargetCheckpoint Checkpoint
	Changes                 Changes
	RevsDiff                RevsDiffResponseMap
	Documents               []Document
	PushedBulkDocs          []DocumentRevisionPair
}

func init() {
	t := &httpclient.Transport{
		ConnectTimeout:        60 * time.Second,
		RequestTimeout:        1 * time.Hour,
		ResponseHeaderTimeout: 60 * time.Second,
	}
	globalClient = &http.Client{Transport: t}
}

func NewReplication(params ReplicationParameters, notificationChan chan ReplicationNotification) *Replication {

	eventChan := make(chan ReplicationEvent)

	replication := &Replication{
		LoggingReplication: LoggingReplication{params},
		EventChan:          eventChan,
		NotificationChan:   notificationChan,
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
func (r Replication) Stop() error {
	return r.sendEventWithTimeout(NewReplicationEvent(REPLICATION_STOP))
}

/*
func (r Replication) GetParameters() ReplicationParameters {
	return r.Parameters
}
*/

func (r Replication) GetStats() ReplicationStats {
	return r.Stats
}

// Run a one-shot replication synchronously (eg, block until finished)
func RunOneShotReplication(params ReplicationParameters) (ReplicationStatus, error) {

	replication := StartOneShotReplication(params)
	return replication.WaitUntilDone()

}

func StartOneShotReplication(params ReplicationParameters) *Replication {
	notificationChan := make(chan ReplicationNotification)
	replication := NewReplication(params, notificationChan)
	replication.Start()
	return replication
}

func (r *Replication) WaitUntilDone() (ReplicationStatus, error) {
	for {
		select {
		case replicationNotification := <-r.NotificationChan:
			if replicationNotification.Status == REPLICATION_ABORTED {
				return REPLICATION_ABORTED, fmt.Errorf("Replication Aborted")
			}
			if replicationNotification.Status == REPLICATION_STOPPED {
				return REPLICATION_STOPPED, nil
			}
		case <-time.After(time.Second * 300):
			return REPLICATION_ABORTED, fmt.Errorf("Replication timed out")
		}
	}
}

func (r *Replication) processEvents() {

	defer close(r.NotificationChan) // No more notifications

	for state := stateFnPreStarted; state != nil; {
		state = state(r)
		r.LogTo("Replicate", "new state: %v", state)
	}
	r.LogTo("Replicate", "processEvents() is done")

}

// Shut down the event channel, because this event loop is just
// about to be shut down.  This must be done before returning
// any notifications on r.NotificationChan that might cause
// caller code to call Start() or Stop(), so that in that case
// those calls will detect that the event channel has already
// been shutdown and therefore will return an error immediately
// rather than trying to put an event on the event channel.
//
// The reason the event channel must be nil'd out at all:
// originally this channel was closed when no longer needed,
// but any outstanding goroutines running an http request would
// then try to write their result to the closed channel and
// cause a panic
func (r *Replication) shutdownEventChannel() {
	r.EventChan = nil
}

func (r Replication) targetCheckpointAddress() string {

	// TODO: this needs to take into account other aspects
	// of replication (filters, filterparams, etc)

	targetUrlString := r.Parameters.GetTargetDbUrl()
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

	client := globalClient
	req, _ := http.NewRequest("GET", destUrl, nil)
	resp, err := client.Do(req)
	r.LogTo("Replicate", "resp: %v, err: %v", resp, err)

	if err != nil {
		r.LogTo("Replicate", "Error getting checkpoint: %v", err)
		event := NewReplicationEvent(FETCH_CHECKPOINT_FAILED)
		r.sendEventWithTimeout(event)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		// valid response, continue with empty remote checkpoint
		r.LogTo("Replicate", "404 trying to get checkpoint, continue..")
		event := NewReplicationEvent(FETCH_CHECKPOINT_SUCCEEDED)
		checkpoint := Checkpoint{LastSequence: "0"}
		event.Data = checkpoint
		r.sendEventWithTimeout(event)
	} else if resp.StatusCode >= 400 {
		// we got an error, lets abort
		r.LogTo("Replicate", "4xx error(not 404) getting checkpoint")
		event := NewReplicationEvent(FETCH_CHECKPOINT_FAILED)
		r.sendEventWithTimeout(event)
	} else if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		// looks like we got a valid checkpoint
		r.LogTo("Replicate", "valid checkpoint")

		bodyText, _ := ioutil.ReadAll(resp.Body)
		r.LogTo("Replicate", "body: %v", string(bodyText))
		checkpoint := Checkpoint{}
		err = json.Unmarshal(bodyText, &checkpoint)
		if err != nil {
			r.LogTo("Replicate", "Error unmarshalling checkpoint")
			clog.Error(err)
			event := NewReplicationEvent(FETCH_CHECKPOINT_FAILED)
			r.sendEventWithTimeout(event)
			return
		}
		if len(checkpoint.LastSequence) == 0 {
			r.LogTo("Replicate", "Invalid checkpoint, no lastsequence")
			event := NewReplicationEvent(FETCH_CHECKPOINT_FAILED)
			r.sendEventWithTimeout(event)
			return
		}
		expectedId := fmt.Sprintf("_local/%s", r.targetCheckpointAddress())
		if checkpoint.Id != expectedId {
			r.LogTo("Replicate", "Got %s, expected %s", checkpoint.Id, expectedId)
			event := NewReplicationEvent(FETCH_CHECKPOINT_FAILED)
			r.sendEventWithTimeout(event)
			return
		}
		r.LogTo("Replicate", "checkpoint: %v", checkpoint.LastSequence)
		event := NewReplicationEvent(FETCH_CHECKPOINT_SUCCEEDED)
		event.Data = checkpoint
		r.LogTo("Replicate", "event: %v", event)

		r.sendEventWithTimeout(event)

	} else {
		// unexpected http status, abort
		r.LogTo("Replicate", "unexpected http status %v", resp.StatusCode)
		event := NewReplicationEvent(FETCH_CHECKPOINT_FAILED)
		r.sendEventWithTimeout(event)
	}

}

func (r Replication) fetchChangesFeed() {

	destUrl := r.getNormalChangesFeedUrl()

	client := globalClient
	req, _ := http.NewRequest("GET", destUrl, nil)
	resp, err := client.Do(req)
	r.LogTo("Replicate", "changes feed resp: %v, err: %v", resp, err)
	if err != nil {
		r.LogTo("Replicate", "Error getting changes feed: %v", err)
		event := NewReplicationEvent(FETCH_CHANGES_FEED_FAILED)
		r.sendEventWithTimeout(event)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		r.LogTo("Replicate", "Error getting changes feed.  Resp: %v", resp)
		event := NewReplicationEvent(FETCH_CHANGES_FEED_FAILED)
		r.LogTo("Replicate", "channel: %v", r.EventChan)
		r.sendEventWithTimeout(event)
		return
	}

	bodyText, _ := ioutil.ReadAll(resp.Body)
	changes := Changes{}
	err = json.Unmarshal(bodyText, &changes)
	if err != nil {
		r.LogTo("Replicate", "Error unmarshalling change")
		clog.Error(err)
		event := NewReplicationEvent(FETCH_CHANGES_FEED_FAILED)
		r.sendEventWithTimeout(event)
		return
	}
	event := NewReplicationEvent(FETCH_CHANGES_FEED_SUCCEEDED)
	event.Data = changes
	r.LogTo("Replicate", "event: %v", event)
	r.sendEventWithTimeout(event)

}

func (r Replication) fetchRevsDiff() {

	revsDiffUrl := r.getRevsDiffUrl()
	revsDiffMap := generateRevsDiffMap(r.Changes)
	revsDiffMapJson, err := json.Marshal(revsDiffMap)
	if err != nil {
		r.LogTo("Replicate", "Error marshaling %v", revsDiffMap)
		clog.Error(err)
		event := NewReplicationEvent(FETCH_REVS_DIFF_FAILED)
		r.sendEventWithTimeout(event)
		return
	}

	req, err := http.NewRequest("POST", revsDiffUrl, bytes.NewReader(revsDiffMapJson))
	if err != nil {
		r.LogTo("Replicate", "Error creating request %v", revsDiffMapJson)
		clog.Error(err)
		event := NewReplicationEvent(FETCH_REVS_DIFF_FAILED)
		r.sendEventWithTimeout(event)
		return
	}

	client := globalClient

	resp, err := client.Do(req)
	r.LogTo("Replicate", "revs diff resp: %v, err: %v", resp, err)
	if err != nil {
		r.LogTo("Replicate", "Error getting revs diff: %v", err)
		event := NewReplicationEvent(FETCH_REVS_DIFF_FAILED)
		r.sendEventWithTimeout(event)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		r.LogTo("Replicate", "Unexpected response getting revs diff: %v", resp)
		event := NewReplicationEvent(FETCH_REVS_DIFF_FAILED)
		r.sendEventWithTimeout(event)
		return
	}

	bodyText, _ := ioutil.ReadAll(resp.Body)
	revsDiffJson := RevsDiffResponseMap{}
	err = json.Unmarshal(bodyText, &revsDiffJson)
	if err != nil {
		r.LogTo("Replicate", "Error unmarshalling json")
		clog.Error(err)
		event := NewReplicationEvent(FETCH_REVS_DIFF_FAILED)
		r.sendEventWithTimeout(event)
		return
	}
	event := NewReplicationEvent(FETCH_REVS_DIFF_SUCCEEDED)
	event.Data = revsDiffJson
	r.LogTo("Replicate", "event: %v", event)
	r.sendEventWithTimeout(event)

}

func (r Replication) fetchBulkGet() {

	bulkGetUrl := r.getBulkGetUrl()
	r.LogTo("Replicate", "bulkGetUrl %v", bulkGetUrl)
	bulkGetRequest := generateBulkGetRequest(r.RevsDiff)

	bulkGetRequestJson, err := json.Marshal(bulkGetRequest)
	if err != nil {
		r.LogTo("Replicate", "Error marshaling %v", bulkGetRequest)
		clog.Error(err)
		event := NewReplicationEvent(FETCH_BULK_GET_FAILED)
		r.sendEventWithTimeout(event)
		return
	}

	req, err := http.NewRequest("POST", bulkGetUrl, bytes.NewReader(bulkGetRequestJson))
	r.LogTo("Replicate", "bulkGet req %v", req)
	if err != nil {
		r.LogTo("Replicate", "Error creating request %v", bulkGetRequestJson)
		clog.Error(err)
		event := NewReplicationEvent(FETCH_BULK_GET_FAILED)
		r.sendEventWithTimeout(event)
		return
	}

	client := globalClient

	resp, err := client.Do(req)
	r.LogTo("Replicate", "bulk get resp: %v, err: %v", resp, err)
	if err != nil {
		r.LogTo("Replicate", "Error getting bulk get: %v", err)
		event := NewReplicationEvent(FETCH_BULK_GET_FAILED)
		r.sendEventWithTimeout(event)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		r.LogTo("Replicate", "Unexpected response getting bulk get: %v", resp)
		event := NewReplicationEvent(FETCH_BULK_GET_FAILED)
		r.sendEventWithTimeout(event)
		return
	}

	contentType := resp.Header.Get("Content-Type")

	mediaType, attrs, _ := mime.ParseMediaType(contentType)
	boundary := attrs["boundary"]

	if mediaType != "multipart/mixed" {
		clog.Panic("unexpected mediaType: %v", mediaType)
	}

	reader := multipart.NewReader(resp.Body, boundary)
	documents := []Document{}

	for {
		mainPart, err := reader.NextPart()
		if err == io.EOF {
			break
		} else if err != nil {
			r.LogTo("Replicate", "Error getting next part: %v", err)
			event := NewReplicationEvent(FETCH_BULK_GET_FAILED)
			event.Data = err
			r.sendEventWithTimeout(event)
			return
		}

		r.LogTo("Replicate", "mainPart: %v.  Header: %v", mainPart, mainPart.Header)
		mainPartContentTypes := mainPart.Header["Content-Type"] // why a slice?
		mainPartContentType := mainPartContentTypes[0]
		contentType, attrs, _ := mime.ParseMediaType(mainPartContentType)
		r.LogTo("Replicate", "contentType: %v", contentType)
		r.LogTo("Replicate", "boundary: %v", attrs["boundary"])
		switch contentType {
		case "application/json":
			documentBody := DocumentBody{}
			decoder := json.NewDecoder(mainPart)

			if err = decoder.Decode(&documentBody); err != nil {
				r.LogTo("Replicate", "Error decoding part: %v", err)
				event := NewReplicationEvent(FETCH_BULK_GET_FAILED)
				event.Data = err
				r.sendEventWithTimeout(event)
				return
			}
			document := Document{
				Body: documentBody,
			}
			documents = append(documents, document)
			mainPart.Close()
		case "multipart/related":
			nestedReader := multipart.NewReader(mainPart, attrs["boundary"])
			nestedDoc := Document{}
			nestedAttachments := []*Attachment{}
			for {
				nestedPart, err := nestedReader.NextPart()
				if err == io.EOF {
					break
				} else if err != nil {
					r.LogTo("Replicate", "err nested nextpart: %v", err)
					event := NewReplicationEvent(FETCH_BULK_GET_FAILED)
					event.Data = err
					r.sendEventWithTimeout(event)
					return
				}
				r.LogTo("Replicate", "nestedPart: %v.  Header: %v", nestedPart, nestedPart.Header)
				nestedPartContentTypes := nestedPart.Header["Content-Type"]
				nestedPartContentType := nestedPartContentTypes[0]
				nestedContentType, nestedAttrs, _ := mime.ParseMediaType(nestedPartContentType)
				r.LogTo("Replicate", "nestedContentType: %v", nestedContentType)
				r.LogTo("Replicate", "nestedAttrs: %v", nestedAttrs)

				switch nestedContentType {
				case "application/json":
					nestedDecoder := json.NewDecoder(nestedPart)
					nestedDocBody := DocumentBody{}
					if err = nestedDecoder.Decode(&nestedDocBody); err != nil {
						r.LogTo("Replicate", "Error decoding part: %v", err)
						event := NewReplicationEvent(FETCH_BULK_GET_FAILED)
						event.Data = err
						r.sendEventWithTimeout(event)
						return
					}
					nestedDoc.Body = nestedDocBody
					nestedPart.Close()

				default:
					// handle attachment
					attachment, err := NewAttachment(&r, nestedPart)
					if err != nil {
						r.LogTo("Replicate", "Error decoding attachment: %v", err)
						event := NewReplicationEvent(FETCH_BULK_GET_FAILED)
						event.Data = err
						r.sendEventWithTimeout(event)
						return
					}
					nestedAttachments = append(nestedAttachments, attachment)
				}

			}
			if len(nestedAttachments) > 0 {
				nestedDoc.Attachments = nestedAttachments
			}
			documents = append(documents, nestedDoc)

			mainPart.Close()
		default:
			r.LogTo("Replicate", "ignoring unexpected content type: %v", contentType)

		}

	}

	event := NewReplicationEvent(FETCH_BULK_GET_SUCCEEDED)
	event.Data = documents
	r.sendEventWithTimeout(event)

}

func (r Replication) pushAttachmentDocs() {

	failed := PUSH_ATTACHMENT_DOCS_FAILED
	docs := subsetDocsWithAttachemnts(r.Documents)
	for _, doc := range docs {
		url := r.getPutDocWithAttatchmentUrl(doc)
		r.LogTo("Replicate", "pushAttatchmentDocs url: %v", url)
		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)

		mimeHeader := textproto.MIMEHeader{}
		mimeHeader.Set("Content-Type", "application/json")
		jsonBytes, err := json.Marshal(doc.Body)
		if err != nil {
			r.sendErrorEvent(failed, "Marshalling body", err)
			return
		}

		part, err := writer.CreatePart(mimeHeader)
		if err != nil {
			r.sendErrorEvent(failed, "Creating part", err)
			return
		}

		_, err = part.Write(jsonBytes)
		if err != nil {
			r.sendErrorEvent(failed, "Writing part", err)
			return
		}

		// add all attachments
		for _, attachment := range doc.Attachments {
			partHeaders := textproto.MIMEHeader{}
			partHeaders.Set("Content-Type", attachment.Headers["Content-Type"])
			partHeaders.Set("Content-Disposition", attachment.Headers["Content-Disposition"])
			partAttach, err := writer.CreatePart(partHeaders)
			if err != nil {
				r.sendErrorEvent(failed, "Creating part", err)
				return
			}
			_, err = partAttach.Write(attachment.Data)
			if err != nil {
				r.sendErrorEvent(failed, "Writing part", err)
				return
			}

		}

		err = writer.Close()
		if err != nil {
			r.sendErrorEvent(failed, "Closing writer", err)
			return
		}

		req, err := http.NewRequest("PUT", url, bytes.NewReader(body.Bytes()))
		if err != nil {
			r.sendErrorEvent(failed, "Creating request", err)
			return
		}

		contentType := fmt.Sprintf("multipart/related; boundary=%q", writer.Boundary())
		req.Header.Set("Content-Type", contentType)

		client := globalClient

		resp, err := client.Do(req)
		r.LogTo("Replicate", "bulk get resp: %v, err: %v", resp, err)
		if err != nil {
			r.sendErrorEvent(failed, "Performing request", err)
			return
		}

		defer resp.Body.Close()
		if resp.StatusCode >= 400 {
			r.LogTo("Replicate", "Unexpected response getting bulk get: %v", resp)
			event := NewReplicationEvent(PUSH_ATTACHMENT_DOCS_FAILED)
			r.sendEventWithTimeout(event)
			return
		}

		// TODO: make sure response looks good
		// TODO: could also collect successful docid/revid pairs

	}
	event := NewReplicationEvent(PUSH_ATTACHMENT_DOCS_SUCCEEDED)
	r.sendEventWithTimeout(event)
}

func (r Replication) sendErrorEvent(signal ReplicationEventSignal, msg string, err error) {
	r.LogTo("Replicate", "%v: %v", msg, err)
	event := NewReplicationEvent(signal)
	event.Data = err
	r.sendEventWithTimeout(event)
}

func (r Replication) pushBulkDocs() {

	bulkDocsUrl := r.getBulkDocsUrl()
	bulkDocsRequest := generateBulkDocsRequest(r, r.Documents)

	bulkDocsRequestJson, err := json.Marshal(bulkDocsRequest)
	if err != nil {
		r.LogTo("Replicate", "Error marshaling %v", bulkDocsRequest)
		clog.Error(err)
		event := NewReplicationEvent(PUSH_BULK_DOCS_FAILED)
		r.sendEventWithTimeout(event)
		return
	}

	req, err := http.NewRequest("POST", bulkDocsUrl, bytes.NewReader(bulkDocsRequestJson))
	if err != nil {
		r.LogTo("Replicate", "Error creating request %v", bulkDocsRequestJson)
		clog.Error(err)
		event := NewReplicationEvent(PUSH_BULK_DOCS_FAILED)
		r.sendEventWithTimeout(event)
		return
	}

	client := globalClient

	resp, err := client.Do(req)
	r.LogTo("Replicate", "bulk get resp: %v, err: %v", resp, err)
	if err != nil {
		r.LogTo("Replicate", "Error getting bulk get: %v", err)
		event := NewReplicationEvent(PUSH_BULK_DOCS_FAILED)
		r.sendEventWithTimeout(event)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		r.LogTo("Replicate", "Unexpected response getting bulk get: %v", resp)
		event := NewReplicationEvent(PUSH_BULK_DOCS_FAILED)
		r.sendEventWithTimeout(event)
		return
	}

	bulkDocsResponse := []DocumentRevisionPair{}
	decoder := json.NewDecoder(resp.Body)
	if err = decoder.Decode(&bulkDocsResponse); err != nil {
		r.LogTo("Replicate", "Error decoding json: %v", err)
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

	checkpointUrl := r.getCheckpointUrl()
	r.LogTo("Replicate", "calling pushCheckpointRequest. r.FetchedTargetCheckpoint: %v", r.FetchedTargetCheckpoint)
	pushCheckpointRequest := r.generatePushCheckpointRequest()
	r.LogTo("Replicate", "pushCheckpointRequest %v", pushCheckpointRequest)
	r.LogTo("Replicate", "r.Changes %v", r.Changes)
	r.LogTo("Replicate", "r.Changes.LastSequence %v", r.Changes.LastSequence)

	requestJson, err := json.Marshal(pushCheckpointRequest)
	if err != nil {
		r.LogTo("Replicate", "Error marshaling %v", pushCheckpointRequest)
		clog.Error(err)
		event := NewReplicationEvent(PUSH_CHECKPOINT_FAILED)
		r.sendEventWithTimeout(event)
		return
	}

	req, err := http.NewRequest("PUT", checkpointUrl, bytes.NewReader(requestJson))
	if err != nil {
		r.LogTo("Replicate", "Error creating request %v", requestJson)
		clog.Error(err)
		event := NewReplicationEvent(PUSH_CHECKPOINT_FAILED)
		r.sendEventWithTimeout(event)
		return
	}

	client := globalClient

	resp, err := client.Do(req)
	r.LogTo("Replicate", "push checkpoint resp: %v, err: %v", resp, err)
	if err != nil {
		r.LogTo("Replicate", "Error pushing checkpoint: %v", err)
		event := NewReplicationEvent(PUSH_CHECKPOINT_FAILED)
		r.sendEventWithTimeout(event)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		r.LogTo("Replicate", "Unexpected response pushing checkpoint: %v", resp)
		event := NewReplicationEvent(PUSH_CHECKPOINT_FAILED)
		r.sendEventWithTimeout(event)
		return
	}

	checkpointResponse := PushCheckpointResponse{}
	decoder := json.NewDecoder(resp.Body)
	if err = decoder.Decode(&checkpointResponse); err != nil {
		r.LogTo("Replicate", "Error decoding json: %v", err)
		event := NewReplicationEvent(PUSH_CHECKPOINT_FAILED)
		r.sendEventWithTimeout(event)
		return
	}

	if checkpointResponse.Ok != true {
		r.LogTo("Replicate", "Error, checkpoint response !ok")
		event := NewReplicationEvent(PUSH_CHECKPOINT_FAILED)
		r.sendEventWithTimeout(event)
		return
	}

	event := NewReplicationEvent(PUSH_CHECKPOINT_SUCCEEDED)
	r.sendEventWithTimeout(event)

}

func (r Replication) getCheckpointUrl() string {
	checkpointAddress := r.targetCheckpointAddress()
	return fmt.Sprintf("%s/_local/%s", r.Parameters.GetTargetDbUrl(), checkpointAddress)
}

func (r Replication) getNormalChangesFeedUrl() string {
	changesFeedParams := NewChangesFeedParams()
	changesFeedParams.channels = r.Parameters.Channels
	return r.getChangesFeedUrl(*changesFeedParams)
}

func (r Replication) getLongpollChangesFeedUrl() string {
	changesFeedParams := NewChangesFeedParams()
	changesFeedParams.channels = r.Parameters.Channels
	changesFeedParams.feedType = FEED_TYPE_LONGPOLL
	return r.getChangesFeedUrl(*changesFeedParams)
}

func (r Replication) getChangesFeedUrl(changesFeedParams ChangesFeedParams) string {
	if !r.FetchedTargetCheckpoint.IsEmpty() {
		changesFeedParams.since = r.FetchedTargetCheckpoint.LastSequence
	}
	changesFeedUrl := r.Parameters.getSourceChangesFeedUrl(changesFeedParams)
	return changesFeedUrl
}

func (r Replication) getRevsDiffUrl() string {
	dbUrl := r.Parameters.GetTargetDbUrl()
	return fmt.Sprintf(
		"%s/_revs_diff",
		dbUrl)

}

func (r Replication) getPutDocWithAttatchmentUrl(doc Document) string {
	dbUrl := r.Parameters.GetTargetDbUrl()
	docId := doc.Body["_id"]
	return fmt.Sprintf(
		"%s/%s?new_edits=false",
		dbUrl,
		docId)
}

func (r Replication) getBulkDocsUrl() string {
	dbUrl := r.Parameters.GetTargetDbUrl()
	return fmt.Sprintf(
		"%s/_bulk_docs",
		dbUrl)
}

func (r Replication) getBulkGetUrl() string {
	dbUrl := r.Parameters.GetSourceDbUrl()
	return fmt.Sprintf(
		"%s/_bulk_get?revs=true&attachments=true",
		dbUrl)

}
