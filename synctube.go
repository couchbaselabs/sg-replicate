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

	"github.com/couchbaselabs/logg"
	"github.com/mreiferson/go-httpclient"
)

var globalClient *http.Client

type Replication struct {
	Parameters              ReplicationParameters
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
		Parameters:       params,
		EventChan:        eventChan,
		NotificationChan: notificationChan,
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

// Run a one-shot replication synchronously (eg, block until finished)
func RunOneShotReplication(params ReplicationParameters) (ReplicationStatus, error) {

	notificationChan := make(chan ReplicationNotification)
	replication := NewReplication(params, notificationChan)
	replication.Start()

	for {
		select {
		case replicationNotification := <-notificationChan:
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
		logg.LogTo("Replicate", "new state: %v", state)
	}
	logg.LogTo("Replicate", "processEvents() is done")

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

	client := globalClient
	req, _ := http.NewRequest("GET", destUrl, nil)
	resp, err := client.Do(req)
	logg.LogTo("Replicate", "resp: %v, err: %v", resp, err)

	if err != nil {
		logg.LogTo("Replicate", "Error getting checkpoint: %v", err)
		event := NewReplicationEvent(FETCH_CHECKPOINT_FAILED)
		r.sendEventWithTimeout(event)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		// valid response, continue with empty remote checkpoint
		logg.LogTo("Replicate", "404 trying to get checkpoint, continue..")
		event := NewReplicationEvent(FETCH_CHECKPOINT_SUCCEEDED)
		checkpoint := Checkpoint{LastSequence: "0"}
		event.Data = checkpoint
		r.sendEventWithTimeout(event)
	} else if resp.StatusCode >= 400 {
		// we got an error, lets abort
		logg.LogTo("Replicate", "4xx error(not 404) getting checkpoint")
		event := NewReplicationEvent(FETCH_CHECKPOINT_FAILED)
		r.sendEventWithTimeout(event)
	} else if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		// looks like we got a valid checkpoint
		logg.LogTo("Replicate", "valid checkpoint")

		bodyText, _ := ioutil.ReadAll(resp.Body)
		logg.LogTo("Replicate", "body: %v", string(bodyText))
		checkpoint := Checkpoint{}
		err = json.Unmarshal(bodyText, &checkpoint)
		if err != nil {
			logg.LogTo("Replicate", "Error unmarshalling checkpoint")
			logg.LogError(err)
			event := NewReplicationEvent(FETCH_CHECKPOINT_FAILED)
			r.sendEventWithTimeout(event)
			return
		}
		if len(checkpoint.LastSequence) == 0 {
			logg.LogTo("Replicate", "Invalid checkpoint, no lastsequence")
			event := NewReplicationEvent(FETCH_CHECKPOINT_FAILED)
			r.sendEventWithTimeout(event)
			return
		}
		expectedId := fmt.Sprintf("_local/%s", r.targetCheckpointAddress())
		if checkpoint.Id != expectedId {
			logg.LogTo("Replicate", "Got %s, expected %s", checkpoint.Id, expectedId)
			event := NewReplicationEvent(FETCH_CHECKPOINT_FAILED)
			r.sendEventWithTimeout(event)
			return
		}
		logg.LogTo("Replicate", "checkpoint: %v", checkpoint.LastSequence)
		event := NewReplicationEvent(FETCH_CHECKPOINT_SUCCEEDED)
		event.Data = checkpoint
		logg.LogTo("Replicate", "event: %v", event)

		r.sendEventWithTimeout(event)

	} else {
		// unexpected http status, abort
		logg.LogTo("Replicate", "unexpected http status %v", resp.StatusCode)
		event := NewReplicationEvent(FETCH_CHECKPOINT_FAILED)
		r.sendEventWithTimeout(event)
	}

}

func (r Replication) fetchChangesFeed() {

	destUrl := r.getNormalChangesFeedUrl()

	client := globalClient
	req, _ := http.NewRequest("GET", destUrl, nil)
	resp, err := client.Do(req)
	logg.LogTo("Replicate", "changes feed resp: %v, err: %v", resp, err)
	if err != nil {
		logg.LogTo("Replicate", "Error getting changes feed: %v", err)
		event := NewReplicationEvent(FETCH_CHANGES_FEED_FAILED)
		r.sendEventWithTimeout(event)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		logg.LogTo("Replicate", "Error getting changes feed.  Resp: %v", resp)
		event := NewReplicationEvent(FETCH_CHANGES_FEED_FAILED)
		logg.LogTo("Replicate", "channel: %v", r.EventChan)
		r.sendEventWithTimeout(event)
		return
	}

	bodyText, _ := ioutil.ReadAll(resp.Body)
	changes := Changes{}
	err = json.Unmarshal(bodyText, &changes)
	if err != nil {
		logg.LogTo("Replicate", "Error unmarshalling change")
		logg.LogError(err)
		event := NewReplicationEvent(FETCH_CHANGES_FEED_FAILED)
		r.sendEventWithTimeout(event)
		return
	}
	event := NewReplicationEvent(FETCH_CHANGES_FEED_SUCCEEDED)
	event.Data = changes
	logg.LogTo("Replicate", "event: %v", event)
	r.sendEventWithTimeout(event)

}

func (r Replication) fetchRevsDiff() {

	revsDiffUrl := r.getRevsDiffUrl()
	revsDiffMap := generateRevsDiffMap(r.Changes)
	revsDiffMapJson, err := json.Marshal(revsDiffMap)
	if err != nil {
		logg.LogTo("Replicate", "Error marshaling %v", revsDiffMap)
		logg.LogError(err)
		event := NewReplicationEvent(FETCH_REVS_DIFF_FAILED)
		r.sendEventWithTimeout(event)
		return
	}

	req, err := http.NewRequest("POST", revsDiffUrl, bytes.NewReader(revsDiffMapJson))
	if err != nil {
		logg.LogTo("Replicate", "Error creating request %v", revsDiffMapJson)
		logg.LogError(err)
		event := NewReplicationEvent(FETCH_REVS_DIFF_FAILED)
		r.sendEventWithTimeout(event)
		return
	}

	client := globalClient

	resp, err := client.Do(req)
	logg.LogTo("Replicate", "revs diff resp: %v, err: %v", resp, err)
	if err != nil {
		logg.LogTo("Replicate", "Error getting revs diff: %v", err)
		event := NewReplicationEvent(FETCH_REVS_DIFF_FAILED)
		r.sendEventWithTimeout(event)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		logg.LogTo("Replicate", "Unexpected response getting revs diff: %v", resp)
		event := NewReplicationEvent(FETCH_REVS_DIFF_FAILED)
		r.sendEventWithTimeout(event)
		return
	}

	bodyText, _ := ioutil.ReadAll(resp.Body)
	revsDiffJson := RevsDiffResponseMap{}
	err = json.Unmarshal(bodyText, &revsDiffJson)
	if err != nil {
		logg.LogTo("Replicate", "Error unmarshalling json")
		logg.LogError(err)
		event := NewReplicationEvent(FETCH_REVS_DIFF_FAILED)
		r.sendEventWithTimeout(event)
		return
	}
	event := NewReplicationEvent(FETCH_REVS_DIFF_SUCCEEDED)
	event.Data = revsDiffJson
	logg.LogTo("Replicate", "event: %v", event)
	r.sendEventWithTimeout(event)

}

func (r Replication) fetchBulkGet() {

	bulkGetUrl := r.getBulkGetUrl()
	logg.LogTo("Replicate", "bulkGetUrl %v", bulkGetUrl)
	bulkGetRequest := generateBulkGetRequest(r.RevsDiff)

	bulkGetRequestJson, err := json.Marshal(bulkGetRequest)
	if err != nil {
		logg.LogTo("Replicate", "Error marshaling %v", bulkGetRequest)
		logg.LogError(err)
		event := NewReplicationEvent(FETCH_BULK_GET_FAILED)
		r.sendEventWithTimeout(event)
		return
	}

	req, err := http.NewRequest("POST", bulkGetUrl, bytes.NewReader(bulkGetRequestJson))
	logg.LogTo("Replicate", "bulkGet req %v", req)
	if err != nil {
		logg.LogTo("Replicate", "Error creating request %v", bulkGetRequestJson)
		logg.LogError(err)
		event := NewReplicationEvent(FETCH_BULK_GET_FAILED)
		r.sendEventWithTimeout(event)
		return
	}

	client := globalClient

	resp, err := client.Do(req)
	logg.LogTo("Replicate", "bulk get resp: %v, err: %v", resp, err)
	if err != nil {
		logg.LogTo("Replicate", "Error getting bulk get: %v", err)
		event := NewReplicationEvent(FETCH_BULK_GET_FAILED)
		r.sendEventWithTimeout(event)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		logg.LogTo("Replicate", "Unexpected response getting bulk get: %v", resp)
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
	documents := []Document{}

	for {
		mainPart, err := reader.NextPart()
		if err == io.EOF {
			break
		} else if err != nil {
			logg.LogTo("Replicate", "Error getting next part: %v", err)
			event := NewReplicationEvent(FETCH_BULK_GET_FAILED)
			event.Data = err
			r.sendEventWithTimeout(event)
			return
		}

		logg.LogTo("Replicate", "mainPart: %v.  Header: %v", mainPart, mainPart.Header)
		mainPartContentTypes := mainPart.Header["Content-Type"] // why a slice?
		mainPartContentType := mainPartContentTypes[0]
		contentType, attrs, _ := mime.ParseMediaType(mainPartContentType)
		logg.LogTo("Replicate", "contentType: %v", contentType)
		logg.LogTo("Replicate", "boundary: %v", attrs["boundary"])
		switch contentType {
		case "application/json":
			documentBody := DocumentBody{}
			decoder := json.NewDecoder(mainPart)

			if err = decoder.Decode(&documentBody); err != nil {
				logg.LogTo("Replicate", "Error decoding part: %v", err)
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
					logg.LogTo("Replicate", "err nested nextpart: %v", err)
					event := NewReplicationEvent(FETCH_BULK_GET_FAILED)
					event.Data = err
					r.sendEventWithTimeout(event)
					return
				}
				logg.LogTo("Replicate", "nestedPart: %v.  Header: %v", nestedPart, nestedPart.Header)
				nestedPartContentTypes := nestedPart.Header["Content-Type"]
				nestedPartContentType := nestedPartContentTypes[0]
				nestedContentType, nestedAttrs, _ := mime.ParseMediaType(nestedPartContentType)
				logg.LogTo("Replicate", "nestedContentType: %v", nestedContentType)
				logg.LogTo("Replicate", "nestedAttrs: %v", nestedAttrs)

				switch nestedContentType {
				case "application/json":
					nestedDecoder := json.NewDecoder(nestedPart)
					nestedDocBody := DocumentBody{}
					if err = nestedDecoder.Decode(&nestedDocBody); err != nil {
						logg.LogTo("Replicate", "Error decoding part: %v", err)
						event := NewReplicationEvent(FETCH_BULK_GET_FAILED)
						event.Data = err
						r.sendEventWithTimeout(event)
						return
					}
					nestedDoc.Body = nestedDocBody
					nestedPart.Close()

				default:
					// handle attachment
					attachment, err := NewAttachment(nestedPart)
					if err != nil {
						logg.LogTo("Replicate", "Error decoding attachment: %v", err)
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
			logg.LogTo("Replicate", "ignoring unexpected content type: %v", contentType)

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
		logg.LogTo("Replicate", "pushAttatchmentDocs url: %v", url)
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
		logg.LogTo("Replicate", "bulk get resp: %v, err: %v", resp, err)
		if err != nil {
			r.sendErrorEvent(failed, "Performing request", err)
			return
		}

		defer resp.Body.Close()
		if resp.StatusCode >= 400 {
			logg.LogTo("Replicate", "Unexpected response getting bulk get: %v", resp)
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
	logg.LogTo("Replicate", "%v: %v", msg, err)
	event := NewReplicationEvent(signal)
	event.Data = err
	r.sendEventWithTimeout(event)
}

func (r Replication) pushBulkDocs() {

	bulkDocsUrl := r.getBulkDocsUrl()
	bulkDocsRequest := generateBulkDocsRequest(r.Documents)

	bulkDocsRequestJson, err := json.Marshal(bulkDocsRequest)
	if err != nil {
		logg.LogTo("Replicate", "Error marshaling %v", bulkDocsRequest)
		logg.LogError(err)
		event := NewReplicationEvent(PUSH_BULK_DOCS_FAILED)
		r.sendEventWithTimeout(event)
		return
	}

	req, err := http.NewRequest("POST", bulkDocsUrl, bytes.NewReader(bulkDocsRequestJson))
	if err != nil {
		logg.LogTo("Replicate", "Error creating request %v", bulkDocsRequestJson)
		logg.LogError(err)
		event := NewReplicationEvent(PUSH_BULK_DOCS_FAILED)
		r.sendEventWithTimeout(event)
		return
	}

	client := globalClient

	resp, err := client.Do(req)
	logg.LogTo("Replicate", "bulk get resp: %v, err: %v", resp, err)
	if err != nil {
		logg.LogTo("Replicate", "Error getting bulk get: %v", err)
		event := NewReplicationEvent(PUSH_BULK_DOCS_FAILED)
		r.sendEventWithTimeout(event)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		logg.LogTo("Replicate", "Unexpected response getting bulk get: %v", resp)
		event := NewReplicationEvent(PUSH_BULK_DOCS_FAILED)
		r.sendEventWithTimeout(event)
		return
	}

	bulkDocsResponse := []DocumentRevisionPair{}
	decoder := json.NewDecoder(resp.Body)
	if err = decoder.Decode(&bulkDocsResponse); err != nil {
		logg.LogTo("Replicate", "Error decoding json: %v", err)
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
	logg.LogTo("Replicate", "calling pushCheckpointRequest. r.FetchedTargetCheckpoint: %v", r.FetchedTargetCheckpoint)
	pushCheckpointRequest := r.generatePushCheckpointRequest()
	logg.LogTo("Replicate", "pushCheckpointRequest %v", pushCheckpointRequest)
	logg.LogTo("Replicate", "r.Changes %v", r.Changes)
	logg.LogTo("Replicate", "r.Changes.LastSequence %v", r.Changes.LastSequence)

	requestJson, err := json.Marshal(pushCheckpointRequest)
	if err != nil {
		logg.LogTo("Replicate", "Error marshaling %v", pushCheckpointRequest)
		logg.LogError(err)
		event := NewReplicationEvent(PUSH_CHECKPOINT_FAILED)
		r.sendEventWithTimeout(event)
		return
	}

	req, err := http.NewRequest("PUT", checkpointUrl, bytes.NewReader(requestJson))
	if err != nil {
		logg.LogTo("Replicate", "Error creating request %v", requestJson)
		logg.LogError(err)
		event := NewReplicationEvent(PUSH_CHECKPOINT_FAILED)
		r.sendEventWithTimeout(event)
		return
	}

	client := globalClient

	resp, err := client.Do(req)
	logg.LogTo("Replicate", "push checkpoint resp: %v, err: %v", resp, err)
	if err != nil {
		logg.LogTo("Replicate", "Error pushing checkpoint: %v", err)
		event := NewReplicationEvent(PUSH_CHECKPOINT_FAILED)
		r.sendEventWithTimeout(event)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		logg.LogTo("Replicate", "Unexpected response pushing checkpoint: %v", resp)
		event := NewReplicationEvent(PUSH_CHECKPOINT_FAILED)
		r.sendEventWithTimeout(event)
		return
	}

	checkpointResponse := PushCheckpointResponse{}
	decoder := json.NewDecoder(resp.Body)
	if err = decoder.Decode(&checkpointResponse); err != nil {
		logg.LogTo("Replicate", "Error decoding json: %v", err)
		event := NewReplicationEvent(PUSH_CHECKPOINT_FAILED)
		r.sendEventWithTimeout(event)
		return
	}

	if checkpointResponse.Ok != true {
		logg.LogTo("Replicate", "Error, checkpoint response !ok")
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
	dbUrl := r.Parameters.getTargetDbUrl()
	return fmt.Sprintf(
		"%s/_revs_diff",
		dbUrl)

}

func (r Replication) getPutDocWithAttatchmentUrl(doc Document) string {
	dbUrl := r.Parameters.getTargetDbUrl()
	docId := doc.Body["_id"]
	return fmt.Sprintf(
		"%s/%s?new_edits=false",
		dbUrl,
		docId)
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
