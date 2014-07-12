package synctube

import (
	"fmt"

	"github.com/couchbaselabs/logg"
)

const FEED_TYPE_LONGPOLL = "longpoll"
const FEED_TYPE_NORMAL = "normal"

type ChangesFeedParams struct {
	feedType            string      // eg, "normal" or "longpoll"
	limit               int         // eg, 50
	heartbeatTimeMillis int         // eg, 300000
	feedStyle           string      // eg, "all_docs"
	since               interface{} // eg, "3"
}

func NewChangesFeedParams() *ChangesFeedParams {
	return &ChangesFeedParams{
		feedType:            FEED_TYPE_NORMAL,
		limit:               DefaultChangesFeedLimit,
		heartbeatTimeMillis: 30 * 1000,
		feedStyle:           "all_docs",
		since:               EMPTY_SEQUENCE_NUMBER,
	}
}

func (p ChangesFeedParams) FeedType() string {
	return p.feedType
}

func (p ChangesFeedParams) Limit() string {
	return fmt.Sprintf("%v", p.limit)
}

func (p ChangesFeedParams) HeartbeatTimeMillis() string {
	return fmt.Sprintf("%v", p.heartbeatTimeMillis)
}

func (p ChangesFeedParams) FeedStyle() string {
	return p.feedStyle
}

func (p ChangesFeedParams) SequenceNumber() string {
	return SequenceNumberToString(p.since)
}

func (p ChangesFeedParams) String() string {
	params := fmt.Sprintf(
		"feed=%s&limit=%s&heartbeat=%s&style=%s",
		p.FeedType(),
		p.Limit(),
		p.HeartbeatTimeMillis(),
		p.FeedStyle(),
	)
	if len(p.SequenceNumber()) > 0 {
		params = fmt.Sprintf("%v&since=%s", params, p.SequenceNumber())
	}
	logg.LogTo("TEST", "changesFeedParams: %v", params)
	return params
}
