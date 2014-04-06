package synctube

import (
	"fmt"
)

const FEED_TYPE_LONGPOLL = "longpoll"
const FEED_TYPE_NORMAL = "normal"

type ChangesFeedParams struct {
	feedType            string // eg, "normal" or "longpoll"
	limit               int    // eg, 50
	heartbeatTimeMillis int    // eg, 300000
	feedStyle           string // eg, "all_docs"
}

func NewChangesFeedParams() *ChangesFeedParams {
	return &ChangesFeedParams{
		feedType:            FEED_TYPE_NORMAL,
		limit:               DefaultChangesFeedLimit,
		heartbeatTimeMillis: 300000,
		feedStyle:           "all_docs",
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
