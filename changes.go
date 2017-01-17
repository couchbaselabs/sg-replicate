package sgreplicate

type ChangedRev struct {
	Revision string `json:"rev"`
}

type Change struct {
	Sequence    interface{}  `json:"seq"`
	Id          string       `json:"id"`
	ChangedRevs []ChangedRev `json:"changes"`
	Removed     []string     `json:"removed"`
	Deleted     bool         `json:"deleted"`
}

type Changes struct {
	Results      []Change    `json:"results"`
	LastSequence interface{} `json:"last_seq"`
}

// If any changes entries have a "removed" property, ignore them.
// See https://github.com/couchbase/sync_gateway/issues/2212 for details.
func filterRemovedChanges(changes Changes) Changes {
	newChanges := Changes{}
	newChanges.LastSequence = changes.LastSequence

	newChanges.Results = []Change{}
	for _, change := range changes.Results {
		if change.Removed != nil && len(change.Removed) > 0 {
			// This doc was removed from channel, ignore it
			continue
		}
		newChanges.Results = append(newChanges.Results, change)
	}

	return newChanges

}
