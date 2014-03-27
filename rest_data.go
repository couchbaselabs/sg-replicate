package synctube

import (
	"github.com/couchbaselabs/logg"
	"strconv"
)

type Checkpoint struct {
	LastSequence string `json:"lastSequence"`
	Id           string `json:"id"`
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

func (change Change) getRevs() []string {
	revs := []string{}
	for _, changedRev := range change.ChangedRevs {
		revs = append(revs, changedRev.Revision)
	}
	return revs
}

type RevsDiffMap map[string][]string

func generateRevsDiffMap(changes Changes) RevsDiffMap {

	revsDiffMap := RevsDiffMap{}
	for _, change := range changes.Results {
		revsDiffMap[change.Id] = change.getRevs()
	}
	return revsDiffMap
}
