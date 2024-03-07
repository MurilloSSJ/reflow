package settings

import (
	"reflow/dag_watch/v2/src/dag_events"
)

type Settings struct {
	DagEventDefault *dag_events.DagEventAPIHandler
}

func NewSettings() *Settings {
	return &Settings{
		DagEventDefault: dag_events.NewCreateDagEventAPI(),
	}
}
