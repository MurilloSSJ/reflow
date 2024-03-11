package event_validator

import (
	"log"
	"os"
	"reflow/dag_watch/v2/src/validators/validator_dag"

	"github.com/fsnotify/fsnotify"
)

func ValidateEvent(event fsnotify.Event) bool {
	fileInfo, err := os.Stat(event.Name)
	if err != nil || fileInfo.IsDir() {
		log.Fatal(err)
		return false
	}
	if notValid := validator_dag.ValidateIsDagFile(event.Name); !notValid {
		return false
	}
	return true
}
