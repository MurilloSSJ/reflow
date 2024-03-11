package main

import (
	"fmt"
	"log"
	"reflow/dag_watch/v2/src/dag_events"
	"reflow/dag_watch/v2/src/validators/event_validator"

	"github.com/fsnotify/fsnotify"
)

func main() {
	dagApi := dag_events.NewCreateDagEventAPI()
	watcher, err := fsnotify.NewWatcher()

	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()

	done := make(chan bool)

	go func() {
		for {
			select {
			case event := <-watcher.Events:
				if notValid := event_validator.ValidateEvent(event); !notValid {
					continue
				}
				dagApi.HandleOnUpdateDag(event.Name)

				if event.Op&fsnotify.Create == fsnotify.Create {
					dagApi.HandleOnCreateDag(event.Name)
					continue
				}
				if event.Op&fsnotify.Remove == fsnotify.Remove {
					dagApi.HandleOnDeleteDag(event.Name)
				}
			case err := <-watcher.Errors:
				log.Println("Erro:", err)
			}
		}
	}()

	err = watcher.Add("../dags")
	if err != nil {
		fmt.Println("Error", err)
		log.Fatal(err)
	}
	<-done
}
