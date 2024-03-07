package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflow/dag_watch/v2/src/dag_events"
	"reflow/dag_watch/v2/src/validators/validator_dag"

	"github.com/fsnotify/fsnotify"
)

func main() {
	dagApi := dag_events.NewCreateDagEventAPI()
	validator := validator_dag.NewValidatorDag()
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
				if event.Op&fsnotify.Write == fsnotify.Write {
					fileInfo, err := os.Stat(event.Name)
					if err != nil || fileInfo.IsDir() {
						log.Fatal(err)
						continue
					}
					if notValid := validator.ValidateIsDagFile(event.Name); !notValid {
						continue
					}
					dagApi.HandleOnUpdateDag(event.Name)

				}
				if event.Op&fsnotify.Create == fsnotify.Create {
					fileInfo, err := os.Stat(event.Name)
					if err != nil || fileInfo.IsDir() {
						log.Fatal(err)
						continue
					}
					absPath, err := filepath.Abs(event.Name)
					if err != nil {
						log.Fatal(err)
					}
					dagApi.HandleOnCreateDag(absPath)
					continue
				}
				if event.Op&fsnotify.Remove == fsnotify.Remove {
					absPath, err := filepath.Abs(event.Name)
					if err != nil {
						log.Fatal(err)
					}
					dagApi.HandleOnDeleteDag(absPath)
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
