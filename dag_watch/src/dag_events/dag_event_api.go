package dag_events

import (
	"fmt"
	"reflow/dag_watch/v2/src/connectors/reflow_api"
	"reflow/dag_watch/v2/src/dag"
	"reflow/dag_watch/v2/src/validators/validator_dag"
)

type DagEventAPIHandler struct {
	apiClient *reflow_api.ReflowAPIClient
}

func NewCreateDagEventAPI() *DagEventAPIHandler {
	return &DagEventAPIHandler{
		apiClient: reflow_api.NewReflowAPIClient(),
	}
}

func (de *DagEventAPIHandler) HandleOnCreateDag(path string) {
	fmt.Println("Creating dag:", path)
	args, err := validator_dag.ExtractArgs(path)
	if err != nil {
		fmt.Println("Error:", err)
		de.apiClient.SetDagError()
		return
	}
	dag := dag.NewDagByString(args[1])
	fmt.Println(dag)
}
func (de *DagEventAPIHandler) HandleOnDeleteDag(path string) {
	fmt.Println("Deleting dag:", path)
}
func (de *DagEventAPIHandler) HandleOnUpdateDag(path string) {
	fmt.Println("Updating dag:", path)
	args, err := validator_dag.ExtractArgs(path)
	if err != nil {
		fmt.Println("Error:", err)
		de.apiClient.SetDagError()
		return
	}
	dag := dag.NewDagByString(args[1])
	fmt.Println(dag)
}
