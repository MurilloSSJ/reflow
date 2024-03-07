package dag_events

import (
	"fmt"
	"reflow/dag_watch/v2/src/connectors/reflow_api"
	"reflow/dag_watch/v2/src/validators/validator_dag"
)

type DagEventAPIHandler struct {
	apiClient *reflow_api.ReflowAPIClient
	validator *validator_dag.ValidatorDag
}

func NewCreateDagEventAPI() *DagEventAPIHandler {
	return &DagEventAPIHandler{
		apiClient: reflow_api.NewReflowAPIClient(),
		validator: validator_dag.NewValidatorDag(),
	}
}

func (de *DagEventAPIHandler) HandleOnCreateDag(path string) {
	fmt.Println("Creating dag:", path)
	args, err := de.validator.ExtractArgs(path)
	if err != nil {
		fmt.Println("Error:", err)
		de.apiClient.SetDagError()
		return
	}
	fmt.Println(args)
}
func (de *DagEventAPIHandler) HandleOnDeleteDag(path string) {
	fmt.Println("Deleting dag:", path)
}
func (de *DagEventAPIHandler) HandleOnUpdateDag(path string) {
	fmt.Println("Updating dag:", path)
	args, err := de.validator.ExtractArgs(path)
	if err != nil {
		fmt.Println("Error:", err)
		de.apiClient.SetDagError()
		return
	}
	fmt.Println(args[1])
}
