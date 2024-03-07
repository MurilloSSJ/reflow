package reflow_api

import (
	"os"
)

type ReflowAPI interface {
	CreateDag()
	DeleteDag()
	UpdateDag()
	SetDagError()
}

type ReflowAPIClient struct {
	baseURL string
	port    int
}

func NewReflowAPIClient() *ReflowAPIClient {
	return &ReflowAPIClient{
		baseURL: os.Getenv("REFLOW_API_URL"),
		port:    8080,
	}
}

func (r *ReflowAPIClient) CreateDag() {
	// This is the actual implementation of the CreateDag method
}

func (r *ReflowAPIClient) DeleteDag() {
	// This is the actual implementation of the DeleteDag method
}

func (r *ReflowAPIClient) UpdateDag() {
	// This is the actual implementation of the UpdateDag method
}

func (r *ReflowAPIClient) SetDagError() {
	// This is the actual implementation of the SetDagError method
}
