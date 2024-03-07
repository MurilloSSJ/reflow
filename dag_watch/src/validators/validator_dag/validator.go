package validator_dag

import (
	"fmt"
	"os"
	"os/exec"
	"regexp"
)

type ValidatorDag struct {
}

func NewValidatorDag() *ValidatorDag {
	return &ValidatorDag{}
}

func (vd *ValidatorDag) ValidateContent(filepath string) ([]byte, error) {
	cmd := fmt.Sprintf("python3 -m py_compile '%s' 2>&1 >/dev/null", filepath)
	return exec.Command("bash", "-c", cmd).Output()
}

func (vd *ValidatorDag) ValidateIsDagFile(filepath string) bool {
	if filepath[len(filepath)-3:] != ".py" {
		return false
	}
	re := regexp.MustCompile(`@DAG\((.*?)\)`)
	content, err := os.ReadFile(filepath)
	if err != nil {
		return false
	}
	return re.Match(content)
}

func (vd *ValidatorDag) ExtractArgs(filepath string) ([]string, error) {
	re := regexp.MustCompile(`@DAG\((.*?)\)`)
	content, err := os.ReadFile(filepath)
	if err != nil {
		return nil, err
	}
	return re.FindStringSubmatch(string(content)), nil
}
