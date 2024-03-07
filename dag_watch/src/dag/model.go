package dag

import (
	"regexp"
	"strconv"
	"strings"
	"time"
)

type Dag struct {
	DefaultArgs              map[string]interface{} `json:"default_args"`
	Owners                   []string               `json:"owners"`
	StartDate                time.Time              `json:"start_date"`
	ScheduleInterval         string                 `json:"schedule_interval"`
	Name                     string                 `json:"name"`
	IsPaused                 bool                   `json:"is_paused"`
	IsActive                 bool                   `json:"is_active"`
	Description              string                 `json:"description"`
	DefaultView              string                 `json:"default_view"`
	TimetableDescription     string                 `json:"timetable_description"`
	Tags                     []string               `json:"tags"`
	MaxActiveTasks           uint8                  `json:"max_active_tasks"`
	MaxActiveRuns            uint8                  `json:"max_active_runs"`
	HasTaskConcurrencyLimits bool                   `json:"has_task_concurrency_limits"`
}

func NewDagByString(input string) *Dag {
	input = strings.TrimSpace(input)
	pairs := strings.Split(input, ",")
	re_datetime := regexp.MustCompile(`datetime\((\d+),(\d+),(\d+)\)`)
	dag := &Dag{}
	for _, pair := range pairs {
		keyValue := strings.SplitN(pair, "=", 2)
		if len(keyValue) != 2 {
			continue
		}

		key := strings.TrimSpace(keyValue[0])
		value := strings.TrimSpace(keyValue[1])

		switch key {
		case "name":
			dag.Name = value
		case "start_date":
			matches := re_datetime.FindStringSubmatch(value)
			year, _ := strconv.Atoi(matches[1])
			month, _ := strconv.Atoi(matches[2])
			day, _ := strconv.Atoi(matches[3])
			dag.StartDate = time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
		case "schedule_interval":
			dag.ScheduleInterval = value
		case "owners":
			dag.Owners = strings.Split(value, " ")
		}

	}
}
