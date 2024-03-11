package dag

import (
	"fmt"
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
	TimetableDescription     string                 `json:"timetable_description"`
	Tags                     []string               `json:"tags"`
	MaxActiveTasks           uint8                  `json:"max_active_tasks"`
	MaxActiveRuns            uint8                  `json:"max_active_runs"`
	HasTaskConcurrencyLimits bool                   `json:"has_task_concurrency_limits"`
}

func NewDagByString(input string) *Dag {
	input = strings.TrimSpace(input)
	pairs := strings.Split(input, ",")
	re_datetime := regexp.MustCompile(`(\d+)-(\d+)-(\d+)`)
	dag := &Dag{}
	for _, pair := range pairs {
		keyValue := strings.SplitN(pair, "=", 2)
		if len(keyValue) != 2 {
			continue
		}
		key := strings.TrimSpace(keyValue[0])
		value := strings.TrimSpace(keyValue[1])
		fmt.Println(key, value)
		switch key {
		case "name":
			dag.Name = value
			fmt.Println(dag.Name)
		case "start_date":
			fmt.Println("Start Date:", value)
			matches := re_datetime.FindStringSubmatch(value)
			fmt.Println(matches)
			var startDate time.Time
			if len(matches) == 4 {
				year, _ := strconv.Atoi(matches[1])
				month, _ := strconv.Atoi(matches[2])
				day, _ := strconv.Atoi(matches[3])
				startDate = time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
			}
			dag.StartDate = startDate
		case "schedule_interval":
			dag.ScheduleInterval = value
		case "owners":
			dag.Owners = strings.Split(value, " ")
		case "is_paused":
			isPaused, err := strconv.ParseBool(value)
			if err != nil {
				isPaused = true
			}
			dag.IsPaused = isPaused

		case "is_active":
			isActive, err := strconv.ParseBool(value)
			if err != nil {
				isActive = false
			}
			dag.IsActive = isActive

		case "description":
			dag.Description = value

		case "timetable_description":
			dag.TimetableDescription = value

		case "tags":
			dag.Tags = strings.Split(value, " ")

		case "max_active_tasks":
			maxActiveTasks, err := strconv.ParseUint(value, 10, 8)
			if err != nil {
				maxActiveTasks = 16
			}
			dag.MaxActiveTasks = uint8(maxActiveTasks)

		case "max_active_runs":
			maxActiveRuns, err := strconv.ParseUint(value, 10, 8)
			if err != nil {
				maxActiveRuns = 16
			}
			dag.MaxActiveRuns = uint8(maxActiveRuns)

		case "has_task_concurrency_limits":
			hasTaskConcurrencyLimits, err := strconv.ParseBool(value)
			if err != nil {
				hasTaskConcurrencyLimits = false
			}
			dag.HasTaskConcurrencyLimits = hasTaskConcurrencyLimits
		}
	}
	return dag
}
