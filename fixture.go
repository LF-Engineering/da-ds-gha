package dadsgha

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

// RawEndpoint holds data source endpoint with possible flags how to generate the final endpoints
// flags can be "type: github_org/github_user" which means that we need to get actual repository list from github org/user
type RawEndpoint struct {
	Name    string            `yaml:"name"`
	Flags   map[string]string `yaml:"flags"`
	Skip    []string          `yaml:"skip"`
	Only    []string          `yaml:"only"`
	Project string            `yaml:"project"`
	SkipREs []*regexp.Regexp  `yaml:"-"`
	OnlyREs []*regexp.Regexp  `yaml:"-"`
}

// EndpointIncluded - checks if given endpoint's origin should be included or excluded based on endpoint's skip/only regular expressions lists
// First return value specifies if endpoint is included or not
// Second value specifies: 1 - included by 'only' condition, 2 - skipped by 'skip' condition
func EndpointIncluded(ctx *Ctx, ep *RawEndpoint, origin string) (bool, int) {
	for _, skipRE := range ep.SkipREs {
		if skipRE.MatchString(origin) {
			if ctx.Debug > 0 {
				fmt.Printf("%s: skipped %s (%v)\n", ep.Name, origin, skipRE)
			}
			return false, 2
		}
	}
	if len(ep.OnlyREs) == 0 {
		if ctx.Debug > 0 {
			fmt.Printf("%s: included all\n", ep.Name)
		}
		return true, 0
	}
	included := false
	inc := 0
	for _, onlyRE := range ep.OnlyREs {
		if onlyRE.MatchString(origin) {
			if ctx.Debug > 0 {
				fmt.Printf("%s: included %s (%v)\n", ep.Name, origin, onlyRE)
			}
			included = true
			inc = 1
			break
		}
	}
	return included, inc
}

// Project holds project data and list of endpoints
type Project struct {
	Name          string        `yaml:"name"`
	RawEndpoints  []RawEndpoint `yaml:"endpoints"`
	HistEndpoints []RawEndpoint `yaml:"historical_endpoints"`
}

// DataSource contains data source spec from dev-analytics-api
type DataSource struct {
	Slug          string        `yaml:"slug"`
	Projects      []Project     `yaml:"projects"`
	RawEndpoints  []RawEndpoint `yaml:"endpoints"`
	HistEndpoints []RawEndpoint `yaml:"historical_endpoints"`
	IndexSuffix   string        `yaml:"index_suffix"`
}

// Native - keeps fixture slug and eventual global affiliation source
type Native struct {
	Slug string `yaml:"slug"`
}

// Fixture contains full YAML structure of dev-analytics-api fixture files
type Fixture struct {
	Disabled    bool         `yaml:"disabled"`
	Native      Native       `yaml:"native"`
	DataSources []DataSource `yaml:"data_sources"`
}

// GetFixtures - read all fixture files
func GetFixtures(ctx *Ctx, path string) (fixtures []string) {
	dtStart := time.Now()
	ctx.ExecOutput = true
	defer func() {
		ctx.ExecOutput = false
	}()
	if path == "" {
		path = "data/"
	}
	res, err := ExecCommand(
		ctx,
		[]string{
			"find",
			path,
			"-type",
			"f",
			"-iname",
			"*.y*ml",
		},
		nil,
		nil,
	)
	dtEnd := time.Now()
	if err != nil {
		Fatalf("Error finding fixtures (took %v): %+v\n", dtEnd.Sub(dtStart), err)
	}
	fixtures = strings.Split(res, "\n")
	if ctx.Debug > 0 {
		Printf("Fixtures to process: %+v\n", fixtures)
	}
	return
}
