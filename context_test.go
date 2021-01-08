package dadsgha

import (
	"fmt"
	"os"
	"reflect"
	"regexp"
	"testing"
	"time"

	lib "github.com/LF-Engineering/da-ds-gha"
)

// Copies Ctx structure
func copyContext(in *lib.Ctx) *lib.Ctx {
	out := lib.Ctx{
		Debug:             in.Debug,
		CmdDebug:          in.CmdDebug,
		ST:                in.ST,
		NCPUs:             in.NCPUs,
		NCPUsScale:        in.NCPUsScale,
		ExecFatal:         in.ExecFatal,
		ExecQuiet:         in.ExecQuiet,
		ExecOutput:        in.ExecOutput,
		GitHubOAuth:       in.GitHubOAuth,
		ESURL:             in.ESURL,
		ESBulkSize:        in.ESBulkSize,
		LoadConfig:        in.LoadConfig,
		SaveConfig:        in.SaveConfig,
		NoIncremental:     in.NoIncremental,
		NoGHAMap:          in.NoGHAMap,
		NoGHARepoDates:    in.NoGHARepoDates,
		MaxParallelSHAs:   in.MaxParallelSHAs,
		MaxJSONsBytes:     in.MaxJSONsBytes,
		MemHeartBeatBytes: in.MemHeartBeatBytes,
		ConfigFile:        in.ConfigFile,
		TestMode:          in.TestMode,
	}
	return &out
}

// Dynamically sets Ctx fields (uses map of field names into their new values)
func dynamicSetFields(t *testing.T, ctx *lib.Ctx, fields map[string]interface{}) *lib.Ctx {
	// Prepare mapping field name -> index
	valueOf := reflect.Indirect(reflect.ValueOf(*ctx))
	nFields := valueOf.Type().NumField()
	namesToIndex := make(map[string]int)
	for i := 0; i < nFields; i++ {
		namesToIndex[valueOf.Type().Field(i).Name] = i
	}

	// Iterate map of interface{} and set values
	elem := reflect.ValueOf(ctx).Elem()
	for fieldName, fieldValue := range fields {
		// Check if structure actually  contains this field
		fieldIndex, ok := namesToIndex[fieldName]
		if !ok {
			t.Errorf("context has no field: \"%s\"", fieldName)
			return ctx
		}
		field := elem.Field(fieldIndex)
		fieldKind := field.Kind()
		// Switch type that comes from interface
		switch interfaceValue := fieldValue.(type) {
		case int:
			// Check if types match
			if fieldKind != reflect.Int {
				t.Errorf("trying to set value %v, type %T for field \"%s\", type %v", interfaceValue, interfaceValue, fieldName, fieldKind)
				return ctx
			}
			field.SetInt(int64(interfaceValue))
		case int64:
			// Check if types match
			if fieldKind != reflect.Int64 {
				t.Errorf("trying to set value %v, type %T for field \"%s\", type %v", interfaceValue, interfaceValue, fieldName, fieldKind)
				return ctx
			}
			field.SetInt(int64(interfaceValue))
		case float64:
			// Check if types match
			if fieldKind != reflect.Float64 {
				t.Errorf("trying to set value %v, type %T for field \"%s\", type %v", interfaceValue, interfaceValue, fieldName, fieldKind)
				return ctx
			}
			field.SetFloat(float64(interfaceValue))
		case bool:
			// Check if types match
			if fieldKind != reflect.Bool {
				t.Errorf("trying to set value %v, type %T for field \"%s\", type %v", interfaceValue, interfaceValue, fieldName, fieldKind)
				return ctx
			}
			field.SetBool(interfaceValue)
		case string:
			// Check if types match
			if fieldKind != reflect.String {
				t.Errorf("trying to set value %v, type %T for field \"%s\", type %v", interfaceValue, interfaceValue, fieldName, fieldKind)
				return ctx
			}
			field.SetString(interfaceValue)
		case time.Time:
			// Check if types match
			fieldType := field.Type()
			if fieldType != reflect.TypeOf(time.Now()) {
				t.Errorf("trying to set value %v, type %T for field \"%s\", type %v", interfaceValue, interfaceValue, fieldName, fieldKind)
				return ctx
			}
			field.Set(reflect.ValueOf(fieldValue))
		case time.Duration:
			// Check if types match
			fieldType := field.Type()
			if fieldType != reflect.TypeOf(time.Now().Sub(time.Now())) {
				t.Errorf("trying to set value %v, type %T for field \"%s\", type %v", interfaceValue, interfaceValue, fieldName, fieldKind)
				return ctx
			}
			field.Set(reflect.ValueOf(fieldValue))
		case []int:
			// Check if types match
			fieldType := field.Type()
			if fieldType != reflect.TypeOf([]int{}) {
				t.Errorf("trying to set value %v, type %T for field \"%s\", type %v", interfaceValue, interfaceValue, fieldName, fieldKind)
				return ctx
			}
			field.Set(reflect.ValueOf(fieldValue))
		case []int64:
			// Check if types match
			fieldType := field.Type()
			if fieldType != reflect.TypeOf([]int64{}) {
				t.Errorf("trying to set value %v, type %T for field \"%s\", type %v", interfaceValue, interfaceValue, fieldName, fieldKind)
				return ctx
			}
			field.Set(reflect.ValueOf(fieldValue))
		case []string:
			// Check if types match
			fieldType := field.Type()
			if fieldType != reflect.TypeOf([]string{}) {
				t.Errorf("trying to set value %v, type %T for field \"%s\", type %v", interfaceValue, interfaceValue, fieldName, fieldKind)
				return ctx
			}
			field.Set(reflect.ValueOf(fieldValue))
		case map[string]bool:
			// Check if types match
			fieldType := field.Type()
			if fieldType != reflect.TypeOf(map[string]bool{}) {
				t.Errorf("trying to set value %v, type %T for field \"%s\", type %v", interfaceValue, interfaceValue, fieldName, fieldKind)
				return ctx
			}
			field.Set(reflect.ValueOf(fieldValue))
		case map[string]map[bool]struct{}:
			// Check if types match
			fieldType := field.Type()
			if fieldType != reflect.TypeOf(map[string]map[bool]struct{}{}) {
				t.Errorf("trying to set value %v, type %T for field \"%s\", type %v", interfaceValue, interfaceValue, fieldName, fieldKind)
				return ctx
			}
			field.Set(reflect.ValueOf(fieldValue))
		case *regexp.Regexp:
			// Check if types match
			fieldType := field.Type()
			if fieldType != reflect.TypeOf(regexp.MustCompile("a")) {
				t.Errorf("trying to set value %v, type %T for field \"%s\", type %v", interfaceValue, interfaceValue, fieldName, fieldKind)
				return ctx
			}
			field.Set(reflect.ValueOf(fieldValue))
		default:
			// Unknown type provided
			t.Errorf("unknown type %T for field \"%s\"", interfaceValue, fieldName)
		}
	}

	// Return dynamically updated structure
	return ctx
}

func TestInit(t *testing.T) {
	// This is the expected default struct state
	defaultContext := lib.Ctx{
		Debug:             0,
		CmdDebug:          0,
		ST:                false,
		NCPUs:             0,
		NCPUsScale:        1.0,
		ExecFatal:         true,
		ExecQuiet:         false,
		ExecOutput:        false,
		GitHubOAuth:       "",
		ESURL:             "",
		ESBulkSize:        1000,
		MaxParallelSHAs:   0,
		MaxJSONsBytes:     0,
		MemHeartBeatBytes: 0,
		LoadConfig:        false,
		SaveConfig:        false,
		NoIncremental:     false,
		NoGHAMap:          false,
		NoGHARepoDates:    false,
		ConfigFile:        "gha_config/",
		TestMode:          true,
	}

	// Test cases
	var testCases = []struct {
		name            string
		environment     map[string]string
		expectedContext *lib.Ctx
	}{
		{
			"Default values",
			map[string]string{},
			&defaultContext,
		},
		{
			"Setting debug level",
			map[string]string{"GHA_DEBUG": "2"},
			dynamicSetFields(
				t,
				copyContext(&defaultContext),
				map[string]interface{}{"Debug": 2},
			),
		},
		{
			"Setting negative debug level",
			map[string]string{"GHA_DEBUG": "-1"},
			dynamicSetFields(
				t,
				copyContext(&defaultContext),
				map[string]interface{}{"Debug": -1},
			),
		},
		{
			"Setting command debug level",
			map[string]string{"GHA_CMDDEBUG": "3"},
			dynamicSetFields(
				t,
				copyContext(&defaultContext),
				map[string]interface{}{"CmdDebug": 3},
			),
		},
		{
			"Setting ST (singlethreading) and NCPUs",
			map[string]string{"GHA_ST": "1", "GHA_NCPUS": "1"},
			dynamicSetFields(
				t,
				copyContext(&defaultContext),
				map[string]interface{}{"ST": true, "NCPUs": 1},
			),
		},
		{
			"Setting NCPUs to 2",
			map[string]string{"GHA_NCPUS": "2"},
			dynamicSetFields(
				t,
				copyContext(&defaultContext),
				map[string]interface{}{"ST": false, "NCPUs": 2},
			),
		},
		{
			"Setting NCPUs to 1 should also set ST mode",
			map[string]string{"GHA_NCPUS": "1"},
			dynamicSetFields(
				t,
				copyContext(&defaultContext),
				map[string]interface{}{"ST": true, "NCPUs": 1},
			),
		},
		{
			"Setting NCPUs Scale to 1.5",
			map[string]string{"GHA_NCPUS_SCALE": "1.5"},
			dynamicSetFields(
				t,
				copyContext(&defaultContext),
				map[string]interface{}{"ST": false, "NCPUsScale": 1.5},
			),
		},
		{
			"Setting ElasticSearch bulk upload size",
			map[string]string{"GHA_ES_BULK_SIZE": "500"},
			dynamicSetFields(
				t,
				copyContext(&defaultContext),
				map[string]interface{}{"ESBulkSize": 500},
			),
		},
		{
			"Set GitHubOAuth",
			map[string]string{"GHA_GITHUB_OAUTH": "key1,key2"},
			dynamicSetFields(
				t,
				copyContext(&defaultContext),
				map[string]interface{}{"GitHubOAuth": "key1,key2"},
			),
		},
		{
			"Set ElasticSearch URL",
			map[string]string{"GHA_ES_URL": "xyz"},
			dynamicSetFields(
				t,
				copyContext(&defaultContext),
				map[string]interface{}{"ESURL": "xyz"},
			),
		},
		{
			"Setting save/load config options",
			map[string]string{"GHA_LOAD_CONFIG": "y", "GHA_SAVE_CONFIG": "1", "GHA_CONFIG_FILE": "f"},
			dynamicSetFields(
				t,
				copyContext(&defaultContext),
				map[string]interface{}{"LoadConfig": true, "SaveConfig": true, "ConfigFile": "f"},
			),
		},
		{
			"Setting no incremental mode",
			map[string]string{"GHA_NO_INCREMENTAL": "1"},
			dynamicSetFields(
				t,
				copyContext(&defaultContext),
				map[string]interface{}{"NoIncremental": true},
			),
		},
		{
			"Setting no GHA map mode",
			map[string]string{"GHA_NO_GHA_MAP": "1"},
			dynamicSetFields(
				t,
				copyContext(&defaultContext),
				map[string]interface{}{"NoGHAMap": true},
			),
		},
		{
			"Setting no GHA repo dates mode",
			map[string]string{"GHA_NO_GHA_REPO_DATES": "1"},
			dynamicSetFields(
				t,
				copyContext(&defaultContext),
				map[string]interface{}{"NoGHARepoDates": true},
			),
		},
		{
			"Setting max parallel SHAs",
			map[string]string{"GHA_MAX_PARALLEL_SHAS": "16"},
			dynamicSetFields(
				t,
				copyContext(&defaultContext),
				map[string]interface{}{"MaxParallelSHAs": 16},
			),
		},
		{
			"Setting max parallel SHAs",
			map[string]string{"GHA_MEM_HEARTBEAT_GBYTES": "17"},
			dynamicSetFields(
				t,
				copyContext(&defaultContext),
				map[string]interface{}{"MemHeartBeatBytes": int64(17) << int64(30)},
			),
		},
	}

	// Execute test cases
	for index, test := range testCases {
		var gotContext lib.Ctx

		// Remember initial environment
		currEnv := make(map[string]string)
		for key := range test.environment {
			currEnv[key] = os.Getenv(key)
		}

		// Set new environment
		for key, value := range test.environment {
			err := os.Setenv(key, value)
			if err != nil {
				t.Errorf(err.Error())
			}
		}

		// Initialize context while new environment is set
		gotContext.TestMode = true
		gotContext.Init()

		// Restore original environment
		for key := range test.environment {
			err := os.Setenv(key, currEnv[key])
			if err != nil {
				t.Errorf(err.Error())
			}
		}

		// Check if we got expected context
		got := fmt.Sprintf("%+v", gotContext)
		expected := fmt.Sprintf("%+v", *test.expectedContext)
		if got != expected {
			t.Errorf(
				"Test case number %d \"%s\"\nExpected:\n%+v\nGot:\n%+v\n",
				index+1, test.name, expected, got,
			)
		}
	}
}
