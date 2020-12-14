package dadsgha

import (
	jsoniter "github.com/json-iterator/go"
)

// PrettyPrintJSON - pretty formats raw JSON bytes
func PrettyPrintJSON(jsonBytes []byte) []byte {
	var jsonObj interface{}
	FatalOnError(jsoniter.Unmarshal(jsonBytes, &jsonObj))
	pretty, err := jsoniter.MarshalIndent(jsonObj, "", "  ")
	FatalOnError(err)
	return pretty
}
