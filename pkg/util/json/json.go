package jsonutil

import "encoding/json"

func DumpJSON(o interface{}) string {
	by, _ := json.Marshal(o)
	return string(by)
}
