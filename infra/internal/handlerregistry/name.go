package handlerregistry

import (
	"fmt"
	"go/token"
	"reflect"
	"runtime"
	"strings"
)

func resolveName(handler any, kind string, names []string) (string, error) {
	function := reflect.ValueOf(handler)
	if !function.IsValid() {
		return "", fmt.Errorf("nil %s handler", kind)
	}
	if function.Kind() != reflect.Func {
		return "", fmt.Errorf("%s handler must be a function", kind)
	}
	if function.IsNil() {
		return "", fmt.Errorf("nil %s handler", kind)
	}
	if len(names) > 1 {
		return "", fmt.Errorf("expected at most one %s handler name", kind)
	}
	var name string
	if len(names) == 1 {
		name = names[0]
	} else {
		var err error
		name, err = inferName(function)
		if err != nil {
			return "", fmt.Errorf("%s handler: %w", kind, err)
		}
	}
	if name == "" || strings.TrimSpace(name) != name {
		return "", fmt.Errorf("invalid %s handler %q", kind, name)
	}
	return name, nil
}

// inferName 只推导已校验非 nil 的包级具名函数；其他函数须显式提供稳定名称。
func inferName(function reflect.Value) (string, error) {
	symbol := runtime.FuncForPC(function.Pointer())
	var fullName string
	if symbol != nil {
		fullName = symbol.Name()
	}
	shortName := fullName[strings.LastIndexByte(fullName, '/')+1:]
	_, name, found := strings.Cut(shortName, ".")
	if !found || !token.IsIdentifier(name) {
		return "", fmt.Errorf("cannot infer handler name from %q; provide an explicit name", fullName)
	}
	return name, nil
}
