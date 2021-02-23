package driver

import (
	"fmt"
	"math/big"
	"reflect"
	"time"
)

func CompareTimes(t1, t2 time.Time) int {
	switch {
	case t1.Before(t2):
		return -1
	case t1.After(t2):
		return 1
	default:
		return 0
	}
}

func CompareNumbers(n1, n2 interface{}) (int, error) {
	v1, ok := n1.(reflect.Value)
	if !ok {
		v1 = reflect.ValueOf(n1)
	}
	v2, ok := n2.(reflect.Value)
	if !ok {
		v2 = reflect.ValueOf(n2)
	}
	f1, err := toBigFloat(v1)
	if err != nil {
		return 0, err
	}
	f2, err := toBigFloat(v2)
	if err != nil {
		return 0, err
	}
	return f1.Cmp(f2), nil
}

func toBigFloat(x reflect.Value) (*big.Float, error) {
	var f big.Float
	switch x.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		f.SetInt64(x.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		f.SetUint64(x.Uint())
	case reflect.Float32, reflect.Float64:
		f.SetFloat64(x.Float())
	default:
		typ := "nil"
		if x.IsValid() {
			typ = fmt.Sprint(x.Type())
		}
		return nil, fmt.Errorf("%v of type %s is not a number", x, typ)
	}
	return &f, nil
}
