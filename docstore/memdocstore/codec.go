package memdocstore

import (
	"fmt"
	"reflect"
	"time"

	"github.com/swaraj1802/GenericStorageSDK/gcerrors"

	"github.com/swaraj1802/GenericStorageSDK/docstore/driver"
)

func encodeDoc(doc driver.Document) (storedDoc, error) {
	var e encoder
	if err := doc.Encode(&e); err != nil {
		return nil, err
	}
	return storedDoc(e.val.(map[string]interface{})), nil
}

func encodeValue(v interface{}) (interface{}, error) {
	var e encoder
	if err := driver.Encode(reflect.ValueOf(v), &e); err != nil {
		return nil, err
	}
	return e.val, nil
}

type encoder struct {
	val interface{}
}

func (e *encoder) EncodeNil()            { e.val = nil }
func (e *encoder) EncodeBool(x bool)     { e.val = x }
func (e *encoder) EncodeInt(x int64)     { e.val = x }
func (e *encoder) EncodeUint(x uint64)   { e.val = int64(x) }
func (e *encoder) EncodeBytes(x []byte)  { e.val = x }
func (e *encoder) EncodeFloat(x float64) { e.val = x }
func (e *encoder) EncodeString(x string) { e.val = x }
func (e *encoder) ListIndex(int)         { panic("impossible") }
func (e *encoder) MapKey(string)         { panic("impossible") }

var typeOfGoTime = reflect.TypeOf(time.Time{})

func (e *encoder) EncodeSpecial(v reflect.Value) (bool, error) {
	if v.Type() == typeOfGoTime {
		e.val = v.Interface()
		return true, nil
	}
	return false, nil
}

func (e *encoder) EncodeList(n int) driver.Encoder {

	s := make([]interface{}, n)
	e.val = s
	return &listEncoder{s: s}
}

type listEncoder struct {
	s []interface{}
	encoder
}

func (e *listEncoder) ListIndex(i int) { e.s[i] = e.val }

type mapEncoder struct {
	m map[string]interface{}
	encoder
}

func (e *encoder) EncodeMap(n int) driver.Encoder {
	m := make(map[string]interface{}, n)
	e.val = m
	return &mapEncoder{m: m}
}

func (e *mapEncoder) MapKey(k string) { e.m[k] = e.val }

func decodeDoc(m storedDoc, ddoc driver.Document, fps [][]string) error {
	var m2 map[string]interface{}
	if len(fps) == 0 {
		m2 = m
	} else {

		m2 = map[string]interface{}{}
		for _, fp := range fps {
			val, err := getAtFieldPath(m, fp)
			if err != nil {
				if gcerrors.Code(err) == gcerrors.NotFound {
					continue
				}
				return err
			}
			if err := setAtFieldPath(m2, fp, val); err != nil {
				return err
			}
		}
	}
	return ddoc.Decode(decoder{m2})
}

type decoder struct {
	val interface{}
}

func (d decoder) String() string {
	return fmt.Sprint(d.val)
}

func (d decoder) AsNull() bool {
	return d.val == nil
}

func (d decoder) AsBool() (bool, bool) {
	b, ok := d.val.(bool)
	return b, ok
}

func (d decoder) AsString() (string, bool) {
	s, ok := d.val.(string)
	return s, ok
}

func (d decoder) AsInt() (int64, bool) {
	i, ok := d.val.(int64)
	return i, ok
}

func (d decoder) AsUint() (uint64, bool) {
	i, ok := d.val.(int64)
	return uint64(i), ok
}

func (d decoder) AsFloat() (float64, bool) {
	f, ok := d.val.(float64)
	return f, ok
}

func (d decoder) AsBytes() ([]byte, bool) {
	bs, ok := d.val.([]byte)
	return bs, ok
}

func (d decoder) AsInterface() (interface{}, error) {
	return d.val, nil
}

func (d decoder) ListLen() (int, bool) {
	if s, ok := d.val.([]interface{}); ok {
		return len(s), true
	}
	return 0, false
}

func (d decoder) DecodeList(f func(i int, d2 driver.Decoder) bool) {
	for i, e := range d.val.([]interface{}) {
		if !f(i, decoder{e}) {
			return
		}
	}
}

func (d decoder) MapLen() (int, bool) {
	if m, ok := d.val.(map[string]interface{}); ok {
		return len(m), true
	}
	return 0, false
}

func (d decoder) DecodeMap(f func(key string, d2 driver.Decoder, _ bool) bool) {
	for k, v := range d.val.(map[string]interface{}) {
		if !f(k, decoder{v}, true) {
			return
		}
	}
}

func (d decoder) AsSpecial(v reflect.Value) (bool, interface{}, error) {
	if v.Type() == typeOfGoTime {
		return true, d.val, nil
	}
	return false, nil, nil
}
