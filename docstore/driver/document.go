package driver

import (
	"reflect"

	"github.com/swaraj1802/GenericStorageSDK/docstore/internal/fields"
	"github.com/swaraj1802/GenericStorageSDK/gcerrors"
	"github.com/swaraj1802/GenericStorageSDK/internal/gcerr"
)

type Document struct {
	Origin interface{}
	m      map[string]interface{}
	s      reflect.Value
	fields fields.List
}

func NewDocument(doc interface{}) (Document, error) {
	if doc == nil {
		return Document{}, gcerr.Newf(gcerr.InvalidArgument, nil, "document cannot be nil")
	}
	if m, ok := doc.(map[string]interface{}); ok {
		if m == nil {
			return Document{}, gcerr.Newf(gcerr.InvalidArgument, nil, "document map cannot be nil")
		}
		return Document{Origin: doc, m: m}, nil
	}
	v := reflect.ValueOf(doc)
	t := v.Type()
	if t.Kind() != reflect.Ptr || t.Elem().Kind() != reflect.Struct {
		return Document{}, gcerr.Newf(gcerr.InvalidArgument, nil, "expecting *struct or map[string]interface{}, got %s", t)
	}
	t = t.Elem()
	if v.IsNil() {
		return Document{}, gcerr.Newf(gcerr.InvalidArgument, nil, "document struct pointer cannot be nil")
	}
	fields, err := fieldCache.Fields(t)
	if err != nil {
		return Document{}, err
	}
	return Document{Origin: doc, s: v.Elem(), fields: fields}, nil
}

func (d Document) GetField(field string) (interface{}, error) {
	if d.m != nil {
		x, ok := d.m[field]
		if !ok {
			return nil, gcerr.Newf(gcerr.NotFound, nil, "field %q not found in map", field)
		}
		return x, nil
	} else {
		v, err := d.structField(field)
		if err != nil {
			return nil, err
		}
		return v.Interface(), nil
	}
}

func (d Document) getDocument(fp []string, create bool) (Document, error) {
	if len(fp) == 0 {
		return d, nil
	}
	x, err := d.GetField(fp[0])
	if err != nil {
		if create && gcerrors.Code(err) == gcerrors.NotFound {

			x = map[string]interface{}{}
			if err := d.SetField(fp[0], x); err != nil {
				return Document{}, err
			}
		} else {
			return Document{}, err
		}
	}
	d2, err := NewDocument(x)
	if err != nil {
		return Document{}, err
	}
	return d2.getDocument(fp[1:], create)
}

func (d Document) Get(fp []string) (interface{}, error) {
	d2, err := d.getDocument(fp[:len(fp)-1], false)
	if err != nil {
		return nil, err
	}
	return d2.GetField(fp[len(fp)-1])
}

func (d Document) structField(name string) (reflect.Value, error) {

	f := d.fields.MatchFold(name)
	if f == nil {
		return reflect.Value{}, gcerr.Newf(gcerr.NotFound, nil, "field %q not found in struct type %s", name, d.s.Type())
	}
	fv, ok := fieldByIndex(d.s, f.Index)
	if !ok {
		return reflect.Value{}, gcerr.Newf(gcerr.InvalidArgument, nil, "nil embedded pointer; cannot get field %q from %s",
			name, d.s.Type())
	}
	return fv, nil
}

func (d Document) Set(fp []string, val interface{}) error {
	d2, err := d.getDocument(fp[:len(fp)-1], true)
	if err != nil {
		return err
	}
	return d2.SetField(fp[len(fp)-1], val)
}

func (d Document) SetField(field string, value interface{}) error {
	if d.m != nil {
		d.m[field] = value
		return nil
	}
	v, err := d.structField(field)
	if err != nil {
		return err
	}
	if !v.CanSet() {
		return gcerr.Newf(gcerr.InvalidArgument, nil, "cannot set field %s in struct of type %s: not addressable",
			field, d.s.Type())
	}
	v.Set(reflect.ValueOf(value))
	return nil
}

func (d Document) FieldNames() []string {
	var names []string
	if d.m != nil {
		for k := range d.m {
			names = append(names, k)
		}
	} else {
		for _, f := range d.fields {
			names = append(names, f.Name)
		}
	}
	return names
}

func (d Document) Encode(e Encoder) error {
	if d.m != nil {
		return encodeMap(reflect.ValueOf(d.m), e)
	}
	return encodeStructWithFields(d.s, d.fields, e)
}

func (d Document) Decode(dec Decoder) error {
	if d.m != nil {
		return decodeMap(reflect.ValueOf(d.m), dec)
	}
	return decodeStruct(d.s, dec)
}

func (d Document) HasField(field string) bool {
	return d.hasField(field, true)
}

func (d Document) HasFieldFold(field string) bool {
	return d.hasField(field, false)
}

func (d Document) hasField(field string, exactMatch bool) bool {
	if d.m != nil {
		_, ok := d.m[field]
		return ok
	}
	if exactMatch {
		return d.fields.MatchExact(field) != nil
	}
	return d.fields.MatchFold(field) != nil
}
