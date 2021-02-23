package driver

import (
	"encoding"
	"fmt"
	"reflect"
	"strconv"

	"github.com/swaraj1802/GenericStorageSDK/docstore/internal/fields"
	"github.com/swaraj1802/GenericStorageSDK/internal/gcerr"
	"github.com/golang/protobuf/proto"
)

var (
	binaryMarshalerType   = reflect.TypeOf((*encoding.BinaryMarshaler)(nil)).Elem()
	binaryUnmarshalerType = reflect.TypeOf((*encoding.BinaryUnmarshaler)(nil)).Elem()
	textMarshalerType     = reflect.TypeOf((*encoding.TextMarshaler)(nil)).Elem()
	textUnmarshalerType   = reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()
	protoMessageType      = reflect.TypeOf((*proto.Message)(nil)).Elem()
)

type Encoder interface {
	EncodeNil()
	EncodeBool(bool)
	EncodeString(string)
	EncodeInt(int64)
	EncodeUint(uint64)
	EncodeFloat(float64)
	EncodeBytes([]byte)

	EncodeList(n int) Encoder
	ListIndex(i int)

	EncodeMap(n int) Encoder
	MapKey(string)

	EncodeSpecial(v reflect.Value) (bool, error)
}

func Encode(v reflect.Value, e Encoder) error {
	return wrap(encode(v, e), gcerr.InvalidArgument)
}

func encode(v reflect.Value, enc Encoder) error {
	if !v.IsValid() {
		enc.EncodeNil()
		return nil
	}
	done, err := enc.EncodeSpecial(v)
	if done {
		return err
	}
	if v.Type().Implements(binaryMarshalerType) {
		bytes, err := v.Interface().(encoding.BinaryMarshaler).MarshalBinary()
		if err != nil {
			return err
		}
		enc.EncodeBytes(bytes)
		return nil
	}
	if v.Type().Implements(protoMessageType) {
		if v.IsNil() {
			enc.EncodeNil()
		} else {
			bytes, err := proto.Marshal(v.Interface().(proto.Message))
			if err != nil {
				return err
			}
			enc.EncodeBytes(bytes)
		}
		return nil
	}
	if reflect.PtrTo(v.Type()).Implements(protoMessageType) {
		bytes, err := proto.Marshal(v.Addr().Interface().(proto.Message))
		if err != nil {
			return err
		}
		enc.EncodeBytes(bytes)
		return nil
	}
	if v.Type().Implements(textMarshalerType) {
		bytes, err := v.Interface().(encoding.TextMarshaler).MarshalText()
		if err != nil {
			return err
		}
		enc.EncodeString(string(bytes))
		return nil
	}
	switch v.Kind() {
	case reflect.Bool:
		enc.EncodeBool(v.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		enc.EncodeInt(v.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		enc.EncodeUint(v.Uint())
	case reflect.Float32, reflect.Float64:
		enc.EncodeFloat(v.Float())
	case reflect.String:
		enc.EncodeString(v.String())
	case reflect.Slice:
		if v.IsNil() {
			enc.EncodeNil()
			return nil
		}
		fallthrough
	case reflect.Array:
		return encodeList(v, enc)
	case reflect.Map:
		return encodeMap(v, enc)
	case reflect.Ptr:
		if v.IsNil() {
			enc.EncodeNil()
			return nil
		}
		return encode(v.Elem(), enc)
	case reflect.Interface:
		if v.IsNil() {
			enc.EncodeNil()
			return nil
		}
		return encode(v.Elem(), enc)

	case reflect.Struct:
		fields, err := fieldCache.Fields(v.Type())
		if err != nil {
			return err
		}
		return encodeStructWithFields(v, fields, enc)

	default:
		return gcerr.Newf(gcerr.InvalidArgument, nil, "cannot encode type %s", v.Type())
	}
	return nil
}

func encodeList(v reflect.Value, enc Encoder) error {

	if v.Type().Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8 {
		enc.EncodeBytes(v.Bytes())
		return nil
	}
	n := v.Len()
	enc2 := enc.EncodeList(n)
	for i := 0; i < n; i++ {
		if err := encode(v.Index(i), enc2); err != nil {
			return err
		}
		enc2.ListIndex(i)
	}
	return nil
}

func encodeMap(v reflect.Value, enc Encoder) error {
	if v.IsNil() {
		enc.EncodeNil()
		return nil
	}
	keys := v.MapKeys()
	enc2 := enc.EncodeMap(len(keys))
	for _, k := range keys {
		sk, err := stringifyMapKey(k)
		if err != nil {
			return err
		}
		if err := encode(v.MapIndex(k), enc2); err != nil {
			return err
		}
		enc2.MapKey(sk)
	}
	return nil
}

func stringifyMapKey(k reflect.Value) (string, error) {

	if k.Kind() == reflect.String {
		return k.String(), nil
	}
	if tm, ok := k.Interface().(encoding.TextMarshaler); ok {
		b, err := tm.MarshalText()
		if err != nil {
			return "", err
		}
		return string(b), nil
	}
	switch k.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(k.Int(), 10), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return strconv.FormatUint(k.Uint(), 10), nil
	default:
		return "", gcerr.Newf(gcerr.InvalidArgument, nil, "cannot encode key %v of type %s", k, k.Type())
	}
}

func encodeStructWithFields(v reflect.Value, fields fields.List, e Encoder) error {
	e2 := e.EncodeMap(len(fields))
	for _, f := range fields {
		fv, ok := fieldByIndex(v, f.Index)
		if !ok {

			continue
		}
		if f.ParsedTag.(tagOptions).omitEmpty && IsEmptyValue(fv) {
			continue
		}
		if err := encode(fv, e2); err != nil {
			return err
		}
		e2.MapKey(f.Name)
	}
	return nil
}

func fieldByIndex(v reflect.Value, index []int) (reflect.Value, bool) {
	for _, i := range index {
		if v.Kind() == reflect.Ptr {
			if v.IsNil() {
				return reflect.Value{}, false
			}
			v = v.Elem()
		}
		v = v.Field(i)
	}
	return v, true
}

type Decoder interface {
	AsString() (string, bool)
	AsInt() (int64, bool)
	AsUint() (uint64, bool)
	AsFloat() (float64, bool)
	AsBytes() ([]byte, bool)
	AsBool() (bool, bool)
	AsNull() bool

	ListLen() (int, bool)

	DecodeList(func(int, Decoder) bool)

	MapLen() (int, bool)

	DecodeMap(func(string, Decoder, bool) bool)

	AsInterface() (interface{}, error)

	AsSpecial(reflect.Value) (bool, interface{}, error)

	String() string
}

func Decode(v reflect.Value, d Decoder) error {
	return wrap(decode(v, d), gcerr.InvalidArgument)
}

func decode(v reflect.Value, d Decoder) error {
	if !v.CanSet() {
		return fmt.Errorf("while decoding: cannot set %+v", v)
	}

	if d.AsNull() {
		switch v.Kind() {
		case reflect.Interface, reflect.Ptr, reflect.Map, reflect.Slice:
			v.Set(reflect.Zero(v.Type()))
			return nil
		}
	}

	if done, val, err := d.AsSpecial(v); done {
		if err != nil {
			return err
		}
		if reflect.TypeOf(val).AssignableTo(v.Type()) {
			v.Set(reflect.ValueOf(val))
			return nil
		}
		return decodingError(v, d)
	}

	if reflect.PtrTo(v.Type()).Implements(binaryUnmarshalerType) {
		if b, ok := d.AsBytes(); ok {
			return v.Addr().Interface().(encoding.BinaryUnmarshaler).UnmarshalBinary(b)
		}
		return decodingError(v, d)
	}
	if reflect.PtrTo(v.Type()).Implements(protoMessageType) {
		if b, ok := d.AsBytes(); ok {
			return proto.Unmarshal(b, v.Addr().Interface().(proto.Message))
		}
		return decodingError(v, d)
	}
	if reflect.PtrTo(v.Type()).Implements(textUnmarshalerType) {
		if s, ok := d.AsString(); ok {
			return v.Addr().Interface().(encoding.TextUnmarshaler).UnmarshalText([]byte(s))
		}
		return decodingError(v, d)
	}

	switch v.Kind() {
	case reflect.Bool:
		if b, ok := d.AsBool(); ok {
			v.SetBool(b)
			return nil
		}

	case reflect.String:
		if s, ok := d.AsString(); ok {
			v.SetString(s)
			return nil
		}

	case reflect.Float32, reflect.Float64:
		if f, ok := d.AsFloat(); ok {
			v.SetFloat(f)
			return nil
		}

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		i, ok := d.AsInt()
		if !ok {

			f, ok := d.AsFloat()
			if !ok {
				return decodingError(v, d)
			}
			i = int64(f)
			if float64(i) != f {
				return gcerr.Newf(gcerr.InvalidArgument, nil, "float %f does not fit into %s", f, v.Type())
			}
		}
		if v.OverflowInt(i) {
			return overflowError(i, v.Type())
		}
		v.SetInt(i)
		return nil

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		u, ok := d.AsUint()
		if !ok {

			f, ok := d.AsFloat()
			if !ok {
				return decodingError(v, d)
			}
			u = uint64(f)
			if float64(u) != f {
				return gcerr.Newf(gcerr.InvalidArgument, nil, "float %f does not fit into %s", f, v.Type())
			}
		}
		if v.OverflowUint(u) {
			return overflowError(u, v.Type())
		}
		v.SetUint(u)
		return nil

	case reflect.Slice, reflect.Array:
		return decodeList(v, d)

	case reflect.Map:
		return decodeMap(v, d)

	case reflect.Ptr:

		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		return decode(v.Elem(), d)

	case reflect.Struct:
		return decodeStruct(v, d)

	case reflect.Interface:
		if v.NumMethod() == 0 {

			if !v.IsNil() && v.Elem().Kind() == reflect.Ptr {
				return decode(v.Elem(), d)
			}

			x, err := d.AsInterface()
			if err != nil {
				return err
			}
			v.Set(reflect.ValueOf(x))
			return nil
		}

	}

	return decodingError(v, d)
}

func decodeList(v reflect.Value, d Decoder) error {

	if v.Type().Elem().Kind() == reflect.Uint8 {
		if b, ok := d.AsBytes(); ok {
			if v.Kind() == reflect.Slice {
				v.SetBytes(b)
				return nil
			}

			err := prepareLength(v, len(b))
			if err != nil {
				return err
			}
			reflect.Copy(v, reflect.ValueOf(b))
			return nil
		}

	}
	dlen, ok := d.ListLen()
	if !ok {
		return decodingError(v, d)
	}
	err := prepareLength(v, dlen)
	if err != nil {
		return err
	}
	d.DecodeList(func(i int, vd Decoder) bool {
		if err != nil || i >= dlen {
			return false
		}
		err = decode(v.Index(i), vd)
		return err == nil
	})
	return err
}

func prepareLength(v reflect.Value, wantLen int) error {
	vLen := v.Len()
	if v.Kind() == reflect.Slice {

		switch {
		case vLen < wantLen:
			if v.Cap() >= wantLen {
				v.SetLen(wantLen)
			} else {
				v.Set(reflect.MakeSlice(v.Type(), wantLen, wantLen))
			}
		case vLen > wantLen:
			v.SetLen(wantLen)
		}
	} else {
		switch {
		case vLen < wantLen:
			return gcerr.Newf(gcerr.InvalidArgument, nil, "array length %d is too short for incoming list of length %d",
				vLen, wantLen)
		case vLen > wantLen:
			z := reflect.Zero(v.Type().Elem())
			for i := wantLen; i < vLen; i++ {
				v.Index(i).Set(z)
			}
		}
	}
	return nil
}

func decodeMap(v reflect.Value, d Decoder) error {
	mapLen, ok := d.MapLen()
	if !ok {
		return decodingError(v, d)
	}
	t := v.Type()
	if v.IsNil() {
		v.Set(reflect.MakeMapWithSize(t, mapLen))
	}
	et := t.Elem()
	var err error
	kt := v.Type().Key()
	d.DecodeMap(func(key string, vd Decoder, _ bool) bool {
		if err != nil {
			return false
		}
		el := reflect.New(et).Elem()
		err = decode(el, vd)
		if err != nil {
			return false
		}
		vk, e := unstringifyMapKey(key, kt)
		if e != nil {
			err = e
			return false
		}
		v.SetMapIndex(vk, el)
		return err == nil
	})
	return err
}

func unstringifyMapKey(key string, keyType reflect.Type) (reflect.Value, error) {

	switch {
	case keyType.Kind() == reflect.String:
		return reflect.ValueOf(key).Convert(keyType), nil
	case reflect.PtrTo(keyType).Implements(textUnmarshalerType):
		tu := reflect.New(keyType)
		if err := tu.Interface().(encoding.TextUnmarshaler).UnmarshalText([]byte(key)); err != nil {
			return reflect.Value{}, err
		}
		return tu.Elem(), nil
	case keyType.Kind() == reflect.Interface && keyType.NumMethod() == 0:

		return reflect.ValueOf(key), nil
	default:
		switch keyType.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			n, err := strconv.ParseInt(key, 10, 64)
			if err != nil {
				return reflect.Value{}, err
			}
			if reflect.Zero(keyType).OverflowInt(n) {
				return reflect.Value{}, overflowError(n, keyType)
			}
			return reflect.ValueOf(n).Convert(keyType), nil
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			n, err := strconv.ParseUint(key, 10, 64)
			if err != nil {
				return reflect.Value{}, err
			}
			if reflect.Zero(keyType).OverflowUint(n) {
				return reflect.Value{}, overflowError(n, keyType)
			}
			return reflect.ValueOf(n).Convert(keyType), nil
		default:
			return reflect.Value{}, gcerr.Newf(gcerr.InvalidArgument, nil, "invalid key type %s", keyType)
		}
	}
}

func decodeStruct(v reflect.Value, d Decoder) error {
	fs, err := fieldCache.Fields(v.Type())
	if err != nil {
		return err
	}
	d.DecodeMap(func(key string, d2 Decoder, exactMatch bool) bool {
		if err != nil {
			return false
		}
		var f *fields.Field
		if exactMatch {
			f = fs.MatchExact(key)
		} else {
			f = fs.MatchFold(key)
		}
		if f == nil {
			err = gcerr.Newf(gcerr.InvalidArgument, nil, "no field matching %q in %s", key, v.Type())
			return false
		}
		fv, ok := fieldByIndexCreate(v, f.Index)
		if !ok {
			err = gcerr.Newf(gcerr.InvalidArgument, nil,
				"setting field %q in %s: cannot create embedded pointer field of unexported type",
				key, v.Type())
			return false
		}
		err = decode(fv, d2)
		return err == nil
	})
	return err
}

func fieldByIndexCreate(v reflect.Value, index []int) (reflect.Value, bool) {
	for _, i := range index {
		if v.Kind() == reflect.Ptr {
			if v.IsNil() {
				if !v.CanSet() {
					return reflect.Value{}, false
				}
				v.Set(reflect.New(v.Type().Elem()))
			}
			v = v.Elem()
		}
		v = v.Field(i)
	}
	return v, true
}

func decodingError(v reflect.Value, d Decoder) error {
	return gcerr.New(gcerr.InvalidArgument, nil, 2, fmt.Sprintf("cannot set type %s to %v", v.Type(), d))
}

func overflowError(x interface{}, t reflect.Type) error {
	return gcerr.New(gcerr.InvalidArgument, nil, 2, fmt.Sprintf("value %v overflows type %s", x, t))
}

func wrap(err error, code gcerr.ErrorCode) error {
	if _, ok := err.(*gcerr.Error); !ok && err != nil {
		err = gcerr.New(code, err, 2, err.Error())
	}
	return err
}

var fieldCache = fields.NewCache(parseTag, nil, nil)

func IsEmptyValue(v reflect.Value) bool {
	switch k := v.Kind(); k {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	}
	return false
}

type tagOptions struct {
	omitEmpty bool
}

func parseTag(t reflect.StructTag) (name string, keep bool, other interface{}, err error) {
	var opts []string
	name, keep, opts = fields.ParseStandardTag("docstore", t)
	tagOpts := tagOptions{}
	for _, opt := range opts {
		switch opt {
		case "omitempty":
			tagOpts.omitEmpty = true
		default:
			return "", false, nil, gcerr.Newf(gcerr.InvalidArgument, nil, "unknown tag option: %q", opt)
		}
	}
	return name, keep, tagOpts, nil
}
