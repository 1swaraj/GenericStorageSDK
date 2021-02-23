package fields

import (
	"bytes"
	"reflect"
	"sort"
	"strings"
	"sync"
)

type Field struct {
	Name        string
	NameFromTag bool
	Type        reflect.Type
	Index       []int
	ParsedTag   interface{}

	nameBytes []byte
	equalFold func(s, t []byte) bool
}

type ParseTagFunc func(reflect.StructTag) (name string, keep bool, other interface{}, err error)

type ValidateFunc func(reflect.Type) error

type LeafTypesFunc func(reflect.Type) bool

type Cache struct {
	parseTag  ParseTagFunc
	validate  ValidateFunc
	leafTypes LeafTypesFunc
	cache     sync.Map
}

func NewCache(parseTag ParseTagFunc, validate ValidateFunc, leafTypes LeafTypesFunc) *Cache {
	if parseTag == nil {
		parseTag = func(reflect.StructTag) (string, bool, interface{}, error) {
			return "", true, nil, nil
		}
	}
	if validate == nil {
		validate = func(reflect.Type) error {
			return nil
		}
	}
	if leafTypes == nil {
		leafTypes = func(reflect.Type) bool {
			return false
		}
	}

	return &Cache{
		parseTag:  parseTag,
		validate:  validate,
		leafTypes: leafTypes,
	}
}

type fieldScan struct {
	typ   reflect.Type
	index []int
}

func (c *Cache) Fields(t reflect.Type) (List, error) {
	if t.Kind() != reflect.Struct {
		panic("fields: Fields of non-struct type")
	}
	return c.cachedTypeFields(t)
}

type List []Field

func (l List) MatchExact(name string) *Field {
	return l.MatchExactBytes([]byte(name))
}

func (l List) MatchExactBytes(name []byte) *Field {
	for _, f := range l {
		if bytes.Equal(f.nameBytes, name) {
			return &f
		}
	}
	return nil
}

func (l List) MatchFold(name string) *Field {
	return l.MatchFoldBytes([]byte(name))
}

func (l List) MatchFoldBytes(name []byte) *Field {
	var f *Field
	for i := range l {
		ff := &l[i]
		if bytes.Equal(ff.nameBytes, name) {
			return ff
		}
		if f == nil && ff.equalFold(ff.nameBytes, name) {
			f = ff
		}
	}
	return f
}

type cacheValue struct {
	fields List
	err    error
}

func (c *Cache) cachedTypeFields(t reflect.Type) (List, error) {
	var cv cacheValue
	x, ok := c.cache.Load(t)
	if ok {
		cv = x.(cacheValue)
	} else {
		if err := c.validate(t); err != nil {
			cv = cacheValue{nil, err}
		} else {
			f, err := c.typeFields(t)
			cv = cacheValue{List(f), err}
		}
		c.cache.Store(t, cv)
	}
	return cv.fields, cv.err
}

func (c *Cache) typeFields(t reflect.Type) ([]Field, error) {
	fields, err := c.listFields(t)
	if err != nil {
		return nil, err
	}
	sort.Sort(byName(fields))

	var out []Field
	for advance, i := 0, 0; i < len(fields); i += advance {

		fi := fields[i]
		name := fi.Name
		for advance = 1; i+advance < len(fields); advance++ {
			fj := fields[i+advance]
			if fj.Name != name {
				break
			}
		}

		dominant, ok := dominantField(fields[i : i+advance])
		if ok {
			out = append(out, dominant)
		}
	}
	sort.Sort(byIndex(out))
	return out, nil
}

func (c *Cache) listFields(t reflect.Type) ([]Field, error) {

	current := []fieldScan{}
	next := []fieldScan{{typ: t}}

	var nextCount map[reflect.Type]int

	visited := map[reflect.Type]bool{}

	var fields []Field

	for len(next) > 0 {
		current, next = next, current[:0]
		count := nextCount
		nextCount = nil

		for _, scan := range current {
			t := scan.typ
			if visited[t] {

				continue
			}
			visited[t] = true
			for i := 0; i < t.NumField(); i++ {
				f := t.Field(i)

				exported := (f.PkgPath == "")

				if !exported && !f.Anonymous {
					continue
				}

				tagName, keep, other, err := c.parseTag(f.Tag)
				if err != nil {
					return nil, err
				}
				if !keep {
					continue
				}
				if c.leafTypes(f.Type) {
					fields = append(fields, newField(f, tagName, other, scan.index, i))
					continue
				}

				var ntyp reflect.Type
				if f.Anonymous {

					ntyp = f.Type
					if ntyp.Kind() == reflect.Ptr {
						ntyp = ntyp.Elem()
					}
				}

				if tagName != "" || ntyp == nil || ntyp.Kind() != reflect.Struct {
					if !exported {
						continue
					}
					fields = append(fields, newField(f, tagName, other, scan.index, i))
					if count[t] > 1 {

						fields = append(fields, fields[len(fields)-1])
					}
					continue
				}

				if nextCount[ntyp] > 0 {
					nextCount[ntyp] = 2
					continue
				}
				if nextCount == nil {
					nextCount = map[reflect.Type]int{}
				}
				nextCount[ntyp] = 1
				if count[t] > 1 {
					nextCount[ntyp] = 2
				}
				var index []int
				index = append(index, scan.index...)
				index = append(index, i)
				next = append(next, fieldScan{ntyp, index})
			}
		}
	}
	return fields, nil
}

func newField(f reflect.StructField, tagName string, other interface{}, index []int, i int) Field {
	name := tagName
	if name == "" {
		name = f.Name
	}
	sf := Field{
		Name:        name,
		NameFromTag: tagName != "",
		Type:        f.Type,
		ParsedTag:   other,
		nameBytes:   []byte(name),
	}
	sf.equalFold = foldFunc(sf.nameBytes)
	sf.Index = append(sf.Index, index...)
	sf.Index = append(sf.Index, i)
	return sf
}

type byName []Field

func (x byName) Len() int { return len(x) }

func (x byName) Swap(i, j int) { x[i], x[j] = x[j], x[i] }

func (x byName) Less(i, j int) bool {
	if x[i].Name != x[j].Name {
		return x[i].Name < x[j].Name
	}
	if len(x[i].Index) != len(x[j].Index) {
		return len(x[i].Index) < len(x[j].Index)
	}
	if x[i].NameFromTag != x[j].NameFromTag {
		return x[i].NameFromTag
	}
	return byIndex(x).Less(i, j)
}

type byIndex []Field

func (x byIndex) Len() int { return len(x) }

func (x byIndex) Swap(i, j int) { x[i], x[j] = x[j], x[i] }

func (x byIndex) Less(i, j int) bool {
	xi := x[i].Index
	xj := x[j].Index
	ln := len(xi)
	if l := len(xj); l < ln {
		ln = l
	}
	for k := 0; k < ln; k++ {
		if xi[k] != xj[k] {
			return xi[k] < xj[k]
		}
	}
	return len(xi) < len(xj)
}

func dominantField(fs []Field) (Field, bool) {

	if len(fs) > 1 && len(fs[0].Index) == len(fs[1].Index) && fs[0].NameFromTag == fs[1].NameFromTag {
		return Field{}, false
	}
	return fs[0], true
}

func ParseStandardTag(key string, t reflect.StructTag) (name string, keep bool, options []string) {
	s := t.Get(key)
	parts := strings.Split(s, ",")
	if parts[0] == "-" && len(parts) == 1 {
		return "", false, nil
	}
	if len(parts) > 1 {
		options = parts[1:]
	}
	return parts[0], true, options
}
