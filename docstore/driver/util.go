package driver

import (
	"reflect"
	"sort"
	"sync"

	"github.com/google/uuid"
)

func UniqueString() string { return uuid.New().String() }

func SplitActions(actions []*Action, split func(a, b *Action) bool) [][]*Action {
	var (
		groups [][]*Action
		cur    []*Action
	)
	collect := func() {
		if len(cur) > 0 {
			groups = append(groups, cur)
			cur = nil
		}
	}
	for _, a := range actions {
		if len(cur) > 0 && split(cur[len(cur)-1], a) {
			collect()
		}
		cur = append(cur, a)
	}
	collect()
	return groups
}

func GroupActions(actions []*Action) (beforeGets, getList, writeList, afterGets []*Action) {

	bgets := map[interface{}]*Action{}
	agets := map[interface{}]*Action{}
	cgets := map[interface{}]*Action{}
	writes := map[interface{}]*Action{}
	var nilkeys []*Action
	for _, a := range actions {
		if a.Key == nil {

			nilkeys = append(nilkeys, a)
		} else if a.Kind == Get {

			if _, ok := writes[a.Key]; ok {
				agets[a.Key] = a
			} else {
				cgets[a.Key] = a
			}
		} else {

			if g, ok := cgets[a.Key]; ok {
				delete(cgets, a.Key)
				bgets[a.Key] = g
			}
			writes[a.Key] = a
		}
	}

	vals := func(m map[interface{}]*Action) []*Action {
		var as []*Action
		for _, v := range m {
			as = append(as, v)
		}

		sort.Slice(as, func(i, j int) bool { return as[i].Index < as[j].Index })
		return as
	}

	return vals(bgets), vals(cgets), append(vals(writes), nilkeys...), vals(agets)
}

func AsFunc(val interface{}) func(interface{}) bool {
	rval := reflect.ValueOf(val)
	wantType := reflect.PtrTo(rval.Type())
	return func(i interface{}) bool {
		if i == nil {
			return false
		}
		ri := reflect.ValueOf(i)
		if ri.Type() != wantType {
			return false
		}
		ri.Elem().Set(rval)
		return true
	}
}

func GroupByFieldPath(gets []*Action) [][]*Action {

	var groups [][]*Action
	seen := map[*Action]bool{}
	for len(seen) < len(gets) {
		var g []*Action
		for _, a := range gets {
			if !seen[a] {
				if len(g) == 0 || fpsEqual(g[0].FieldPaths, a.FieldPaths) {
					g = append(g, a)
					seen[a] = true
				}
			}
		}
		groups = append(groups, g)
	}
	return groups
}

func fpsEqual(fps1, fps2 [][]string) bool {

	if len(fps1) != len(fps2) {
		return false
	}
	for i, fp1 := range fps1 {
		if !FieldPathsEqual(fp1, fps2[i]) {
			return false
		}
	}
	return true
}

func FieldPathsEqual(fp1, fp2 []string) bool {
	if len(fp1) != len(fp2) {
		return false
	}
	for i, s1 := range fp1 {
		if s1 != fp2[i] {
			return false
		}
	}
	return true
}

func FieldPathEqualsField(fp []string, s string) bool {
	return len(fp) == 1 && fp[0] == s
}

type Throttle struct {
	c  chan struct{}
	wg sync.WaitGroup
}

func NewThrottle(max int) *Throttle {
	t := &Throttle{}
	if max > 0 {
		t.c = make(chan struct{}, max)
	}
	return t
}

func (t *Throttle) Acquire() {
	t.wg.Add(1)
	if t.c != nil {
		t.c <- struct{}{}
	}
}

func (t *Throttle) Release() {
	if t.c != nil {
		<-t.c
	}
	t.wg.Done()
}

func (t *Throttle) Wait() {
	t.wg.Wait()
}
