package drivertest

import (
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/swaraj1802/GenericStorageSDK/genericstorage"
	"github.com/swaraj1802/GenericStorageSDK/genericstorage/driver"
	"github.com/swaraj1802/GenericStorageSDK/gcerrors"
	"github.com/swaraj1802/GenericStorageSDK/internal/escape"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

type Harness interface {
	MakeDriver(ctx context.Context) (driver.Bucket, error)

	MakeDriverForNonexistentBucket(ctx context.Context) (driver.Bucket, error)

	HTTPClient() *http.Client

	Close()
}

type HarnessMaker func(ctx context.Context, t *testing.T) (Harness, error)

type AsTest interface {
	Name() string

	BucketCheck(b *genericstorage.Bucket) error

	ErrorCheck(b *genericstorage.Bucket, err error) error

	BeforeRead(as func(interface{}) bool) error

	BeforeWrite(as func(interface{}) bool) error

	BeforeCopy(as func(interface{}) bool) error

	BeforeList(as func(interface{}) bool) error

	BeforeSign(as func(interface{}) bool) error

	AttributesCheck(attrs *genericstorage.Attributes) error

	ReaderCheck(r *genericstorage.Reader) error

	ListObjectCheck(o *genericstorage.ListObject) error
}

type verifyAsFailsOnNil struct{}

func (verifyAsFailsOnNil) Name() string {
	return "verify As returns false when passed nil"
}

func (verifyAsFailsOnNil) BucketCheck(b *genericstorage.Bucket) error {
	if b.As(nil) {
		return errors.New("want Bucket.As to return false when passed nil")
	}
	return nil
}

func (verifyAsFailsOnNil) ErrorCheck(b *genericstorage.Bucket, err error) (ret error) {
	defer func() {
		if recover() == nil {
			ret = errors.New("want ErrorAs to panic when passed nil")
		}
	}()
	b.ErrorAs(err, nil)
	return nil
}

func (verifyAsFailsOnNil) BeforeRead(as func(interface{}) bool) error {
	if as(nil) {
		return errors.New("want BeforeReader's As to return false when passed nil")
	}
	return nil
}

func (verifyAsFailsOnNil) BeforeWrite(as func(interface{}) bool) error {
	if as(nil) {
		return errors.New("want BeforeWrite's As to return false when passed nil")
	}
	return nil
}

func (verifyAsFailsOnNil) BeforeCopy(as func(interface{}) bool) error {
	if as(nil) {
		return errors.New("want BeforeCopy's As to return false when passed nil")
	}
	return nil
}

func (verifyAsFailsOnNil) BeforeList(as func(interface{}) bool) error {
	if as(nil) {
		return errors.New("want BeforeList's As to return false when passed nil")
	}
	return nil
}

func (verifyAsFailsOnNil) BeforeSign(as func(interface{}) bool) error {
	if as(nil) {
		return errors.New("want BeforeSign's As to return false when passed nil")
	}
	return nil
}

func (verifyAsFailsOnNil) AttributesCheck(attrs *genericstorage.Attributes) error {
	if attrs.As(nil) {
		return errors.New("want Attributes.As to return false when passed nil")
	}
	return nil
}

func (verifyAsFailsOnNil) ReaderCheck(r *genericstorage.Reader) error {
	if r.As(nil) {
		return errors.New("want Reader.As to return false when passed nil")
	}
	return nil
}

func (verifyAsFailsOnNil) ListObjectCheck(o *genericstorage.ListObject) error {
	if o.As(nil) {
		return errors.New("want ListObject.As to return false when passed nil")
	}
	return nil
}

func RunConformanceTests(t *testing.T, newHarness HarnessMaker, asTests []AsTest) {
	t.Run("TestNonexistentBucket", func(t *testing.T) {
		testNonexistentBucket(t, newHarness)
	})
	t.Run("TestList", func(t *testing.T) {
		testList(t, newHarness)
	})
	t.Run("TestListWeirdKeys", func(t *testing.T) {
		testListWeirdKeys(t, newHarness)
	})
	t.Run("TestListDelimiters", func(t *testing.T) {
		testListDelimiters(t, newHarness)
	})
	t.Run("TestRead", func(t *testing.T) {
		testRead(t, newHarness)
	})
	t.Run("TestAttributes", func(t *testing.T) {
		testAttributes(t, newHarness)
	})
	t.Run("TestWrite", func(t *testing.T) {
		testWrite(t, newHarness)
	})
	t.Run("TestCanceledWrite", func(t *testing.T) {
		testCanceledWrite(t, newHarness)
	})
	t.Run("TestConcurrentWriteAndRead", func(t *testing.T) {
		testConcurrentWriteAndRead(t, newHarness)
	})
	t.Run("TestMetadata", func(t *testing.T) {
		testMetadata(t, newHarness)
	})
	t.Run("TestMD5", func(t *testing.T) {
		testMD5(t, newHarness)
	})
	t.Run("TestCopy", func(t *testing.T) {
		testCopy(t, newHarness)
	})
	t.Run("TestDelete", func(t *testing.T) {
		testDelete(t, newHarness)
	})
	t.Run("TestKeys", func(t *testing.T) {
		testKeys(t, newHarness)
	})
	t.Run("TestSignedURL", func(t *testing.T) {
		testSignedURL(t, newHarness)
	})
	asTests = append(asTests, verifyAsFailsOnNil{})
	t.Run("TestAs", func(t *testing.T) {
		for _, st := range asTests {
			if st.Name() == "" {
				t.Fatalf("AsTest.Name is required")
			}
			t.Run(st.Name(), func(t *testing.T) {
				testAs(t, newHarness, st)
			})
		}
	})
}

func RunBenchmarks(b *testing.B, bkt *genericstorage.Bucket) {
	b.Run("BenchmarkRead", func(b *testing.B) {
		benchmarkRead(b, bkt)
	})
	b.Run("BenchmarkWriteReadDelete", func(b *testing.B) {
		benchmarkWriteReadDelete(b, bkt)
	})
}

func testNonexistentBucket(t *testing.T, newHarness HarnessMaker) {
	ctx := context.Background()
	h, err := newHarness(ctx, t)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	{
		drv, err := h.MakeDriverForNonexistentBucket(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if drv == nil {

			t.Skip()
		}
		b := genericstorage.NewBucket(drv)
		defer b.Close()
		exists, err := b.IsAccessible(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if exists {
			t.Error("got IsAccessible true for nonexistent bucket, want false")
		}
	}

	{
		drv, err := h.MakeDriver(ctx)
		if err != nil {
			t.Fatal(err)
		}
		b := genericstorage.NewBucket(drv)
		defer b.Close()
		exists, err := b.IsAccessible(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Error("got IsAccessible false for real bucket, want true")
		}
	}
}

func testList(t *testing.T, newHarness HarnessMaker) {
	const keyPrefix = "genericstorage-for-list"
	content := []byte("hello")

	keyForIndex := func(i int) string { return fmt.Sprintf("%s-%d", keyPrefix, i) }
	gotIndices := func(t *testing.T, objs []*driver.ListObject) []int {
		var got []int
		for _, obj := range objs {
			if !strings.HasPrefix(obj.Key, keyPrefix) {
				t.Errorf("got name %q, expected it to have prefix %q", obj.Key, keyPrefix)
				continue
			}
			i, err := strconv.Atoi(obj.Key[len(keyPrefix)+1:])
			if err != nil {
				t.Error(err)
				continue
			}
			got = append(got, i)
		}
		return got
	}

	tests := []struct {
		name      string
		pageSize  int
		prefix    string
		wantPages [][]int
		want      []int
	}{
		{
			name:      "no objects",
			prefix:    "no-objects-with-this-prefix",
			wantPages: [][]int{nil},
		},
		{
			name:      "exactly 1 object due to prefix",
			prefix:    keyForIndex(1),
			wantPages: [][]int{{1}},
			want:      []int{1},
		},
		{
			name:      "no pagination",
			prefix:    keyPrefix,
			wantPages: [][]int{{0, 1, 2}},
			want:      []int{0, 1, 2},
		},
		{
			name:      "by 1",
			prefix:    keyPrefix,
			pageSize:  1,
			wantPages: [][]int{{0}, {1}, {2}},
			want:      []int{0, 1, 2},
		},
		{
			name:      "by 2",
			prefix:    keyPrefix,
			pageSize:  2,
			wantPages: [][]int{{0, 1}, {2}},
			want:      []int{0, 1, 2},
		},
		{
			name:      "by 3",
			prefix:    keyPrefix,
			pageSize:  3,
			wantPages: [][]int{{0, 1, 2}},
			want:      []int{0, 1, 2},
		},
	}

	ctx := context.Background()

	init := func(t *testing.T) (driver.Bucket, func()) {
		h, err := newHarness(ctx, t)
		if err != nil {
			t.Fatal(err)
		}
		drv, err := h.MakeDriver(ctx)
		if err != nil {
			t.Fatal(err)
		}

		b := genericstorage.NewBucket(drv)
		iter := b.List(&genericstorage.ListOptions{Prefix: keyPrefix})
		found := iterToSetOfKeys(ctx, t, iter)
		for i := 0; i < 3; i++ {
			key := keyForIndex(i)
			if !found[key] {
				if err := b.WriteAll(ctx, key, content, nil); err != nil {
					b.Close()
					t.Fatal(err)
				}
			}
		}
		return drv, func() { b.Close(); h.Close() }
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			drv, done := init(t)
			defer done()

			var gotPages [][]int
			var got []int
			var nextPageToken []byte
			for {
				page, err := drv.ListPaged(ctx, &driver.ListOptions{
					PageSize:  tc.pageSize,
					Prefix:    tc.prefix,
					PageToken: nextPageToken,
				})
				if err != nil {
					t.Fatal(err)
				}
				gotThisPage := gotIndices(t, page.Objects)
				got = append(got, gotThisPage...)
				gotPages = append(gotPages, gotThisPage)
				if len(page.NextPageToken) == 0 {
					break
				}
				nextPageToken = page.NextPageToken
			}
			if diff := cmp.Diff(gotPages, tc.wantPages); diff != "" {
				t.Errorf("got\n%v\nwant\n%v\ndiff\n%s", gotPages, tc.wantPages, diff)
			}
			if diff := cmp.Diff(got, tc.want); diff != "" {
				t.Errorf("got\n%v\nwant\n%v\ndiff\n%s", got, tc.want, diff)
			}
		})
	}

	t.Run("PaginationConsistencyAfterInsert", func(t *testing.T) {
		drv, done := init(t)
		defer done()

		page, err := drv.ListPaged(ctx, &driver.ListOptions{
			PageSize: 2,
			Prefix:   keyPrefix,
		})
		if err != nil {
			t.Fatal(err)
		}
		got := gotIndices(t, page.Objects)
		want := []int{0, 1}
		if diff := cmp.Diff(got, want); diff != "" {
			t.Fatalf("got\n%v\nwant\n%v\ndiff\n%s", got, want, diff)
		}

		b := genericstorage.NewBucket(drv)
		defer b.Close()
		key := page.Objects[0].Key + "a"
		if err := b.WriteAll(ctx, key, content, nil); err != nil {
			t.Fatal(err)
		}
		defer func() {
			_ = b.Delete(ctx, key)
		}()

		page, err = drv.ListPaged(ctx, &driver.ListOptions{
			Prefix:    keyPrefix,
			PageToken: page.NextPageToken,
		})
		if err != nil {
			t.Fatal(err)
		}
		got = gotIndices(t, page.Objects)
		want = []int{2}
		if diff := cmp.Diff(got, want); diff != "" {
			t.Errorf("got\n%v\nwant\n%v\ndiff\n%s", got, want, diff)
		}
	})

	t.Run("PaginationConsistencyAfterDelete", func(t *testing.T) {
		drv, done := init(t)
		defer done()

		page, err := drv.ListPaged(ctx, &driver.ListOptions{
			PageSize: 2,
			Prefix:   keyPrefix,
		})
		if err != nil {
			t.Fatal(err)
		}
		got := gotIndices(t, page.Objects)
		want := []int{0, 1}
		if diff := cmp.Diff(got, want); diff != "" {
			t.Fatalf("got\n%v\nwant\n%v\ndiff\n%s", got, want, diff)
		}

		b := genericstorage.NewBucket(drv)
		defer b.Close()
		key := page.Objects[1].Key
		if err := b.Delete(ctx, key); err != nil {
			t.Fatal(err)
		}
		defer func() {
			_ = b.WriteAll(ctx, key, content, nil)
		}()

		page, err = drv.ListPaged(ctx, &driver.ListOptions{
			Prefix:    keyPrefix,
			PageToken: page.NextPageToken,
		})
		if err != nil {
			t.Fatal(err)
		}
		got = gotIndices(t, page.Objects)
		want = []int{2}
		if diff := cmp.Diff(got, want); diff != "" {
			t.Errorf("got\n%v\nwant\n%v\ndiff\n%s", got, want, diff)
		}
	})
}

func testListWeirdKeys(t *testing.T, newHarness HarnessMaker) {
	const keyPrefix = "list-weirdkeys-"
	content := []byte("hello")
	ctx := context.Background()

	want := map[string]bool{}
	for _, k := range escape.WeirdStrings {
		want[keyPrefix+k] = true
	}

	init := func(t *testing.T) (*genericstorage.Bucket, func()) {
		h, err := newHarness(ctx, t)
		if err != nil {
			t.Fatal(err)
		}
		drv, err := h.MakeDriver(ctx)
		if err != nil {
			t.Fatal(err)
		}

		b := genericstorage.NewBucket(drv)
		iter := b.List(&genericstorage.ListOptions{Prefix: keyPrefix})
		found := iterToSetOfKeys(ctx, t, iter)
		for _, k := range escape.WeirdStrings {
			key := keyPrefix + k
			if !found[key] {
				if err := b.WriteAll(ctx, key, content, nil); err != nil {
					b.Close()
					t.Fatal(err)
				}
			}
		}
		return b, func() { b.Close(); h.Close() }
	}

	b, done := init(t)
	defer done()

	iter := b.List(&genericstorage.ListOptions{Prefix: keyPrefix})
	got := iterToSetOfKeys(ctx, t, iter)

	if diff := cmp.Diff(got, want); diff != "" {
		t.Errorf("got\n%v\nwant\n%v\ndiff\n%s", got, want, diff)
	}
}

type listResult struct {
	Key   string
	IsDir bool

	Sub []listResult
}

func doList(ctx context.Context, b *genericstorage.Bucket, prefix, delim string, recurse bool) ([]listResult, error) {
	iter := b.List(&genericstorage.ListOptions{
		Prefix:    prefix,
		Delimiter: delim,
	})
	var retval []listResult
	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			if obj != nil {
				return nil, errors.New("obj is not nil on EOF")
			}
			break
		}
		if err != nil {
			return nil, err
		}
		var sub []listResult
		if obj.IsDir && recurse {
			sub, err = doList(ctx, b, obj.Key, delim, true)
			if err != nil {
				return nil, err
			}
		}
		retval = append(retval, listResult{
			Key:   obj.Key,
			IsDir: obj.IsDir,
			Sub:   sub,
		})
	}
	return retval, nil
}

func testListDelimiters(t *testing.T, newHarness HarnessMaker) {
	const keyPrefix = "genericstorage-for-delimiters-"
	content := []byte("hello")

	keys := [][]string{
		{"dir1", "a.txt"},
		{"dir1", "b.txt"},
		{"dir1", "subdir", "c.txt"},
		{"dir1", "subdir", "d.txt"},
		{"dir2", "e.txt"},
		{"f.txt"},
	}

	tests := []struct {
		name, delim string

		wantFlat []listResult

		wantRecursive []listResult

		wantPaged []listResult

		wantAfterDel []listResult
	}{
		{
			name:  "fwdslash",
			delim: "/",
			wantFlat: []listResult{
				{Key: keyPrefix + "/dir1/a.txt"},
				{Key: keyPrefix + "/dir1/b.txt"},
				{Key: keyPrefix + "/dir1/subdir/c.txt"},
				{Key: keyPrefix + "/dir1/subdir/d.txt"},
				{Key: keyPrefix + "/dir2/e.txt"},
				{Key: keyPrefix + "/f.txt"},
			},
			wantRecursive: []listResult{
				{
					Key:   keyPrefix + "/dir1/",
					IsDir: true,
					Sub: []listResult{
						{Key: keyPrefix + "/dir1/a.txt"},
						{Key: keyPrefix + "/dir1/b.txt"},
						{
							Key:   keyPrefix + "/dir1/subdir/",
							IsDir: true,
							Sub: []listResult{
								{Key: keyPrefix + "/dir1/subdir/c.txt"},
								{Key: keyPrefix + "/dir1/subdir/d.txt"},
							},
						},
					},
				},
				{
					Key:   keyPrefix + "/dir2/",
					IsDir: true,
					Sub: []listResult{
						{Key: keyPrefix + "/dir2/e.txt"},
					},
				},
				{Key: keyPrefix + "/f.txt"},
			},
			wantPaged: []listResult{
				{
					Key:   keyPrefix + "/dir1/",
					IsDir: true,
				},
				{
					Key:   keyPrefix + "/dir2/",
					IsDir: true,
				},
				{Key: keyPrefix + "/f.txt"},
			},
			wantAfterDel: []listResult{
				{
					Key:   keyPrefix + "/dir1/",
					IsDir: true,
				},
				{Key: keyPrefix + "/f.txt"},
			},
		},
		{
			name:  "backslash",
			delim: "\\",
			wantFlat: []listResult{
				{Key: keyPrefix + "\\dir1\\a.txt"},
				{Key: keyPrefix + "\\dir1\\b.txt"},
				{Key: keyPrefix + "\\dir1\\subdir\\c.txt"},
				{Key: keyPrefix + "\\dir1\\subdir\\d.txt"},
				{Key: keyPrefix + "\\dir2\\e.txt"},
				{Key: keyPrefix + "\\f.txt"},
			},
			wantRecursive: []listResult{
				{
					Key:   keyPrefix + "\\dir1\\",
					IsDir: true,
					Sub: []listResult{
						{Key: keyPrefix + "\\dir1\\a.txt"},
						{Key: keyPrefix + "\\dir1\\b.txt"},
						{
							Key:   keyPrefix + "\\dir1\\subdir\\",
							IsDir: true,
							Sub: []listResult{
								{Key: keyPrefix + "\\dir1\\subdir\\c.txt"},
								{Key: keyPrefix + "\\dir1\\subdir\\d.txt"},
							},
						},
					},
				},
				{
					Key:   keyPrefix + "\\dir2\\",
					IsDir: true,
					Sub: []listResult{
						{Key: keyPrefix + "\\dir2\\e.txt"},
					},
				},
				{Key: keyPrefix + "\\f.txt"},
			},
			wantPaged: []listResult{
				{
					Key:   keyPrefix + "\\dir1\\",
					IsDir: true,
				},
				{
					Key:   keyPrefix + "\\dir2\\",
					IsDir: true,
				},
				{Key: keyPrefix + "\\f.txt"},
			},
			wantAfterDel: []listResult{
				{
					Key:   keyPrefix + "\\dir1\\",
					IsDir: true,
				},
				{Key: keyPrefix + "\\f.txt"},
			},
		},
		{
			name:  "abc",
			delim: "abc",
			wantFlat: []listResult{
				{Key: keyPrefix + "abcdir1abca.txt"},
				{Key: keyPrefix + "abcdir1abcb.txt"},
				{Key: keyPrefix + "abcdir1abcsubdirabcc.txt"},
				{Key: keyPrefix + "abcdir1abcsubdirabcd.txt"},
				{Key: keyPrefix + "abcdir2abce.txt"},
				{Key: keyPrefix + "abcf.txt"},
			},
			wantRecursive: []listResult{
				{
					Key:   keyPrefix + "abcdir1abc",
					IsDir: true,
					Sub: []listResult{
						{Key: keyPrefix + "abcdir1abca.txt"},
						{Key: keyPrefix + "abcdir1abcb.txt"},
						{
							Key:   keyPrefix + "abcdir1abcsubdirabc",
							IsDir: true,
							Sub: []listResult{
								{Key: keyPrefix + "abcdir1abcsubdirabcc.txt"},
								{Key: keyPrefix + "abcdir1abcsubdirabcd.txt"},
							},
						},
					},
				},
				{
					Key:   keyPrefix + "abcdir2abc",
					IsDir: true,
					Sub: []listResult{
						{Key: keyPrefix + "abcdir2abce.txt"},
					},
				},
				{Key: keyPrefix + "abcf.txt"},
			},
			wantPaged: []listResult{
				{
					Key:   keyPrefix + "abcdir1abc",
					IsDir: true,
				},
				{
					Key:   keyPrefix + "abcdir2abc",
					IsDir: true,
				},
				{Key: keyPrefix + "abcf.txt"},
			},
			wantAfterDel: []listResult{
				{
					Key:   keyPrefix + "abcdir1abc",
					IsDir: true,
				},
				{Key: keyPrefix + "abcf.txt"},
			},
		},
	}

	ctx := context.Background()

	init := func(t *testing.T, delim string) (driver.Bucket, *genericstorage.Bucket, func()) {
		h, err := newHarness(ctx, t)
		if err != nil {
			t.Fatal(err)
		}
		drv, err := h.MakeDriver(ctx)
		if err != nil {
			t.Fatal(err)
		}
		b := genericstorage.NewBucket(drv)

		prefix := keyPrefix + delim
		iter := b.List(&genericstorage.ListOptions{Prefix: prefix})
		found := iterToSetOfKeys(ctx, t, iter)
		for _, keyParts := range keys {
			key := prefix + strings.Join(keyParts, delim)
			if !found[key] {
				if err := b.WriteAll(ctx, key, content, nil); err != nil {
					b.Close()
					t.Fatal(err)
				}
			}
		}
		return drv, b, func() { b.Close(); h.Close() }
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			drv, b, done := init(t, tc.delim)
			defer done()

			got, err := doList(ctx, b, keyPrefix+tc.delim, "", true)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(got, tc.wantFlat); diff != "" {
				t.Errorf("with no delimiter, got\n%v\nwant\n%v\ndiff\n%s", got, tc.wantFlat, diff)
			}

			got, err = doList(ctx, b, keyPrefix+tc.delim, tc.delim, true)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(got, tc.wantRecursive); diff != "" {
				t.Errorf("with delimiter, got\n%v\nwant\n%v\ndiff\n%s", got, tc.wantRecursive, diff)
			}

			var nextPageToken []byte
			got = nil
			for {
				page, err := drv.ListPaged(ctx, &driver.ListOptions{
					Prefix:    keyPrefix + tc.delim,
					Delimiter: tc.delim,
					PageSize:  1,
					PageToken: nextPageToken,
				})
				if err != nil {
					t.Fatal(err)
				}
				if len(page.Objects) > 1 {
					t.Errorf("got %d objects on a page, want 0 or 1", len(page.Objects))
				}
				for _, obj := range page.Objects {
					got = append(got, listResult{
						Key:   obj.Key,
						IsDir: obj.IsDir,
					})
				}
				if len(page.NextPageToken) == 0 {
					break
				}
				nextPageToken = page.NextPageToken
			}
			if diff := cmp.Diff(got, tc.wantPaged); diff != "" {
				t.Errorf("paged got\n%v\nwant\n%v\ndiff\n%s", got, tc.wantPaged, diff)
			}

			key := strings.Join(append([]string{keyPrefix}, "dir2", "e.txt"), tc.delim)
			if err := b.Delete(ctx, key); err != nil {
				t.Fatal(err)
			}

			defer func() {
				_ = b.WriteAll(ctx, key, content, nil)
			}()

			got, err = doList(ctx, b, keyPrefix+tc.delim, tc.delim, false)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(got, tc.wantAfterDel); diff != "" {
				t.Errorf("after delete, got\n%v\nwant\n%v\ndiff\n%s", got, tc.wantAfterDel, diff)
			}
		})
	}
}

func iterToSetOfKeys(ctx context.Context, t *testing.T, iter *genericstorage.ListIterator) map[string]bool {
	retval := map[string]bool{}
	for {
		if item, err := iter.Next(ctx); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		} else {
			retval[item.Key] = true
		}
	}
	return retval
}

func testRead(t *testing.T, newHarness HarnessMaker) {
	const key = "genericstorage-for-reading"
	content := []byte("abcdefghijklmnopqurstuvwxyz")
	contentSize := int64(len(content))

	tests := []struct {
		name           string
		key            string
		offset, length int64
		want           []byte
		wantReadSize   int64
		wantErr        bool

		skipCreate bool
	}{
		{
			name:    "read of nonexistent key fails",
			key:     "key-does-not-exist",
			length:  -1,
			wantErr: true,
		},
		{
			name:       "negative offset fails",
			key:        key,
			offset:     -1,
			wantErr:    true,
			skipCreate: true,
		},
		{
			name: "length 0 read",
			key:  key,
			want: []byte{},
		},
		{
			name:         "read from positive offset to end",
			key:          key,
			offset:       10,
			length:       -1,
			want:         content[10:],
			wantReadSize: contentSize - 10,
		},
		{
			name:         "read a part in middle",
			key:          key,
			offset:       10,
			length:       5,
			want:         content[10:15],
			wantReadSize: 5,
		},
		{
			name:         "read in full",
			key:          key,
			length:       -1,
			want:         content,
			wantReadSize: contentSize,
		},
		{
			name:         "read in full with negative length not -1",
			key:          key,
			length:       -42,
			want:         content,
			wantReadSize: contentSize,
		},
	}

	ctx := context.Background()

	init := func(t *testing.T, skipCreate bool) (*genericstorage.Bucket, func()) {
		h, err := newHarness(ctx, t)
		if err != nil {
			t.Fatal(err)
		}

		drv, err := h.MakeDriver(ctx)
		if err != nil {
			t.Fatal(err)
		}
		b := genericstorage.NewBucket(drv)
		if skipCreate {
			return b, func() { b.Close(); h.Close() }
		}
		if err := b.WriteAll(ctx, key, content, nil); err != nil {
			b.Close()
			t.Fatal(err)
		}
		return b, func() {
			_ = b.Delete(ctx, key)
			b.Close()
			h.Close()
		}
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b, done := init(t, tc.skipCreate)
			defer done()

			r, err := b.NewRangeReader(ctx, tc.key, tc.offset, tc.length, nil)
			if (err != nil) != tc.wantErr {
				t.Errorf("got err %v want error %v", err, tc.wantErr)
			}
			if err != nil {
				return
			}
			defer r.Close()

			got := make([]byte, tc.wantReadSize+10)
			n, err := r.Read(got)

			if err != nil && err != io.EOF {
				t.Errorf("unexpected error during read: %v", err)
			}
			if int64(n) != tc.wantReadSize {
				t.Errorf("got read length %d want %d", n, tc.wantReadSize)
			}
			if !cmp.Equal(got[:tc.wantReadSize], tc.want) {
				t.Errorf("got %q want %q", string(got), string(tc.want))
			}
			if r.Size() != contentSize {
				t.Errorf("got size %d want %d", r.Size(), contentSize)
			}
			if r.ModTime().IsZero() {
				t.Errorf("got zero mod time, want non-zero")
			}
		})
	}
}

func testAttributes(t *testing.T, newHarness HarnessMaker) {
	const (
		dirKey             = "someDir"
		key                = dirKey + "/genericstorage-for-attributes"
		contentType        = "text/plain"
		cacheControl       = "no-cache"
		contentDisposition = "inline"
		contentEncoding    = "identity"
		contentLanguage    = "en"
	)
	content := []byte("Hello World!")

	ctx := context.Background()

	init := func(t *testing.T) (*genericstorage.Bucket, func()) {
		h, err := newHarness(ctx, t)
		if err != nil {
			t.Fatal(err)
		}
		drv, err := h.MakeDriver(ctx)
		if err != nil {
			t.Fatal(err)
		}
		b := genericstorage.NewBucket(drv)
		opts := &genericstorage.WriterOptions{
			ContentType:        contentType,
			CacheControl:       cacheControl,
			ContentDisposition: contentDisposition,
			ContentEncoding:    contentEncoding,
			ContentLanguage:    contentLanguage,
		}
		if err := b.WriteAll(ctx, key, content, opts); err != nil {
			b.Close()
			t.Fatal(err)
		}
		return b, func() {
			_ = b.Delete(ctx, key)
			b.Close()
			h.Close()
		}
	}

	b, done := init(t)
	defer done()

	for _, badKey := range []string{
		"not-found",
		dirKey,
		dirKey + "/",
	} {
		_, err := b.Attributes(ctx, badKey)
		if err == nil {
			t.Errorf("got nil want error")
		} else if gcerrors.Code(err) != gcerrors.NotFound {
			t.Errorf("got %v want NotFound error", err)
		} else if !strings.Contains(err.Error(), badKey) {
			t.Errorf("got %v want error to include missing key", err)
		}
	}

	a, err := b.Attributes(ctx, key)
	if err != nil {
		t.Fatalf("failed Attributes: %v", err)
	}

	r, err := b.NewReader(ctx, key, nil)
	if err != nil {
		t.Fatalf("failed Attributes: %v", err)
	}
	if a.CacheControl != cacheControl {
		t.Errorf("got CacheControl %q want %q", a.CacheControl, cacheControl)
	}
	if a.ContentDisposition != contentDisposition {
		t.Errorf("got ContentDisposition %q want %q", a.ContentDisposition, contentDisposition)
	}
	if a.ContentEncoding != contentEncoding {
		t.Errorf("got ContentEncoding %q want %q", a.ContentEncoding, contentEncoding)
	}
	if a.ContentLanguage != contentLanguage {
		t.Errorf("got ContentLanguage %q want %q", a.ContentLanguage, contentLanguage)
	}
	if a.ContentType != contentType {
		t.Errorf("got ContentType %q want %q", a.ContentType, contentType)
	}
	if r.ContentType() != contentType {
		t.Errorf("got Reader.ContentType() %q want %q", r.ContentType(), contentType)
	}
	if !a.CreateTime.IsZero() {
		if a.CreateTime.After(a.ModTime) {
			t.Errorf("CreateTime %v is after ModTime %v", a.CreateTime, a.ModTime)
		}
	}
	if a.ModTime.IsZero() {
		t.Errorf("ModTime not set")
	}
	if a.Size != int64(len(content)) {
		t.Errorf("got Size %d want %d", a.Size, len(content))
	}
	if r.Size() != int64(len(content)) {
		t.Errorf("got Reader.Size() %d want %d", r.Size(), len(content))
	}
	if a.ETag == "" {
		t.Error("ETag not set")
	}

	if !strings.HasPrefix(a.ETag, "W/\"") && !strings.HasPrefix(a.ETag, "\"") {
		t.Errorf("ETag should start with W/\" or \" (got %s)", a.ETag)
	}
	if !strings.HasSuffix(a.ETag, "\"") {
		t.Errorf("ETag should end with \" (got %s)", a.ETag)
	}
	r.Close()

	if err := b.WriteAll(ctx, key, content, nil); err != nil {
		t.Fatal(err)
	}

	a2, err := b.Attributes(ctx, key)
	if err != nil {
		t.Errorf("failed Attributes#2: %v", err)
	}
	if a2.ModTime.Before(a.ModTime) {
		t.Errorf("ModTime %v is before %v", a2.ModTime, a.ModTime)
	}
}

func loadTestData(t testing.TB, name string) []byte {
	data, err := Asset(name)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func testWrite(t *testing.T, newHarness HarnessMaker) {
	const key = "genericstorage-for-reading"
	const existingContent = "existing content"
	smallText := loadTestData(t, "test-small.txt")
	mediumHTML := loadTestData(t, "test-medium.html")
	largeJpg := loadTestData(t, "test-large.jpg")
	helloWorld := []byte("hello world")
	helloWorldMD5 := md5.Sum(helloWorld)

	tests := []struct {
		name            string
		key             string
		exists          bool
		content         []byte
		contentType     string
		contentMD5      []byte
		firstChunk      int
		wantContentType string
		wantErr         bool
		wantReadErr     bool
	}{
		{
			name:        "write to empty key fails",
			wantErr:     true,
			wantReadErr: true,
		},
		{
			name: "no write then close results in empty genericstorage",
			key:  key,
		},
		{
			name: "no write then close results in empty genericstorage, genericstorage existed",
			key:  key,
		},
		{
			name:        "invalid ContentType fails",
			key:         key,
			contentType: "application/octet/stream",
			wantErr:     true,
		},
		{
			name:            "ContentType is discovered if not provided",
			key:             key,
			content:         mediumHTML,
			wantContentType: "text/html",
		},
		{
			name:            "write with explicit ContentType overrides discovery",
			key:             key,
			content:         mediumHTML,
			contentType:     "application/json",
			wantContentType: "application/json",
		},
		{
			name:       "Content md5 match",
			key:        key,
			content:    helloWorld,
			contentMD5: helloWorldMD5[:],
		},
		{
			name:       "Content md5 did not match",
			key:        key,
			content:    []byte("not hello world"),
			contentMD5: helloWorldMD5[:],
			wantErr:    true,
		},
		{
			name:       "Content md5 did not match, genericstorage existed",
			exists:     true,
			key:        key,
			content:    []byte("not hello world"),
			contentMD5: helloWorldMD5[:],
			wantErr:    true,
		},
		{
			name:            "a small text file",
			key:             key,
			content:         smallText,
			wantContentType: "text/html",
		},
		{
			name:            "a large jpg file",
			key:             key,
			content:         largeJpg,
			wantContentType: "image/jpg",
		},
		{
			name:            "a large jpg file written in two chunks",
			key:             key,
			firstChunk:      10,
			content:         largeJpg,
			wantContentType: "image/jpg",
		},

		/*
			{
				name:            "ContentType is parsed and reformatted",
				key:             key,
				content:         []byte("foo"),
				contentType:     `FORM-DATA;name="foo"`,
				wantContentType: `form-data; name=foo`,
			},
		*/
	}

	ctx := context.Background()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h, err := newHarness(ctx, t)
			if err != nil {
				t.Fatal(err)
			}
			defer h.Close()
			drv, err := h.MakeDriver(ctx)
			if err != nil {
				t.Fatal(err)
			}
			b := genericstorage.NewBucket(drv)
			defer b.Close()

			if tc.exists {
				if err := b.WriteAll(ctx, key, []byte(existingContent), nil); err != nil {
					t.Fatal(err)
				}
				defer func() {
					_ = b.Delete(ctx, key)
				}()
			}

			opts := &genericstorage.WriterOptions{
				ContentType: tc.contentType,
				ContentMD5:  tc.contentMD5[:],
			}
			w, err := b.NewWriter(ctx, tc.key, opts)
			if err == nil {
				if len(tc.content) > 0 {
					if tc.firstChunk == 0 {

						_, err = w.Write(tc.content)
					} else {

						_, err = w.Write(tc.content[:tc.firstChunk])
						if err == nil {
							_, err = w.Write(tc.content[tc.firstChunk:])
						}
					}
				}
				if err == nil {
					err = w.Close()
				}
			}
			if (err != nil) != tc.wantErr {
				t.Errorf("NewWriter or Close got err %v want error %v", err, tc.wantErr)
			}
			if err != nil {

				buf, err := b.ReadAll(ctx, tc.key)
				if tc.exists {

					if !bytes.Equal(buf, []byte(existingContent)) {
						t.Errorf("Write failed as expected, but content doesn't match expected previous content; got \n%s\n want \n%s", string(buf), existingContent)
					}
				} else {

					if err == nil {
						t.Error("Write failed as expected, but Read after that didn't return an error")
					} else if !tc.wantReadErr && gcerrors.Code(err) != gcerrors.NotFound {
						t.Errorf("Write failed as expected, but Read after that didn't return the right error; got %v want NotFound", err)
					} else if !strings.Contains(err.Error(), tc.key) {
						t.Errorf("got %v want error to include missing key", err)
					}
				}
				return
			}
			defer func() { _ = b.Delete(ctx, tc.key) }()

			buf, err := b.ReadAll(ctx, tc.key)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(buf, tc.content) {
				if len(buf) < 100 && len(tc.content) < 100 {
					t.Errorf("read didn't match write; got \n%s\n want \n%s", string(buf), string(tc.content))
				} else {
					t.Error("read didn't match write, content too large to display")
				}
			}
		})
	}
}

func testCanceledWrite(t *testing.T, newHarness HarnessMaker) {
	const key = "genericstorage-for-canceled-write"
	content := []byte("hello world")
	cancelContent := []byte("going to cancel")

	tests := []struct {
		description string
		contentType string
		exists      bool
	}{
		{

			description: "EmptyContentType",
		},
		{

			description: "NonEmptyContentType",
			contentType: "text/plain",
		},
		{
			description: "BlobExists",
			exists:      true,
		},
	}

	ctx := context.Background()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			cancelCtx, cancel := context.WithCancel(ctx)
			h, err := newHarness(ctx, t)
			if err != nil {
				t.Fatal(err)
			}
			defer h.Close()
			drv, err := h.MakeDriver(ctx)
			if err != nil {
				t.Fatal(err)
			}
			b := genericstorage.NewBucket(drv)
			defer b.Close()

			opts := &genericstorage.WriterOptions{
				ContentType: test.contentType,
			}

			if test.exists {
				if err := b.WriteAll(ctx, key, content, opts); err != nil {
					t.Fatal(err)
				}
				defer func() {
					_ = b.Delete(ctx, key)
				}()
			}

			w, err := b.NewWriter(cancelCtx, key, opts)
			if err != nil {
				t.Fatal(err)
			}

			if _, err := w.Write(cancelContent); err != nil {
				t.Fatal(err)
			}

			got, err := b.ReadAll(ctx, key)
			if test.exists {

				if !cmp.Equal(got, content) {
					t.Errorf("during unclosed write, got %q want %q", string(got), string(content))
				}
			} else {

				if err == nil {
					t.Error("wanted read to return an error when write is not yet Closed")
				}
			}

			cancel()

			if err := w.Close(); err == nil {
				t.Errorf("got Close error %v want canceled ctx error", err)
			}

			got, err = b.ReadAll(ctx, key)
			if test.exists {

				if !cmp.Equal(got, content) {
					t.Errorf("after canceled write, got %q want %q", string(got), string(content))
				}
			} else {

				if err == nil {
					t.Error("wanted read to return an error when write was canceled")
				}
			}
		})
	}
}

func testMetadata(t *testing.T, newHarness HarnessMaker) {
	const key = "genericstorage-for-metadata"
	hello := []byte("hello")

	weirdMetadata := map[string]string{}
	for _, k := range escape.WeirdStrings {
		weirdMetadata[k] = k
	}

	tests := []struct {
		name        string
		metadata    map[string]string
		content     []byte
		contentType string
		want        map[string]string
		wantErr     bool
	}{
		{
			name:     "empty",
			content:  hello,
			metadata: map[string]string{},
			want:     nil,
		},
		{
			name:     "empty key fails",
			content:  hello,
			metadata: map[string]string{"": "empty key value"},
			wantErr:  true,
		},
		{
			name:     "duplicate case-insensitive key fails",
			content:  hello,
			metadata: map[string]string{"abc": "foo", "aBc": "bar"},
			wantErr:  true,
		},
		{
			name:    "valid metadata",
			content: hello,
			metadata: map[string]string{
				"key_a": "value-a",
				"kEy_B": "value-b",
				"key_c": "vAlUe-c",
			},
			want: map[string]string{
				"key_a": "value-a",
				"key_b": "value-b",
				"key_c": "vAlUe-c",
			},
		},
		{
			name:     "valid metadata with empty body",
			content:  nil,
			metadata: map[string]string{"foo": "bar"},
			want:     map[string]string{"foo": "bar"},
		},
		{
			name:        "valid metadata with content type",
			content:     hello,
			contentType: "text/plain",
			metadata:    map[string]string{"foo": "bar"},
			want:        map[string]string{"foo": "bar"},
		},
		{
			name:     "weird metadata keys",
			content:  hello,
			metadata: weirdMetadata,
			want:     weirdMetadata,
		},
		{
			name:     "non-utf8 metadata key",
			content:  hello,
			metadata: map[string]string{escape.NonUTF8String: "bar"},
			wantErr:  true,
		},
		{
			name:     "non-utf8 metadata value",
			content:  hello,
			metadata: map[string]string{"foo": escape.NonUTF8String},
			wantErr:  true,
		},
	}

	ctx := context.Background()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h, err := newHarness(ctx, t)
			if err != nil {
				t.Fatal(err)
			}
			defer h.Close()

			drv, err := h.MakeDriver(ctx)
			if err != nil {
				t.Fatal(err)
			}
			b := genericstorage.NewBucket(drv)
			defer b.Close()
			opts := &genericstorage.WriterOptions{
				Metadata:    tc.metadata,
				ContentType: tc.contentType,
			}
			err = b.WriteAll(ctx, key, hello, opts)
			if (err != nil) != tc.wantErr {
				t.Errorf("got error %v want error %v", err, tc.wantErr)
			}
			if err != nil {
				return
			}
			defer func() {
				_ = b.Delete(ctx, key)
			}()
			a, err := b.Attributes(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(a.Metadata, tc.want); diff != "" {
				t.Errorf("got\n%v\nwant\n%v\ndiff\n%s", a.Metadata, tc.want, diff)
			}
		})
	}
}

func testMD5(t *testing.T, newHarness HarnessMaker) {
	ctx := context.Background()

	const aKey, bKey = "genericstorage-for-md5-aaa", "genericstorage-for-md5-bbb"
	aContent, bContent := []byte("hello"), []byte("goodbye")
	aMD5 := md5.Sum(aContent)
	bMD5 := md5.Sum(bContent)

	h, err := newHarness(ctx, t)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	drv, err := h.MakeDriver(ctx)
	if err != nil {
		t.Fatal(err)
	}
	b := genericstorage.NewBucket(drv)
	defer b.Close()

	if err := b.WriteAll(ctx, aKey, aContent, nil); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Delete(ctx, aKey) }()
	if err := b.WriteAll(ctx, bKey, bContent, nil); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Delete(ctx, bKey) }()

	aAttr, err := b.Attributes(ctx, aKey)
	if err != nil {
		t.Fatal(err)
	}
	if aAttr.MD5 != nil && !bytes.Equal(aAttr.MD5, aMD5[:]) {
		t.Errorf("got MD5\n%x\nwant\n%x", aAttr.MD5, aMD5)
	}

	bAttr, err := b.Attributes(ctx, bKey)
	if err != nil {
		t.Fatal(err)
	}
	if bAttr.MD5 != nil && !bytes.Equal(bAttr.MD5, bMD5[:]) {
		t.Errorf("got MD5\n%x\nwant\n%x", bAttr.MD5, bMD5)
	}

	iter := b.List(&genericstorage.ListOptions{Prefix: "genericstorage-for-md5-"})
	obj, err := iter.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if obj.Key != aKey {
		t.Errorf("got name %q want %q", obj.Key, aKey)
	}
	if obj.MD5 != nil && !bytes.Equal(obj.MD5, aMD5[:]) {
		t.Errorf("got MD5\n%x\nwant\n%x", obj.MD5, aMD5)
	}
	obj, err = iter.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if obj.Key != bKey {
		t.Errorf("got name %q want %q", obj.Key, bKey)
	}
	if obj.MD5 != nil && !bytes.Equal(obj.MD5, bMD5[:]) {
		t.Errorf("got MD5\n%x\nwant\n%x", obj.MD5, bMD5)
	}
}

func testCopy(t *testing.T, newHarness HarnessMaker) {
	const (
		srcKey             = "genericstorage-for-copying-src"
		dstKey             = "genericstorage-for-copying-dest"
		dstKeyExists       = "genericstorage-for-copying-dest-exists"
		contentType        = "text/plain"
		cacheControl       = "no-cache"
		contentDisposition = "inline"
		contentEncoding    = "identity"
		contentLanguage    = "en"
	)
	var contents = []byte("Hello World")

	ctx := context.Background()
	t.Run("NonExistentSourceFails", func(t *testing.T) {
		h, err := newHarness(ctx, t)
		if err != nil {
			t.Fatal(err)
		}
		defer h.Close()
		drv, err := h.MakeDriver(ctx)
		if err != nil {
			t.Fatal(err)
		}
		b := genericstorage.NewBucket(drv)
		defer b.Close()

		err = b.Copy(ctx, dstKey, "does-not-exist", nil)
		if err == nil {
			t.Errorf("got nil want error")
		} else if gcerrors.Code(err) != gcerrors.NotFound {
			t.Errorf("got %v want NotFound error", err)
		} else if !strings.Contains(err.Error(), "does-not-exist") {
			t.Errorf("got %v want error to include missing key", err)
		}
	})

	t.Run("Works", func(t *testing.T) {
		h, err := newHarness(ctx, t)
		if err != nil {
			t.Fatal(err)
		}
		defer h.Close()
		drv, err := h.MakeDriver(ctx)
		if err != nil {
			t.Fatal(err)
		}
		b := genericstorage.NewBucket(drv)
		defer b.Close()

		wopts := &genericstorage.WriterOptions{
			ContentType:        contentType,
			CacheControl:       cacheControl,
			ContentDisposition: contentDisposition,
			ContentEncoding:    contentEncoding,
			ContentLanguage:    contentLanguage,
			Metadata:           map[string]string{"foo": "bar"},
		}
		if err := b.WriteAll(ctx, srcKey, contents, wopts); err != nil {
			t.Fatal(err)
		}

		wantAttr, err := b.Attributes(ctx, srcKey)
		if err != nil {
			t.Fatal(err)
		}

		clearUncomparableFields := func(a *genericstorage.Attributes) {
			a.CreateTime = time.Time{}
			a.ModTime = time.Time{}
			a.ETag = ""
		}
		clearUncomparableFields(wantAttr)

		if err := b.WriteAll(ctx, dstKeyExists, []byte("clobber me"), nil); err != nil {
			t.Fatal(err)
		}

		if err := b.Copy(ctx, dstKey, srcKey, nil); err != nil {
			t.Errorf("got unexpected error copying genericstorage: %v", err)
		}

		got, err := b.ReadAll(ctx, dstKey)
		if err != nil {
			t.Fatal(err)
		}
		if !cmp.Equal(got, contents) {
			t.Errorf("got %q want %q", string(got), string(contents))
		}

		gotAttr, err := b.Attributes(ctx, dstKey)
		if err != nil {
			t.Fatal(err)
		}
		clearUncomparableFields(gotAttr)
		if diff := cmp.Diff(gotAttr, wantAttr, cmpopts.IgnoreUnexported(genericstorage.Attributes{})); diff != "" {
			t.Errorf("got %v want %v diff %s", gotAttr, wantAttr, diff)
		}

		if err := b.Copy(ctx, dstKeyExists, srcKey, nil); err != nil {
			t.Errorf("got unexpected error copying genericstorage: %v", err)
		}

		got, err = b.ReadAll(ctx, dstKeyExists)
		if err != nil {
			t.Fatal(err)
		}
		if !cmp.Equal(got, contents) {
			t.Errorf("got %q want %q", string(got), string(contents))
		}

		gotAttr, err = b.Attributes(ctx, dstKeyExists)
		if err != nil {
			t.Fatal(err)
		}
		clearUncomparableFields(gotAttr)
		if diff := cmp.Diff(gotAttr, wantAttr, cmpopts.IgnoreUnexported(genericstorage.Attributes{})); diff != "" {
			t.Errorf("got %v want %v diff %s", gotAttr, wantAttr, diff)
		}
	})
}

func testDelete(t *testing.T, newHarness HarnessMaker) {
	const key = "genericstorage-for-deleting"

	ctx := context.Background()
	t.Run("NonExistentFails", func(t *testing.T) {
		h, err := newHarness(ctx, t)
		if err != nil {
			t.Fatal(err)
		}
		defer h.Close()
		drv, err := h.MakeDriver(ctx)
		if err != nil {
			t.Fatal(err)
		}
		b := genericstorage.NewBucket(drv)
		defer b.Close()

		err = b.Delete(ctx, "does-not-exist")
		if err == nil {
			t.Errorf("got nil want error")
		} else if gcerrors.Code(err) != gcerrors.NotFound {
			t.Errorf("got %v want NotFound error", err)
		} else if !strings.Contains(err.Error(), "does-not-exist") {
			t.Errorf("got %v want error to include missing key", err)
		}
	})

	t.Run("Works", func(t *testing.T) {
		h, err := newHarness(ctx, t)
		if err != nil {
			t.Fatal(err)
		}
		defer h.Close()
		drv, err := h.MakeDriver(ctx)
		if err != nil {
			t.Fatal(err)
		}
		b := genericstorage.NewBucket(drv)
		defer b.Close()

		if err := b.WriteAll(ctx, key, []byte("Hello world"), nil); err != nil {
			t.Fatal(err)
		}

		if err := b.Delete(ctx, key); err != nil {
			t.Errorf("got unexpected error deleting genericstorage: %v", err)
		}

		_, err = b.NewReader(ctx, key, nil)
		if err == nil {
			t.Errorf("read after delete got nil, want error")
		} else if gcerrors.Code(err) != gcerrors.NotFound {
			t.Errorf("read after delete want NotFound error, got %v", err)
		} else if !strings.Contains(err.Error(), key) {
			t.Errorf("got %v want error to include missing key", err)
		}

		err = b.Delete(ctx, key)
		if err == nil {
			t.Errorf("delete after delete got nil, want error")
		} else if gcerrors.Code(err) != gcerrors.NotFound {
			t.Errorf("delete after delete got %v, want NotFound error", err)
		} else if !strings.Contains(err.Error(), key) {
			t.Errorf("got %v want error to include missing key", err)
		}
	})
}

func testConcurrentWriteAndRead(t *testing.T, newHarness HarnessMaker) {
	ctx := context.Background()
	h, err := newHarness(ctx, t)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	drv, err := h.MakeDriver(ctx)
	if err != nil {
		t.Fatal(err)
	}
	b := genericstorage.NewBucket(drv)
	defer b.Close()

	const numKeys = 20
	const dataSize = 4 * 1024
	keyData := make(map[int][]byte)
	for k := 0; k < numKeys; k++ {
		data := make([]byte, dataSize)
		for i := 0; i < dataSize; i++ {
			data[i] = byte(k)
		}
		keyData[k] = data
	}

	blobName := func(k int) string {
		return fmt.Sprintf("key%d", k)
	}

	var wg sync.WaitGroup

	for k := 0; k < numKeys; k++ {
		wg.Add(1)
		go func(key int) {
			if err := b.WriteAll(ctx, blobName(key), keyData[key], nil); err != nil {
				t.Fatal(err)
			}
			wg.Done()
		}(k)
		defer b.Delete(ctx, blobName(k))
	}
	wg.Wait()

	for k := 0; k < numKeys; k++ {
		wg.Add(1)
		go func(key int) {
			buf, err := b.ReadAll(ctx, blobName(key))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(buf, keyData[key]) {
				t.Errorf("read data mismatch for key %d", key)
			}
			wg.Done()
		}(k)
	}
	wg.Wait()
}

func testKeys(t *testing.T, newHarness HarnessMaker) {
	const keyPrefix = "weird-keys"
	content := []byte("hello")
	ctx := context.Background()

	t.Run("non-UTF8 fails", func(t *testing.T) {
		h, err := newHarness(ctx, t)
		if err != nil {
			t.Fatal(err)
		}
		defer h.Close()
		drv, err := h.MakeDriver(ctx)
		if err != nil {
			t.Fatal(err)
		}
		b := genericstorage.NewBucket(drv)
		defer b.Close()

		key := keyPrefix + escape.NonUTF8String
		if err := b.WriteAll(ctx, key, content, nil); err == nil {
			t.Error("got nil error, expected error for using non-UTF8 string as key")
		}
	})

	for description, key := range escape.WeirdStrings {
		t.Run(description, func(t *testing.T) {
			h, err := newHarness(ctx, t)
			if err != nil {
				t.Fatal(err)
			}
			defer h.Close()
			drv, err := h.MakeDriver(ctx)
			if err != nil {
				t.Fatal(err)
			}
			b := genericstorage.NewBucket(drv)
			defer b.Close()

			key = keyPrefix + key
			if err := b.WriteAll(ctx, key, content, nil); err != nil {
				t.Fatal(err)
			}

			defer func() {
				err := b.Delete(ctx, key)
				if err != nil {
					t.Error(err)
				}
			}()

			got, err := b.ReadAll(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			if !cmp.Equal(got, content) {
				t.Errorf("got %q want %q", string(got), string(content))
			}

			_, err = b.Attributes(ctx, key)
			if err != nil {
				t.Error(err)
			}

			url, err := b.SignedURL(ctx, key, nil)
			if gcerrors.Code(err) != gcerrors.Unimplemented {
				if err != nil {
					t.Error(err)
				}
				client := h.HTTPClient()
				if client == nil {
					t.Error("can't verify SignedURL, Harness.HTTPClient() returned nil")
				}
				resp, err := client.Get(url)
				if err != nil {
					t.Fatal(err)
				}
				defer resp.Body.Close()
				if resp.StatusCode != 200 {
					t.Errorf("got status code %d, want 200", resp.StatusCode)
				}
				got, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(got, content) {
					t.Errorf("got body %q, want %q", string(got), string(content))
				}
			}
		})
	}
}

func testSignedURL(t *testing.T, newHarness HarnessMaker) {
	const key = "genericstorage-for-signing"
	const contents = "hello world"

	ctx := context.Background()

	h, err := newHarness(ctx, t)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	drv, err := h.MakeDriver(ctx)
	if err != nil {
		t.Fatal(err)
	}
	b := genericstorage.NewBucket(drv)
	defer b.Close()

	_, err = b.SignedURL(ctx, key, &genericstorage.SignedURLOptions{Expiry: -1 * time.Minute})
	if err == nil {
		t.Error("got nil error, expected error for negative SignedURLOptions.Expiry")
	}

	getURL, err := b.SignedURL(ctx, key, nil)
	if err != nil {
		if gcerrors.Code(err) == gcerrors.Unimplemented {
			t.Skipf("SignedURL not supported")
			return
		}
		t.Fatal(err)
	} else if getURL == "" {
		t.Fatal("got empty GET url")
	}

	getURLNoParamsURL, err := url.Parse(getURL)
	if err != nil {
		t.Fatalf("failed to parse getURL: %v", err)
	}
	getURLNoParamsURL.RawQuery = ""
	getURLNoParams := getURLNoParamsURL.String()
	const (
		allowedContentType   = "text/plain"
		differentContentType = "application/octet-stream"
	)
	putURLWithContentType, err := b.SignedURL(ctx, key, &genericstorage.SignedURLOptions{
		Method:      http.MethodPut,
		ContentType: allowedContentType,
	})
	if gcerrors.Code(err) == gcerrors.Unimplemented {
		t.Log("PUT URLs with content type not supported, skipping")
	} else if err != nil {
		t.Fatal(err)
	} else if putURLWithContentType == "" {
		t.Fatal("got empty PUT url")
	}
	putURLEnforcedAbsentContentType, err := b.SignedURL(ctx, key, &genericstorage.SignedURLOptions{
		Method:                   http.MethodPut,
		EnforceAbsentContentType: true,
	})
	if gcerrors.Code(err) == gcerrors.Unimplemented {
		t.Log("PUT URLs with enforced absent content type not supported, skipping")
	} else if err != nil {
		t.Fatal(err)
	} else if putURLEnforcedAbsentContentType == "" {
		t.Fatal("got empty PUT url")
	}
	putURLWithoutContentType, err := b.SignedURL(ctx, key, &genericstorage.SignedURLOptions{
		Method: http.MethodPut,
	})
	if err != nil {
		t.Fatal(err)
	} else if putURLWithoutContentType == "" {
		t.Fatal("got empty PUT url")
	}
	deleteURL, err := b.SignedURL(ctx, key, &genericstorage.SignedURLOptions{Method: http.MethodDelete})
	if err != nil {
		t.Fatal(err)
	} else if deleteURL == "" {
		t.Fatal("got empty DELETE url")
	}

	client := h.HTTPClient()
	if client == nil {
		t.Fatal("can't verify SignedURL, Harness.HTTPClient() returned nil")
	}

	type signedURLTest struct {
		urlMethod   string
		contentType string
		url         string
		wantSuccess bool
	}
	tests := []signedURLTest{
		{http.MethodGet, "", getURL, false},
		{http.MethodDelete, "", deleteURL, false},
	}
	if putURLWithContentType != "" {
		tests = append(tests, signedURLTest{http.MethodPut, allowedContentType, putURLWithContentType, true})
		tests = append(tests, signedURLTest{http.MethodPut, differentContentType, putURLWithContentType, false})
		tests = append(tests, signedURLTest{http.MethodPut, "", putURLWithContentType, false})
	}
	if putURLEnforcedAbsentContentType != "" {
		tests = append(tests, signedURLTest{http.MethodPut, "", putURLWithoutContentType, true})
		tests = append(tests, signedURLTest{http.MethodPut, differentContentType, putURLWithoutContentType, false})
	}
	if putURLWithoutContentType != "" {
		tests = append(tests, signedURLTest{http.MethodPut, "", putURLWithoutContentType, true})
	}
	for _, test := range tests {
		req, err := http.NewRequest(http.MethodPut, test.url, strings.NewReader(contents))
		if err != nil {
			t.Fatalf("failed to create PUT HTTP request using %s URL (content-type=%q): %v", test.urlMethod, test.contentType, err)
		}
		if test.contentType != "" {
			req.Header.Set("Content-Type", test.contentType)
		}
		if resp, err := client.Do(req); err != nil {
			t.Fatalf("PUT failed with %s URL (content-type=%q): %v", test.urlMethod, test.contentType, err)
		} else {
			defer resp.Body.Close()
			success := resp.StatusCode >= 200 && resp.StatusCode < 300
			if success != test.wantSuccess {
				t.Errorf("PUT with %s URL (content-type=%q) got status code %d, want 2xx? %v", test.urlMethod, test.contentType, resp.StatusCode, test.wantSuccess)
				gotBody, _ := ioutil.ReadAll(resp.Body)
				t.Errorf(string(gotBody))
			}
		}
	}

	for _, test := range []struct {
		urlMethod   string
		url         string
		wantSuccess bool
	}{
		{http.MethodDelete, deleteURL, false},
		{http.MethodPut, putURLWithoutContentType, false},
		{http.MethodGet, getURLNoParams, false},
		{http.MethodGet, getURL, true},
	} {
		if resp, err := client.Get(test.url); err != nil {
			t.Fatalf("GET with %s URL failed: %v", test.urlMethod, err)
		} else {
			defer resp.Body.Close()
			success := resp.StatusCode >= 200 && resp.StatusCode < 300
			if success != test.wantSuccess {
				t.Errorf("GET with %s URL got status code %d, want 2xx? %v", test.urlMethod, resp.StatusCode, test.wantSuccess)
				gotBody, _ := ioutil.ReadAll(resp.Body)
				t.Errorf(string(gotBody))
			} else if success {
				gotBody, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					t.Errorf("GET with %s URL failed to read response body: %v", test.urlMethod, err)
				} else if gotBodyStr := string(gotBody); gotBodyStr != contents {
					t.Errorf("GET with %s URL got body %q, want %q", test.urlMethod, gotBodyStr, contents)
				}
			}
		}
	}

	for _, test := range []struct {
		urlMethod   string
		url         string
		wantSuccess bool
	}{
		{http.MethodGet, getURL, false},
		{http.MethodPut, putURLWithoutContentType, false},
		{http.MethodDelete, deleteURL, true},
	} {
		req, err := http.NewRequest(http.MethodDelete, test.url, nil)
		if err != nil {
			t.Fatalf("failed to create DELETE HTTP request using %s URL: %v", test.urlMethod, err)
		}
		if resp, err := client.Do(req); err != nil {
			t.Fatalf("DELETE with %s URL failed: %v", test.urlMethod, err)
		} else {
			defer resp.Body.Close()
			success := resp.StatusCode >= 200 && resp.StatusCode < 300
			if success != test.wantSuccess {
				t.Fatalf("DELETE with %s URL got status code %d, want 2xx? %v", test.urlMethod, resp.StatusCode, test.wantSuccess)
				gotBody, _ := ioutil.ReadAll(resp.Body)
				t.Errorf(string(gotBody))
			}
		}
	}

	if resp, err := client.Get(getURL); err != nil {
		t.Errorf("GET after DELETE failed: %v", err)
	} else {
		defer resp.Body.Close()
		if resp.StatusCode != 404 {
			t.Errorf("GET after DELETE got status code %d, want 404", resp.StatusCode)
			gotBody, _ := ioutil.ReadAll(resp.Body)
			t.Errorf(string(gotBody))
		}
	}
}

func testAs(t *testing.T, newHarness HarnessMaker, st AsTest) {
	const (
		dir     = "mydir"
		key     = dir + "/as-test"
		copyKey = dir + "/as-test-copy"
	)
	var content = []byte("hello world")
	ctx := context.Background()

	h, err := newHarness(ctx, t)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	drv, err := h.MakeDriver(ctx)
	if err != nil {
		t.Fatal(err)
	}
	b := genericstorage.NewBucket(drv)
	defer b.Close()

	if err := st.BucketCheck(b); err != nil {
		t.Error(err)
	}

	if err := b.WriteAll(ctx, key, content, &genericstorage.WriterOptions{BeforeWrite: st.BeforeWrite}); err != nil {
		t.Error(err)
	}
	defer func() { _ = b.Delete(ctx, key) }()

	attrs, err := b.Attributes(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.AttributesCheck(attrs); err != nil {
		t.Error(err)
	}

	r, err := b.NewReader(ctx, key, &genericstorage.ReaderOptions{BeforeRead: st.BeforeRead})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err := st.ReaderCheck(r); err != nil {
		t.Error(err)
	}

	iter := b.List(&genericstorage.ListOptions{Prefix: dir, Delimiter: "/", BeforeList: st.BeforeList})
	found := false
	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if found {
			t.Fatal("got a second object returned from List, only wanted one")
		}
		found = true
		if err != nil {
			log.Fatal(err)
		}
		if err := st.ListObjectCheck(obj); err != nil {
			t.Error(err)
		}
	}

	iter = b.List(&genericstorage.ListOptions{Prefix: key, BeforeList: st.BeforeList})
	found = false
	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if found {
			t.Fatal("got a second object returned from List, only wanted one")
		}
		found = true
		if err != nil {
			log.Fatal(err)
		}
		if err := st.ListObjectCheck(obj); err != nil {
			t.Error(err)
		}
	}

	_, gotErr := b.NewReader(ctx, "key-does-not-exist", nil)
	if gotErr == nil {
		t.Fatalf("got nil error from NewReader for nonexistent key, want an error")
	}
	if err := st.ErrorCheck(b, gotErr); err != nil {
		t.Error(err)
	}

	if err := b.Copy(ctx, copyKey, key, &genericstorage.CopyOptions{BeforeCopy: st.BeforeCopy}); err != nil {
		t.Error(err)
	} else {
		defer func() { _ = b.Delete(ctx, copyKey) }()
	}

	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
		_, err = b.SignedURL(ctx, key, &genericstorage.SignedURLOptions{Method: method, BeforeSign: st.BeforeSign})
		if err != nil && gcerrors.Code(err) != gcerrors.Unimplemented {
			t.Errorf("got err %v when signing url with method %q", err, method)
		}
	}
}

func benchmarkRead(b *testing.B, bkt *genericstorage.Bucket) {
	ctx := context.Background()
	const key = "readbenchmark-genericstorage"

	content := loadTestData(b, "test-large.jpg")
	if err := bkt.WriteAll(ctx, key, content, nil); err != nil {
		b.Fatal(err)
	}
	defer func() {
		_ = bkt.Delete(ctx, key)
	}()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf, err := bkt.ReadAll(ctx, key)
			if err != nil {
				b.Error(err)
			}
			if !bytes.Equal(buf, content) {
				b.Error("read didn't match write")
			}
		}
	})
}

func benchmarkWriteReadDelete(b *testing.B, bkt *genericstorage.Bucket) {
	ctx := context.Background()
	const baseKey = "writereaddeletebenchmark-genericstorage-"

	content := loadTestData(b, "test-large.jpg")
	var nextID uint32

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		key := fmt.Sprintf("%s%d", baseKey, atomic.AddUint32(&nextID, 1))
		for pb.Next() {
			if err := bkt.WriteAll(ctx, key, content, nil); err != nil {
				b.Error(err)
				continue
			}
			buf, err := bkt.ReadAll(ctx, key)
			if err != nil {
				b.Error(err)
			}
			if !bytes.Equal(buf, content) {
				b.Error("read didn't match write")
			}
			if err := bkt.Delete(ctx, key); err != nil {
				b.Error(err)
				continue
			}
		}
	})
}
