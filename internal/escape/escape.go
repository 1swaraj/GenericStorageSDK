package escape

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

const NonUTF8String = "\xbd\xb2"

func IsASCIIAlphanumeric(r rune) bool {
	switch {
	case 'A' <= r && r <= 'Z':
		return true
	case 'a' <= r && r <= 'z':
		return true
	case '0' <= r && r <= '9':
		return true
	}
	return false
}

func HexEscape(s string, shouldEscape func(s []rune, i int) bool) string {

	runes := []rune(s)
	var toEscape []int
	for i := range runes {
		if shouldEscape(runes, i) {
			toEscape = append(toEscape, i)
		}
	}
	if len(toEscape) == 0 {
		return s
	}

	escaped := make([]rune, len(runes)+13*len(toEscape))
	n := 0
	j := 0
	for i, r := range runes {
		if n < len(toEscape) && i == toEscape[n] {

			for _, x := range fmt.Sprintf("__%#x__", r) {
				escaped[j] = x
				j++
			}
			n++
		} else {
			escaped[j] = r
			j++
		}
	}
	return string(escaped[0:j])
}

func unescape(r []rune, i int) (bool, rune, int) {

	if r[i] != '_' {
		return false, 0, 0
	}
	i++
	if i >= len(r) || r[i] != '_' {
		return false, 0, 0
	}
	i++
	if i >= len(r) || r[i] != '0' {
		return false, 0, 0
	}
	i++
	if i >= len(r) || r[i] != 'x' {
		return false, 0, 0
	}
	i++

	var hexdigits []rune
	for ; i < len(r) && r[i] != '_'; i++ {
		hexdigits = append(hexdigits, r[i])
	}

	if i >= len(r) || r[i] != '_' {
		return false, 0, 0
	}
	i++
	if i >= len(r) || r[i] != '_' {
		return false, 0, 0
	}

	retval, err := strconv.ParseInt(string(hexdigits), 16, 32)
	if err != nil {
		return false, 0, 0
	}
	return true, rune(retval), i
}

func HexUnescape(s string) string {
	var unescaped []rune
	runes := []rune(s)
	for i := 0; i < len(runes); i++ {
		if ok, newR, newI := unescape(runes, i); ok {

			if unescaped == nil {

				unescaped = make([]rune, i)
				copy(unescaped, runes)
			}
			unescaped = append(unescaped, newR)
			i = newI
		} else if unescaped != nil {
			unescaped = append(unescaped, runes[i])
		}
	}
	if unescaped == nil {
		return s
	}
	return string(unescaped)
}

func URLEscape(s string) string {
	return url.PathEscape(s)
}

func URLUnescape(s string) string {
	if u, err := url.PathUnescape(s); err == nil {
		return u
	}
	return s
}

func makeASCIIString(start, end int) string {
	var s []byte
	for i := start; i < end; i++ {
		if i >= 'a' && i <= 'z' {
			continue
		}
		if i >= 'A' && i <= 'Z' {
			continue
		}
		if i >= '0' && i <= '9' {
			continue
		}
		s = append(s, byte(i))
	}
	return string(s)
}

var WeirdStrings = map[string]string{
	"fwdslashes":          "foo/bar/baz",
	"repeatedfwdslashes":  "foo//bar///baz",
	"dotdotslash":         "../foo/../bar/../../baz../",
	"backslashes":         "foo\\bar\\baz",
	"repeatedbackslashes": "..\\foo\\\\bar\\\\\\baz",
	"dotdotbackslash":     "..\\foo\\..\\bar\\..\\..\\baz..\\",
	"quote":               "foo\"bar\"baz",
	"spaces":              "foo bar baz",
	"startwithdigit":      "12345",
	"unicode":             strings.Repeat("☺", 3),
	"ascii-1":             makeASCIIString(0, 16),
	"ascii-2":             makeASCIIString(16, 32),
	"ascii-3":             makeASCIIString(32, 48),
	"ascii-4":             makeASCIIString(48, 64),
	"ascii-5":             makeASCIIString(64, 80),
	"ascii-6":             makeASCIIString(80, 96),
	"ascii-7":             makeASCIIString(96, 112),
	"ascii-8":             makeASCIIString(112, 128),
}
