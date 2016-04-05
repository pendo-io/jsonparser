package jsonparser

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"unsafe"
)

// Errors
var (
	KeyPathNotFoundError  = errors.New("Key path not found")
	UnknownValueTypeError = errors.New("Unknown value type")
	MalformedJsonError    = errors.New("Malformed JSON error")
	MalformedStringError  = errors.New("Value is string, but can't find closing '\"' symbol")
	MalformedArrayError   = errors.New("Value is array, but can't find closing ']' symbol")
	MalformedObjectError  = errors.New("Value looks like object, but can't find closing '}' symbol")
	MalformedNumberError  = errors.New("Value looks like number, but can't find its end: ',' or '}' symbol")
	MalformedLiteralError = errors.New("Value looks like Boolean/null, but can't find its end: ',' or '}' symbol")

	ExpectedArrayError = errors.New("Expected an array as input, but received something that is not an array")
)

func tokenEnd(data []byte) int {
	for i, c := range data {
		switch c {
		case ' ', '\n', '\r', '\t', ',', '}', ']':
			return i
		}
	}

	return -1
}

// Find position of next character that is not whitespace
func nextToken(data []byte) int {
	for i, c := range data {
		switch c {
		case ' ', '\n', '\r', '\t':
			continue
		default:
			return i
		}
	}

	return -1
}

// Find position of next element in an array by skipping whitespace and exactly one comma.
// If a non-whitespace, non-comma character is hit before hitting a comma, returns an error (-1)
func nextArrayElement(data []byte) int {
	seenComma := false
	for i, c := range data {
		switch c {
		case ' ', '\n', '\r', '\t':
			continue
		case ',':
			if seenComma {
				return -1
			} else {
				seenComma = true
			}
		default:
			if !seenComma {
				return -1
			} else {
				return i
			}
		}
	}

	return -1
}

// Tries to find the end of string
// Support if string contains escaped quote symbols.
func stringEnd(data []byte) int {
	for i, c := range data {
		if c == '"' {
			// Check backwards for backslashes to determine if the quote is escaped
			j := i - 1
			for {
				if j < 0 || data[j] != '\\' {
					return i + 1 // even number of backslashes
				}
				j--
				if j < 0 || data[j] != '\\' {
					break // odd number of backslashes
				}
				j--
			}
		}
	}

	return -1
}

// Find end of the data structure, array or object.
// For array openSym and closeSym will be '[' and ']', for object '{' and '}'
func blockEnd(data []byte, openSym byte, closeSym byte) int {
	level := 0
	i := 0
	ln := len(data)

	for i < ln {
		switch data[i] {
		case '"': // If inside string, skip it
			se := stringEnd(data[i+1:])
			if se == -1 {
				return -1
			}
			i += se
		case openSym: // If open symbol, increase level
			level++
		case closeSym: // If close symbol, increase level
			level--

			// If we have returned to the original level, we're done
			if level == 0 {
				return i + 1
			}
		}
		i++
	}

	return -1
}

func searchKeys(data []byte, keys ...string) int {
	keyLevel := 0
	level := 0
	i := 0
	ln := len(data)
	lk := len(keys)

	for i < ln {
		switch data[i] {
		case '"':
			i++
			keyBegin := i

			strEnd := stringEnd(data[i:])
			if strEnd == -1 {
				return -1
			}
			i += strEnd
			keyEnd := i - 1

			valueOffset := nextToken(data[i:])
			if valueOffset == -1 {
				return -1
			}

			i += valueOffset

			// if string is a Key, and key level match
			if data[i] == ':' {
				key := unsafeBytesToString(data[keyBegin:keyEnd])

				if keyLevel == level-1 && // If key nesting level match current object nested level
					keys[level-1] == key {
					keyLevel++
					// If we found all keys in path
					if keyLevel == lk {
						return i + 1
					}
				}
			} else {
				i--
			}
		case '{':
			level++
		case '}':
			level--
		case '[':
			// Do not search for keys inside arrays
			arraySkip := blockEnd(data[i:], '[', ']')
			i += arraySkip - 1
		}

		i++
	}

	return -1
}

// Data types available in valid JSON data.
type ValueType int

const (
	NotExist = ValueType(iota)
	String
	Number
	Object
	Array
	Boolean
	Null
	Unknown
)

/*
Get - Receives data structure, and key path to extract value from.

Returns:
`value` - Pointer to original data structure containing key value, or just empty slice if nothing found or error
`dataType` -    Can be: `NotExist`, `String`, `Number`, `Object`, `Array`, `Boolean` or `Null`
`offset` - Offset from provided data structure where key value ends. Used mostly internally, for example for `ArrayEach` helper.
`err` - If key not found or any other parsing issue it should return error. If key not found it also sets `dataType` to `NotExist`

Accept multiple keys to specify path to JSON value (in case of quering nested structures).
If no keys provided it will try to extract closest JSON value (simple ones or object/array), useful for reading streams or arrays, see `ArrayEach` implementation.
*/
func Get(data []byte, keys ...string) (value []byte, dataType ValueType, offset int, err error) {
	if len(keys) > 0 {
		if offset = searchKeys(data, keys...); offset == -1 {
			return nil, NotExist, -1, KeyPathNotFoundError
		}
	}

	// Go to closest value
	if skipToToken := nextToken(data[offset:]); skipToToken == -1 {
		return nil, Unknown, -1, errors.New("Malformed JSON error")
	} else {
		offset += skipToToken
	}

	if value, dataType, valueOffset, err := GetValue(data[offset:]); err != nil {
		return nil, Unknown, -1, err
	} else {
		return value, dataType, offset + valueOffset, nil
	}
}

// JSON literal keywords as byte slices for comparison in Get()
var (
	trueLiteral  = []byte("true")
	falseLiteral = []byte("false")
	nullLiteral  = []byte("null")
)

func GetValue(data []byte) (value []byte, dataType ValueType, offset int, err error) {
	if len(data) == 0 {
		return nil, Unknown, -1, UnknownValueTypeError
	}

	switch data[0] {
	case '"': // string value
		if strEndQuoteOffMinus1 := stringEnd(data[1:]); strEndQuoteOffMinus1 == -1 {
			return nil, Unknown, -1, MalformedStringError
		} else {
			return data[1:strEndQuoteOffMinus1], String, strEndQuoteOffMinus1 + 1, nil
		}
	case '[': // if array value
		if blockLen := blockEnd(data, '[', ']'); blockLen == -1 {
			return nil, Unknown, -1, MalformedArrayError
		} else {
			return data[:blockLen], Array, blockLen, nil
		}
	case '{': // if object value
		if blockLen := blockEnd(data, '{', '}'); blockLen == -1 {
			return nil, Unknown, -1, MalformedObjectError
		} else {
			return data[:blockLen], Object, blockLen, nil
		}
	case 't', 'f', 'n': // true, false, or nil
		if tokenLen := tokenEnd(data); tokenLen == -1 {
			return nil, Unknown, -1, MalformedLiteralError
		} else {
			if value := data[:tokenLen]; bytes.Equal(value, nullLiteral) {
				return nil, Null, tokenLen, nil
			} else if bytes.Equal(value, trueLiteral) || bytes.Equal(value, falseLiteral) {
				return value, Boolean, tokenLen, nil
			} else {
				return nil, Unknown, -1, UnknownValueTypeError
			}
		}
	case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '-':
		if tokenLen := tokenEnd(data); tokenLen == -1 {
			return nil, Unknown, -1, MalformedNumberError
		} else {
			return data[:tokenLen], Number, tokenLen, nil
		}
	default:
		return nil, Unknown, -1, errors.New("Unknown value type")
	}
}

func nextArrayItem(data []byte) int {
	for i, c := range data {
		switch c {
		case ' ', '\n', '\r', '\t':
			continue
		default:
			return i
		}
	}

	return -1
}

// ArrayEach is used when iterating arrays, accepts a callback function with the same return arguments as `Get`.
func ArrayEach(data []byte, cb func(value []byte, dataType ValueType, offset int, err error), keys ...string) (err error) {
	if arrayValue, dataType, arrayOffset, err := Get(data, keys...); err != nil {
		return err
	} else if dataType != Array {
		return ExpectedArrayError
	} else {
		arrayBeginOffset := arrayOffset - len(arrayValue) // overall offset of arrayValue within data
		offsetInArray := 1                                // skip the '[' (guaranteed to exist because we know it's an Array type)

		// Skip to the first value in the array
		if skipToFirstValue := nextToken(arrayValue[offsetInArray:]); skipToFirstValue == -1 {
			return MalformedArrayError
		} else {
			offsetInArray += skipToFirstValue
		}

		// Keep processing elements until we hit the end of the array (or there's an error inside the array)
		endOffsetInArray := len(arrayValue) - 1
		for offsetInArray < endOffsetInArray {
			elementValue, elementType, elementOffset, err := GetValue(arrayValue[offsetInArray:])
			offsetInArray += elementOffset // update offsetInArray before calling cb() so that it points to the end of the element value

			// If we have reached the end of the array, stop. Otherwise, invoke the callback (even if an error occurred)
			if elementType == NotExist {
				break
			} else {
				cb(elementValue, elementType, arrayBeginOffset+offsetInArray, err)
			}

			// If we received an error, stop.
			if err != nil {
				break
			}

			// Skip to the next value in the array
			if skipToNextValue := nextArrayElement(arrayValue[offsetInArray:]); skipToNextValue == -1 {
				return MalformedArrayError
			} else {
				offsetInArray += skipToNextValue
			}
		}
	}

	return nil
}

// GetUnsafeString returns the value retrieved by `Get`, use creates string without memory allocation by mapping string to slice memory. It does not handle escape symbols.
func GetUnsafeString(data []byte, keys ...string) (val string, err error) {
	v, _, _, e := Get(data, keys...)

	if e != nil {
		return "", e
	}

	return unsafeBytesToString(v), nil
}

// GetString returns the value retrieved by `Get`, cast to a string if possible, trying to properly handle escape and utf8 symbols
// If key data type do not match, it will return an error.
func GetString(data []byte, keys ...string) (val string, err error) {
	v, t, _, e := Get(data, keys...)

	if e != nil {
		return "", e
	}

	if t != String {
		return "", fmt.Errorf("Value is not a number: %s", string(v))
	}

	// If no escapes return raw conten
	if bytes.IndexByte(v, '\\') == -1 {
		return string(v), nil
	}

	s, err := strconv.Unquote(`"` + unsafeBytesToString(v) + `"`)

	return s, err
}

// GetFloat returns the value retrieved by `Get`, cast to a float64 if possible.
// The offset is the same as in `Get`.
// If key data type do not match, it will return an error.
func GetFloat(data []byte, keys ...string) (val float64, err error) {
	v, t, _, e := Get(data, keys...)

	if e != nil {
		return 0, e
	}

	if t != Number {
		return 0, fmt.Errorf("Value is not a number: %s", string(v))
	}

	val, err = strconv.ParseFloat(unsafeBytesToString(v), 64)
	return
}

// GetInt returns the value retrieved by `Get`, cast to a float64 if possible.
// If key data type do not match, it will return an error.
func GetInt(data []byte, keys ...string) (val int64, err error) {
	v, t, _, e := Get(data, keys...)

	if e != nil {
		return 0, e
	}

	if t != Number {
		return 0, fmt.Errorf("Value is not a number: %s", string(v))
	}

	val, err = strconv.ParseInt(unsafeBytesToString(v), 10, 64)
	return
}

// GetBoolean returns the value retrieved by `Get`, cast to a bool if possible.
// The offset is the same as in `Get`.
// If key data type do not match, it will return error.
func GetBoolean(data []byte, keys ...string) (val bool, err error) {
	v, t, _, e := Get(data, keys...)

	if e != nil {
		return false, e
	}

	if t != Boolean {
		return false, fmt.Errorf("Value is not a boolean: %s", string(v))
	}

	if v[0] == 't' {
		val = true
	} else {
		val = false
	}

	return
}

// A hack until issue golang/go#2632 is fixed.
// See: https://github.com/golang/go/issues/2632
func unsafeBytesToString(data []byte) string {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&data))
	sh := reflect.StringHeader{Data: h.Data, Len: h.Len}
	return *(*string)(unsafe.Pointer(&sh))
}
