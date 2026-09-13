package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/gomaja/go-m3ua/internal/perfstats"
)

const (
	passingExitStatus = iota
	failingExitStatus
	inconclusiveExitStatus
	invalidInputExitStatus
)

const maximumJSONInputBytes = 64 * 1024

type request struct {
	Direction perfstats.Direction `json:"direction"`
	Boundary  *float64            `json:"boundary"`
	Pairs     []requestPair       `json:"pairs"`
}

type requestPair struct {
	ID        string   `json:"id"`
	Baseline  *float64 `json:"baseline"`
	Candidate *float64 `json:"candidate"`
}

type invalidResponse struct {
	Decision string `json:"decision"`
	Error    string `json:"error"`
}

func main() {
	os.Exit(run(os.Stdin, os.Stdout))
}

func run(input io.Reader, output io.Writer) int {
	request, err := decodeRequest(input)
	if err != nil {
		writeInvalidResponse(output, err)
		return invalidInputExitStatus
	}

	pairs, err := request.pairs()
	if err != nil {
		writeInvalidResponse(output, err)
		return invalidInputExitStatus
	}
	result, err := perfstats.Compare(pairs, perfstats.Gate{Direction: request.Direction, Boundary: *request.Boundary})
	if err != nil {
		writeInvalidResponse(output, err)
		return invalidInputExitStatus
	}
	if err := json.NewEncoder(output).Encode(result); err != nil {
		return invalidInputExitStatus
	}

	switch result.Decision {
	case perfstats.Pass:
		return passingExitStatus
	case perfstats.Fail:
		return failingExitStatus
	case perfstats.Inconclusive:
		return inconclusiveExitStatus
	default:
		return invalidInputExitStatus
	}
}

func decodeRequest(input io.Reader) (request, error) {
	data, err := io.ReadAll(io.LimitReader(input, maximumJSONInputBytes+1))
	if err != nil {
		return request{}, fmt.Errorf("read request: %w", err)
	}
	if len(data) > maximumJSONInputBytes {
		return request{}, fmt.Errorf("request exceeds %d-byte limit", maximumJSONInputBytes)
	}
	if err := rejectDuplicateJSONKeys(data); err != nil {
		return request{}, fmt.Errorf("decode request: %w", err)
	}
	if err := enforceExactJSONFields(data); err != nil {
		return request{}, fmt.Errorf("decode request: %w", err)
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()

	var decoded request
	if err := decoder.Decode(&decoded); err != nil {
		return request{}, fmt.Errorf("decode request: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return request{}, fmt.Errorf("decode request: trailing JSON value")
		}
		return request{}, fmt.Errorf("decode request: %w", err)
	}
	if decoded.Boundary == nil {
		return request{}, fmt.Errorf("boundary is required and cannot be null")
	}
	return decoded, nil
}

func enforceExactJSONFields(data []byte) error {
	var topLevel map[string]json.RawMessage
	if err := json.Unmarshal(data, &topLevel); err != nil {
		return err
	}
	if err := rejectFieldsOutside(topLevel, map[string]struct{}{
		"direction": {},
		"boundary":  {},
		"pairs":     {},
	}, "request"); err != nil {
		return err
	}

	pairsJSON, exists := topLevel["pairs"]
	if !exists {
		return nil
	}
	var pairs []json.RawMessage
	if err := json.Unmarshal(pairsJSON, &pairs); err != nil {
		return fmt.Errorf("pairs must be an array: %w", err)
	}
	for index, pairJSON := range pairs {
		var pairFields map[string]json.RawMessage
		if err := json.Unmarshal(pairJSON, &pairFields); err != nil {
			return fmt.Errorf("pair %d must be an object: %w", index+1, err)
		}
		if err := rejectFieldsOutside(pairFields, map[string]struct{}{
			"id":        {},
			"baseline":  {},
			"candidate": {},
		}, fmt.Sprintf("pair %d", index+1)); err != nil {
			return err
		}
	}
	return nil
}

func rejectFieldsOutside(fields map[string]json.RawMessage, allowed map[string]struct{}, context string) error {
	for field := range fields {
		if _, exists := allowed[field]; !exists {
			return fmt.Errorf("%s contains unknown field %q", context, field)
		}
	}
	return nil
}

func rejectDuplicateJSONKeys(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := scanJSONValue(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return errors.New("trailing JSON value")
		}
		return err
	}
	return nil
}

func scanJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		return nil
	}

	switch delimiter {
	case '{':
		keys := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, isString := keyToken.(string)
			if !isString {
				return errors.New("object key is not a string")
			}
			if _, exists := keys[key]; exists {
				return fmt.Errorf("duplicate object key %q", key)
			}
			keys[key] = struct{}{}
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
	case '[':
		for decoder.More() {
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unexpected delimiter %q", delimiter)
	}

	closingToken, err := decoder.Token()
	if err != nil {
		return err
	}
	closingDelimiter, valid := closingToken.(json.Delim)
	if !valid || (delimiter == '{' && closingDelimiter != '}') || (delimiter == '[' && closingDelimiter != ']') {
		return errors.New("mismatched JSON delimiter")
	}
	return nil
}

func (value request) pairs() ([]perfstats.Pair, error) {
	pairs := make([]perfstats.Pair, len(value.Pairs))
	for index, pair := range value.Pairs {
		if pair.Baseline == nil {
			return nil, fmt.Errorf("pair %d baseline is required and cannot be null", index+1)
		}
		if pair.Candidate == nil {
			return nil, fmt.Errorf("pair %d candidate is required and cannot be null", index+1)
		}
		pairs[index] = perfstats.Pair{
			ID:        pair.ID,
			Baseline:  *pair.Baseline,
			Candidate: *pair.Candidate,
		}
	}
	return pairs, nil
}

func writeInvalidResponse(output io.Writer, err error) {
	_ = json.NewEncoder(output).Encode(invalidResponse{Decision: "invalid-input", Error: err.Error()})
}
