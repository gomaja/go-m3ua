package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestRunPairCountSelection(testContext *testing.T) {
	tests := []struct {
		name      string
		field     string
		count     int
		wantCount int
		wantT     float64
	}{
		{name: "absent defaults to twenty", count: 20, wantCount: 20, wantT: 2.093024054408263},
		{name: "explicit twenty", field: `,"pair_count":20`, count: 20, wantCount: 20, wantT: 2.093024054408263},
		{name: "explicit five", field: `,"pair_count":5`, count: 5, wantCount: 5, wantT: 2.7764451051977943},
	}
	for _, test := range tests {
		testContext.Run(test.name, func(testContext *testing.T) {
			input := countedRequestJSON(test.count, test.field)
			var output bytes.Buffer
			if status := run(strings.NewReader(input), &output); status != passingExitStatus {
				testContext.Fatalf("status = %d, want pass; output = %s", status, output.String())
			}
			var response struct {
				PairCount    int     `json:"pair_count"`
				TCritical975 float64 `json:"t_critical_975"`
			}
			if err := json.Unmarshal(output.Bytes(), &response); err != nil {
				testContext.Fatal(err)
			}
			if response.PairCount != test.wantCount || response.TCritical975 != test.wantT {
				testContext.Fatalf("response = %+v, want count %d, t %.17g", response, test.wantCount, test.wantT)
			}
		})
	}
}

func TestRunRejectsInvalidPairCounts(testContext *testing.T) {
	for _, count := range []int{-1, 0, 4, 6, 19, 21} {
		testContext.Run(fmt.Sprint(count), func(testContext *testing.T) {
			input := countedRequestJSON(20, fmt.Sprintf(`,"pair_count":%d`, count))
			assertInvalidPairCount(testContext, input, "pair_count must be 5 or 20")
		})
	}
	for _, field := range []string{
		`,"pair_count":null`,
		`,"pair_count":5.5`,
		`,"pair_count":5.0`,
		`,"pair_count":"5"`,
		`,"pair_count":true`,
		`,"pair_count":[]`,
		`,"pair_count":{}`,
		`,"pair_count":999999999999999999999999`,
		`,"pair_count":5,"pair_count":20`,
		`,"Pair_count":20`,
		`,"pair_count":20,"Pair_count":20`,
	} {
		testContext.Run(field, func(testContext *testing.T) {
			// Twenty otherwise valid pairs ensure null and case aliases cannot
			// accidentally pass by falling back to the default count.
			assertInvalidPairCount(testContext, countedRequestJSON(20, field), "")
		})
	}
}

func TestRunRequiresDeclaredNumberOfPairs(testContext *testing.T) {
	for _, test := range []struct {
		count int
		field string
		want  string
	}{
		{count: 5, want: "exactly 20 matched pairs"},
		{count: 4, field: `,"pair_count":5`, want: "exactly 5 matched pairs"},
		{count: 6, field: `,"pair_count":5`, want: "exactly 5 matched pairs"},
		{count: 20, field: `,"pair_count":5`, want: "exactly 5 matched pairs"},
		{count: 5, field: `,"pair_count":20`, want: "exactly 20 matched pairs"},
	} {
		testContext.Run(fmt.Sprintf("%d pairs%s", test.count, test.field), func(testContext *testing.T) {
			assertInvalidPairCount(testContext, countedRequestJSON(test.count, test.field), test.want)
		})
	}
}

func FuzzRunPairCount(fuzz *testing.F) {
	for _, count := range []int{0, 4, 5, 6, 19, 20, 21} {
		fuzz.Add(count, uint8(count))
	}
	fuzz.Add(5, uint8(20))
	fuzz.Add(20, uint8(5))
	fuzz.Fuzz(func(testContext *testing.T, count int, actualCount uint8) {
		// Keep requests within the input limit while exploring arbitrary declared
		// counts. Each pair is positive with ratio 1, so a valid request passes.
		actual := int(actualCount % 25)
		input := countedRequestJSON(actual, fmt.Sprintf(`,"pair_count":%d`, count))
		var output bytes.Buffer
		status := run(strings.NewReader(input), &output)
		if !json.Valid(output.Bytes()) {
			testContext.Fatalf("output is not JSON: %q", output.String())
		}
		wantStatus := invalidInputExitStatus
		if (count == 5 || count == 20) && actual == count {
			wantStatus = passingExitStatus
		}
		if status != wantStatus {
			testContext.Fatalf("declared %d, actual %d: status = %d, want %d; output = %s", count, actual, status, wantStatus, output.String())
		}
	})
}

func countedRequestJSON(count int, field string) string {
	pairs := make([]string, count)
	for index := range pairs {
		pairs[index] = fmt.Sprintf(`{"id":"pair-%d","baseline":1,"candidate":1}`, index+1)
	}
	return `{"direction":"lower","boundary":0.95,"pairs":[` + strings.Join(pairs, ",") + `]` + field + `}`
}

func assertInvalidPairCount(testContext *testing.T, input string, wantError string) {
	testContext.Helper()
	var output bytes.Buffer
	if status := run(strings.NewReader(input), &output); status != invalidInputExitStatus {
		testContext.Fatalf("status = %d, want invalid input; output = %s", status, output.String())
	}
	var response invalidResponse
	if err := json.Unmarshal(output.Bytes(), &response); err != nil {
		testContext.Fatal(err)
	}
	if response.Decision != "invalid-input" || response.Error == "" || !strings.Contains(response.Error, wantError) {
		testContext.Fatalf("response = %+v, want invalid input containing %q", response, wantError)
	}
}
