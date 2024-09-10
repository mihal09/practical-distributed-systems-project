package main

import (
	"encoding/json"
	aero "github.com/aerospike/aerospike-client-go/v7"
	"slices"
	"strconv"
	"strings"
	"time"
)

func compare_func(a, b []byte) int {
	user_tag_a := UserTag{}
	user_tag_b := UserTag{}
	json.Unmarshal(a, &user_tag_a)
	json.Unmarshal(b, &user_tag_b)

	time_a := parse_timestamp(user_tag_a.Time)
	time_b := parse_timestamp(user_tag_b.Time)

	return -time_a.Compare(time_b)

}

func parse_records(records interface{}, output_records [][]byte) [][]byte {
	if bin_list, ok := records.([]interface{}); ok {
		for _, item := range bin_list {
			if byte_item, ok := item.([]byte); ok {
				output_records = append(output_records, byte_item)
			}
		}
	}
	return output_records
}

func parse_timestamp(timestamp string) time.Time {
	ret_time, err := time.Parse("2006-01-02T15:04:05.000Z", timestamp)
	if err != nil {
		ret_time, _ = time.Parse("2006-01-02T15:04:05Z", timestamp)
	}

	return ret_time
}

func parse_timerange(time_range string, layout string) (time.Time, time.Time) {
	ranges := strings.Split(time_range, "_")
	start_range, _ := time.Parse(layout, ranges[0])
	end_range, _ := time.Parse(layout, ranges[1])
	return start_range, end_range
}

func generate_query_keys(time_range, action, origin, brand_id, category_id string) []string {
	time_cursor, end_time := parse_timerange(time_range, "2006-01-02T15:04:05")
	keys := []string{}

	for ; time_cursor.Before(end_time); time_cursor = time_cursor.Add(time.Minute) {
		key := strconv.FormatInt(time_cursor.Unix(), 10) + "|" + action + "|" + origin + "|" + brand_id + "|" + category_id
		keys = append(keys, key)
	}

	return keys
}

func process_user_events(user_events *aero.Record, start_range time.Time, end_range time.Time) []UserTag {
	processed_events := []UserTag{}
	if user_events != nil {
		events := [][]byte{}
		user_tag := UserTag{}

		events = parse_records(user_events.Bins["value"], events)
		slices.SortFunc(events, compare_func)
		events = events[:min(len(events), 200)]

		processed_events = make([]UserTag, 0, len(events))

		for _, event := range events {
			json.Unmarshal(event, &user_tag)
			user_time := parse_timestamp(user_tag.Time)

			if user_time.Compare(start_range) >= 0 && user_time.Compare(end_range) < 0 {
				processed_events = append(processed_events, user_tag)
			}

		}
	}

	return processed_events

}
