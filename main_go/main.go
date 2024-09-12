package main

import (
	"fmt"
	"encoding/json"
	aero "github.com/aerospike/aerospike-client-go/v7"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/go-chi/chi/v5"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"
)

var aerospike_client *aero.Client
var kafka_producer *kafka.Producer

type UserTag struct {
	Time         string      `json:"time"`
	Cookie       string      `json:"cookie"`
	Country      string      `json:"country"`
	Device       string      `json:"device"`
	Action       string      `json:"action"`
	Origin       string      `json:"origin"`
	Product_info ProductInfo `json:"product_info"`
}

type ProductInfo struct {
	Product_id  int32  `json:"product_id"`
	Brand_id    string `json:"brand_id"`
	Category_id string `json:"category_id"`
	Price       int32  `json:"price"`
}

type UserProfileResult struct {
	Cookie string    `json:"cookie"`
	Views  []UserTag `json:"views"`
	Buys   []UserTag `json:"buys"`
}

type AggregatesResult struct {
	Columns []string   `json:"columns"`
	Rows    [][]string `json:"rows"`
}

var (
	host        = "aerospikedb"
	port        = 3000
	namespace   = "mimuw"
	set         = "tags"
	kafka_topic = "user_tags"
)

func main() {
	r := chi.NewRouter()

	var err aero.Error
	clientPolicy := aero.NewClientPolicy();
	clientPolicy.ConnectionQueueSize = 1024
	clientPolicy.IdleTimeout = 15*time.Second
	clientPolicy.Timeout = 5 * time.Second
	clientPolicy.MinConnectionsPerNode = 100
	aerospike_client, err = aero.NewClientWithPolicy(clientPolicy, "aerospikedb", 3000)
	if err != nil {
		panic(err)
	}

	var ok error
	kafka_producer, ok = kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "broker-1:19092,broker-2:19092",
		"linger.ms":         5000,
		"compression.type":  "snappy",
	})
	if ok != nil {
		panic(ok)
	}


	r.Route("/user_tags", func(r chi.Router) {
		r.Post("/", addUserTags)
	})

	r.Route("/user_profiles/{cookie}", func(r chi.Router) {
		r.Post("/", getUserProfiles)
	})

	r.Route("/aggregates", func(r chi.Router) {
		r.Post("/", aggregateUserActions)
	})

	http.ListenAndServe("0.0.0.0:5000", r)
}

func addUserTags(w http.ResponseWriter, r *http.Request) {
	body_bytes, _ := io.ReadAll(r.Body)
	user_tag := UserTag{}
	json.Unmarshal(body_bytes, &user_tag)

	key_string := user_tag.Cookie + ":" + strings.ToLower(user_tag.Action)
	key, _ := aero.NewKey(namespace, set, key_string)

	write_policy := aero.NewWritePolicy(0, 0)
	write_policy.RecordExistsAction = aero.REPLACE
	write_policy.GenerationPolicy = aero.EXPECT_GEN_EQUAL
	write_policy.MaxRetries = 1
	
	err := kafka_producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &kafka_topic, Partition: kafka.PartitionAny},
		Key:            []byte(key_string),
		Value:          body_bytes}, nil)

	if err != nil {
		if err.(kafka.Error).Code() == kafka.ErrQueueFull {
			fmt.Printf("[DEBUG] Producer queue is full")
		}
		fmt.Printf("[DEBUG] Failed to produce message: %v\n", err)
	}

	aerospikeMaxRetries := 3
	counter := 0
	for {
		var write_err aero.Error

		read_policy := aero.NewPolicy()
		read_policy.TotalTimeout = 100 * time.Millisecond
		read_policy.SocketTimeout = 80 * time.Millisecond
		records, _ := aerospike_client.Get(read_policy, key)
		updated_records := [][]byte{body_bytes}

		if records != nil {
			write_policy.Generation = records.Generation
			updated_records = parse_records(records.Bins["value"], updated_records)
		}

		if len(updated_records) >= 200 {
			slices.SortFunc(updated_records, compare_func)
		}

		write_err = aerospike_client.Put(write_policy, key, map[string]interface{}{"value": updated_records[:min(len(updated_records), 200)]})
		counter++
		if write_err != nil {
			fmt.Printf("Iteration %d: Error writing records: %v\n", counter, write_err)
			if counter > aerospikeMaxRetries {
				fmt.Println("Max retries reached. Exiting loop.")
				break
			}
		} else {
			break
		}
	}

	w.WriteHeader(http.StatusNoContent)
}

func getUserProfiles(w http.ResponseWriter, r *http.Request) {
	start_range, end_range := parse_timerange(r.URL.Query().Get("time_range"), "2006-01-02T15:04:05.000")
	cookie := chi.URLParam(r, "cookie")
	limit := 200
	limit_str := chi.URLParam(r, "limit")
	if limit_str != "" {
		limit, _ = strconv.Atoi(limit_str)
	}

	views_key, _ := aero.NewKey(namespace, set, cookie+":view")
	buys_key, _ := aero.NewKey(namespace, set, cookie+":buy")
	user_views, _ := aerospike_client.Get(aero.NewPolicy(), views_key)
	user_buys, _ := aerospike_client.Get(aero.NewPolicy(), buys_key)

	send_views := []UserTag{}
	send_buys := []UserTag{}

	user_tag := UserTag{}

	if user_views != nil {
		views := [][]byte{}
		views = parse_records(user_views.Bins["value"], views)
		slices.SortFunc(views, compare_func)
		views = views[:min(len(views), 200)]
		send_views = make([]UserTag, 0, len(views))
		for _, v := range views {
			json.Unmarshal(v, &user_tag)
			user_time := parse_timestamp(user_tag.Time)

			if user_time.Compare(start_range) >= 0 && user_time.Compare(end_range) < 0 {
				send_views = append(send_views, user_tag)
			}

		}
	}

	if user_buys != nil {

		buys := [][]byte{}
		buys = parse_records(user_buys.Bins["value"], buys)

		slices.SortFunc(buys, compare_func)

		buys = buys[:min(len(buys), 200)]

		send_buys = make([]UserTag, 0, len(buys))

		for _, v := range buys {
			json.Unmarshal(v, &user_tag)
			user_time := parse_timestamp(user_tag.Time)
			if user_time.Compare(start_range) >= 0 && user_time.Compare(end_range) < 0 {
				send_buys = append(send_buys, user_tag)
			}

		}
	}

	send_views = send_views[:min(len(send_views), limit)]
	send_buys = send_buys[:min(len(send_buys), limit)]

	w.Header().Set("Content-Type", "application/json")
	send_bytes, _ := json.Marshal(UserProfileResult{Cookie: cookie, Views: send_views, Buys: send_buys})
	w.Write(send_bytes)

}

func aggregatesMock(w http.ResponseWriter, r *http.Request) {
	body_bytes, _ := io.ReadAll(r.Body)
	w.Header().Set("Content-Type", "application/json")
	w.Write(body_bytes)
}

func aggregateUserActions(w http.ResponseWriter, r *http.Request) {
	time_range := r.URL.Query().Get("time_range")
	action := r.URL.Query().Get("action")
	aggregates := r.URL.Query()["aggregates"]
	origin := r.URL.Query().Get("origin")
	brand_id := r.URL.Query().Get("brand_id")
	category_id := r.URL.Query().Get("category_id")

	keys := generate_query_keys(time_range, action, origin, brand_id, category_id)
	rows := [][]string{}

	for _, key := range keys {
		as_key, _ := aero.NewKey(namespace, "aggregates", key)
		record, _ := aerospike_client.Get(aero.NewPolicy(), as_key)
        i, _ := strconv.ParseInt(strings.Split(key, "|")[0], 10, 64)
        timestamp := time.Unix(i, 0).Format("2006-01-02T15:04:05")
        row := []string{}
        for _, row_val := range []string{timestamp, action, origin, brand_id, category_id} {
            if row_val != "" {
                row = append(row, row_val)
            }
        }

		if record != nil {
			for _, aggregate_val := range aggregates {
				row = append(row, strconv.Itoa(record.Bins[strings.ToLower(aggregate_val)].(int)))
			}
		} else {
            for range aggregates {
                row = append(row, "0")
            }
        }
        rows = append(rows, row)
        
	}

	cols := []string{"1m_bucket", "action"}


    if origin != "" {
        cols = append(cols, "origin")
    }
    if brand_id != "" {
        cols = append(cols, "brand_id")
    }
    if category_id != "" {
        cols = append(cols, "category_id")
    }


	for _, agg_val := range aggregates {
		if agg_val != "" {
			cols = append(cols, strings.ToLower(agg_val))
		}
	}

	result_bytes, _ := json.Marshal(AggregatesResult{Columns: cols, Rows: rows})

	w.Header().Set("Content-Type", "application/json")
	w.Write(result_bytes)
}

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
