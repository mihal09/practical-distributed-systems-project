package main

import (
	"encoding/json"
	"fmt"
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
var kafka_topic = "user_tags"

const (
	AEROSPIKE_HOST          = "aerospikedb"
	AEROSPIKE_MAX_RETRIES   = 5
	AEROSPIKE_PORT          = 3000
	AEROSPIKE_NAMESPACE     = "mimuw"
	AEROSPIKE_TAG_SET       = "tags"
	AEROSPIKE_AGGREGATE_SET = "aggregates"
	KAFKA_BROKERS           = "broker-1:19092,broker-2:19092"
	APPLICATION_HOST        = "0.0.0.0:5000"
)

func main() {
	r := chi.NewRouter()

	var err aero.Error
	aerospike_client, err = aero.NewClient(AEROSPIKE_HOST, AEROSPIKE_PORT)
	if err != nil {
		panic(err)
	}

	var ok error
	kafka_producer, ok = kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": KAFKA_BROKERS,
		"linger.ms":         5000,
		"compression.type":  "snappy",
	})
	if ok != nil {
		panic(ok)
	}

	go func() {
		for e := range kafka_producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				// The message delivery report, indicating success or
				// permanent failure after retries have been exhausted.
				// Application level retries won't help since the client
				// is already configured to do that.
				m := ev
				if m.TopicPartition.Error != nil {
					fmt.Printf("[DEBUG EVENT] Delivery failed: %v\n", m.TopicPartition.Error)
				} else {
					// fmt.Printf("[DEBUG EVENT] Delivered message to topic %s [%d] at offset %v\n",
					// 	*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
				}
			case kafka.Error:
				// Generic client instance-level errors, such as
				// broker connection failures, authentication issues, etc.
				//
				// These errors should generally be considered informational
				// as the underlying client will automatically try to
				// recover from any errors encountered, the application
				// does not need to take action on them.
				fmt.Printf("[DEBUG EVENT] Error: %v\n", ev)
			default:
				fmt.Printf("[DEBUG EVENT] Ignored event: %s\n", ev)
			}
		}
	}()

	r.Route("/user_tags", func(r chi.Router) {
		r.Post("/", addUserTags)
	})

	r.Route("/user_profiles/{cookie}", func(r chi.Router) {
		r.Post("/", getUserProfiles)
	})

	r.Route("/aggregates", func(r chi.Router) {
		r.Post("/", aggregateUserActions)
	})

	http.ListenAndServe(APPLICATION_HOST, r)
}

func addUserTags(w http.ResponseWriter, r *http.Request) {
	body_bytes, _ := io.ReadAll(r.Body)
	user_tag := UserTag{}
	json.Unmarshal(body_bytes, &user_tag)

	key_string := user_tag.Cookie + ":" + strings.ToLower(user_tag.Action)
	key, _ := aero.NewKey(AEROSPIKE_NAMESPACE, AEROSPIKE_TAG_SET, key_string)

	write_policy := aero.NewWritePolicy(0, 0)
	write_policy.RecordExistsAction = aero.REPLACE
	write_policy.GenerationPolicy = aero.EXPECT_GEN_EQUAL

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

	for counter := 0; ; counter += 1 {

		var write_err aero.Error

		records, _ := aerospike_client.Get(aero.NewPolicy(), key)
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
			if counter > AEROSPIKE_MAX_RETRIES {
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

	views_key, _ := aero.NewKey(AEROSPIKE_NAMESPACE, AEROSPIKE_TAG_SET, cookie+":view")
	buys_key, _ := aero.NewKey(AEROSPIKE_NAMESPACE, AEROSPIKE_TAG_SET, cookie+":buy")
	user_views, _ := aerospike_client.Get(aero.NewPolicy(), views_key)
	user_buys, _ := aerospike_client.Get(aero.NewPolicy(), buys_key)

	send_views := process_user_events(user_views, start_range, end_range)
	send_buys := process_user_events(user_buys, start_range, end_range)

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
		as_key, _ := aero.NewKey(AEROSPIKE_NAMESPACE, AEROSPIKE_AGGREGATE_SET, key)
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
