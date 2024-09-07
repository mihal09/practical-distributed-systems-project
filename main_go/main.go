package main

import (
	"encoding/json"
	aero "github.com/aerospike/aerospike-client-go/v7"
	"github.com/go-chi/chi/v5"
	"io"
	"net/http"
	"slices"
	"strings"
    "strconv"
	"time"
)

var aerospike_client *aero.Client

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

var (
	host          = "aerospikedb"
	port          = 3000
	namespace     = "mimuw"
	set           = "tags"
)

func main() {
	r := chi.NewRouter()

	var err aero.Error
	aerospike_client, err = aero.NewClient("aerospikedb", 3000)

	if err != nil {
		panic(err)
	}

	r.Route("/user_tags", func(r chi.Router) {
		r.Post("/", addUserTags)
	})

	r.Route("/user_profiles/{cookie}", func(r chi.Router) {
		r.Post("/", getUserProfiles)
	})

	r.Route("/aggregates", func(r chi.Router) {
		r.Post("/", aggregatesMock)
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


	for {
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

		if write_err == nil {
            break
		} 
	}
	w.WriteHeader(http.StatusNoContent)
}

func getUserProfiles(w http.ResponseWriter, r *http.Request) {
	time_range := strings.Split(r.URL.Query().Get("time_range"), "_")
	start_range, _ := time.Parse("2006-01-02T15:04:05.000", time_range[0])
	end_range, _ := time.Parse("2006-01-02T15:04:05.000", time_range[1])

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
	w.WriteHeader(http.StatusOK)
	send_bytes, _ := json.Marshal(UserProfileResult{Cookie: cookie, Views: send_views, Buys: send_buys})
	w.Write(send_bytes)

}

func aggregatesMock(w http.ResponseWriter, r *http.Request) {
	body_bytes, _ := io.ReadAll(r.Body)
	w.Header().Set("Content-Type", "application/json")
	w.Write(body_bytes)
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
