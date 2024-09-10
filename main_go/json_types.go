package main

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
