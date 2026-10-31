package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"io"
	"math/rand/v2"
	"mime/multipart"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var regionCountry = map[string]string{"asia": "JP", "europe": "DE", "america": "US"}

var httpClient *http.Client
var sseClient *http.Client

func initHTTP(conns int) {
	tr := &http.Transport{
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
		MaxIdleConns:        conns * 4,
		MaxIdleConnsPerHost: conns * 4,
		IdleConnTimeout:     30 * time.Second,
		ForceAttemptHTTP2:   false,
	}
	httpClient = &http.Client{Transport: tr, Timeout: 20 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	sseClient = &http.Client{Transport: tr.Clone()}
}

var ipCounter atomic.Uint32
var ipRunByte = rand.IntN(200) + 20

func nextIP() string {
	n := ipCounter.Add(1)
	return fmt.Sprintf("10.%d.%d.%d", ipRunByte, (n/250)%250, n%250+1)
}

// Stats

type stat struct {
	durs   []time.Duration
	errors int
	status map[int]int
}

type Stats struct {
	mu sync.Mutex
	m  map[string]*stat
}

var stats = &Stats{m: map[string]*stat{}}

var idRe = regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)
var dateRe = regexp.MustCompile(`\d{4}-\d{2}-\d{2}`)

func normPath(method, path string) string {
	if i := strings.IndexByte(path, '?'); i >= 0 {
		path = path[:i]
	}
	path = idRe.ReplaceAllString(path, ":id")
	path = dateRe.ReplaceAllString(path, ":date")
	return method + " " + path
}

func (s *Stats) add(key string, d time.Duration, code int, isErr bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.m[key]
	if st == nil {
		st = &stat{status: map[int]int{}}
		s.m[key] = st
	}
	st.durs = append(st.durs, d)
	st.status[code]++
	if isErr {
		st.errors++
	}
}

// Client is one user (or admin) with its own region and client IP.
type Client struct {
	Base    string
	Region  string
	Country string
	IP      string
	Token   string
	// user info
	ID, Username, SecretKey string
}

func NewClient(base, region string) *Client {
	c := &Client{Base: base, Region: region, Country: regionCountry[region], IP: nextIP()}
	if c.Country == "" {
		c.Country = "JP"
	}
	return c
}

// Via returns a shallow copy that sends requests to another region's server.
func (c *Client) Via(region string) *Client {
	cc := *c
	cc.Country = regionCountry[region]
	return &cc
}

type Resp struct {
	Code int
	Body []byte
	Hdr  http.Header
	Err  error
}

func (r *Resp) JSON(v any) error {
	if r.Err != nil {
		return r.Err
	}
	return json.Unmarshal(r.Body, v)
}

func (r *Resp) ErrCode() string {
	var e struct {
		Error string `json:"error"`
	}
	_ = json.Unmarshal(r.Body, &e)
	return e.Error
}

func (r *Resp) String() string {
	if r.Err != nil {
		return "err=" + r.Err.Error()
	}
	b := r.Body
	if len(b) > 200 {
		b = b[:200]
	}
	return fmt.Sprintf("%d %s", r.Code, strings.TrimSpace(string(b)))
}

func (c *Client) do(method, path string, body io.Reader, ctype string, expectOK bool) *Resp {
	req, err := http.NewRequest(method, c.Base+path, body)
	if err != nil {
		return &Resp{Err: err}
	}
	req.Host = "isureal.local"
	req.Header.Set("X-Country-Code", c.Country)
	req.Header.Set("X-Client-IP", c.IP)
	if ctype != "" {
		req.Header.Set("Content-Type", ctype)
	}
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	start := time.Now()
	res, err := httpClient.Do(req)
	key := normPath(method, path)
	if err != nil {
		stats.add(key, time.Since(start), 0, true)
		return &Resp{Err: err}
	}
	defer res.Body.Close()
	b, err := io.ReadAll(res.Body)
	d := time.Since(start)
	stats.add(key, d, res.StatusCode, err != nil || res.StatusCode >= 500 || (expectOK && res.StatusCode >= 400))
	return &Resp{Code: res.StatusCode, Body: b, Hdr: res.Header, Err: err}
}

func (c *Client) Get(path string) *Resp { return c.do("GET", path, nil, "", true) }
func (c *Client) Delete(path string) *Resp {
	return c.do("DELETE", path, nil, "", true)
}
func (c *Client) JSONReq(method, path string, v any) *Resp {
	var body io.Reader
	ctype := ""
	if v != nil {
		b, _ := json.Marshal(v)
		body = bytes.NewReader(b)
		ctype = "application/json"
	}
	return c.do(method, path, body, ctype, true)
}

// GetNoExpect issues a GET where 4xx is an expected outcome (not counted as an error).
func (c *Client) GetNoExpect(path string) *Resp { return c.do("GET", path, nil, "", false) }
func (c *Client) JSONNoExpect(method, path string, v any) *Resp {
	var body io.Reader
	ctype := ""
	if v != nil {
		b, _ := json.Marshal(v)
		body = bytes.NewReader(b)
		ctype = "application/json"
	}
	return c.do(method, path, body, ctype, false)
}

func (c *Client) Multipart(path string, fields map[string]string, img []byte, expectOK bool) *Resp {
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	for k, v := range fields {
		_ = w.WriteField(k, v)
	}
	if img != nil {
		fw, _ := w.CreateFormFile("image", "image.jpg")
		_, _ = fw.Write(img)
	}
	_ = w.Close()
	return c.do("POST", path, &buf, w.FormDataContentType(), expectOK)
}

// API helpers

type UserJSON struct {
	ID              string  `json:"id"`
	Username        string  `json:"username"`
	DisplayName     string  `json:"displayName"`
	Bio             string  `json:"bio"`
	Region          string  `json:"region"`
	AccountType     string  `json:"accountType"`
	Status          string  `json:"status"`
	ProfileImageURL *string `json:"profileImageUrl"`
}

type PostJSON struct {
	ID              string   `json:"id"`
	Author          UserJSON `json:"author"`
	PostingWindowID string   `json:"postingWindowId"`
	ImageURL        string   `json:"imageUrl"`
	Caption         string   `json:"caption"`
	Visibility      string   `json:"visibility"`
	Location        struct {
		Kind string `json:"kind"`
	} `json:"location"`
	PostedAt    time.Time `json:"postedAt"`
	OnTime      bool      `json:"onTime"`
	LateSeconds int64     `json:"lateSeconds"`
	Score       *int64    `json:"score"`
	ImagePath   string    `json:"imagePath"`
	RealMojis   []struct {
		ID string `json:"id"`
	} `json:"realmojis"`
	Comments []struct {
		ID string `json:"id"`
	} `json:"comments"`
}

type ListJSON[T any] struct {
	Items   []T  `json:"items"`
	HasMore bool `json:"hasMore"`
}

func (c *Client) Register(username, display string) *Resp {
	r := c.JSONReq("POST", "/api/auth/register", map[string]string{"username": username, "displayName": display, "region": c.Region})
	if r.Code == 201 {
		var v struct {
			SecretKey string   `json:"secretKey"`
			User      UserJSON `json:"user"`
		}
		if r.JSON(&v) == nil {
			c.SecretKey, c.ID, c.Username = v.SecretKey, v.User.ID, v.User.Username
		}
	}
	return r
}

func (c *Client) Login() *Resp {
	r := c.JSONReq("POST", "/api/auth/login", map[string]string{"username": c.Username, "secretKey": c.SecretKey})
	if r.Code == 200 {
		var v struct {
			Token string   `json:"token"`
			User  UserJSON `json:"user"`
		}
		if r.JSON(&v) == nil {
			c.Token, c.ID = v.Token, v.User.ID
		}
	}
	return r
}

func (c *Client) AdminLogin() *Resp {
	r := c.JSONReq("POST", "/api/admin/auth/login", map[string]string{"username": "admin", "password": "password1234"})
	if r.Code == 200 {
		var v struct {
			Token string `json:"token"`
		}
		if r.JSON(&v) == nil {
			c.Token = v.Token
		}
	}
	return r
}

func (c *Client) Post(visibility, caption string, img []byte) (*Resp, PostJSON) {
	r := c.Multipart("/api/posts", map[string]string{"caption": caption, "visibility": visibility, "location_kind": "none"}, img, true)
	var p PostJSON
	if r.Code == 201 {
		_ = r.JSON(&p)
	}
	return r, p
}

func (c *Client) Feed(kind string) (*Resp, []PostJSON) {
	r := c.Get("/api/feeds/" + kind)
	var l ListJSON[PostJSON]
	if r.Code == 200 {
		_ = r.JSON(&l)
	}
	return r, l.Items
}

// SSE waits for one notification event. Must be called before the window opens.
type sseResult struct {
	Data string
	Err  error
}

func (c *Client) SubscribeSSE(ctx context.Context) (<-chan sseResult, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", c.Base+"/api/notifications", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Country-Code", c.Country)
	req.Header.Set("X-Client-IP", c.IP)
	req.Header.Set("Authorization", "Bearer "+c.Token)
	req.Header.Set("Accept", "text/event-stream")
	start := time.Now()
	res, err := sseClient.Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != 200 {
		res.Body.Close()
		return nil, fmt.Errorf("sse status %d", res.StatusCode)
	}
	ch := make(chan sseResult, 1)
	go func() {
		defer res.Body.Close()
		sc := bufio.NewScanner(res.Body)
		for sc.Scan() {
			line := sc.Text()
			if strings.HasPrefix(line, "data:") {
				stats.add("GET /api/notifications (SSE)", time.Since(start), 200, false)
				ch <- sseResult{Data: strings.TrimSpace(strings.TrimPrefix(line, "data:"))}
				return
			}
		}
		err := sc.Err()
		if err == nil {
			err = io.EOF
		}
		ch <- sseResult{Err: err}
	}()
	return ch, nil
}

// images

func makeJPEG(w, h int) []byte {
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	r, g, b := uint8(rand.IntN(256)), uint8(rand.IntN(256)), uint8(rand.IntN(256))
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			img.Set(x, y, color.RGBA{r + uint8(x), g + uint8(y), b ^ uint8(x*y), 255})
		}
	}
	var buf bytes.Buffer
	_ = jpeg.Encode(&buf, img, &jpeg.Options{Quality: 80})
	return buf.Bytes()
}
