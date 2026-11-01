package provider

import (
	"net"
	"net/http"
	"time"

	"github.com/Bhavd33p/llm-gateway/internal/config"
)

// NewHTTPClient builds one pooled client per upstream. A separate pool per upstream means a slow provider cannot
// hold every idle connection hostage, and each pool can be sized for its own concurrency.
func NewHTTPClient(u config.Upstream) *http.Client {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          u.MaxIdleConns,
		MaxIdleConnsPerHost:   u.MaxIdleConns, // the default of 2 would close and redial most connections under load
		MaxConnsPerHost:       u.MaxConns,     // 0 = unlimited
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
	// No Client.Timeout and no ResponseHeaderTimeout: a non-streaming answer only sends headers once the whole
	// generation is done, and a stream can run for minutes. Deadlines come from each request's context instead:
	// the router's first-token timer for streams and the upstream timeout for non-streaming requests.
	return &http.Client{Transport: transport}
}
