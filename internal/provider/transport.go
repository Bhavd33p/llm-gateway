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
		ResponseHeaderTimeout: u.FirstTokenTimeout,
		ExpectContinueTimeout: time.Second,
	}
	// No Client.Timeout: it would cut long streams off mid-generation. Deadlines come from each request's context.
	return &http.Client{Transport: transport}
}
