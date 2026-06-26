package rabbitmq

import (
	"cmp"
	"net/url"
)

// promoteCredentials returns address with credentials promoted into the URL
// userinfo, where amqp091-go expects them. The smarty messaging/v3 connector
// also honors credentials supplied as ?username=&password= query parameters, but
// amqp091 reads only the userinfo and otherwise falls back to guest/guest —
// yielding a 403. Promoting them here lets a single message_host config work for
// both the consumer and the dispatcher. Userinfo takes precedence over query
// parameters, matching messaging/v3; an unparseable address is returned as-is.
func promoteCredentials(address string) string {
	endpoint, err := url.Parse(address)
	if err != nil || endpoint == nil {
		return address
	}
	query := endpoint.Query()
	username := query.Get("username")
	password := query.Get("password")
	if username == "" && password == "" {
		return address
	}
	if endpoint.User != nil {
		username = cmp.Or(endpoint.User.Username(), username)
		if existing, ok := endpoint.User.Password(); ok {
			password = cmp.Or(existing, password)
		}
	}
	endpoint.User = url.UserPassword(username, password)
	query.Del("username")
	query.Del("password")
	endpoint.RawQuery = query.Encode()
	return endpoint.String()
}
