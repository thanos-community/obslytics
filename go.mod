module github.com/thanos-community/obslytics

go 1.14

require (
	github.com/go-kit/kit v0.10.0
	github.com/oklog/run v1.1.0
	github.com/pkg/errors v0.9.1
	github.com/prometheus/common v0.10.0
	github.com/thanos-io/thanos v0.14.0
	go.uber.org/automaxprocs v1.3.0
	gopkg.in/alecthomas/kingpin.v2 v2.2.6
)

replace k8s.io/client-go => k8s.io/client-go v0.0.0-20190620085101-78d2af792bab
