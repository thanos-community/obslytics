module github.com/thanos-community/obslytics

go 1.14

require (
	github.com/alicebob/gopher-json v0.0.0-20200520072559-a9ecdc9d1d3a // indirect
	github.com/baiyubin/aliyun-sts-go-sdk v0.0.0-20180326062324-cfa1a18b161f // indirect
	github.com/cortexproject/cortex v1.8.1-0.20210422151339-cf1c444e0905
	github.com/go-kit/kit v0.10.0
	github.com/gogo/googleapis v1.4.0 // indirect
	github.com/gomodule/redigo v1.8.4 // indirect
	github.com/googleapis/gnostic v0.5.1 // indirect
	github.com/hashicorp/go-hclog v0.14.1 // indirect
	github.com/mattn/go-colorable v0.1.7 // indirect
	github.com/oklog/run v1.1.0
	github.com/pkg/errors v0.9.1
	github.com/prometheus/common v0.21.0
	github.com/prometheus/prometheus v1.8.2-0.20210421143221-52df5ef7a3be
	github.com/thanos-io/thanos v0.20.1
	github.com/xitongsys/parquet-go v1.5.2
	github.com/xitongsys/parquet-go-source v0.0.0-20200817004010-026bad9b25d0
	github.com/yuin/gopher-lua v0.0.0-20200816102855-ee81675732da // indirect
	go.uber.org/automaxprocs v1.3.0
	google.golang.org/grpc v1.36.0
	gopkg.in/alecthomas/kingpin.v2 v2.2.6
	gopkg.in/yaml.v2 v2.4.0
)

// Compatibility constraints

replace (
	// Using a 3rd-party branch for custom dialer - see https://github.com/bradfitz/gomemcache/pull/86.
	// Required by Cortex https://github.com/cortexproject/cortex/pull/3051.
	github.com/bradfitz/gomemcache => github.com/themihai/gomemcache v0.0.0-20180902122335-24332e2d58ab
	// Update to v1.1.1 to make sure windows CI pass.
	github.com/elastic/go-sysinfo => github.com/elastic/go-sysinfo v1.1.1

	// TODO: Remove this: https://github.com/thanos-io/thanos/issues/3967.
	github.com/minio/minio-go/v7 => github.com/bwplotka/minio-go/v7 v7.0.11-0.20210324165441-f9927e5255a6
	// Make sure Prometheus version is pinned as Prometheus semver does not include Go APIs.
	github.com/prometheus/prometheus => github.com/prometheus/prometheus v1.8.2-0.20210421143221-52df5ef7a3be
	github.com/sercand/kuberesolver => github.com/sercand/kuberesolver v2.4.0+incompatible
	github.com/thanos-io/thanos => github.com/thanos-io/thanos v0.20.1

	google.golang.org/grpc => google.golang.org/grpc v1.29.1

	// Overriding to use latest commit
	gopkg.in/alecthomas/kingpin.v2 => github.com/alecthomas/kingpin v1.3.8-0.20210301060133-17f40c25f497

	// From Prometheus.
	k8s.io/klog => github.com/simonpasquier/klog-gokit v0.3.0
	k8s.io/klog/v2 => github.com/simonpasquier/klog-gokit/v2 v2.0.1
)
