module github.com/thanos-community/obslytics

go 1.14

require (
	github.com/Azure/azure-sdk-for-go v45.0.0+incompatible // indirect
	github.com/Azure/go-autorest/autorest/to v0.4.0 // indirect
	github.com/Azure/go-autorest/autorest/validation v0.3.0 // indirect
	github.com/OneOfOne/xxhash v1.2.6 // indirect
	github.com/aws/aws-sdk-go v1.33.17 // indirect
	github.com/baiyubin/aliyun-sts-go-sdk v0.0.0-20180326062324-cfa1a18b161f // indirect
	github.com/containerd/containerd v1.3.6 // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/cortexproject/cortex v1.3.1-0.20200901115931-255ff3306960
	github.com/elastic/go-sysinfo v1.1.1 // indirect
	github.com/elastic/go-windows v1.0.1 // indirect
	github.com/evanphx/json-patch v4.5.0+incompatible // indirect
	github.com/go-kit/kit v0.10.0
	github.com/gogo/googleapis v1.4.0 // indirect
	github.com/google/go-cmp v0.5.1 // indirect
	github.com/googleapis/gnostic v0.5.1 // indirect
	github.com/gopherjs/gopherjs v0.0.0-20191106031601-ce3c9ade29de // indirect
	github.com/gorilla/websocket v1.4.0 // indirect
	github.com/hashicorp/go-hclog v0.14.1 // indirect
	github.com/hashicorp/go-msgpack v0.5.5 // indirect
	github.com/hashicorp/serf v0.9.3 // indirect
	github.com/klauspost/cpuid v1.3.1 // indirect
	github.com/mattn/go-colorable v0.1.7 // indirect
	github.com/mattn/go-ieproxy v0.0.0-20191113090002-7c0f6868bffe // indirect
	github.com/miekg/dns v1.1.31 // indirect
	github.com/mitchellh/mapstructure v1.3.3 // indirect
	github.com/oklog/run v1.1.0
	github.com/onsi/ginkgo v1.14.0 // indirect
	github.com/pkg/errors v0.9.1
	github.com/prometheus/common v0.11.1
	github.com/prometheus/prometheus v1.8.2-0.20200819132913-cb830b0a9c78
	github.com/sercand/kuberesolver v2.4.0+incompatible // indirect
	github.com/smartystreets/assertions v1.0.1 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/stretchr/testify v1.6.1 // indirect
	github.com/thanos-io/thanos v0.13.1-0.20200807203500-9b578afb4763
	github.com/tmc/grpc-websocket-proxy v0.0.0-20190109142713-0ad062ec5ee5 // indirect
	github.com/xitongsys/parquet-go v1.5.2
	github.com/xitongsys/parquet-go-source v0.0.0-20200817004010-026bad9b25d0
	go.uber.org/automaxprocs v1.3.0
	go.uber.org/zap v1.14.1 // indirect
	golang.org/x/sys v0.0.0-20200803210538-64077c9b5642 // indirect
	golang.org/x/tools v0.0.0-20200806022845-90696ccdc692 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	google.golang.org/grpc v1.30.0
	gopkg.in/alecthomas/kingpin.v2 v2.2.6
	gopkg.in/yaml.v2 v2.3.0
	honnef.co/go/tools v0.0.1-2020.1.4 // indirect
	k8s.io/client-go v11.0.0+incompatible // indirect
	k8s.io/klog/v2 v2.3.0 // indirect
	k8s.io/utils v0.0.0-20200731180307-f00132d28269 // indirect
)

// Compatibility constraints
replace (
	// Using a 3rd-party branch for custom dialer - see https://github.com/bradfitz/gomemcache/pull/86
	github.com/bradfitz/gomemcache => github.com/themihai/gomemcache v0.0.0-20180902122335-24332e2d58ab
	github.com/googleapis/gnostic => github.com/googleapis/gnostic v0.4.0
	github.com/prometheus/prometheus => github.com/prometheus/prometheus v1.8.2-0.20200811193703-869f1bc587e6
	github.com/sercand/kuberesolver => github.com/sercand/kuberesolver v2.4.0+incompatible
	github.com/thanos-io/thanos => github.com/thanos-io/thanos v0.13.1-0.20200914133838-a0ef98215086
	google.golang.org/grpc => google.golang.org/grpc v1.29.1
	k8s.io/client-go => k8s.io/client-go v0.18.3
	k8s.io/klog => k8s.io/klog v0.3.1
	k8s.io/kube-openapi => k8s.io/kube-openapi v0.0.0-20190228160746-b3a7cee44a30
)
