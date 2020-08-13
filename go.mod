module github.com/thanos-community/obslytics

go 1.14

require (
	cloud.google.com/go v0.62.0 // indirect
	github.com/Azure/azure-sdk-for-go v45.0.0+incompatible // indirect
	github.com/Azure/go-autorest v14.2.0+incompatible // indirect
	github.com/Azure/go-autorest/autorest v0.11.2 // indirect
	github.com/Azure/go-autorest/autorest/to v0.4.0 // indirect
	github.com/Azure/go-autorest/autorest/validation v0.3.0 // indirect
	github.com/aws/aws-sdk-go v1.33.17 // indirect
	github.com/containerd/containerd v1.3.6 // indirect
	github.com/digitalocean/godo v1.42.0 // indirect
	github.com/go-kit/kit v0.10.0
	github.com/gogo/googleapis v1.4.0 // indirect
	github.com/gogo/status v1.1.0 // indirect
	github.com/googleapis/gnostic v0.5.1 // indirect
	github.com/gophercloud/gophercloud v0.12.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.2.0 // indirect
	github.com/hashicorp/consul/api v1.5.0 // indirect
	github.com/hashicorp/go-hclog v0.14.1 // indirect
	github.com/hashicorp/serf v0.9.3 // indirect
	github.com/mattn/go-colorable v0.1.7 // indirect
	github.com/miekg/dns v1.1.31 // indirect
	github.com/mitchellh/mapstructure v1.3.3 // indirect
	github.com/oklog/run v1.1.0
	github.com/opentracing-contrib/go-stdlib v1.0.0 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.7.1 // indirect
	github.com/prometheus/common v0.10.0
	github.com/prometheus/prometheus v1.8.2-0.20200629082805-315564210816
	github.com/samuel/go-zookeeper v0.0.0-20200724154423-2164a8ac840e // indirect
	github.com/sirupsen/logrus v1.6.0 // indirect
	github.com/thanos-io/thanos v0.14.0
	github.com/uber/jaeger-client-go v2.25.0+incompatible // indirect
	github.com/xitongsys/parquet-go v1.5.2
	github.com/xitongsys/parquet-go-source v0.0.0-20200805105948-52b27ba08556
	go.uber.org/automaxprocs v1.3.0
	golang.org/x/crypto v0.0.0-20200728195943-123391ffb6de // indirect
	golang.org/x/sys v0.0.0-20200802091954-4b90ce9b60b3 // indirect
	golang.org/x/time v0.0.0-20200630173020-3af7569d3a1e // indirect
	google.golang.org/genproto v0.0.0-20200731012542-8145dea6a485 // indirect
	google.golang.org/grpc v1.31.0
	gopkg.in/alecthomas/kingpin.v2 v2.2.6
	gopkg.in/yaml.v2 v2.3.0
	k8s.io/api v0.18.6 // indirect
	k8s.io/client-go v11.0.0+incompatible // indirect
	k8s.io/klog/v2 v2.3.0 // indirect
	k8s.io/utils v0.0.0-20200731180307-f00132d28269 // indirect
)

// Compatibility constraints
replace (
	github.com/googleapis/gnostic => github.com/googleapis/gnostic v0.4.0
	github.com/prometheus/prometheus => github.com/prometheus/prometheus v1.8.2-0.20200629082805-315564210816
	k8s.io/client-go => k8s.io/client-go v0.18.3
	k8s.io/klog => k8s.io/klog v0.3.1
	k8s.io/kube-openapi => k8s.io/kube-openapi v0.0.0-20190228160746-b3a7cee44a30
)
