module github.com/mkuratczyk/go-amqp-perf

go 1.25.0

require github.com/Azure/go-amqp v1.6.0

// Fork: keep the require, add a replace (fork must still use module github.com/Azure/go-amqp), then go mod tidy.
//
// replace github.com/Azure/go-amqp => ../go-amqp
replace github.com/Azure/go-amqp => github.com/mkuratczyk/go-amqp v0.0.0-20260424062206-20cdc688dd28
