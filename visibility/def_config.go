package visibility

import (
	"context"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/cyberax/go-dd-service-base/utils"
	"go.uber.org/zap"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	"gopkg.in/DataDog/dd-trace-go.v1/profiler"
	"os"
)

func SetupTracing(ctx context.Context, appName, envName string, logger *zap.Logger) (
	statsd.ClientInterface, error) {

	if logger == nil {
		logger = zap.NewNop()
	}

	agentHost := os.Getenv("DD_AGENT_HOST")
	if agentHost == "" {
		logger.Info("No DD_AGENT_HOST set, tracing and metrics are disabled")
		return &statsd.NoOpClient{}, nil
	}

	// Start the metrics submitter
	statsTags := []statsd.Option {
		statsd.WithNamespace(appName+"."),
		statsd.WithTags([]string{"env:"+envName}),
	}

	var cli statsd.ClientInterface
	cli, err := statsd.New("", statsTags...)
	if err != nil {
		cli = &statsd.NoOpClient{}
		logger.Error("Failed to initialize the stats daemon", zap.Error(err))
	}

	// Start the tracer
	options := []tracer.StartOption{
		tracer.WithAnalytics(true),
		tracer.WithServiceName(utils.ToSnakeCase(appName, '-')),
		tracer.WithGlobalTag("env", envName),
	}
	profilerOptions := []profiler.Option{
		profiler.WithService(utils.ToSnakeCase(appName, '-')),
		profiler.WithEnv(envName),
		profiler.WithStatsd(cli),
		profiler.WithProfileTypes(
			profiler.HeapProfile, profiler.CPUProfile, profiler.BlockProfile,
			profiler.MutexProfile, profiler.GoroutineProfile),
		profiler.WithAPIKey(""), // Clear the API key to enable the local agent use
	}

	// Hostname is not always pulled automatically
	ddHost := os.Getenv("DD_HOSTNAME")
	if ddHost != "" {
		options = append(options, tracer.WithGlobalTag("host", ddHost))
		profilerOptions = append(profilerOptions, profiler.WithTags("host:" +ddHost))
	}
	tracer.Start(options...)

	// Start the profiler
	err = profiler.Start(profilerOptions...)
	if err != nil {
		logger.Error("Failed to initialize the profiler", zap.Error(err))
	}

	return cli, nil
}

func TearDownTracing(ctx context.Context, client statsd.ClientInterface) {
	tracer.Stop()
	profiler.Stop()
	_ = client.Flush()
	_ = client.Close()
}
