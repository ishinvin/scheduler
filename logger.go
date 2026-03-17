package scheduler

import "log/slog"

type slogLogger struct{}

func (*slogLogger) Info(msg string, keysAndValues ...any) {
	slog.Info(msg, keysAndValues...)
}

func (*slogLogger) Error(msg string, keysAndValues ...any) {
	slog.Error(msg, keysAndValues...)
}
