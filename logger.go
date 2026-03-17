package scheduler

import "log/slog"

// Logger is a minimal structured logger.
type Logger interface {
	Info(msg string, keysAndValues ...any)
	Error(msg string, keysAndValues ...any)
}

type slogLogger struct{}

func (*slogLogger) Info(msg string, keysAndValues ...any) {
	slog.Info(msg, keysAndValues...)
}

func (*slogLogger) Error(msg string, keysAndValues ...any) {
	slog.Error(msg, keysAndValues...)
}
