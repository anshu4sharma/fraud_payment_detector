package utils

import (
	"fmt"
	"os"
	"path/filepath"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Logger struct {
	log *zap.Logger
}

func NewLogger(env, serviceName string) *Logger {
	var cfg zap.Config

	if env == "prod" {
		cfg = zap.NewProductionConfig()
	} else {
		cfg = zap.NewDevelopmentConfig()
	}

	cfg.Encoding = "json"
	cfg.EncoderConfig.TimeKey = "ts"
	cfg.EncoderConfig.EncodeTime = zapcore.RFC3339TimeEncoder
	cfg.EncoderConfig.LevelKey = "level"
	cfg.EncoderConfig.MessageKey = "msg"
	cfg.EncoderConfig.CallerKey = "caller"
	cfg.EncoderConfig.EncodeLevel = zapcore.LowercaseLevelEncoder
	cfg.EncoderConfig.EncodeCaller = zapcore.ShortCallerEncoder

	outputs := []string{"stdout"}
	errorOutputs := []string{"stderr"}

	if logDir := os.Getenv("LOG_DIR"); logDir != "" {
		if err := os.MkdirAll(logDir, 0755); err != nil {
			fmt.Fprintf(os.Stderr, "failed to create log dir: %v\n", err)
		} else {
			logFile := filepath.Join(logDir, serviceName+".log")
			outputs = append(outputs, logFile)
			errorOutputs = append(errorOutputs, logFile)
		}
	}

	cfg.OutputPaths = outputs
	cfg.ErrorOutputPaths = errorOutputs
	cfg.InitialFields = map[string]any{
		"service": serviceName,
	}

	logger, err := cfg.Build()
	if err != nil {
		panic(err)
	}

	return &Logger{log: logger}
}

func (l *Logger) Info(msg string, fields ...zap.Field)  { l.log.Info(msg, fields...) }
func (l *Logger) Warn(msg string, fields ...zap.Field)  { l.log.Warn(msg, fields...) }
func (l *Logger) Error(msg string, fields ...zap.Field) { l.log.Error(msg, fields...) }
func (l *Logger) Debug(msg string, fields ...zap.Field) { l.log.Debug(msg, fields...) }
func (l *Logger) Sync() error                           { return l.log.Sync() }
