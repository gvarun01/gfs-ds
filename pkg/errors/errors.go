// Package errors provides standardized error handling for the GFS system.
// It defines common error types, codes, and provides utilities for consistent error reporting.
package errors

import (
	"fmt"

	"github.com/Mit-Vin/GFS-Distributed-Systems/pkg/constants"
)

// ErrorCode represents standardized error codes for the GFS system
type ErrorCode string

// Standard error codes
const (
	ErrCodeFileNotFound       ErrorCode = "FILE_NOT_FOUND"
	ErrCodeChunkNotFound      ErrorCode = "CHUNK_NOT_FOUND"
	ErrCodeInvalidFilename    ErrorCode = "INVALID_FILENAME"
	ErrCodeInvalidOperation   ErrorCode = "INVALID_OPERATION"
	ErrCodeNetworkFailure     ErrorCode = "NETWORK_FAILURE"
	ErrCodePermissionDenied   ErrorCode = "PERMISSION_DENIED"
	ErrCodeQuotaExceeded      ErrorCode = "QUOTA_EXCEEDED"
	ErrCodeServiceUnavailable ErrorCode = "SERVICE_UNAVAILABLE"
	ErrCodeTimeout            ErrorCode = "TIMEOUT"
	ErrCodeInternalError      ErrorCode = "INTERNAL_ERROR"
	ErrCodeConfigError        ErrorCode = "CONFIG_ERROR"
	ErrCodeValidationError    ErrorCode = "VALIDATION_ERROR"
)

// GFSError represents a structured error in the GFS system
type GFSError struct {
	Code     ErrorCode              `json:"code"`
	Message  string                 `json:"message"`
	Details  map[string]interface{} `json:"details,omitempty"`
	Cause    error                  `json:"-"`
	Category string                 `json:"category"`
}

// Error implements the error interface
func (e *GFSError) Error() string {
	if e.Details != nil && len(e.Details) > 0 {
		return fmt.Sprintf("[%s] %s (details: %+v)", e.Code, e.Message, e.Details)
	}
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

// Unwrap returns the underlying cause error
func (e *GFSError) Unwrap() error {
	return e.Cause
}

// New creates a new GFSError with the specified code and message
func New(code ErrorCode, message string) *GFSError {
	return &GFSError{
		Code:     code,
		Message:  message,
		Category: getCategoryForCode(code),
	}
}

// Newf creates a new GFSError with formatted message
func Newf(code ErrorCode, format string, args ...interface{}) *GFSError {
	return &GFSError{
		Code:     code,
		Message:  fmt.Sprintf(format, args...),
		Category: getCategoryForCode(code),
	}
}

// Wrap wraps an existing error with a GFS error code and message
func Wrap(err error, code ErrorCode, message string) *GFSError {
	return &GFSError{
		Code:     code,
		Message:  message,
		Cause:    err,
		Category: getCategoryForCode(code),
	}
}

// Wrapf wraps an existing error with a GFS error code and formatted message
func Wrapf(err error, code ErrorCode, format string, args ...interface{}) *GFSError {
	return &GFSError{
		Code:     code,
		Message:  fmt.Sprintf(format, args...),
		Cause:    err,
		Category: getCategoryForCode(code),
	}
}

// WithDetails adds details to an existing GFSError
func (e *GFSError) WithDetails(key string, value interface{}) *GFSError {
	if e.Details == nil {
		e.Details = make(map[string]interface{})
	}
	e.Details[key] = value
	return e
}

// WithDetailsMap adds multiple details to an existing GFSError
func (e *GFSError) WithDetailsMap(details map[string]interface{}) *GFSError {
	if e.Details == nil {
		e.Details = make(map[string]interface{})
	}
	for k, v := range details {
		e.Details[k] = v
	}
	return e
}

// IsGFSError checks if an error is a GFSError
func IsGFSError(err error) bool {
	_, ok := err.(*GFSError)
	return ok
}

// GetCode extracts the error code from an error if it's a GFSError
func GetCode(err error) ErrorCode {
	if gfsErr, ok := err.(*GFSError); ok {
		return gfsErr.Code
	}
	return ErrCodeInternalError
}

// getCategoryForCode returns the category for a given error code
func getCategoryForCode(code ErrorCode) string {
	switch code {
	case ErrCodeFileNotFound, ErrCodeChunkNotFound:
		return "NotFound"
	case ErrCodeInvalidFilename, ErrCodeInvalidOperation, ErrCodeValidationError:
		return "Validation"
	case ErrCodeNetworkFailure, ErrCodeTimeout, ErrCodeServiceUnavailable:
		return "Network"
	case ErrCodePermissionDenied:
		return "Security"
	case ErrCodeQuotaExceeded:
		return "Resource"
	case ErrCodeConfigError:
		return "Configuration"
	case ErrCodeInternalError:
		return "Internal"
	default:
		return "Unknown"
	}
}

// Common error constructors for frequently used errors

// ErrFileNotFound creates a file not found error
func ErrFileNotFound(filename string) *GFSError {
	return New(ErrCodeFileNotFound, constants.ErrFileNotFound).
		WithDetails("filename", filename)
}

// ErrChunkNotFound creates a chunk not found error
func ErrChunkNotFound(chunkID string) *GFSError {
	return New(ErrCodeChunkNotFound, constants.ErrChunkNotFound).
		WithDetails("chunk_id", chunkID)
}

// ErrInvalidFilename creates an invalid filename error
func ErrInvalidFilename(filename string) *GFSError {
	return New(ErrCodeInvalidFilename, constants.ErrInvalidFilename).
		WithDetails("filename", filename)
}

// ErrInvalidOperation creates an invalid operation error
func ErrInvalidOperation(operation string) *GFSError {
	return New(ErrCodeInvalidOperation, constants.ErrInvalidOperation).
		WithDetails("operation", operation)
}

// ErrNetworkFailure creates a network failure error
func ErrNetworkFailure(details string) *GFSError {
	return New(ErrCodeNetworkFailure, constants.ErrNetworkFailure).
		WithDetails("details", details)
}

// ErrConfigValidation creates a configuration validation error
func ErrConfigValidation(field, reason string) *GFSError {
	return New(ErrCodeConfigError, "configuration validation failed").
		WithDetails("field", field).
		WithDetails("reason", reason)
}

// ErrTimeout creates a timeout error
func ErrTimeout(operation string, duration interface{}) *GFSError {
	return New(ErrCodeTimeout, "operation timed out").
		WithDetails("operation", operation).
		WithDetails("timeout", duration)
}
