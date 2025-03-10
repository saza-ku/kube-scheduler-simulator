// Code generated by MockGen. DO NOT EDIT.
// Source: sigs.k8s.io/kube-scheduler-simulator/simulator/resourcewatcher/streamwriter (interfaces: ResponseStream)
//
// Generated by this command:
//
//	mockgen -destination=./mock_streamwriter/responseStream.go . ResponseStream
//

// Package mock_streamwriter is a generated GoMock package.
package mock_streamwriter

import (
	reflect "reflect"

	gomock "go.uber.org/mock/gomock"
)

// MockResponseStream is a mock of ResponseStream interface.
type MockResponseStream struct {
	ctrl     *gomock.Controller
	recorder *MockResponseStreamMockRecorder
	isgomock struct{}
}

// MockResponseStreamMockRecorder is the mock recorder for MockResponseStream.
type MockResponseStreamMockRecorder struct {
	mock *MockResponseStream
}

// NewMockResponseStream creates a new mock instance.
func NewMockResponseStream(ctrl *gomock.Controller) *MockResponseStream {
	mock := &MockResponseStream{ctrl: ctrl}
	mock.recorder = &MockResponseStreamMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockResponseStream) EXPECT() *MockResponseStreamMockRecorder {
	return m.recorder
}

// Flush mocks base method.
func (m *MockResponseStream) Flush() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Flush")
}

// Flush indicates an expected call of Flush.
func (mr *MockResponseStreamMockRecorder) Flush() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Flush", reflect.TypeOf((*MockResponseStream)(nil).Flush))
}

// Write mocks base method.
func (m *MockResponseStream) Write(p []byte) (int, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Write", p)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Write indicates an expected call of Write.
func (mr *MockResponseStreamMockRecorder) Write(p any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Write", reflect.TypeOf((*MockResponseStream)(nil).Write), p)
}
