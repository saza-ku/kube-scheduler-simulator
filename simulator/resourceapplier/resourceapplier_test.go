package resourceapplier

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicFake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/restmapper"
	scheduling "k8s.io/kubernetes/pkg/apis/scheduling/v1"
	storage "k8s.io/kubernetes/pkg/apis/storage/v1"
)

func TestResourceApplier_createPods(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		podToApply    *corev1.Pod
		podAfterApply *corev1.Pod
		wantErr       bool
	}{
		{
			name: "create a Pod",
			podToApply: &corev1.Pod{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Pod",
					APIVersion: "v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pod-1",
					Namespace: "default",
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "container-1",
							Image: "image-1",
						},
					},
				},
			},
			podAfterApply: &corev1.Pod{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Pod",
					APIVersion: "v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pod-1",
					Namespace: "default",
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "container-1",
							Image: "image-1",
						},
					},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := runtime.NewScheme()
			v1.AddToScheme(s)
			scheduling.AddToScheme(s)
			storage.AddToScheme(s)
			client := dynamicFake.NewSimpleDynamicClient(s)
			resources := []*restmapper.APIGroupResources{
				{
					Group: metav1.APIGroup{
						Versions: []metav1.GroupVersionForDiscovery{
							{Version: "v1"},
						},
					},
					VersionedResources: map[string][]metav1.APIResource{
						"v1": {
							{Name: "pods", Namespaced: true, Kind: "Pod"},
						},
					},
				},
				{
					Group: metav1.APIGroup{
						Versions: []metav1.GroupVersionForDiscovery{
							{Version: "v1"},
						},
					},
					VersionedResources: map[string][]metav1.APIResource{
						"v1": {
							{Name: "nodes", Namespaced: true, Kind: "Node"},
						},
					},
				},
			}
			mapper := restmapper.NewDiscoveryRESTMapper(resources)
			service := New(client, mapper, Options{})

			p, err := runtime.DefaultUnstructuredConverter.ToUnstructured(tt.podToApply)
			if err != nil {
				t.Fatalf("failed to convert pod to unstructured: %v", err)
			}
			unstructedPod := &unstructured.Unstructured{Object: p}
			err = service.Create(context.Background(), unstructedPod)
			if (err != nil) != tt.wantErr {
				t.Errorf("createPods() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			got, err := client.Resource(v1.Resource("pods").WithVersion("v1")).Namespace("default").Get(context.Background(), tt.podToApply.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatalf("failed to get pod when comparing: %v", err)
			}
			var gotPod corev1.Pod
			err = runtime.DefaultUnstructuredConverter.FromUnstructured(got.UnstructuredContent(), &gotPod)
			if err != nil {
				t.Fatalf("failed to convert got unstructured to pod: %v", err)
			}

			if diff := cmp.Diff(tt.podAfterApply, gotPod); diff != "" {
				t.Errorf("createPods() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
