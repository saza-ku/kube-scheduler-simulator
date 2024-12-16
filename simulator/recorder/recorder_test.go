package recorder

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	dynamicFake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/restmapper"
	appsv1 "k8s.io/kubernetes/pkg/apis/apps/v1"
	schedulingv1 "k8s.io/kubernetes/pkg/apis/scheduling/v1"
	storagev1 "k8s.io/kubernetes/pkg/apis/storage/v1"
)

func TestRecorder(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name             string
		resourceToCreate []unstructured.Unstructured
		resourceToUpdate []unstructured.Unstructured
		resourceToDelete []unstructured.Unstructured
		want             []Record
		wantErr          bool
	}{
		{
			name: "should record creating pods",
			resourceToCreate: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"apiVersion": "v1",
						"kind":       "Pod",
						"metadata": map[string]interface{}{
							"name":      "pod-1",
							"namespace": "default",
						},
					},
				},
				{
					Object: map[string]interface{}{
						"apiVersion": "v1",
						"kind":       "Pod",
						"metadata": map[string]interface{}{
							"name":      "pod-2",
							"namespace": "default",
						},
					},
				},
			},
			want: []Record{
				{
					Event: Add,
					Resource: unstructured.Unstructured{
						Object: map[string]interface{}{
							"apiVersion": "v1",
							"kind":       "Pod",
							"metadata": map[string]interface{}{
								"name":      "pod-1",
								"namespace": "default",
							},
						},
					},
				},
				{
					Event: Add,
					Resource: unstructured.Unstructured{
						Object: map[string]interface{}{
							"apiVersion": "v1",
							"kind":       "Pod",
							"metadata": map[string]interface{}{
								"name":      "pod-2",
								"namespace": "default",
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "should record updating a pod",
			resourceToCreate: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"apiVersion": "v1",
						"kind":       "Pod",
						"metadata": map[string]interface{}{
							"name":      "pod-1",
							"namespace": "default",
						},
						"spec": map[string]interface{}{
							"containers": []interface{}{
								map[string]interface{}{
									"name":  "nginx",
									"image": "nginx:latest",
								},
							},
						},
					},
				},
			},
			resourceToUpdate: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"apiVersion": "v1",
						"kind":       "Pod",
						"metadata": map[string]interface{}{
							"name":      "pod-1",
							"namespace": "default",
						},
						"spec": map[string]interface{}{
							"containers": []interface{}{
								map[string]interface{}{
									"name":  "nginx",
									"image": "nginx:latest",
								},
							},
							"nodeName": "node-1",
						},
					},
				},
			},
			want: []Record{
				{
					Event: Add,
					Resource: unstructured.Unstructured{
						Object: map[string]interface{}{
							"apiVersion": "v1",
							"kind":       "Pod",
							"metadata": map[string]interface{}{
								"name":      "pod-1",
								"namespace": "default",
							},
							"spec": map[string]interface{}{
								"containers": []interface{}{
									map[string]interface{}{
										"name":  "nginx",
										"image": "nginx:latest",
									},
								},
							},
						},
					},
				},
				{
					Event: Update,
					Resource: unstructured.Unstructured{
						Object: map[string]interface{}{
							"apiVersion": "v1",
							"kind":       "Pod",
							"metadata": map[string]interface{}{
								"name":      "pod-1",
								"namespace": "default",
							},
							"spec": map[string]interface{}{
								"containers": []interface{}{
									map[string]interface{}{
										"name":  "nginx",
										"image": "nginx:latest",
									},
								},
								"nodeName": "node-1",
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "should record deleting a pod",
			resourceToCreate: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"apiVersion": "v1",
						"kind":       "Pod",
						"metadata": map[string]interface{}{
							"name":      "pod-1",
							"namespace": "default",
						},
					},
				},
			},
			resourceToDelete: []unstructured.Unstructured{
				{
					Object: map[string]interface{}{
						"apiVersion": "v1",
						"kind":       "Pod",
						"metadata": map[string]interface{}{
							"name":      "pod-1",
							"namespace": "default",
						},
					},
				},
			},
			want: []Record{
				{
					Event: Add,
					Resource: unstructured.Unstructured{
						Object: map[string]interface{}{
							"apiVersion": "v1",
							"kind":       "Pod",
							"metadata": map[string]interface{}{
								"name":      "pod-1",
								"namespace": "default",
							},
						},
					},
				},
				{
					Event: Delete,
					Resource: unstructured.Unstructured{
						Object: map[string]interface{}{
							"apiVersion": "v1",
							"kind":       "Pod",
							"metadata": map[string]interface{}{
								"name":      "pod-1",
								"namespace": "default",
							},
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

			tempPath := path.Join(os.TempDir(), strings.ReplaceAll(tt.name, " ", "_")+".json")
			defer os.Remove(tempPath)

			s := runtime.NewScheme()
			corev1.AddToScheme(s)
			appsv1.AddToScheme(s)
			schedulingv1.AddToScheme(s)
			storagev1.AddToScheme(s)
			client := dynamicFake.NewSimpleDynamicClient(s)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			service := New(client, Options{Path: tempPath})
			err := service.Run(ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("Service.Record() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			for _, resource := range tt.resourceToCreate {
				gvr, err := findGVR(&resource)
				if err != nil {
					t.Fatalf("failed to find GVR: %v", err)
				}
				ns := resource.GetNamespace()

				_, err = client.Resource(gvr).Namespace(ns).Create(context.Background(), &resource, metav1.CreateOptions{})
				if err != nil {
					t.Fatalf("failed to create a pod: %v", err)
				}
			}

			for _, resource := range tt.resourceToUpdate {
				gvr, err := findGVR(&resource)
				if err != nil {
					t.Fatalf("failed to find GVR: %v", err)
				}
				ns := resource.GetNamespace()

				_, err = client.Resource(gvr).Namespace(ns).Update(context.Background(), &resource, metav1.UpdateOptions{})
				if err != nil {
					t.Fatalf("failed to update a pod: %v", err)
				}
			}

			for _, resource := range tt.resourceToDelete {
				gvr, err := findGVR(&resource)
				if err != nil {
					t.Fatalf("failed to find GVR: %v", err)
				}
				ns := resource.GetNamespace()

				err = client.Resource(gvr).Namespace(ns).Delete(context.Background(), resource.GetName(), metav1.DeleteOptions{})
				if err != nil {
					t.Fatalf("failed to delete a pod: %v", err)
				}
			}

			errMessage := ""
			err = wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 5*time.Second, false, func(context.Context) (bool, error) {
				got := []Record{}
				var b []byte
				b, err = os.ReadFile(tempPath)
				if err != nil {
					errMessage = fmt.Sprintf("failed to read the temporary file: %v", err)
					return false, nil
				}

				err = json.Unmarshal(b, &got)
				if err != nil {
					errMessage = fmt.Sprintf("failed to unmarshal the temporary file: %v", err)
					return false, nil
				}

				if len(got) != len(tt.want) {
					errMessage = fmt.Sprintf("Service.Record() got = %d records, want %d records", len(got), len(tt.want))
					return false, nil
				}

				for i := range got {
					if got[i].Event != tt.want[i].Event {
						errMessage = fmt.Sprintf("Service.Record() got = %v, want %v", got[i].Event, tt.want[i].Event)
						return true, fmt.Errorf("%s", errMessage)
					}

					if diff := cmp.Diff(tt.want[i].Resource, got[i].Resource); diff != "" {
						errMessage = fmt.Sprintf("Service.Record() mismatch (-want +got):\n%s", diff)
						return true, fmt.Errorf("%s", errMessage)
					}
				}

				return true, nil
			})
			if err != nil {
				t.Fatal(errMessage)
			}
		})
	}
}

var (
	resources = []*restmapper.APIGroupResources{
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
	mapper = restmapper.NewDiscoveryRESTMapper(resources)
)

func findGVR(obj *unstructured.Unstructured) (schema.GroupVersionResource, error) {
	gvk := obj.GroupVersionKind()
	m, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return schema.GroupVersionResource{}, err
	}

	return m.Resource, nil
}
