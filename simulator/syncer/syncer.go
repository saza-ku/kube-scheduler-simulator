package syncer

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// GVRs is a list of GroupVersionResource that we want to sync.
// Note that this order matters - When first importing resources, we want to sync namespaces first, then priorityclasses, storageclasses...
var GVRs = []schema.GroupVersionResource{
	{Group: "", Version: "v1", Resource: "namespaces"},
	{Group: "scheduling.k8s.io", Version: "v1", Resource: "priorityclasses"},
	{Group: "storage.k8s.io", Version: "v1", Resource: "storageclasses"},
	{Group: "", Version: "v1", Resource: "persistentvolumeclaims"},
	{Group: "", Version: "v1", Resource: "nodes"},
	// Note that we are not syncing pods that are already scheduled.
	{Group: "", Version: "v1", Resource: "pods"},
	{Group: "", Version: "v1", Resource: "persistentvolumes"},
}

// Event is a type of events that occur in the source cluster.
type Event int

const (
	Add Event = iota
	Update
)

// MutatingFunctions is a map of GroupVersionResource to mutatingFunction.
// This is public so that outside users can add their own mutating functions.
var MutatingFunctions = map[schema.GroupVersionResource]MutatingFunction{
	{Group: "", Version: "v1", Resource: "persistentvolumes"}: mutatePV,
}

// ValidatingFunctions is a map of GroupVersionResource to validatingFunction.
// This is public so that outside users can add their own validating functions.
var ValidatingFunctions = map[schema.GroupVersionResource]ValidatingFunction{
	{Group: "", Version: "v1", Resource: "pods"}: validatePods,
}

// ValidatingFunction is a function that validates a resource.
// If it returns false, the resource will not be imported.
type ValidatingFunction func(ctx context.Context, resource *unstructured.Unstructured, clients *Clients, event Event) (bool, error)

// MutatingFunction is a function that mutates a resource before importing it.
type MutatingFunction func(ctx context.Context, resource *unstructured.Unstructured, clients *Clients, event Event) (*unstructured.Unstructured, error)

type Service struct {
	clients *Clients

	gvkToGVR map[schema.GroupVersionKind]schema.GroupVersionResource

	mutatingFunctions   map[schema.GroupVersionResource]MutatingFunction
	validatingFunctions map[schema.GroupVersionResource]ValidatingFunction
}

type Clients struct {
	// srcDynamicClient is the dynamic client for the source cluster, which the resource is supposed to be copied from.
	srcDynamicClient dynamic.Interface
	// destDynamicClient is the dynamic client for the destination cluster, which the resource is supposed to be copied to.
	destDynamicClient dynamic.Interface
	restMapper        meta.RESTMapper
}

func New(srcDynamicClient, destDynamicClient dynamic.Interface, restMapper meta.RESTMapper) *Service {
	s := &Service{
		clients: &Clients{
			srcDynamicClient:  srcDynamicClient,
			destDynamicClient: destDynamicClient,
			restMapper:        restMapper,
		},
		gvkToGVR: make(map[schema.GroupVersionKind]schema.GroupVersionResource),
	}

	s.mutatingFunctions = MutatingFunctions
	s.validatingFunctions = ValidatingFunctions

	return s
}

func (s *Service) Run(ctx context.Context) error {
	klog.Info("Starting the cluster resource importer")

	infFact := dynamicinformer.NewFilteredDynamicSharedInformerFactory(s.clients.srcDynamicClient, 0, metav1.NamespaceAll, nil)
	for _, gvr := range GVRs {
		inf := infFact.ForResource(gvr).Informer()
		_, err := inf.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    s.addFunc,
			UpdateFunc: s.updateFunc,
			DeleteFunc: s.deleteFunc,
		})
		if err != nil {
			return fmt.Errorf("failed to add event handler: %v", err)
		}
		go inf.Run(ctx.Done())
		infFact.WaitForCacheSync(ctx.Done())
	}

	klog.Info("Cluster resource importer started")

	<-ctx.Done()

	return nil
}

// createResourceOnDestinationCluster creates the resource on the destination cluster
func (s *Service) createResourceOnDestinationCluster(
	ctx context.Context,
	resource *unstructured.Unstructured,
) error {
	// Extract the GroupVersionResource from the Unstructured object
	gvk := resource.GroupVersionKind()
	gvr, err := s.findGVRForGVK(gvk)
	if err != nil {
		return err
	}

	// Namespaces resources should be created within the namespace defined in the Unstructured object
	namespace := resource.GetNamespace()

	// Run the validating function for the resource.
	if validatingFn, ok := s.validatingFunctions[gvr]; ok {
		if ok, err := validatingFn(ctx, resource, s.clients, Add); !ok || err != nil {
			return err
		}
	}

	// When creating a resource on the destination cluster, we must remove the metadata such as UID and Generation.
	// It's done for all resources.
	resource = removeMetadata(resource)

	// Run the mutating function for the resource.
	if mutatingFn, ok := s.mutatingFunctions[gvr]; ok {
		resource, err = mutatingFn(ctx, resource, s.clients, Add)
		if err != nil {
			return fmt.Errorf("failed to mutate resource: %v", err)
		}
	}

	// Create the resource on the destination cluster using the dynamic client
	_, err = s.clients.destDynamicClient.Resource(gvr).Namespace(namespace).Create(
		ctx,
		resource,
		metav1.CreateOptions{},
	)
	if err != nil {
		return fmt.Errorf("failed to create resource: %v", err)
	}

	return nil
}

func (s *Service) updateResourceOnDestinationCluster(
	ctx context.Context,
	resource *unstructured.Unstructured,
) error {
	// Extract the GroupVersionResource from the Unstructured object
	gvk := resource.GroupVersionKind()
	gvr, err := s.findGVRForGVK(gvk)
	if err != nil {
		return err
	}

	// Namespaces resources should be created within the namespace defined in the Unstructured object
	namespace := resource.GetNamespace()

	// Run the validating function for the resource.
	if validatingFn, ok := s.validatingFunctions[gvr]; ok {
		if ok, err := validatingFn(ctx, resource, s.clients, Update); !ok || err != nil {
			return err
		}
	}

	// Run the mutating function for the resource.
	if mutatingFn, ok := s.mutatingFunctions[gvr]; ok {
		resource, err = mutatingFn(ctx, resource, s.clients, Update)
		if err != nil {
			return fmt.Errorf("failed to mutate resource: %v", err)
		}
	}

	// Create the resource on the destination cluster using the dynamic client
	_, err = s.clients.destDynamicClient.Resource(gvr).Namespace(namespace).Update(
		ctx,
		resource,
		metav1.UpdateOptions{},
	)
	if err != nil {
		return fmt.Errorf("failed to create resource: %v", err)
	}

	return nil
}

// removeMetadata removes the metadata from the resource.
func removeMetadata(resource *unstructured.Unstructured) *unstructured.Unstructured {
	resource.SetUID("")
	resource.SetGeneration(0)
	resource.SetResourceVersion("")

	return resource
}

// validatePods checks if a pod is already scheduled when it's updated.
// We only want to update pods that are not yet scheduled.
func validatePods(_ context.Context, resource *unstructured.Unstructured, _ *Clients, event Event) (bool, error) {
	if event != Update {
		return true, nil
	}

	var pod v1.Pod
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(resource.UnstructuredContent(), &pod)
	if err != nil {
		return false, err
	}

	if pod.Spec.NodeName != "" {
		klog.InfoS("Pod is scheduled. We ignore Pods already scheduled", "pod", pod.Name, "namespace", pod.Namespace)
		return false, nil
	}

	// This Pod should be applied on the destination cluster.
	return true, nil
}

func mutatePV(ctx context.Context, resource *unstructured.Unstructured, clients *Clients, _ Event) (*unstructured.Unstructured, error) {
	var pv v1.PersistentVolume
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(resource.UnstructuredContent(), &pv)
	if err != nil {
		return nil, err
	}

	if pv.Status.Phase == "Bound" {
		// PersistentVolumeClaims's UID is changed in a destination cluster when importing from a source cluster,
		// and thus we need to update the PVC UID in the PersistentVolume.
		// Get PVC of pv.Spec.ClaimRef.Name.
		pvc, err := clients.srcDynamicClient.Resource(schema.GroupVersionResource{
			Group:    "",
			Version:  "v1",
			Resource: "persistentvolumeclaims",
		}).Namespace(pv.Spec.ClaimRef.Namespace).Get(ctx, pv.Spec.ClaimRef.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}

		pv.Spec.ClaimRef.UID = pvc.GetUID()
	}

	modifiedUnstructed, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&pv)
	return &unstructured.Unstructured{Object: modifiedUnstructed}, err
}

func (s *Service) deleteResourceOnDestinationCluster(
	ctx context.Context,
	resource *unstructured.Unstructured,
) error {
	// Extract the GroupVersionResource from the Unstructured object
	gvk := resource.GroupVersionKind()
	gvr, err := s.findGVRForGVK(gvk)
	if err != nil {
		return err
	}

	// Namespaces resources should be created within the namespace defined in the Unstructured object
	namespace := resource.GetNamespace()

	// Create the resource on the destination cluster using the dynamic client
	err = s.clients.destDynamicClient.Resource(gvr).Namespace(namespace).Delete(
		ctx,
		resource.GetName(),
		metav1.DeleteOptions{},
	)
	if err != nil {
		return fmt.Errorf("failed to delete resource: %v", err)
	}

	return nil
}

// findGVRForGVK uses the discovery client to get the GroupVersionResource for a given GroupVersionKind
func (s *Service) findGVRForGVK(gvk schema.GroupVersionKind) (schema.GroupVersionResource, error) {
	m, err := s.clients.restMapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return schema.GroupVersionResource{}, err
	}

	return m.Resource, nil
}

func (s *Service) addFunc(obj interface{}) {
	ctx := context.Background()
	unstructObj, ok := obj.(*unstructured.Unstructured)
	if !ok {
		klog.Error("Failed to convert runtime.Object to *unstructured.Unstructured")
		return
	}

	err := s.createResourceOnDestinationCluster(ctx, unstructObj)
	if err != nil {
		klog.ErrorS(err, "Failed to create resource on destination cluster")
	}
}

func (s *Service) updateFunc(oldObj, newObj interface{}) {
	ctx := context.Background()
	unstructObj, ok := newObj.(*unstructured.Unstructured)
	if !ok {
		klog.Error("Failed to convert runtime.Object to *unstructured.Unstructured")
		return
	}

	err := s.updateResourceOnDestinationCluster(ctx, unstructObj)
	if err != nil {
		klog.ErrorS(err, "Failed to update resource on destination cluster")
	}
}

func (s *Service) deleteFunc(obj interface{}) {
	ctx := context.Background()
	unstructObj, ok := obj.(*unstructured.Unstructured)
	if !ok {
		klog.Error("Failed to convert runtime.Object to *unstructured.Unstructured")
		return
	}

	err := s.deleteResourceOnDestinationCluster(ctx, unstructObj)
	if err != nil {
		klog.ErrorS(err, "Failed to delete resource on destination cluster")
	}
}
