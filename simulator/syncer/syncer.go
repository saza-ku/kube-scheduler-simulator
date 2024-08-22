package syncer

import (
	"context"

	"golang.org/x/xerrors"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

type Service struct {
	clients *Clients

	gvrs               []schema.GroupVersionResource
	mutatingFunctions  map[schema.GroupVersionResource]MutatingFunction
	filteringFunctions map[schema.GroupVersionResource]FilteringFunction
}

type Clients struct {
	// SrcDynamicClient is the dynamic client for the source cluster, which the resource is supposed to be copied from.
	SrcDynamicClient dynamic.Interface
	// DestDynamicClient is the dynamic client for the destination cluster, which the resource is supposed to be copied to.
	DestDynamicClient dynamic.Interface
	RestMapper        meta.RESTMapper
}

type Options struct {
	// GVRsToSync is a list of GroupVersionResource that will be synced. It must contain MandatoryGVRs.
	// If GVRsToSync is nil MandatoryGVRs will be synced.
	GVRsToSync []schema.GroupVersionResource
	// AdditionalMutatingFunctions is a list of mutating functions that users add.
	// When users add mutating functions, they must call MandatoryMutatingFunctions in them if they exist for the keys.
	AdditionalMutatingFunctions map[schema.GroupVersionResource]MutatingFunction
	// AdditionalFilteringFunctions is a list of filtering functions that users add.
	// When users add filtering functions, they must call MandatoryFilteringFunctions in them if they exist for the keys.
	AdditionalFilteringFunctions map[schema.GroupVersionResource]FilteringFunction
}

func New(srcDynamicClient, destDynamicClient dynamic.Interface, restMapper meta.RESTMapper, options Options) *Service {
	s := &Service{
		clients: &Clients{
			SrcDynamicClient:  srcDynamicClient,
			DestDynamicClient: destDynamicClient,
			RestMapper:        restMapper,
		},
	}

	if options.GVRsToSync != nil {
		s.gvrs = options.GVRsToSync
	} else {
		s.gvrs = MandatoryGVRs
	}

	s.mutatingFunctions = MandatoryMutatingFunctions
	for k, v := range options.AdditionalMutatingFunctions {
		s.mutatingFunctions[k] = v
	}

	s.filteringFunctions = MandatoryFilteringFunctions
	for k, v := range options.AdditionalFilteringFunctions {
		s.filteringFunctions[k] = v
	}

	return s
}

func (s *Service) Run(ctx context.Context) error {
	klog.Info("Starting the cluster resource importer")

	infFact := dynamicinformer.NewFilteredDynamicSharedInformerFactory(s.clients.SrcDynamicClient, 0, metav1.NamespaceAll, nil)
	for _, gvr := range s.gvrs {
		inf := infFact.ForResource(gvr).Informer()
		_, err := inf.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    s.addFunc,
			UpdateFunc: s.updateFunc,
			DeleteFunc: s.deleteFunc,
		})
		if err != nil {
			return xerrors.Errorf("failed to add event handler: %w", err)
		}
		go inf.Run(ctx.Done())
		infFact.WaitForCacheSync(ctx.Done())
	}

	klog.Info("Cluster resource importer started")

	return nil
}

// createResourceOnDestinationCluster creates the resource on the destination cluster.
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

	// Run the filtering function for the resource.
	if filteringFn, ok := s.filteringFunctions[gvr]; ok {
		if ok, err := filteringFn(ctx, resource, s.clients, Add); !ok || err != nil {
			return err
		}
	}

	// When creating a resource on the destination cluster, we must remove the metadata such as UID and Generation.
	// It's done for all resources.
	resource = removeUnnecessaryMetadata(resource)

	// Run the mutating function for the resource.
	if mutatingFn, ok := s.mutatingFunctions[gvr]; ok {
		resource, err = mutatingFn(ctx, resource, s.clients, Add)
		if err != nil {
			return xerrors.Errorf("failed to mutate resource: %w", err)
		}
	}

	// Create the resource on the destination cluster using the dynamic client
	_, err = s.clients.DestDynamicClient.Resource(gvr).Namespace(namespace).Create(
		ctx,
		resource,
		metav1.CreateOptions{},
	)
	if err != nil {
		return xerrors.Errorf("failed to create resource: %w", err)
	}

	return nil
}

func (s *Service) updateResourceOnDestinationCluster(
	ctx context.Context,
	resource *unstructured.Unstructured,
) error {
	// Extract the GroupVersionResource from the Unstructured object.
	gvk := resource.GroupVersionKind()
	gvr, err := s.findGVRForGVK(gvk)
	if err != nil {
		return err
	}

	// Namespaces resources should be created within the namespace defined in the Unstructured object.
	namespace := resource.GetNamespace()

	// Run the filtering function for the resource.
	if filteringFn, ok := s.filteringFunctions[gvr]; ok {
		if ok, err := filteringFn(ctx, resource, s.clients, Update); !ok || err != nil {
			return err
		}
	}

	// Run the mutating function for the resource.
	if mutatingFn, ok := s.mutatingFunctions[gvr]; ok {
		resource, err = mutatingFn(ctx, resource, s.clients, Update)
		if err != nil {
			return xerrors.Errorf("failed to mutate resource: %w", err)
		}
	}

	// Create the resource on the destination cluster using the dynamic client
	_, err = s.clients.DestDynamicClient.Resource(gvr).Namespace(namespace).Update(
		ctx,
		resource,
		metav1.UpdateOptions{},
	)
	if err != nil {
		return xerrors.Errorf("failed to create resource: %w", err)
	}

	return nil
}

// removeUnnecessaryMetadata removes the metadata from the resource.
func removeUnnecessaryMetadata(resource *unstructured.Unstructured) *unstructured.Unstructured {
	resource.SetUID("")
	resource.SetGeneration(0)
	resource.SetResourceVersion("")

	return resource
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
	err = s.clients.DestDynamicClient.Resource(gvr).Namespace(namespace).Delete(
		ctx,
		resource.GetName(),
		metav1.DeleteOptions{},
	)
	if err != nil {
		return xerrors.Errorf("failed to delete resource: %w", err)
	}

	return nil
}

// findGVRForGVK uses the discovery client to get the GroupVersionResource for a given GroupVersionKind.
func (s *Service) findGVRForGVK(gvk schema.GroupVersionKind) (schema.GroupVersionResource, error) {
	m, err := s.clients.RestMapper.RESTMapping(gvk.GroupKind(), gvk.Version)
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

func (s *Service) updateFunc(_, newObj interface{}) {
	ctx := context.Background()
	unstructObj, ok := newObj.(*unstructured.Unstructured)
	if !ok {
		klog.Error("Failed to convert runtime.Object to *unstructured.Unstructured")
		return
	}

	err := s.updateResourceOnDestinationCluster(ctx, unstructObj)
	if err != nil {
		if errors.IsNotFound(err) {
			klog.Info("Skipped to update resource on destination: ", err)
		} else {
			klog.ErrorS(err, "Failed to update resource on destination cluster")
		}
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
		if errors.IsNotFound(err) {
			klog.Info("Skipped to delete resource on destination: ", err)
		} else {
			klog.ErrorS(err, "Failed to delete resource on destination cluster")
		}
	}
}
