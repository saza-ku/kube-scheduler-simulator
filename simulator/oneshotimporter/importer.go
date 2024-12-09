package oneshotimporter

//go:generate mockgen -destination=./mock_$GOPACKAGE/replicate.go . ReplicateService

import (
	"context"

	"golang.org/x/xerrors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	configv1 "k8s.io/kube-scheduler/config/v1"
	"sigs.k8s.io/kube-scheduler-simulator/simulator/resourceapplier"
	"sigs.k8s.io/kube-scheduler-simulator/simulator/util"
)

type SchedulerService interface {
	GetSchedulerConfig() (*configv1.KubeSchedulerConfiguration, error)
	RestartScheduler(cfg *configv1.KubeSchedulerConfiguration) error
}

// Service has two ReplicateServices.
// importService is used to import(replicate) these resources to the simulator.
// exportService is used to export resources from a target cluster.
type Service struct {
	schedulerService      SchedulerService
	srcDynamicClient      dynamic.Interface
	resouceApplierService *resourceapplier.Service
}

// GVRs is a list of GroupVersionResource that we import.
// Note that this order matters - When first importing resources, we want to import namespaces first, then priorityclasses, storageclasses...
var GVRs = []schema.GroupVersionResource{
	{Group: "", Version: "v1", Resource: "namespaces"},
	{Group: "scheduling.k8s.io", Version: "v1", Resource: "priorityclasses"},
	{Group: "storage.k8s.io", Version: "v1", Resource: "storageclasses"},
	{Group: "", Version: "v1", Resource: "persistentvolumeclaims"},
	{Group: "", Version: "v1", Resource: "nodes"},
	{Group: "", Version: "v1", Resource: "persistentvolumes"},
	{Group: "", Version: "v1", Resource: "pods"},
}

// NewService initializes Service.
// func NewService(e ReplicateService, i ReplicateService) *Service {
// 	return &Service{}
// }

// ImportClusterResources gets resources from the target cluster via exportService
// and then apply those resources to the simulator.
// Note: this method doesn't handle scheduler configuration.
// If you want to use the scheduler configuration along with the imported resources on the simulator,
// you need to set the path of the scheduler configuration file to `kubeSchedulerConfigPath` value in the Simulator Server Configuration.
func (s *Service) ImportClusterResources(ctx context.Context) error {
	cfg, err := s.schedulerService.GetSchedulerConfig()

	for _, gvr := range GVRs {
		if err := s.importResource(ctx, gvr); err != nil {
			return xerrors.Errorf("import resource %s: %w", gvr.String(), err)
		}
	}

	return nil
}

func (s *Service) importResource(ctx context.Context, gvr schema.GroupVersionResource) error {
	resources, err := s.srcDynamicClient.Resource(gvr).List(ctx, metav1.ListOptions{})
	if err != nil {
		return xerrors.Errorf("list resources: %w", err)
	}

	eg := util.NewErrGroupWithSemaphore(ctx)
	for _, resource := range resources.Items {
		if err := eg.Go(func() error {
			return s.resouceApplierService.Create(ctx, &resource)
		}); err != nil {
			return xerrors.Errorf("start error group: %w", err)
		}
	}

	return nil
}
