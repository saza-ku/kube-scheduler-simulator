package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/xerrors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"

	"sigs.k8s.io/kube-scheduler-simulator/simulator/config"
	"sigs.k8s.io/kube-scheduler-simulator/simulator/server"
	"sigs.k8s.io/kube-scheduler-simulator/simulator/server/di"
)

func main() {
	if err := startSimulator(); err != nil {
		klog.Fatalf("failed with error on running simulator: %+v", err)
	}
}

// entry point.
func startSimulator() error {
	cfg, err := config.NewConfig()
	if err != nil {
		return xerrors.Errorf("get config: %w", err)
	}

	restCfg := &rest.Config{
		Host: cfg.KubeAPIServerURL,
		TLSClientConfig: rest.TLSClientConfig{
			Insecure: true,
		},
	}
	client := clientset.NewForConfigOrDie(restCfg)
	dynamicClient, err := dynamic.NewForConfig(restCfg)
	if err != nil {
		return xerrors.Errorf("creates dynamic clientset: %w", err)
	}
	discoveryClient := discovery.NewDiscoveryClient(client.RESTClient())

	importClusterResourceClient := &clientset.Clientset{}
	var importClusterDynamicClient dynamic.Interface
	if cfg.ExternalImportEnabled || cfg.ResourceSyncEnabled {
		importClusterResourceClient, err = clientset.NewForConfig(cfg.ExternalKubeClientCfg)
		if err != nil {
			return xerrors.Errorf("creates a new Clientset for the ExternalKubeClientCfg: %w", err)
		}

		importClusterDynamicClient, err = dynamic.NewForConfig(cfg.ExternalKubeClientCfg)
		if err != nil {
			return xerrors.Errorf("creates a new dynamic Clientset for the ExternalKubeClientCfg: %w", err)
		}
	}

	etcdclient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{cfg.EtcdURL},
		DialTimeout: 2 * time.Second,
	})
	if err != nil {
		return xerrors.Errorf("create an etcd client: %w", err)
	}

	timeoutctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	err = wait.PollUntilContextCancel(timeoutctx, time.Second*5, true, func(ctx context.Context) (bool, error) {
		_, err := client.CoreV1().Namespaces().Get(context.Background(), "kube-system", metav1.GetOptions{})
		if err != nil {
			klog.Infof("waiting for kube-system namespace to be ready: %v", err)
			return false, nil
		}
		klog.Info("kubeapi-server is ready")
		return true, nil
	})
	if err != nil {
		return xerrors.Errorf("kubeapi-server is not ready: %w", err)
	}

	dic, err := di.NewDIContainer(client, dynamicClient, discoveryClient, etcdclient, restCfg, cfg.InitialSchedulerCfg, cfg.ExternalImportEnabled, cfg.ResourceSyncEnabled, importClusterResourceClient, importClusterDynamicClient, cfg.ExternalSchedulerEnabled, cfg.Port)
	if err != nil {
		return xerrors.Errorf("create di container: %w", err)
	}
	if !cfg.ExternalSchedulerEnabled {
		if err := dic.SchedulerService().StartScheduler(cfg.InitialSchedulerCfg); err != nil {
			return xerrors.Errorf("start scheduler: %w", err)
		}
		defer dic.SchedulerService().ShutdownScheduler()
	}

	// If ExternalImportEnabled is enabled, the simulator import resources
	// from the target cluster that indicated by the `KUBECONFIG`.
	if cfg.ExternalImportEnabled {
		// This must be called after `StartScheduler`
		if err := dic.OneshotClusterResourceImporter().ImportClusterResources(context.Background()); err != nil { // TODO: set context
			return xerrors.Errorf("import from the target cluster: %w", err)
		}
	}

	if cfg.ResourceSyncEnabled {
		// Start the resource syncer to sync resources from the target cluster.
		go dic.ResourceSyncer().Run(context.Background()) // TODO: set context
	}

	// start simulator server
	s := server.NewSimulatorServer(cfg, dic)
	shutdownFn, err := s.Start(cfg.Port)
	if err != nil {
		return xerrors.Errorf("start simulator server: %w", err)
	}
	defer shutdownFn()

	// wait the signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, os.Interrupt)
	<-quit

	return nil
}
