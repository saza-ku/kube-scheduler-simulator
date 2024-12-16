package recorder

import (
	"context"
	"time"

	"golang.org/x/xerrors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog"
)

type Event string

var (
	Add    Event = "Add"
	Update Event = "Update"
	Delete Event = "Delete"
)

var (
	recordCh = make(chan Record)
)

type Record struct {
	Time     time.Time `json:"time"`
	Event    Event     `json:"event"`
	Resource unstructured.Unstructured
}

var DefaultGVRs = []schema.GroupVersionResource{
	{Group: "", Version: "v1", Resource: "namespaces"},
	{Group: "scheduling.k8s.io", Version: "v1", Resource: "priorityclasses"},
	{Group: "storage.k8s.io", Version: "v1", Resource: "storageclasses"},
	{Group: "", Version: "v1", Resource: "persistentvolumeclaims"},
	{Group: "", Version: "v1", Resource: "nodes"},
	{Group: "", Version: "v1", Resource: "persistentvolumes"},
	{Group: "", Version: "v1", Resource: "pods"},
}

func Run(ctx context.Context) error {
	infFact := dynamicinformer.NewFilteredDynamicSharedInformerFactory(s.clients.SrcDynamicClient, 0, metav1.NamespaceAll, nil)
	for _, gvr := range DefaultGVRs {
		inf := infFact.ForResource(gvr).Informer()
		_, err := inf.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    func(obj interface{}) {},
			UpdateFunc: func(obj interface{}) {},
			DeleteFunc: func(obj interface{}) {},
		})
		if err != nil {
			return xerrors.Errorf("failed to add event handler: %w", err)
		}
		go inf.Run(ctx.Done())
		infFact.WaitForCacheSync(ctx.Done())
	}
}

func RecordEvent(obj interface{}) {
	ctx := context.Background()
	unstructObj, ok := obj.(*unstructured.Unstructured)
	if !ok {
		klog.Error("Failed to convert runtime.Object to *unstructured.Unstructured")
		return
	}

	r := Record{
		Event:    Add,
		Time:     time.Now(),
		Resource: *unstructObj,
	}
}
