/*

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package dcluster

import (
	"context"

	"github.com/microsoft/azure-databricks-operator/controllers_new/shared"
	"github.com/microsoft/azure-databricks-operator/pkg/reconciler"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/go-logr/logr"
	"github.com/microsoft/azure-databricks-operator/api/v1alpha1"
	dbazure "github.com/xinsnake/databricks-sdk-golang/azure"
)

type ControllerFactory struct {
	ClientCreator func(logr.Logger, record.EventRecorder, dbazure.DBClient) ResourceManager
	Scheme        *runtime.Scheme
	APIClient     dbazure.DBClient
}

// +kubebuilder:rbac:groups=databricks.microsoft.com,resources=dclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=databricks.microsoft.com,resources=dclusters/status,verbs=get;update;patch

const ResourceKind = "Dcluster"
const FinalizerName = "dcluster.finalizers.com"

func (factory *ControllerFactory) SetupWithManager(mgr ctrl.Manager, parameters reconciler.ReconcileParameters) error {
	gc, err := factory.create(mgr.GetClient(),
		ctrl.Log.WithName("controllers").WithName(ResourceKind),
		mgr.GetEventRecorderFor(ResourceKind+"-controller"), parameters)
	if err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.Dcluster{}).
		Complete(gc)
}

func (factory *ControllerFactory) create(kubeClient client.Client, logger logr.Logger, recorder record.EventRecorder, parameters reconciler.ReconcileParameters) (*reconciler.GenericController, error) {
	resourceManagerClient := factory.ClientCreator(logger, recorder, factory.APIClient)

	return reconciler.CreateGenericController(parameters, ResourceKind, kubeClient, logger, recorder, factory.Scheme, &resourceManagerClient, &definitionManager{}, FinalizerName, shared.AnnotationBaseName, nil)
}

type definitionManager struct{}

func (dm *definitionManager) GetDefinition(ctx context.Context, namespacedName types.NamespacedName) *reconciler.ResourceDefinition {
	return &reconciler.ResourceDefinition{
		InitialInstance: &v1alpha1.Dcluster{},
		StatusAccessor:  GetStatus,
		StatusUpdater:   updateStatus,
	}
}

func (dm *definitionManager) GetDependencies(ctx context.Context, thisInstance runtime.Object) (*reconciler.DependencyDefinitions, error) {
	return &reconciler.NoDependencies, nil
}
