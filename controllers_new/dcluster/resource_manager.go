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
	"fmt"
	"reflect"
	"strings"

	"github.com/go-logr/logr"
	databricksv1alpha1 "github.com/microsoft/azure-databricks-operator/api/v1alpha1"
	"github.com/microsoft/azure-databricks-operator/pkg/reconciler"
	dbazure "github.com/xinsnake/databricks-sdk-golang/azure"
	"k8s.io/client-go/tools/record"
)

const storageAccountResourceFmt = "/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Storage/storageAccounts/%s"

type ResourceManager struct {
	Logger    logr.Logger
	Recorder  record.EventRecorder
	APIClient dbazure.DBClient
}

func CreateResourceManagerClient(logger logr.Logger, recorder record.EventRecorder, apiClient dbazure.DBClient) ResourceManager {
	return ResourceManager{
		Logger:    logger,
		Recorder:  recorder,
		APIClient: apiClient,
	}
}

func (r *ResourceManager) Create(ctx context.Context, s reconciler.ResourceSpec) (reconciler.ApplyResponse, error) {
	instance, err := convertInstance(s.Instance)
	if err != nil {
		return reconciler.ApplyError, err
	}
	if instance.Status != nil && instance.Status.ClusterInfo != nil && instance.Status.ClusterInfo.ClusterID != "" {
		err := r.APIClient.Clusters().PermanentDelete(instance.Status.ClusterInfo.ClusterID)
		if err != nil {
			return reconciler.ApplyError, err
		}
	}

	clusterInfo, err := r.APIClient.Clusters().Create(*instance.Spec)
	if err != nil {
		return reconciler.ApplyError, err
	}

	var info databricksv1alpha1.DclusterInfo
	dbInfo := info.FromDataBricksClusterInfo(clusterInfo)
	return reconciler.ApplyAwaitingVerificationWithStatus(dbInfo), err
}

func (_ *ResourceManager) Update(ctx context.Context, r reconciler.ResourceSpec) (reconciler.ApplyResponse, error) {
	return reconciler.ApplyError, fmt.Errorf("Updating eventhub not currently supported")
}

func (r *ResourceManager) Verify(ctx context.Context, s reconciler.ResourceSpec) (reconciler.VerifyResponse, error) {
	instance, err := convertInstance(s.Instance)
	status := instance.Status
	if err != nil {
		return reconciler.VerifyError, err
	}

	r.Logger.Info(fmt.Sprintf("Refresh cluster %s", instance.GetName()))

	if status == nil || status.ClusterInfo == nil {
		return reconciler.VerifyMissing, nil
	}

	clusterInfo, err := r.APIClient.Clusters().Get(status.ClusterInfo.ClusterID)
	if err != nil {
		if strings.Contains(err.Error(), "404") {
			return reconciler.VerifyMissing, nil
		}
		return reconciler.VerifyError, err
	}

	if reflect.DeepEqual(status.ClusterInfo, &clusterInfo) {
		return reconciler.VerifyReadyWithStatus(status.ClusterInfo), nil
	}

	var info databricksv1alpha1.DclusterInfo
	dbInfo := info.FromDataBricksClusterInfo(clusterInfo)
	return reconciler.VerifyReadyWithStatus(dbInfo), nil
}

func (r *ResourceManager) Delete(ctx context.Context, s reconciler.ResourceSpec) (reconciler.DeleteResult, error) {
	instance, err := convertInstance(s.Instance)
	if err != nil {
		return reconciler.DeleteError, err
	}
	err = r.APIClient.Clusters().PermanentDelete(instance.Status.ClusterInfo.ClusterID)
	if err != nil {
		return reconciler.DeleteError, err
	}

	return reconciler.DeleteSucceeded, nil
}
