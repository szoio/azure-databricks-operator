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
	"fmt"

	"github.com/microsoft/azure-databricks-operator/api/v1alpha1"
	"github.com/microsoft/azure-databricks-operator/pkg/reconciler"
	"k8s.io/apimachinery/pkg/runtime"
)

func GetStatus(instance runtime.Object) (*reconciler.Status, error) {
	x, err := convertInstance(instance)
	if err != nil {
		return nil, err
	}
	status := x.Status
	if status == nil {
		status = &v1alpha1.DclusterStatus{}
	}

	return &reconciler.Status{
		State:         reconciler.ProvisionState(status.State),
		StatusPayload: status.ClusterInfo,
	}, nil
}

func updateStatus(instance runtime.Object, status *reconciler.Status) error {
	x, err := convertInstance(instance)
	if err != nil {
		return err
	}
	if x.Status == nil {
		x.Status = &v1alpha1.DclusterStatus{}
	}
	x.Status.State = string(status.State)
	if status.StatusPayload != nil {
		x.Status.ClusterInfo = status.StatusPayload.(*v1alpha1.DclusterInfo)
	}
	return nil
}

func convertInstance(obj runtime.Object) (*v1alpha1.Dcluster, error) {
	local, ok := obj.(*v1alpha1.Dcluster)
	if !ok {
		return nil, fmt.Errorf("failed type assertion on kind: Dcluster")
	}
	return local, nil
}
