package assembler

import (
	"fmt"
	"strings"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

const computeResourcesCapability = "compute-resources"

func requiresComputeResources(capabilities []string) bool {
	for _, capability := range capabilities {
		if strings.ToLower(strings.TrimSpace(capability)) == computeResourcesCapability {
			return true
		}
	}
	return false
}

// Nil supporting-container resources delegate to the runner's configured
// defaults; an explicit but incomplete allocation is a configuration error.
func runnerComputeResources(value *agentsv1.ComputeResources) (*runnerv1.ComputeResources, error) {
	if value == nil {
		return nil, nil
	}
	quantities := make([]resource.Quantity, 0, 4)
	for _, field := range []struct{ name, value string }{
		{"requests_cpu", value.GetRequestsCpu()}, {"requests_memory", value.GetRequestsMemory()},
		{"limits_cpu", value.GetLimitsCpu()}, {"limits_memory", value.GetLimitsMemory()},
	} {
		quantity, err := resource.ParseQuantity(strings.TrimSpace(field.value))
		if err != nil || quantity.Sign() <= 0 {
			return nil, fmt.Errorf("%s must be a positive resource quantity", field.name)
		}
		quantities = append(quantities, quantity)
	}
	if quantities[0].Cmp(quantities[2]) > 0 || quantities[1].Cmp(quantities[3]) > 0 {
		return nil, fmt.Errorf("resource requests must not exceed limits")
	}
	return &runnerv1.ComputeResources{RequestsCpu: value.GetRequestsCpu(), RequestsMemory: value.GetRequestsMemory(),
		LimitsCpu: value.GetLimitsCpu(), LimitsMemory: value.GetLimitsMemory()}, nil
}
