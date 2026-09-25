package assembler

import (
	"fmt"
	"math"
	"strings"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

type resourceTotals struct {
	cpuMillicores int64
	ramBytes      int64
}

// sumAllocatedResources accounts for declared requests, not a whole-Pod budget:
// runner-owned supporting defaults and Kubernetes overhead are not known here.
func sumAllocatedResources(agent *agentsv1.Agent, mcps []mcpAssignment, mainResources *agentsv1.ComputeResources) (int32, int64, error) {
	if agent == nil {
		return 0, 0, fmt.Errorf("agent missing")
	}
	meta := agent.GetMeta()
	if meta == nil || meta.GetId() == "" {
		return 0, 0, fmt.Errorf("agent meta id missing")
	}
	label := fmt.Sprintf("agent %s", meta.GetId())
	totals := resourceTotals{}
	if err := totals.add(mainResources, label); err != nil {
		return 0, 0, err
	}
	for _, mcp := range mcps {
		if mcp.mcp == nil {
			return 0, 0, fmt.Errorf("mcp %s missing", mcp.id)
		}
		mcpLabel := fmt.Sprintf("mcp %s", mcp.id)
		if err := totals.add(mcp.mcp.GetResources(), mcpLabel); err != nil {
			return 0, 0, err
		}
	}
	if totals.cpuMillicores > math.MaxInt32 {
		return 0, 0, fmt.Errorf("allocated cpu millicores overflow: %d", totals.cpuMillicores)
	}
	return int32(totals.cpuMillicores), totals.ramBytes, nil
}

func (t *resourceTotals) add(resources *agentsv1.ComputeResources, label string) error {
	if resources == nil {
		return nil
	}
	cpu, err := parseCPUMillicores(resources.GetRequestsCpu(), label)
	if err != nil {
		return err
	}
	ram, err := parseRAMBytes(resources.GetRequestsMemory(), label)
	if err != nil {
		return err
	}
	if err := addInt64(&t.cpuMillicores, cpu, label+" requests_cpu"); err != nil {
		return err
	}
	if err := addInt64(&t.ramBytes, ram, label+" requests_memory"); err != nil {
		return err
	}
	return nil
}

func addInt64(total *int64, value int64, label string) error {
	if value < 0 {
		return fmt.Errorf("%s must be non-negative", label)
	}
	if value > 0 && *total > math.MaxInt64-value {
		return fmt.Errorf("%s overflow", label)
	}
	*total += value
	return nil
}

func parseCPUMillicores(value, label string) (int64, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0, nil
	}
	quantity, err := resource.ParseQuantity(trimmed)
	if err != nil {
		return 0, fmt.Errorf("%s requests_cpu %q: %w", label, trimmed, err)
	}
	milli := quantity.MilliValue()
	if milli < 0 {
		return 0, fmt.Errorf("%s requests_cpu must be non-negative", label)
	}
	return milli, nil
}

func parseRAMBytes(value, label string) (int64, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0, nil
	}
	quantity, err := resource.ParseQuantity(trimmed)
	if err != nil {
		return 0, fmt.Errorf("%s requests_memory %q: %w", label, trimmed, err)
	}
	bytes := quantity.Value()
	if bytes < 0 {
		return 0, fmt.Errorf("%s requests_memory must be non-negative", label)
	}
	return bytes, nil
}
