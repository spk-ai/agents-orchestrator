package assembler

import (
	"context"
	"testing"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
)

func resourceFixture() *agentEnvironmentFixture {
	f := newAgentEnvironmentFixture()
	f.agent.Capabilities = []string{computeResourcesCapability}
	f.agent.Resources = &agentsv1.ComputeResources{RequestsCpu: "9", RequestsMemory: "9Gi"}
	f.runners.listFlavors = func(context.Context, *runnersv1.ListFlavorsRequest, ...grpc.CallOption) (*runnersv1.ListFlavorsResponse, error) {
		return &runnersv1.ListFlavorsResponse{Flavors: []*runnersv1.Flavor{{RunnerId: testAgentEnvironmentRunnerID, Name: testAgentEnvironmentFlavor,
			Resources: &runnersv1.ComputeResources{RequestsCpu: "500m", RequestsMemory: "1Gi", LimitsCpu: "2", LimitsMemory: "2Gi"}}}}, nil
	}
	return f
}

func TestAssemblerComputeResourcesUsesFlavorAndMcpBounds(t *testing.T) {
	f := resourceFixture()
	f.agents.ListMcpsFunc = func(context.Context, *agentsv1.ListMcpsRequest, ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error) {
		return &agentsv1.ListMcpsResponse{Mcps: []*agentsv1.Mcp{{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, Name: "tool", Image: "tool:1", Command: "tool",
			Resources: &agentsv1.ComputeResources{RequestsCpu: "100m", RequestsMemory: "64Mi", LimitsCpu: "500m", LimitsMemory: "256Mi"}}}}, nil
	}
	result := f.assemble(t)
	main, tool := result.Request.Main.Resources, result.Request.Sidecars[0].Resources
	if main.GetRequestsCpu() != "500m" || main.GetRequestsMemory() != "1Gi" || main.GetLimitsCpu() != "2" || main.GetLimitsMemory() != "2Gi" {
		t.Fatalf("main did not use selected flavor: %+v", main)
	}
	if tool.GetRequestsCpu() != "100m" || tool.GetLimitsMemory() != "256Mi" {
		t.Fatalf("MCP bounds lost: %+v", tool)
	}
	if !requiresComputeResources(result.Request.Capabilities) {
		t.Fatal("capability was lost")
	}
	if result.AllocatedCPUMillicores != 600 || result.AllocatedRAMBytes != (1<<30)+(64<<20) {
		t.Fatal("declared request accounting still uses deprecated agent resources")
	}
	if f.agent.Resources.GetRequestsCpu() != "9" {
		t.Fatal("assembly mutated the source agent")
	}
	for _, init := range result.Request.InitContainers {
		if init.Resources != nil {
			t.Fatal("supporting defaults are runner-owned")
		}
	}
}

func TestAssemblerComputeResourcesIsOptIn(t *testing.T) {
	f := resourceFixture()
	f.agent.Capabilities = nil
	result := f.assemble(t)
	if result.Request.Main.Resources != nil {
		t.Fatal("legacy agent silently opted in")
	}
	if result.AllocatedCPUMillicores != 9000 {
		t.Fatal("legacy resource accounting changed")
	}
	if requiresComputeResources([]string{"compute-resources-other"}) {
		t.Fatal("capability prefix matched")
	}
}

func TestAssemblerComputeResourcesRejectsMissingFlavorBounds(t *testing.T) {
	f := newAgentEnvironmentFixture()
	f.agent.Capabilities = []string{computeResourcesCapability}
	f.cfg.ImageProxyHost = testCatalogProxyHost
	a := withCatalog(NewWithRunners(f.agents, f.runners, f.secrets, f.cfg), "org-1")
	if _, err := a.Assemble(context.Background(), f.agentID, f.threadID, f.threadID); err == nil {
		t.Fatal("missing flavor bounds accepted")
	}
}

func TestRunnerComputeResourcesRejectsPartialAndInvalidOverrides(t *testing.T) {
	if got, err := runnerComputeResources(nil); err != nil || got != nil {
		t.Fatal("nil override must leave runner defaults available")
	}
	for _, value := range []*agentsv1.ComputeResources{{},
		{RequestsCpu: "100m", RequestsMemory: "64Mi", LimitsCpu: "500m"},
		{RequestsCpu: "-1", RequestsMemory: "64Mi", LimitsCpu: "500m", LimitsMemory: "256Mi"},
		{RequestsCpu: "2", RequestsMemory: "64Mi", LimitsCpu: "500m", LimitsMemory: "256Mi"},
		{RequestsCpu: "100m", RequestsMemory: "1Gi", LimitsCpu: "500m", LimitsMemory: "256Mi"}} {
		if _, err := runnerComputeResources(value); err == nil {
			t.Fatalf("invalid override accepted: %+v", value)
		}
	}
}
