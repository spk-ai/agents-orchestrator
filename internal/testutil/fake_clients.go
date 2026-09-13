package testutil

import (
	"context"
	"errors"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	secretsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/secrets/v1"
	"google.golang.org/grpc"
)

var ErrNotImplemented = errors.New("not implemented")

type FakeAgentsClient struct {
	SetSandboxLayoutDirectoriesFunc func(context.Context, *agentsv1.SetSandboxLayoutDirectoriesRequest, ...grpc.CallOption) (*agentsv1.SetSandboxLayoutDirectoriesResponse, error)
	GetAgentFunc                    func(context.Context, *agentsv1.GetAgentRequest, ...grpc.CallOption) (*agentsv1.GetAgentResponse, error)
	GetEnvironmentFunc              func(context.Context, *agentsv1.GetEnvironmentRequest, ...grpc.CallOption) (*agentsv1.GetEnvironmentResponse, error)
	ResolveAgentIdentityFunc        func(context.Context, *agentsv1.ResolveAgentIdentityRequest, ...grpc.CallOption) (*agentsv1.ResolveAgentIdentityResponse, error)
	ListAgentsFunc                  func(context.Context, *agentsv1.ListAgentsRequest, ...grpc.CallOption) (*agentsv1.ListAgentsResponse, error)
	ListSkillsFunc                  func(context.Context, *agentsv1.ListSkillsRequest, ...grpc.CallOption) (*agentsv1.ListSkillsResponse, error)
	ListEnvsFunc                    func(context.Context, *agentsv1.ListEnvsRequest, ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error)
	ListInitScriptsFunc             func(context.Context, *agentsv1.ListInitScriptsRequest, ...grpc.CallOption) (*agentsv1.ListInitScriptsResponse, error)
	ListVolumesFunc                 func(context.Context, *agentsv1.ListVolumesRequest, ...grpc.CallOption) (*agentsv1.ListVolumesResponse, error)
	GetVolumeFunc                   func(context.Context, *agentsv1.GetVolumeRequest, ...grpc.CallOption) (*agentsv1.GetVolumeResponse, error)
	ListMcpsFunc                    func(context.Context, *agentsv1.ListMcpsRequest, ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error)
	GetSandboxFunc                  func(context.Context, *agentsv1.GetSandboxRequest, ...grpc.CallOption) (*agentsv1.GetSandboxResponse, error)
	ListSandboxesFunc               func(context.Context, *agentsv1.ListSandboxesRequest, ...grpc.CallOption) (*agentsv1.ListSandboxesResponse, error)
	UpdateSandboxRuntimeStateFunc   func(context.Context, *agentsv1.UpdateSandboxRuntimeStateRequest, ...grpc.CallOption) (*agentsv1.UpdateSandboxRuntimeStateResponse, error)
	DeleteSandboxFunc               func(context.Context, *agentsv1.DeleteSandboxRequest, ...grpc.CallOption) (*agentsv1.DeleteSandboxResponse, error)
	ListInstancesFunc               func(context.Context, *agentsv1.ListInstancesRequest, ...grpc.CallOption) (*agentsv1.ListInstancesResponse, error)
	GetInstanceFunc                 func(context.Context, *agentsv1.GetInstanceRequest, ...grpc.CallOption) (*agentsv1.GetInstanceResponse, error)
	PauseInstanceFunc               func(context.Context, *agentsv1.PauseInstanceRequest, ...grpc.CallOption) (*agentsv1.PauseInstanceResponse, error)
}

func (f *FakeAgentsClient) CreateAgent(context.Context, *agentsv1.CreateAgentRequest, ...grpc.CallOption) (*agentsv1.CreateAgentResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) GetAgent(ctx context.Context, req *agentsv1.GetAgentRequest, opts ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
	if f.GetAgentFunc != nil {
		return f.GetAgentFunc(ctx, req, opts...)
	}
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) ResolveAgentIdentity(ctx context.Context, req *agentsv1.ResolveAgentIdentityRequest, opts ...grpc.CallOption) (*agentsv1.ResolveAgentIdentityResponse, error) {
	if f.ResolveAgentIdentityFunc != nil {
		return f.ResolveAgentIdentityFunc(ctx, req, opts...)
	}
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) UpdateAgent(context.Context, *agentsv1.UpdateAgentRequest, ...grpc.CallOption) (*agentsv1.UpdateAgentResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) DeleteAgent(context.Context, *agentsv1.DeleteAgentRequest, ...grpc.CallOption) (*agentsv1.DeleteAgentResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) ListAgents(ctx context.Context, req *agentsv1.ListAgentsRequest, opts ...grpc.CallOption) (*agentsv1.ListAgentsResponse, error) {
	if f.ListAgentsFunc != nil {
		return f.ListAgentsFunc(ctx, req, opts...)
	}
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) SetAgentRole(context.Context, *agentsv1.SetAgentRoleRequest, ...grpc.CallOption) (*agentsv1.SetAgentRoleResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) RemoveAgentRole(context.Context, *agentsv1.RemoveAgentRoleRequest, ...grpc.CallOption) (*agentsv1.RemoveAgentRoleResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) ListAgentRoles(context.Context, *agentsv1.ListAgentRolesRequest, ...grpc.CallOption) (*agentsv1.ListAgentRolesResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) ListMyAgentRoles(context.Context, *agentsv1.ListMyAgentRolesRequest, ...grpc.CallOption) (*agentsv1.ListMyAgentRolesResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) CreateEnvironment(context.Context, *agentsv1.CreateEnvironmentRequest, ...grpc.CallOption) (*agentsv1.CreateEnvironmentResponse, error) {
	return nil, ErrNotImplemented
}

// The snapshot before a stop is best-effort, so the default here succeeds
// silently: a test that does not care about shell directories should not have
// to wire a stub to stop a workload.
func (f *FakeAgentsClient) SetSandboxLayoutDirectories(ctx context.Context, req *agentsv1.SetSandboxLayoutDirectoriesRequest, opts ...grpc.CallOption) (*agentsv1.SetSandboxLayoutDirectoriesResponse, error) {
	if f.SetSandboxLayoutDirectoriesFunc != nil {
		return f.SetSandboxLayoutDirectoriesFunc(ctx, req, opts...)
	}
	return &agentsv1.SetSandboxLayoutDirectoriesResponse{}, nil
}

func (f *FakeAgentsClient) GetEnvironment(ctx context.Context, req *agentsv1.GetEnvironmentRequest, opts ...grpc.CallOption) (*agentsv1.GetEnvironmentResponse, error) {
	if f.GetEnvironmentFunc != nil {
		return f.GetEnvironmentFunc(ctx, req, opts...)
	}
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) UpdateEnvironment(context.Context, *agentsv1.UpdateEnvironmentRequest, ...grpc.CallOption) (*agentsv1.UpdateEnvironmentResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) DeleteEnvironment(context.Context, *agentsv1.DeleteEnvironmentRequest, ...grpc.CallOption) (*agentsv1.DeleteEnvironmentResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) ListEnvironments(context.Context, *agentsv1.ListEnvironmentsRequest, ...grpc.CallOption) (*agentsv1.ListEnvironmentsResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) CreateSandbox(context.Context, *agentsv1.CreateSandboxRequest, ...grpc.CallOption) (*agentsv1.CreateSandboxResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) GetSandbox(ctx context.Context, req *agentsv1.GetSandboxRequest, opts ...grpc.CallOption) (*agentsv1.GetSandboxResponse, error) {
	if f.GetSandboxFunc != nil {
		return f.GetSandboxFunc(ctx, req, opts...)
	}
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) ListSandboxes(ctx context.Context, req *agentsv1.ListSandboxesRequest, opts ...grpc.CallOption) (*agentsv1.ListSandboxesResponse, error) {
	if f.ListSandboxesFunc != nil {
		return f.ListSandboxesFunc(ctx, req, opts...)
	}
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) StopSandbox(context.Context, *agentsv1.StopSandboxRequest, ...grpc.CallOption) (*agentsv1.StopSandboxResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) UpdateSandboxRuntimeState(ctx context.Context, req *agentsv1.UpdateSandboxRuntimeStateRequest, opts ...grpc.CallOption) (*agentsv1.UpdateSandboxRuntimeStateResponse, error) {
	if f.UpdateSandboxRuntimeStateFunc != nil {
		return f.UpdateSandboxRuntimeStateFunc(ctx, req, opts...)
	}
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) DeleteSandbox(ctx context.Context, req *agentsv1.DeleteSandboxRequest, opts ...grpc.CallOption) (*agentsv1.DeleteSandboxResponse, error) {
	if f.DeleteSandboxFunc != nil {
		return f.DeleteSandboxFunc(ctx, req, opts...)
	}
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) EnsureSandboxRunning(context.Context, *agentsv1.EnsureSandboxRunningRequest, ...grpc.CallOption) (*agentsv1.EnsureSandboxRunningResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) UpdateSandboxLastSession(context.Context, *agentsv1.UpdateSandboxLastSessionRequest, ...grpc.CallOption) (*agentsv1.UpdateSandboxLastSessionResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) CreateInstance(context.Context, *agentsv1.CreateInstanceRequest, ...grpc.CallOption) (*agentsv1.CreateInstanceResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) GetInstance(ctx context.Context, req *agentsv1.GetInstanceRequest, opts ...grpc.CallOption) (*agentsv1.GetInstanceResponse, error) {
	if f.GetInstanceFunc != nil {
		return f.GetInstanceFunc(ctx, req, opts...)
	}
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) ListInstances(ctx context.Context, req *agentsv1.ListInstancesRequest, opts ...grpc.CallOption) (*agentsv1.ListInstancesResponse, error) {
	if f.ListInstancesFunc != nil {
		return f.ListInstancesFunc(ctx, req, opts...)
	}
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) PauseInstance(ctx context.Context, req *agentsv1.PauseInstanceRequest, opts ...grpc.CallOption) (*agentsv1.PauseInstanceResponse, error) {
	if f.PauseInstanceFunc != nil {
		return f.PauseInstanceFunc(ctx, req, opts...)
	}
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) ResumeInstance(context.Context, *agentsv1.ResumeInstanceRequest, ...grpc.CallOption) (*agentsv1.ResumeInstanceResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) DeleteInstance(context.Context, *agentsv1.DeleteInstanceRequest, ...grpc.CallOption) (*agentsv1.DeleteInstanceResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) WriteInboxItem(context.Context, *agentsv1.WriteInboxItemRequest, ...grpc.CallOption) (*agentsv1.WriteInboxItemResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) FanoutInboxItem(context.Context, *agentsv1.FanoutInboxItemRequest, ...grpc.CallOption) (*agentsv1.FanoutInboxItemResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) GetUnackedInboxItems(context.Context, *agentsv1.GetUnackedInboxItemsRequest, ...grpc.CallOption) (*agentsv1.GetUnackedInboxItemsResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) AckInboxItems(context.Context, *agentsv1.AckInboxItemsRequest, ...grpc.CallOption) (*agentsv1.AckInboxItemsResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) GetUnackedInboxCount(context.Context, *agentsv1.GetUnackedInboxCountRequest, ...grpc.CallOption) (*agentsv1.GetUnackedInboxCountResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) CreateVolume(context.Context, *agentsv1.CreateVolumeRequest, ...grpc.CallOption) (*agentsv1.CreateVolumeResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) UpdateVolume(context.Context, *agentsv1.UpdateVolumeRequest, ...grpc.CallOption) (*agentsv1.UpdateVolumeResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) DeleteVolume(context.Context, *agentsv1.DeleteVolumeRequest, ...grpc.CallOption) (*agentsv1.DeleteVolumeResponse, error) {
	return nil, ErrNotImplemented
}

// An environment or MCP declaring no volumes is the ordinary case, so an unset
// hook reports none rather than refusing.
func (f *FakeAgentsClient) ListVolumes(ctx context.Context, req *agentsv1.ListVolumesRequest, opts ...grpc.CallOption) (*agentsv1.ListVolumesResponse, error) {
	if f.ListVolumesFunc != nil {
		return f.ListVolumesFunc(ctx, req, opts...)
	}
	return &agentsv1.ListVolumesResponse{}, nil
}

func (f *FakeAgentsClient) GetVolume(ctx context.Context, req *agentsv1.GetVolumeRequest, opts ...grpc.CallOption) (*agentsv1.GetVolumeResponse, error) {
	if f.GetVolumeFunc != nil {
		return f.GetVolumeFunc(ctx, req, opts...)
	}
	return &agentsv1.GetVolumeResponse{Volume: &agentsv1.Volume{Meta: &agentsv1.EntityMeta{Id: req.GetId()}}}, nil
}

func (f *FakeAgentsClient) CreateVolumeAttachment(context.Context, *agentsv1.CreateVolumeAttachmentRequest, ...grpc.CallOption) (*agentsv1.CreateVolumeAttachmentResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) GetVolumeAttachment(context.Context, *agentsv1.GetVolumeAttachmentRequest, ...grpc.CallOption) (*agentsv1.GetVolumeAttachmentResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) DeleteVolumeAttachment(context.Context, *agentsv1.DeleteVolumeAttachmentRequest, ...grpc.CallOption) (*agentsv1.DeleteVolumeAttachmentResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) CreateMcp(context.Context, *agentsv1.CreateMcpRequest, ...grpc.CallOption) (*agentsv1.CreateMcpResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) GetMcp(context.Context, *agentsv1.GetMcpRequest, ...grpc.CallOption) (*agentsv1.GetMcpResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) UpdateMcp(context.Context, *agentsv1.UpdateMcpRequest, ...grpc.CallOption) (*agentsv1.UpdateMcpResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) DeleteMcp(context.Context, *agentsv1.DeleteMcpRequest, ...grpc.CallOption) (*agentsv1.DeleteMcpResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) ListMcps(ctx context.Context, req *agentsv1.ListMcpsRequest, opts ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error) {
	if f.ListMcpsFunc != nil {
		return f.ListMcpsFunc(ctx, req, opts...)
	}
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) CreateSkill(context.Context, *agentsv1.CreateSkillRequest, ...grpc.CallOption) (*agentsv1.CreateSkillResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) GetSkill(context.Context, *agentsv1.GetSkillRequest, ...grpc.CallOption) (*agentsv1.GetSkillResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) UpdateSkill(context.Context, *agentsv1.UpdateSkillRequest, ...grpc.CallOption) (*agentsv1.UpdateSkillResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) DeleteSkill(context.Context, *agentsv1.DeleteSkillRequest, ...grpc.CallOption) (*agentsv1.DeleteSkillResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) ListSkills(ctx context.Context, req *agentsv1.ListSkillsRequest, opts ...grpc.CallOption) (*agentsv1.ListSkillsResponse, error) {
	if f.ListSkillsFunc != nil {
		return f.ListSkillsFunc(ctx, req, opts...)
	}
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) CreateEnv(context.Context, *agentsv1.CreateEnvRequest, ...grpc.CallOption) (*agentsv1.CreateEnvResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) GetEnv(context.Context, *agentsv1.GetEnvRequest, ...grpc.CallOption) (*agentsv1.GetEnvResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) UpdateEnv(context.Context, *agentsv1.UpdateEnvRequest, ...grpc.CallOption) (*agentsv1.UpdateEnvResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) DeleteEnv(context.Context, *agentsv1.DeleteEnvRequest, ...grpc.CallOption) (*agentsv1.DeleteEnvResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) ListEnvs(ctx context.Context, req *agentsv1.ListEnvsRequest, opts ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
	if f.ListEnvsFunc != nil {
		return f.ListEnvsFunc(ctx, req, opts...)
	}
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) CreateInitScript(context.Context, *agentsv1.CreateInitScriptRequest, ...grpc.CallOption) (*agentsv1.CreateInitScriptResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) GetInitScript(context.Context, *agentsv1.GetInitScriptRequest, ...grpc.CallOption) (*agentsv1.GetInitScriptResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) UpdateInitScript(context.Context, *agentsv1.UpdateInitScriptRequest, ...grpc.CallOption) (*agentsv1.UpdateInitScriptResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) DeleteInitScript(context.Context, *agentsv1.DeleteInitScriptRequest, ...grpc.CallOption) (*agentsv1.DeleteInitScriptResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeAgentsClient) ListInitScripts(ctx context.Context, req *agentsv1.ListInitScriptsRequest, opts ...grpc.CallOption) (*agentsv1.ListInitScriptsResponse, error) {
	if f.ListInitScriptsFunc != nil {
		return f.ListInitScriptsFunc(ctx, req, opts...)
	}
	return nil, ErrNotImplemented
}

type FakeSecretsClient struct {
	ResolveSecretExistsFunc func(context.Context, *secretsv1.ResolveSecretExistsRequest, ...grpc.CallOption) (*secretsv1.ResolveSecretExistsResponse, error)
	ResolveSecretFunc       func(context.Context, *secretsv1.ResolveSecretRequest, ...grpc.CallOption) (*secretsv1.ResolveSecretResponse, error)
}

func (f *FakeSecretsClient) CreateSecretProvider(context.Context, *secretsv1.CreateSecretProviderRequest, ...grpc.CallOption) (*secretsv1.CreateSecretProviderResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeSecretsClient) GetSecretProvider(context.Context, *secretsv1.GetSecretProviderRequest, ...grpc.CallOption) (*secretsv1.GetSecretProviderResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeSecretsClient) UpdateSecretProvider(context.Context, *secretsv1.UpdateSecretProviderRequest, ...grpc.CallOption) (*secretsv1.UpdateSecretProviderResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeSecretsClient) DeleteSecretProvider(context.Context, *secretsv1.DeleteSecretProviderRequest, ...grpc.CallOption) (*secretsv1.DeleteSecretProviderResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeSecretsClient) ListSecretProviders(context.Context, *secretsv1.ListSecretProvidersRequest, ...grpc.CallOption) (*secretsv1.ListSecretProvidersResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeSecretsClient) CreateSecret(context.Context, *secretsv1.CreateSecretRequest, ...grpc.CallOption) (*secretsv1.CreateSecretResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeSecretsClient) GetSecret(context.Context, *secretsv1.GetSecretRequest, ...grpc.CallOption) (*secretsv1.GetSecretResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeSecretsClient) UpdateSecret(context.Context, *secretsv1.UpdateSecretRequest, ...grpc.CallOption) (*secretsv1.UpdateSecretResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeSecretsClient) DeleteSecret(context.Context, *secretsv1.DeleteSecretRequest, ...grpc.CallOption) (*secretsv1.DeleteSecretResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeSecretsClient) ListSecrets(context.Context, *secretsv1.ListSecretsRequest, ...grpc.CallOption) (*secretsv1.ListSecretsResponse, error) {
	return nil, ErrNotImplemented
}

func (f *FakeSecretsClient) ResolveSecret(ctx context.Context, req *secretsv1.ResolveSecretRequest, opts ...grpc.CallOption) (*secretsv1.ResolveSecretResponse, error) {
	if f.ResolveSecretFunc != nil {
		return f.ResolveSecretFunc(ctx, req, opts...)
	}
	return nil, ErrNotImplemented
}

func (f *FakeSecretsClient) ResolveSecretExists(ctx context.Context, req *secretsv1.ResolveSecretExistsRequest, opts ...grpc.CallOption) (*secretsv1.ResolveSecretExistsResponse, error) {
	if f.ResolveSecretExistsFunc != nil {
		return f.ResolveSecretExistsFunc(ctx, req, opts...)
	}
	return nil, ErrNotImplemented
}
