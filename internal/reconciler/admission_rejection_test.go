package reconciler

import (
	"context"
	imageproxyv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/image_proxy/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	zitimgmtv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/ziti_management/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"testing"
)

type admissionPullProxy struct {
	ImageProxyClient
	revoked int
}

func (p *admissionPullProxy) RevokePullCredential(context.Context, *imageproxyv1.RevokePullCredentialRequest, ...grpc.CallOption) (*imageproxyv1.RevokePullCredentialResponse, error) {
	p.revoked++
	return &imageproxyv1.RevokePullCredentialResponse{}, nil
}

func TestConfirmedAdmissionRejectionCompensatesAndRetriesReopenedVolume(t *testing.T) {
	for _, sandbox := range []bool{false, true} {
		t.Run(map[bool]string{false: "agent", true: "sandbox"}[sandbox], func(t *testing.T) {
			f := newPreparedControllerFixture(t, sandbox)
			f.metadata.ZitiIdentityId = "fixture-identity"
			deleted := 0
			f.r.zitiMgmt = &fakeZitiMgmtClient{deleteIdentity: func(context.Context, *zitimgmtv1.DeleteIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.DeleteIdentityResponse, error) {
				deleted++
				return &zitimgmtv1.DeleteIdentityResponse{}, nil
			}}
			proxy := &admissionPullProxy{}
			f.r.imageProxy = proxy
			create := f.registry.createPreparedWorkload
			f.registry.createPreparedWorkload = func(context.Context, *runnersv1.CreatePreparedWorkloadRequest, ...grpc.CallOption) (*runnersv1.CreatePreparedWorkloadResponse, error) {
				return nil, status.Error(codes.ResourceExhausted, "workload_flavor_capacity_exhausted")
			}
			if _, err := f.start(); status.Code(err) != codes.ResourceExhausted {
				t.Fatalf("admission rejection: %v", err)
			}
			if f.w != nil || f.prepares != 0 || f.activations != 0 || f.v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_FAILED || f.v.LifecycleRevision != 2 || deleted != 1 || proxy.revoked != 1 {
				t.Fatal("confirmed rejection did not compensate exactly its unused resources")
			}
			// Exercise the real checked create/reuse path; no reset to revision 1 and
			// no invented first-create receipt is allowed after the rejected attempt.
			req := &runnersv1.CreateVolumeRequest{Id: f.v.Meta.Id, RunnerId: f.v.RunnerId, OrganizationId: f.v.OrganizationId, OwnerKind: f.v.OwnerKind, OwnerId: f.v.OwnerId, AgentId: f.v.AgentId, ThreadId: f.v.ThreadId, VolumeId: f.v.VolumeId, SizeGb: f.v.SizeGb, Status: runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING}
			f.registry.createVolumeChecked = func(context.Context, *runnersv1.CreateVolumeCheckedRequest, ...grpc.CallOption) (*runnersv1.CreateVolumeCheckedResponse, error) {
				return nil, status.Error(codes.AlreadyExists, "volume")
			}
			reopened, owned, err := f.r.createOrReuseCheckedVolume(context.Background(), req)
			if err != nil || !owned || reopened.LifecycleRevision != 3 {
				t.Fatalf("safe reopen failed: %v", err)
			}
			f.created[0].checked = proto.Clone(reopened).(*runnersv1.Volume)
			f.metadata.ZitiIdentityId = "replacement-fixture-identity"
			f.registry.createPreparedWorkload = create
			if _, err := f.start(); err != nil {
				t.Fatalf("retry after capacity freed: %v", err)
			}
			if f.activations != 1 || f.v.AnchorReservation.GetAllocationRevision() != 4 || f.v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE || deleted != 1 || proxy.revoked != 1 {
				t.Fatal("retry lost allocation provenance or compensated active resources")
			}
		})
	}
}

func TestAmbiguousAdmissionErrorsRetainResources(t *testing.T) {
	for _, message := range []string{"received message larger than max", "lost reply"} {
		t.Run(message, func(t *testing.T) {
			f := newPreparedControllerFixture(t, false)
			f.metadata.ZitiIdentityId = "fixture-identity"
			f.r.zitiMgmt = &fakeZitiMgmtClient{deleteIdentity: func(context.Context, *zitimgmtv1.DeleteIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.DeleteIdentityResponse, error) {
				t.Fatal("ambiguous outcome compensated")
				return nil, nil
			}}
			proxy := &admissionPullProxy{}
			f.r.imageProxy = proxy
			create := f.registry.createPreparedWorkload
			f.registry.createPreparedWorkload = func(ctx context.Context, req *runnersv1.CreatePreparedWorkloadRequest, opts ...grpc.CallOption) (*runnersv1.CreatePreparedWorkloadResponse, error) {
				if _, err := create(ctx, req, opts...); err != nil {
					t.Fatal(err)
				}
				return nil, status.Error(codes.ResourceExhausted, message)
			}
			if _, err := f.start(); err == nil {
				t.Fatal("missing ambiguous error")
			}
			if f.w == nil || f.v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING || f.v.LifecycleRevision != 1 || proxy.revoked != 0 || f.prepares != 0 {
				t.Fatal("uncertain reservation resources changed")
			}
		})
	}
}
