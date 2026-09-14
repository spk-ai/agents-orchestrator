package assembler

import (
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestSandboxDNSInterceptionPolicy(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(strconv.FormatBool(enabled), func(t *testing.T) {
			fixture := newSandboxFixture()
			fixture.cfg.ZitiEnabled = enabled
			fixture.cfg.ZitiSidecarImage = "ziti-image"
			fixture.cfg.WorkloadDNSUpstream = "10.43.0.10"
			request := fixture.assemble(t).Request
			if !enabled {
				if request.DnsConfig != nil {
					t.Fatal("standalone DNS behavior changed")
				}
				return
			}
			if request.DnsConfig == nil || !reflect.DeepEqual(request.DnsConfig.Nameservers, []string{zitiDNSNameserver}) {
				t.Fatal("sandbox exposes a non-intercepting resolver")
			}
			var foundWait, foundSidecar bool
			for _, container := range request.InitContainers {
				if container.Name == zitiWaitContainerName {
					foundWait = true
					if strings.Contains(strings.Join(container.Cmd, " "), fixture.cfg.WorkloadDNSUpstream) {
						t.Fatal("wait restores bypass resolver")
					}
				}
				if container.Name == ZitiSidecarContainerName {
					foundSidecar = true
					if container.Cmd[len(container.Cmd)-1] != fixture.cfg.WorkloadDNSUpstream {
						t.Fatal("tunnel lost upstream DNS")
					}
				}
			}
			if !foundWait || !foundSidecar {
				t.Fatal("overlay bootstrap missing")
			}
		})
	}
}

func TestZitiWaitUsesOnlyInterceptingDNS(t *testing.T) {
	command := buildZitiWaitCommand("gateway.agyn:443", zitiServiceTarget{host: "llm-proxy.agyn", port: "80"})
	resolver := "nameserver 127.0.0.1\nsearch svc.cluster.local cluster.local\noptions ndots:5 timeout:1 attempts:1\n"
	if !strings.Contains(command[1], strconv.Quote(resolver)) || strings.Contains(command[1], "10.43.0.10") {
		t.Fatal("wait can resolve a service outside the intercepting DNS")
	}
	if !strings.Contains(zitiSidecarScript, `--dnsUpstream "udp://${workload_dns_upstream}:53"`) {
		t.Fatal("ordinary DNS must still be forwarded by the tunnel")
	}
}
