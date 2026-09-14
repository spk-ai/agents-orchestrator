package reconciler

import (
	"fmt"
	"strconv"
	"strings"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/agents-orchestrator/internal/assembler"
	"k8s.io/apimachinery/pkg/api/resource"
)

type volumeRecord struct {
	id       string
	volumeID string
	sizeGB   string
	checked  *runnersv1.Volume
}

func buildVolumeRecords(volumes []assembler.PersistentVolumeInfo) ([]volumeRecord, error) {
	if len(volumes) == 0 {
		return nil, nil
	}
	records := make([]volumeRecord, 0, len(volumes))
	for _, info := range volumes {
		if info.Volume == nil {
			return nil, fmt.Errorf("volume %s missing", info.ID.String())
		}
		if info.Spec == nil {
			return nil, fmt.Errorf("volume spec missing for %s", info.ID.String())
		}
		sizeGB, err := volumeSizeGB(info.Volume)
		if err != nil {
			return nil, fmt.Errorf("volume %s: %w", info.ID.String(), err)
		}
		recordID := info.Key()
		if info.Spec.Labels == nil {
			info.Spec.Labels = map[string]string{}
		}
		info.Spec.Labels[assembler.LabelVolumeKey] = recordID
		records = append(records, volumeRecord{
			id:       recordID,
			volumeID: info.ID.String(),
			sizeGB:   sizeGB,
		})
	}
	return records, nil
}

func volumeSizeGB(volume *agentsv1.Volume) (string, error) {
	if volume == nil {
		return "", fmt.Errorf("volume missing")
	}
	trimmed := strings.TrimSpace(volume.GetSize())
	if trimmed == "" {
		return "", fmt.Errorf("size missing")
	}
	quantity, err := resource.ParseQuantity(trimmed)
	if err != nil {
		return "", fmt.Errorf("parse size %q: %w", trimmed, err)
	}
	value := quantity.AsApproximateFloat64() / float64(1<<30)
	if value <= 0 {
		return "", fmt.Errorf("size must be positive")
	}
	return strconv.FormatFloat(value, 'f', -1, 64), nil
}
