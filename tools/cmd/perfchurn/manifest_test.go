package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadValuesRecordsWhatItCannotRead(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "khugepaged"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "khugepaged", "max_ptes_none"), []byte("511\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	values := readValues(root, "khugepaged/max_ptes_none", "enabled")
	if values["khugepaged/max_ptes_none"] != "511" || len(values["enabled"]) < len("unavailable") || values["enabled"][:11] != "unavailable" {
		t.Fatalf("values %v", values)
	}
}

func TestManifestPinsTheEnvironment(t *testing.T) {
	record := currentManifest(roleASP, commandConfig{Routes: referenceRouteCount})
	if len(record.TransparentHugePages) != 6 || len(record.Kernel) == 0 || len(record.Cgroup) == 0 {
		t.Fatalf("environment maps %v %v %v", record.TransparentHugePages, record.Kernel, record.Cgroup)
	}
	if record.Limits.DataQueueMessages != dataQueueSize || record.Limits.SSNMState.MaxRecords != stateRecords ||
		record.Limits.TransferFlowCacheEntries == 0 || record.Topology.Partitions != partitionCount {
		t.Fatalf("limits %+v topology %+v", record.Limits, record.Topology)
	}
}
