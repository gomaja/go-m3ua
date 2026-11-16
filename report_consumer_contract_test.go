package m3ua

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestLegacyReportConsumerRequiresMigration(t *testing.T) {
	goBinary, err := exec.LookPath("go")
	if err != nil {
		t.Skipf("requires a Go toolchain on PATH: %v", err)
	}
	root, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	folder := t.TempDir()
	module := "module legacyreportconsumer\n\ngo 1.26\n\nrequire github.com/gomaja/go-m3ua v0.0.0\nreplace github.com/gomaja/go-m3ua => " + strconv.Quote(filepath.ToSlash(root)) + "\n"
	source := `package consumer
import m3ua "github.com/gomaja/go-m3ua"
func apply(event m3ua.SSNMEvent, replica *m3ua.SSNMPartitionKnowledge) {
 if event.Kind == m3ua.SSNMReportEvent {
  for _, update := range event.Updated {
   for i := range replica.Destinations {
    if replica.Destinations[i].Destination == update.Destination { replica.Destinations[i] = update }
   }
  }
 }
}`
	if err = os.WriteFile(filepath.Join(folder, "go.mod"), []byte(module), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(folder, "consumer.go"), []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(goBinary, "test", "-mod=mod", "-run", "^$", ".")
	command.Dir = folder
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatal("legacy whole-destination consumer still compiles")
	}
	if !strings.Contains(string(output), "undefined: m3ua.SSNMReportEvent") || !strings.Contains(string(output), "event.Updated undefined") {
		t.Fatalf("legacy consumer failed for another reason: %v\n%s", err, output)
	}
}
