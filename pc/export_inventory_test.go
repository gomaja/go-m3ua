// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package pc_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/gomaja/go-m3ua/pc"
)

// The codec export cleanup removed a deprecated wrapper family from messages
// and messages/params. Point Code conversion was never part of that family and
// was explicitly left alone, so this package's exported surface is a fixed
// point of the change: nothing here may be removed to tidy up, and nothing may
// be added under cover of the cleanup.
//
// The list below is that approved surface, read back from the source. It is
// deliberately exhaustive rather than a spot check, because either direction of
// drift is the thing being guarded against.
var approvedExports = []string{
	"const Variant383",
	"const Variant4343",
	"const Variant437",
	"const Variant446",
	"const Variant545",
	"const Variant662",
	"const Variant68",
	"const Variant745",
	"const Variant77",
	"const Variant888",
	"const VariantNone",
	"func NewPointCode",
	"func NewPointCodeFrom",
	"method (PointCode).ConvertTo",
	"method (PointCode).String",
	"method (PointCode).Uint32",
	"method (PointCode).Variant",
	"method (Variant).BitLength",
	"method (Variant).String",
	"type PointCode",
	"type Variant",
}

// TestApprovedPointCodeExportsAreUnchanged reads the package source and
// compares its exported surface against the approved list.
func TestApprovedPointCodeExportsAreUnchanged(t *testing.T) {
	fileSet := token.NewFileSet()
	pkgs, err := parser.ParseDir(fileSet, ".", func(info fs.FileInfo) bool {
		return !strings.HasSuffix(info.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parsing the pc package: %v", err)
	}

	var got []string
	for name, pkg := range pkgs {
		if strings.HasSuffix(name, "_test") {
			continue
		}
		for _, file := range pkg.Files {
			for _, decl := range file.Decls {
				switch typed := decl.(type) {
				case *ast.FuncDecl:
					if !typed.Name.IsExported() {
						continue
					}
					if typed.Recv == nil || len(typed.Recv.List) == 0 {
						got = append(got, "func "+typed.Name.Name)
						continue
					}
					got = append(got, "method ("+receiverName(typed.Recv.List[0].Type)+")."+typed.Name.Name)
				case *ast.GenDecl:
					got = append(got, exportedGenDeclNames(typed)...)
				}
			}
		}
	}
	sort.Strings(got)

	if !reflect.DeepEqual(got, approvedExports) {
		t.Errorf("pc exported surface changed.\n got: %v\nwant: %v\nadded: %v\nremoved: %v",
			got, approvedExports,
			difference(got, approvedExports), difference(approvedExports, got))
	}
}

// TestPointCodeConversionStillWorks keeps the inventory honest: an export that
// exists but no longer converts is not a retained operation.
func TestPointCodeConversionStillWorks(t *testing.T) {
	pointCode := pc.NewPointCode(0x123456, pc.Variant888)
	if pointCode == nil {
		t.Fatal("pc.NewPointCode returned nil for a 8-8-8 point code")
	}
	if got := pointCode.Uint32(); got != 0x123456 {
		t.Errorf("Uint32() = %#x, want %#x", got, 0x123456)
	}
	if got := pointCode.Variant(); got != string(pc.Variant888) {
		t.Errorf("Variant() = %q, want %q", got, pc.Variant888)
	}
	converted, err := pointCode.ConvertTo(pc.Variant888)
	if err != nil {
		t.Fatalf("ConvertTo(8-8-8) error = %v", err)
	}
	if converted != "18-52-86" {
		t.Errorf("ConvertTo(8-8-8) = %q, want %q", converted, "18-52-86")
	}
	if back := pc.NewPointCodeFrom(converted, pc.Variant888); back == nil || back.Uint32() != 0x123456 {
		t.Errorf("round trip through %q lost the value: %v", converted, back)
	}
	// A narrower variant is a different 14-bit field layout, not the same
	// number rendered differently; it must still convert without error.
	if _, err := pc.NewPointCode(0x1234, pc.Variant383).ConvertTo(pc.Variant383); err != nil {
		t.Errorf("ConvertTo(3-8-3) error = %v", err)
	}
	if got := pc.Variant888.BitLength(); got != 24 {
		t.Errorf("Variant888.BitLength() = %d, want 24", got)
	}
	if got := pointCode.String(); got == "" {
		t.Error("PointCode.String() is empty")
	}
}

func receiverName(expr ast.Expr) string {
	switch typed := expr.(type) {
	case *ast.StarExpr:
		return receiverName(typed.X)
	case *ast.Ident:
		return typed.Name
	default:
		return ""
	}
}

func exportedGenDeclNames(decl *ast.GenDecl) []string {
	var names []string
	for _, spec := range decl.Specs {
		switch typed := spec.(type) {
		case *ast.TypeSpec:
			if typed.Name.IsExported() {
				names = append(names, "type "+typed.Name.Name)
			}
		case *ast.ValueSpec:
			for _, name := range typed.Names {
				if !name.IsExported() {
					continue
				}
				kind := "var"
				if decl.Tok == token.CONST {
					kind = "const"
				}
				names = append(names, kind+" "+name.Name)
			}
		}
	}
	return names
}

func difference(from, without []string) []string {
	index := make(map[string]struct{}, len(without))
	for _, name := range without {
		index[name] = struct{}{}
	}
	var out []string
	for _, name := range from {
		if _, found := index[name]; !found {
			out = append(out, name)
		}
	}
	return out
}
