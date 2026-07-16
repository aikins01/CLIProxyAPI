package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAuditWebAssetsReportsOnlyMissingContracts(t *testing.T) {
	baseline := webBaseline{
		Schema:    webBaselineSchema,
		SourceURL: "https://ampcode.com/",
		Routes:    []string{"/(app)/threads/[thread=threadID]/(active)"},
		Contracts: []webContract{
			{Name: "actor", Scope: "runtime", RequiredAll: []string{"threadActorConfig", "wsToken"}},
			{Name: "terminal", Scope: "terminal", RequiredAny: [][]string{{"openExecutorRelay", "openDirectTerminal"}}},
		},
	}
	assets := []webAsset{{Path: "app.js", Body: []byte(`{"/(app)/threads/[thread=threadID]/(active)":[]} threadActorConfig wsToken openExecutorRelay`)}}
	report := auditWebAssets("fixture", assets, baseline)
	if len(report.Findings) != 0 {
		t.Fatalf("findings = %#v", report.Findings)
	}

	assets[0].Body = []byte(`{"/(app)/threads/[thread=threadID]/(active)":[]} threadActorConfig`)
	report = auditWebAssets("fixture", assets, baseline)
	if len(report.Findings) != 2 {
		t.Fatalf("findings = %#v, want 2", report.Findings)
	}
	if report.Findings[0].Contract != "actor" || strings.Join(report.Findings[0].Missing, ",") != "wsToken" {
		t.Fatalf("actor finding = %#v", report.Findings[0])
	}
	if report.Findings[1].Contract != "terminal" || strings.Join(report.Findings[1].Missing, ",") != "openExecutorRelay | openDirectTerminal" {
		t.Fatalf("terminal finding = %#v", report.Findings[1])
	}
}

func TestFetchWebAssetsSelectsRouteDependencies(t *testing.T) {
	const route = "/(app)/threads/[thread=threadID]/(active)/terminal"
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/":
			_, _ = response.Write([]byte(`<link rel="modulepreload" href="/_app/immutable/entry/app.test.js">`))
		case "/_app/immutable/entry/app.test.js":
			_, _ = response.Write([]byte(`const __vite__mapDeps=(i,m=__vite__mapDeps,d=(m.f||(m.f=["/_app/immutable/chunks/runtime.js","/_app/immutable/assets/ignored.css"])))=>i.map(i=>d[i]);var nodes=[()=>0,()=>0,()=>0,()=>__vitePreload(()=>import("../nodes/3.route.js"),__vite__mapDeps([0,1]),import.meta.url)];const dictionary={"` + route + `":[3]};`))
		case "/_app/immutable/nodes/3.route.js":
			_, _ = response.Write([]byte("threadActorConfig"))
		case "/_app/immutable/chunks/runtime.js":
			_, _ = response.Write([]byte("amp-terminal-v1"))
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()

	assets, err := fetchWebAssets(server.Client(), server.URL+"/", []string{route})
	if err != nil {
		t.Fatal(err)
	}
	if len(assets) != 3 {
		t.Fatalf("asset count = %d, want 3", len(assets))
	}
	if !assetsContain(assets, "threadActorConfig") || !assetsContain(assets, "amp-terminal-v1") {
		t.Fatalf("selected assets = %#v", assets)
	}
}

func TestFetchWebAssetsDecodesNegativeAndServerOnlyRouteNodes(t *testing.T) {
	const clientRoute = "/(app)/projects"
	const serverOnlyRoute = "/(app)/settings/plugins"
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/":
			_, _ = response.Write([]byte(`<link rel="modulepreload" href="/_app/immutable/entry/app.test.js">`))
		case "/_app/immutable/entry/app.test.js":
			_, _ = response.Write([]byte(`const __vite__mapDeps=(i,m=__vite__mapDeps,d=(m.f||(m.f=["/_app/immutable/chunks/projects.js"])))=>i.map(i=>d[i]);var nodes=[()=>0,()=>0,()=>0,()=>__vitePreload(()=>import("../nodes/3.projects.js"),__vite__mapDeps([0]),import.meta.url)];const dictionary={"` + clientRoute + `":[-4],"` + serverOnlyRoute + `":[-5]};`))
		case "/_app/immutable/nodes/3.projects.js":
			_, _ = response.Write([]byte("projectsRouteNode"))
		case "/_app/immutable/chunks/projects.js":
			_, _ = response.Write([]byte("projectsRouteDependency"))
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()

	assets, err := fetchWebAssets(server.Client(), server.URL+"/", []string{clientRoute, serverOnlyRoute})
	if err != nil {
		t.Fatal(err)
	}
	if !assetsContain(assets, "projectsRouteNode") || !assetsContain(assets, "projectsRouteDependency") {
		t.Fatalf("selected assets = %#v", assets)
	}
}

func TestFetchAssetPathsEnforcesAggregateLimit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		_, _ = response.Write([]byte("1234"))
	}))
	defer server.Close()
	base, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	paths := map[string]struct{}{"/one.js": {}, "/two.js": {}, "/three.js": {}}
	assets, err := fetchAssetPathsLimited(server.Client(), base, paths, 6)
	if err == nil || !strings.Contains(err.Error(), "exceed 6 bytes") {
		t.Fatalf("error = %v, want aggregate limit failure", err)
	}
	if assets != nil {
		t.Fatalf("assets = %#v, want none after aggregate limit failure", assets)
	}
}

func TestRemainingWebBundleBytesIncludesEntryAsset(t *testing.T) {
	remaining, err := remainingWebBundleBytes(4, 10)
	if err != nil || remaining != 6 {
		t.Fatalf("remaining = %d, err = %v; want 6", remaining, err)
	}
	if _, err := remainingWebBundleBytes(11, 10); err == nil {
		t.Fatal("entry asset larger than aggregate limit was accepted")
	}
}

func TestRunStrictWithAssetDirectory(t *testing.T) {
	dir := t.TempDir()
	baselinePath := filepath.Join(dir, "baseline.json")
	assetDir := filepath.Join(dir, "assets")
	if err := os.Mkdir(assetDir, 0o700); err != nil {
		t.Fatal(err)
	}
	baseline := webBaseline{
		Schema:    webBaselineSchema,
		SourceURL: "https://ampcode.com/",
		Routes:    []string{"/(app)/projects"},
		Contracts: []webContract{{Name: "projects", Scope: "web", RequiredAll: []string{"workingDirectory"}}},
	}
	data, err := json.Marshal(baseline)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(baselinePath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(assetDir, "app.js"), []byte(`{"/(app)/projects":[]} workingDirectory`), 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if exitCode := run([]string{"-strict", "-baseline", baselinePath, "-asset-dir", assetDir}, &stdout, &stderr); exitCode != 0 {
		t.Fatalf("exit = %d, stderr = %s", exitCode, stderr.String())
	}
	if !strings.Contains(stdout.String(), "Amp web parity passed") {
		t.Fatalf("stdout = %q", stdout.String())
	}

	if err := os.WriteFile(filepath.Join(assetDir, "app.js"), []byte(`{"/(app)/projects":[]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	stderr.Reset()
	if exitCode := run([]string{"-strict", "-baseline", baselinePath, "-asset-dir", assetDir}, &stdout, &stderr); exitCode != 2 {
		t.Fatalf("exit = %d, want 2; stderr = %s", exitCode, stderr.String())
	}
}

func TestRepositoryWebBaselineIsValid(t *testing.T) {
	path := filepath.Join("..", "..", "dev", "amp-web-parity-baseline.json")
	baseline, err := readWebBaseline(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateWebBaseline(baseline); err != nil {
		t.Fatal(err)
	}
}
