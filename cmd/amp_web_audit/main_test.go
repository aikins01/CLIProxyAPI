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

func TestAuditWebAssetsRequiresTogetherMarkersInOneAsset(t *testing.T) {
	baseline := webBaseline{
		Schema:    webBaselineSchema,
		SourceURL: "https://ampcode.com/",
		Routes:    []string{"/(app)/projects"},
		Contracts: []webContract{{
			Name:             "create-thread",
			Scope:            "upstream-web",
			RequiredTogether: [][]string{{"createProjectThread", "result.initialThread"}},
		}},
	}
	assets := []webAsset{
		{Path: "create.js", Body: []byte(`{"/(app)/projects":[]} createProjectThread`)},
		{Path: "thread.js", Body: []byte("result.initialThread")},
	}
	report := auditWebAssets("fixture", assets, baseline)
	if len(report.Findings) != 1 || strings.Join(report.Findings[0].Missing, ",") != "createProjectThread & result.initialThread" {
		t.Fatalf("findings = %#v, want one co-location failure", report.Findings)
	}

	assets[0].Body = append(assets[0].Body, []byte(" result.initialThread")...)
	report = auditWebAssets("fixture", assets, baseline)
	if len(report.Findings) != 0 {
		t.Fatalf("co-located markers were rejected: %#v", report.Findings)
	}
}

func TestValidateWebBaselineAcceptsSchema1WithoutRequiredTogether(t *testing.T) {
	baseline := webBaseline{
		Schema:    1,
		SourceURL: "https://ampcode.com/",
		Routes:    []string{"/(app)/projects"},
		Contracts: []webContract{{Name: "projects", Scope: "upstream-web", RequiredAll: []string{"createProjectThread"}}},
	}
	if err := validateWebBaseline(baseline); err != nil {
		t.Fatalf("schema 1 baseline was rejected: %v", err)
	}

	baseline.Contracts[0].RequiredTogether = [][]string{{"createProjectThread", "result.initialThread"}}
	if err := validateWebBaseline(baseline); err == nil || !strings.Contains(err.Error(), "requires schema 2") {
		t.Fatalf("schema 1 required_together error = %v", err)
	}
}

func TestFetchWebAssetsSelectsRouteDependencies(t *testing.T) {
	const route = "/(app)/threads/[thread=threadID]/(active)/terminal"
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/":
			_, _ = response.Write([]byte(`<link rel="modulepreload" href="/_app/immutable/entry/app.test.js">`))
		case "/_app/immutable/entry/app.test.js":
			_, _ = response.Write([]byte(`const __vite__mapDeps=(i,m=__vite__mapDeps,d=(m.f||(m.f=["/_app/immutable/chunks/runtime.js","/_app/immutable/assets/ignored.css","/_app/immutable/chunks/lazy.js","/_app/immutable/chunks/leaf.js","/_app/immutable/chunks/unrelated.js"])))=>i.map(i=>d[i]);var nodes=[()=>0,()=>0,()=>0,()=>__vitePreload(()=>import("../nodes/3.route.js"),__vite__mapDeps([0,1]),import.meta.url),()=>__vitePreload(()=>import("../nodes/4.unrelated.js"),__vite__mapDeps([4]),import.meta.url)];const dictionary={"` + route + `":[3]};`))
		case "/_app/immutable/nodes/3.route.js":
			_, _ = response.Write([]byte(`threadActorConfig __vite__mapDeps([2]); import{value}from"../chunks/sibling.js"`))
		case "/_app/immutable/nodes/4.unrelated.js":
			_, _ = response.Write([]byte("unrelatedRouteNode"))
		case "/_app/immutable/chunks/runtime.js":
			_, _ = response.Write([]byte("amp-terminal-v1"))
		case "/_app/immutable/chunks/lazy.js":
			_, _ = response.Write([]byte(`const help="./unrelated-string.js"; import("./leaf.js"); client_resume`))
		case "/_app/immutable/chunks/leaf.js":
			_, _ = response.Write([]byte("sendUserMessage"))
		case "/_app/immutable/chunks/sibling.js":
			_, _ = response.Write([]byte("actorReady"))
		case "/_app/immutable/chunks/unrelated.js":
			_, _ = response.Write([]byte("unrelatedRouteAsset"))
		case "/_app/immutable/chunks/unrelated-string.js":
			_, _ = response.Write([]byte("unrelatedImportString"))
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()

	assets, err := fetchWebAssets(server.Client(), server.URL+"/", []string{route})
	if err != nil {
		t.Fatal(err)
	}
	if len(assets) != 6 {
		t.Fatalf("asset count = %d, want 6", len(assets))
	}
	for _, marker := range []string{"threadActorConfig", "amp-terminal-v1", "client_resume", "sendUserMessage", "actorReady"} {
		if !assetsContain(assets, marker) {
			t.Fatalf("selected assets do not contain %q: %#v", marker, assets)
		}
	}
	if assetsContain(assets, "unrelatedRouteNode") || assetsContain(assets, "unrelatedRouteAsset") || assetsContain(assets, "unrelatedImportString") {
		t.Fatalf("selected assets include unrelated entry graph: %#v", assets)
	}
}

func TestParseDependencyMapRejectsNonAppAssetPaths(t *testing.T) {
	const entryTemplate = `const __vite__mapDeps=(i,m=__vite__mapDeps,d=(m.f||(m.f=["DEPENDENCY"])))=>i.map(i=>d[i]);`
	for _, dependencyPath := range []string{
		"https://example.com/other.js",
		"//example.com/other.js",
		"/_app/immutable/../other.js",
		"/_app/immutable/chunks/runtime.js%3Fignored",
		"/_app/immutable/chunks/runtime.js%23ignored",
	} {
		entry := []byte(strings.Replace(entryTemplate, "DEPENDENCY", dependencyPath, 1))
		if _, err := parseDependencyMap(entry); err == nil || !strings.Contains(err.Error(), "outside canonical /_app/immutable") {
			t.Fatalf("parseDependencyMap(%q) error = %v", dependencyPath, err)
		}
	}
}

func TestNestedWebAssetPathsRejectsImportOutsideAppAssets(t *testing.T) {
	asset := webAsset{
		Path: "/_app/immutable/nodes/3.route.js",
		Body: []byte(`import("../../chunks/other.js")`),
	}
	if _, err := nestedWebAssetPaths(asset, nil); err == nil || !strings.Contains(err.Error(), "outside canonical /_app/immutable") {
		t.Fatalf("nestedWebAssetPaths() error = %v", err)
	}
}

func TestNestedWebAssetPathsIgnoresImportLikeNonCode(t *testing.T) {
	asset := webAsset{
		Path: "/_app/immutable/chunks/lazy.js",
		Body: []byte("const sample = 'import(\"./missing.js\")'; // import(\"./comment.js\") __vite__mapDeps([0])\n/* from\"./block.js\" */ const template = `import(\"./template.js\")`;"),
	}
	paths, err := nestedWebAssetPaths(asset, []string{"/_app/immutable/chunks/dependency.js"})
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 0 {
		t.Fatalf("nestedWebAssetPaths() = %v, want no paths from non-code text", paths)
	}
}

func TestNestedWebAssetPathsIgnoresImportLikeRegex(t *testing.T) {
	asset := webAsset{
		Path: "/_app/immutable/chunks/lazy.js",
		Body: []byte(`const matcher=/from"./leaf.js"/;`),
	}
	paths, err := nestedWebAssetPaths(asset, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 0 {
		t.Fatalf("nestedWebAssetPaths() = %v, want no paths from regex literal", paths)
	}
}

func TestNestedWebAssetPathsDistinguishesDivisionFromRegex(t *testing.T) {
	asset := webAsset{
		Path: "/_app/immutable/chunks/lazy.js",
		Body: []byte(`const ratio=total/count; import("./leaf.js")`),
	}
	paths, err := nestedWebAssetPaths(asset, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := paths["/_app/immutable/chunks/leaf.js"]; !exists {
		t.Fatalf("nestedWebAssetPaths() = %v, want import after division expression", paths)
	}
}

func TestNestedWebAssetPathsPreservesImportAfterPostfixDivision(t *testing.T) {
	asset := webAsset{
		Path: "/_app/immutable/chunks/lazy.js",
		Body: []byte(`i++/2; import("./leaf.js"); const ratio=a/b`),
	}
	paths, err := nestedWebAssetPaths(asset, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := paths["/_app/immutable/chunks/leaf.js"]; !exists {
		t.Fatalf("nestedWebAssetPaths() = %v, want import after postfix division", paths)
	}
}

func TestNestedWebAssetPathsIncludesImportAfterCommentTrivia(t *testing.T) {
	asset := webAsset{
		Path: "/_app/immutable/chunks/lazy.js",
		Body: []byte(`import(/* @vite-ignore */ "./leaf.js")`),
	}
	paths, err := nestedWebAssetPaths(asset, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := paths["/_app/immutable/chunks/leaf.js"]; !exists {
		t.Fatalf("nestedWebAssetPaths() = %v, want import after comment trivia", paths)
	}
}

func TestNestedWebAssetPathsIgnoresComputedDynamicImport(t *testing.T) {
	asset := webAsset{
		Path: "/_app/immutable/chunks/lazy.js",
		Body: []byte(`import("./leaf.js"+suffix)`),
	}
	paths, err := nestedWebAssetPaths(asset, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 0 {
		t.Fatalf("nestedWebAssetPaths() = %v, want no fixed path from computed dynamic import", paths)
	}
}

func TestNestedWebAssetPathsIncludesImportInTemplateInterpolation(t *testing.T) {
	asset := webAsset{
		Path: "/_app/immutable/chunks/lazy.js",
		Body: []byte("const x = `${import(\"./leaf.js\")}`"),
	}
	paths, err := nestedWebAssetPaths(asset, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := paths["/_app/immutable/chunks/leaf.js"]; !exists {
		t.Fatalf("nestedWebAssetPaths() = %v, want template interpolation import", paths)
	}
}

func TestFetchWebAssetsIncludesRouteLayoutAndErrorDependencies(t *testing.T) {
	const route = "/(app)/projects"
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/":
			_, _ = response.Write([]byte(`<link rel="modulepreload" href="/_app/immutable/entry/app.test.js">`))
		case "/_app/immutable/entry/app.test.js":
			_, _ = response.Write([]byte(`const __vite__mapDeps=(i,m=__vite__mapDeps,d=(m.f||(m.f=["/_app/immutable/chunks/route.js","/_app/immutable/chunks/sidebar.js","/_app/immutable/chunks/error.js"])))=>i.map(i=>d[i]);var nodes=[()=>0,()=>0,()=>0,()=>__vitePreload(()=>import("../nodes/3.route.js"),__vite__mapDeps([0]),import.meta.url),()=>__vitePreload(()=>import("../nodes/4.layout.js"),__vite__mapDeps([1]),import.meta.url),()=>__vitePreload(()=>import("../nodes/5.error.js"),__vite__mapDeps([2]),import.meta.url)];const dictionary={"` + route + `":[3,[4],[5]]};`))
		case "/_app/immutable/nodes/3.route.js":
			_, _ = response.Write([]byte("projectsRouteNode"))
		case "/_app/immutable/nodes/4.layout.js":
			_, _ = response.Write([]byte("appLayoutNode"))
		case "/_app/immutable/nodes/5.error.js":
			_, _ = response.Write([]byte("appErrorNode"))
		case "/_app/immutable/chunks/route.js":
			_, _ = response.Write([]byte("projectsRouteDependency"))
		case "/_app/immutable/chunks/sidebar.js":
			_, _ = response.Write([]byte("listThreadListSidebar createProjectThread"))
		case "/_app/immutable/chunks/error.js":
			_, _ = response.Write([]byte("renderRouteError"))
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()

	assets, err := fetchWebAssets(server.Client(), server.URL+"/", []string{route})
	if err != nil {
		t.Fatal(err)
	}
	for _, marker := range []string{"projectsRouteNode", "appLayoutNode", "appErrorNode", "projectsRouteDependency", "listThreadListSidebar", "createProjectThread", "renderRouteError"} {
		if !assetsContain(assets, marker) {
			t.Fatalf("selected assets do not contain %q: %#v", marker, assets)
		}
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
	if !strings.Contains(stdout.String(), "Amp upstream web parity passed") {
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
