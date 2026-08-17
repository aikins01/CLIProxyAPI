package orbconfig

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestBundleDigestIsDeterministic(t *testing.T) {
	files := []DecodedFile{
		{Path: "skills/demo/SKILL.md", Content: []byte("---\nname: demo\n---\n")},
		{Path: "checks/review.md", Content: []byte("review")},
	}
	extensions := []Extension{{Repo: "owner/gh-zeta", Version: strings.Repeat("b", 40)}, {Repo: "owner/gh-alpha", Version: strings.Repeat("a", 40)}}
	first, err := New(files, extensions)
	if err != nil {
		t.Fatal(err)
	}
	second, err := New([]DecodedFile{files[1], files[0]}, []Extension{extensions[1], extensions[0]})
	if err != nil {
		t.Fatal(err)
	}
	if first.Digest != second.Digest || !reflect.DeepEqual(first, second) {
		t.Fatalf("deterministic bundles differ:\n%#v\n%#v", first, second)
	}
	changed, err := New([]DecodedFile{{Path: "checks/review.md", Content: []byte("changed")}, files[0]}, extensions)
	if err != nil {
		t.Fatal(err)
	}
	if changed.Digest == first.Digest {
		t.Fatal("digest did not cover file content")
	}
	withoutExtension, err := New(files, extensions[:1])
	if err != nil {
		t.Fatal(err)
	}
	if withoutExtension.Digest == first.Digest {
		t.Fatal("digest did not cover extensions")
	}
}

func TestValidateCanonicalizesExternalBundle(t *testing.T) {
	bundle, err := New(
		[]DecodedFile{
			{Path: "checks/alpha.md", Content: []byte("alpha")},
			{Path: "checks/zeta.md", Content: []byte("zeta")},
		},
		[]Extension{
			{Repo: "owner/gh-alpha", Version: strings.Repeat("a", 40)},
			{Repo: "owner/gh-zeta", Version: strings.Repeat("b", 40)},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	external := bundle
	external.Files = []File{bundle.Files[1], bundle.Files[0]}
	external.Extensions = []Extension{bundle.Extensions[1], bundle.Extensions[0]}
	normalized, _, err := Validate(external)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(normalized, bundle) {
		t.Fatalf("normalized external bundle differs:\n%#v\n%#v", normalized, bundle)
	}
}

func TestBundleRejectsUnsafeFilesAndSkills(t *testing.T) {
	paths := []string{
		"/checks/review.md",
		"checks/../secret",
		"checks//review.md",
		"other/review.md",
		"checks/auth-token.json",
		"skills/demo/mcp.json",
		"skills/using-open-browser-use/SKILL.md",
		"skills/demo/a/b/c/d/e/f/g",
		"checks/bad\x00name",
	}
	for _, filePath := range paths {
		t.Run(strings.ReplaceAll(filePath, "/", "_"), func(t *testing.T) {
			if _, err := New([]DecodedFile{{Path: filePath, Content: []byte("x")}}, nil); err == nil {
				t.Fatalf("unsafe path %q was accepted", filePath)
			}
		})
	}
	for _, skill := range []string{
		"---\nname: [\n---\n",
		"---\nname: demo\n",
		"---\nmcpServers: {}\n---\n",
	} {
		if _, err := New([]DecodedFile{{Path: "skills/demo/SKILL.md", Content: []byte(skill)}}, nil); err == nil {
			t.Fatalf("unsafe skill was accepted: %q", skill)
		}
	}
	if _, err := New([]DecodedFile{{Path: "skills/demo/reference.md", Content: []byte("x")}}, nil); err == nil || !strings.Contains(err.Error(), "missing SKILL.md") {
		t.Fatalf("skill without SKILL.md error = %v", err)
	}
	if _, err := New([]DecodedFile{
		{Path: "checks/Foo.md", Content: []byte("a")},
		{Path: "checks/foo.md", Content: []byte("b")},
	}, nil); err == nil || !strings.Contains(err.Error(), "duplicated") {
		t.Fatalf("case-fold duplicate error = %v", err)
	}
	if _, err := New([]DecodedFile{{Path: "checks/native", Content: []byte{0xcf, 0xfa, 0xed, 0xfe, 0x01}}}, nil); err == nil || !strings.Contains(err.Error(), "macOS binary") {
		t.Fatalf("Mach-O error = %v", err)
	}
	if _, err := New([]DecodedFile{{Path: "checks/local-path.md", Content: []byte("read /Users/example/private")}}, nil); err == nil || !strings.Contains(err.Error(), "Mac path") {
		t.Fatalf("absolute Mac path error = %v", err)
	}
	if _, err := New([]DecodedFile{{Path: "checks/labeled-path.md", Content: []byte("home:/Users/example/private")}}, nil); err == nil || !strings.Contains(err.Error(), "Mac path") {
		t.Fatalf("labeled absolute Mac path error = %v", err)
	}
	if _, err := New([]DecodedFile{{Path: "checks/key-material.md", Content: []byte("-----BEGIN OPENSSH PRIVATE KEY-----")}}, nil); err == nil || !strings.Contains(err.Error(), "credential") {
		t.Fatalf("private key content error = %v", err)
	}
	if _, err := New([]DecodedFile{{Path: "checks/reference.md", Content: []byte("see https://example.com/System/Library/reference and C:/Users/example/docs")}}, nil); err != nil {
		t.Fatalf("portable documentation path error = %v", err)
	}
	if _, err := New([]DecodedFile{{Path: "checks/leaked.md", Content: []byte("ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")}}, nil); err == nil || !strings.Contains(err.Error(), "credential") {
		t.Fatalf("credential content error = %v", err)
	}
}

func TestBundleRejectsAggregateDecodedSizeBeforeEncoding(t *testing.T) {
	content := make([]byte, MaxFileBytes)
	files := make([]DecodedFile, 0, MaxDecodedBytes/MaxFileBytes+1)
	for index := range cap(files) {
		files = append(files, DecodedFile{Path: "checks/large-" + string(rune('a'+index)), Content: content})
	}
	if _, err := New(files, nil); err == nil || !strings.Contains(err.Error(), "decoded files") {
		t.Fatalf("aggregate size error = %v", err)
	}
}

func TestBundleRejectsTamperingAndUnsafeExtensions(t *testing.T) {
	commit := strings.Repeat("a", 40)
	bundle, err := New([]DecodedFile{{Path: "checks/review.md", Content: []byte("review")}}, []Extension{{Repo: "owner/gh-repo", Version: commit}})
	if err != nil {
		t.Fatal(err)
	}
	bundle.Files[0].Content = "dGFtcGVyZWQ="
	if _, _, err := Validate(bundle); err == nil || !strings.Contains(err.Error(), "sha256") {
		t.Fatalf("tampered content error = %v", err)
	}
	for _, extension := range []Extension{
		{Repo: "github.com/owner/gh-repo", Version: commit},
		{Repo: "owner/gh-repo;rm", Version: commit},
		{Repo: "owner/repo", Version: commit},
		{Repo: "owner/GH-repo", Version: commit},
		{Repo: "owner/gh-repo", Version: "../../main"},
		{Repo: "owner/gh-repo", Version: "release/../main"},
		{Repo: "owner/gh-repo", Version: "-latest"},
		{Repo: "owner/gh-repo", Version: "release//v1"},
		{Repo: "owner/gh-repo", Version: "release/v1/"},
		{Repo: "owner/gh-repo", Version: "release/v1."},
		{Repo: "owner/gh-repo", Version: "release\\v1"},
		{Repo: "owner/gh-repo", Version: "release/@{1}"},
		{Repo: "owner/gh-repo", Version: "release/.hidden"},
		{Repo: "owner/gh-repo", Version: "release/v1.lock"},
		{Repo: "owner/gh-repo", Version: "v1 beta"},
		{Repo: "owner/gh-repo", Version: "v1\nbeta"},
		{Repo: "owner/gh-repo", Version: "v1;rm"},
		{Repo: "owner/gh-repo", Version: "v1$(id)"},
		{Repo: "owner/gh-repo", Version: "v1^2"},
		{Repo: "owner/gh-repo", Version: strings.Repeat("a", maxExtensionRef+1)},
	} {
		if ValidateExtension(extension) == nil {
			t.Fatalf("unsafe extension was accepted: %#v", extension)
		}
	}
	for _, version := range []string{"v1", "v1.2.3", "1.2.3-rc.1+build.2", "main", "release/v1.2.3", "pull/123/head", strings.Repeat("a", 40), strings.Repeat("F", 40)} {
		if err := ValidateExtension(Extension{Repo: "owner/gh-repo", Version: version}); err != nil {
			t.Fatalf("extension pin %q was rejected: %v", version, err)
		}
	}
	if _, err := New(nil, []Extension{{Repo: "alpha/gh-tool", Version: strings.Repeat("a", 40)}, {Repo: "beta/gh-tool", Version: strings.Repeat("b", 40)}}); err == nil || !strings.Contains(err.Error(), "name") {
		t.Fatalf("duplicate extension name error = %v", err)
	}
	empty, err := New(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(empty)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"files":[]`) || !strings.Contains(string(raw), `"extensions":[]`) {
		t.Fatalf("empty bundle is not complete: %s", raw)
	}
}
