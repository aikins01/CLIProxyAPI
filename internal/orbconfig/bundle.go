package orbconfig

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"regexp"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"gopkg.in/yaml.v3"
)

const (
	EndpointPath     = "/ampcode/local-broker/orb-config-bundle.json"
	SupportHeader    = "X-Cliproxy-Orb-Config"
	DigestHeader     = "X-Cliproxy-Orb-Config-Digest"
	Schema           = 1
	MaxBodyBytes     = 32 << 20
	MaxDecodedBytes  = 24 << 20
	MaxFiles         = 4096
	MaxFileBytes     = 4 << 20
	MaxPathBytes     = 256
	MaxPathDepth     = 8
	MaxExtensions    = 64
	MaxManifestBytes = 64 << 10
	maxExtensionPart = 100
	maxExtensionRef  = 200
)

var (
	digestPattern         = regexp.MustCompile(`^[0-9a-f]{64}$`)
	extensionPartPattern  = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)
	extensionRefPattern   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/+@-]*$`)
	secretContentPatterns = []*regexp.Regexp{
		regexp.MustCompile(`-----\s*BEGIN[ A-Z0-9_-]*PRIVATE KEY(?: BLOCK)?\s*-----`),
		regexp.MustCompile(`ghp_[0-9A-Za-z]{36}`),
		regexp.MustCompile(`github_pat_[0-9A-Za-z]{22}_[0-9A-Za-z]{59}`),
		regexp.MustCompile(`(?:A3T[A-Z0-9]|AKIA|ASIA)[A-Z0-9]{16}`),
		regexp.MustCompile(`(?:xox[baoprs]-|xapp-|xwfp-)[0-9A-Za-z-]{10,100}`),
	}
)

type Bundle struct {
	Schema     int         `json:"schema"`
	Digest     string      `json:"digest"`
	Files      []File      `json:"files"`
	Extensions []Extension `json:"extensions"`
}

type File struct {
	Path    string `json:"path"`
	SHA256  string `json:"sha256"`
	Content string `json:"content"`
}

type Extension struct {
	Repo    string `json:"repo"`
	Version string `json:"version"`
}

type DecodedFile struct {
	Path    string
	Content []byte
}

func New(files []DecodedFile, extensions []Extension) (Bundle, error) {
	if len(files) > MaxFiles {
		return Bundle{}, fmt.Errorf("files exceeds %d entries", MaxFiles)
	}
	if len(extensions) > MaxExtensions {
		return Bundle{}, fmt.Errorf("extensions exceeds %d entries", MaxExtensions)
	}
	totalBytes := 0
	for _, file := range files {
		if len(file.Content) > MaxFileBytes {
			return Bundle{}, fmt.Errorf("file %q exceeds %d bytes", file.Path, MaxFileBytes)
		}
		if len(file.Content) > MaxDecodedBytes-totalBytes {
			return Bundle{}, fmt.Errorf("decoded files exceed %d bytes", MaxDecodedBytes)
		}
		totalBytes += len(file.Content)
	}
	bundle := Bundle{
		Schema:     Schema,
		Files:      make([]File, 0, len(files)),
		Extensions: append([]Extension(nil), extensions...),
	}
	for _, file := range files {
		digest := sha256.Sum256(file.Content)
		bundle.Files = append(bundle.Files, File{
			Path:    file.Path,
			SHA256:  hex.EncodeToString(digest[:]),
			Content: base64.StdEncoding.EncodeToString(file.Content),
		})
	}
	normalize(&bundle)
	digest, err := computeDigest(bundle)
	if err != nil {
		return Bundle{}, err
	}
	bundle.Digest = digest
	normalized, _, err := validate(bundle, false)
	return normalized, err
}

func Validate(bundle Bundle) (Bundle, map[string][]byte, error) {
	return validate(bundle, true)
}

func validate(bundle Bundle, normalizeInput bool) (Bundle, map[string][]byte, error) {
	if bundle.Schema != Schema {
		return Bundle{}, nil, fmt.Errorf("schema must be %d", Schema)
	}
	if !digestPattern.MatchString(bundle.Digest) {
		return Bundle{}, nil, errors.New("digest is invalid")
	}
	if bundle.Files == nil {
		return Bundle{}, nil, errors.New("files is required")
	}
	if bundle.Extensions == nil {
		return Bundle{}, nil, errors.New("extensions is required")
	}
	if len(bundle.Files) > MaxFiles {
		return Bundle{}, nil, fmt.Errorf("files exceeds %d entries", MaxFiles)
	}
	if len(bundle.Extensions) > MaxExtensions {
		return Bundle{}, nil, fmt.Errorf("extensions exceeds %d entries", MaxExtensions)
	}

	if normalizeInput {
		normalize(&bundle)
	}
	decoded := make(map[string][]byte, len(bundle.Files))
	foldedPaths := make(map[string]bool, len(bundle.Files))
	skillFiles := map[string]map[string][]byte{}
	totalBytes := 0
	for _, file := range bundle.Files {
		parts, err := ValidatePath(file.Path)
		if err != nil {
			return Bundle{}, nil, err
		}
		folded := strings.ToLower(file.Path)
		if foldedPaths[folded] {
			return Bundle{}, nil, fmt.Errorf("path %q is duplicated", file.Path)
		}
		foldedPaths[folded] = true
		if !digestPattern.MatchString(file.SHA256) {
			return Bundle{}, nil, fmt.Errorf("file %q has an invalid sha256", file.Path)
		}
		content, err := base64.StdEncoding.Strict().DecodeString(file.Content)
		if err != nil {
			return Bundle{}, nil, fmt.Errorf("file %q content is not valid base64", file.Path)
		}
		if len(content) > MaxFileBytes {
			return Bundle{}, nil, fmt.Errorf("file %q exceeds %d bytes", file.Path, MaxFileBytes)
		}
		totalBytes += len(content)
		if totalBytes > MaxDecodedBytes {
			return Bundle{}, nil, fmt.Errorf("decoded files exceed %d bytes", MaxDecodedBytes)
		}
		contentDigest := sha256.Sum256(content)
		if hex.EncodeToString(contentDigest[:]) != file.SHA256 {
			return Bundle{}, nil, fmt.Errorf("file %q sha256 does not match content", file.Path)
		}
		if isMachOBinary(content) {
			return Bundle{}, nil, fmt.Errorf("file %q is a macOS binary", file.Path)
		}
		if ContainsSecret(content) {
			return Bundle{}, nil, fmt.Errorf("file %q contains credential material", file.Path)
		}
		if ContainsMacAbsolutePath(content) {
			return Bundle{}, nil, fmt.Errorf("file %q contains an absolute Mac path", file.Path)
		}
		decoded[file.Path] = content
		if parts[0] == "skills" {
			skillName := parts[1]
			if skillFiles[skillName] == nil {
				skillFiles[skillName] = map[string][]byte{}
			}
			skillFiles[skillName][strings.Join(parts[2:], "/")] = content
		}
	}
	for skillName, files := range skillFiles {
		skill, ok := files["SKILL.md"]
		if !ok {
			return Bundle{}, nil, fmt.Errorf("skill %q is missing SKILL.md", skillName)
		}
		if err := ValidateSkill(skill); err != nil {
			return Bundle{}, nil, fmt.Errorf("skill %q is invalid: %w", skillName, err)
		}
	}

	foldedRepos := make(map[string]bool, len(bundle.Extensions))
	foldedNames := make(map[string]bool, len(bundle.Extensions))
	for _, extension := range bundle.Extensions {
		if err := ValidateExtension(extension); err != nil {
			return Bundle{}, nil, err
		}
		folded := strings.ToLower(extension.Repo)
		if foldedRepos[folded] {
			return Bundle{}, nil, fmt.Errorf("extension repo %q is duplicated", extension.Repo)
		}
		foldedRepos[folded] = true
		name := strings.ToLower(path.Base(extension.Repo))
		if foldedNames[name] {
			return Bundle{}, nil, fmt.Errorf("extension name %q is duplicated", name)
		}
		foldedNames[name] = true
	}
	digest, err := computeDigest(bundle)
	if err != nil {
		return Bundle{}, nil, err
	}
	if digest != bundle.Digest {
		return Bundle{}, nil, errors.New("bundle digest does not match content")
	}
	return bundle, decoded, nil
}

func ValidatePath(value string) ([]string, error) {
	if value == "" || len(value) > MaxPathBytes || !utf8.ValidString(value) {
		return nil, fmt.Errorf("path %q is invalid", value)
	}
	if path.IsAbs(value) || strings.Contains(value, "\\") {
		return nil, fmt.Errorf("path %q must be relative", value)
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return nil, fmt.Errorf("path %q contains control characters", value)
		}
	}
	parts := strings.Split(value, "/")
	if len(parts) < 2 || len(parts) > MaxPathDepth {
		return nil, fmt.Errorf("path %q has invalid depth", value)
	}
	if parts[0] != "checks" && parts[0] != "skills" {
		return nil, fmt.Errorf("path %q is outside checks and skills", value)
	}
	if parts[0] == "skills" && len(parts) < 3 {
		return nil, fmt.Errorf("path %q is not inside a skill", value)
	}
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return nil, fmt.Errorf("path %q contains an invalid component", value)
		}
		if SecretLikeName(part) {
			return nil, fmt.Errorf("path %q is credential-shaped", value)
		}
		if strings.EqualFold(part, "mcp.json") {
			return nil, fmt.Errorf("path %q contains mcp.json", value)
		}
	}
	if parts[0] == "skills" && strings.EqualFold(parts[1], "using-open-browser-use") {
		return nil, fmt.Errorf("path %q contains a Mac-only skill", value)
	}
	return parts, nil
}

func SecretLikeName(name string) bool {
	lower := strings.ToLower(name)
	for _, suffix := range []string{".pem", ".key", ".token", ".keystore", ".p12", ".pfx"} {
		if strings.HasSuffix(lower, suffix) {
			return true
		}
	}
	base := strings.TrimLeft(lower, ".")
	switch base {
	case "id_rsa", "id_dsa", "id_ecdsa", "id_ed25519":
		return true
	}
	for _, segment := range strings.FieldsFunc(base, func(character rune) bool {
		return character == '.' || character == '_' || character == '-'
	}) {
		switch segment {
		case "auth", "token", "tokens", "secret", "secrets", "credential", "credentials", "password", "passwd", "apikey", "key", "env", "envrc":
			return true
		}
	}
	return false
}

func ContainsSecret(content []byte) bool {
	for _, pattern := range secretContentPatterns {
		if pattern.Match(content) {
			return true
		}
	}
	return false
}

func ContainsMacAbsolutePath(content []byte) bool {
	for _, prefix := range []string{
		"/Users/",
		"/Applications/",
		"/System/",
		"/Library/",
		"/private/var/",
		"/opt/homebrew/",
	} {
		if ContainsAbsolutePathPrefix(content, prefix) {
			return true
		}
	}
	return false
}

func ContainsAbsolutePathPrefix(content []byte, prefix string) bool {
	if prefix == "" || prefix[0] != '/' {
		return false
	}
	start := 0
	for start < len(content) {
		index := bytes.Index(content[start:], []byte(prefix))
		if index < 0 {
			return false
		}
		index += start
		if index == 0 || absolutePathBoundary(content, index) {
			return true
		}
		start = index + len(prefix)
	}
	return false
}

func absolutePathBoundary(content []byte, index int) bool {
	previous := content[index-1]
	if previous >= 0x80 || previous >= 'a' && previous <= 'z' || previous >= 'A' && previous <= 'Z' || previous >= '0' && previous <= '9' {
		return false
	}
	switch previous {
	case '.', '_', '~', '%', '-':
		return false
	case ':':
		if index >= 2 {
			drive := content[index-2]
			if (drive >= 'a' && drive <= 'z' || drive >= 'A' && drive <= 'Z') && (index == 2 || absolutePathBoundary(content, index-2)) {
				return false
			}
		}
	}
	return true
}

func ValidateSkill(content []byte) error {
	if !utf8.Valid(content) {
		return errors.New("SKILL.md is not UTF-8")
	}
	lines := strings.Split(strings.ReplaceAll(string(content), "\r\n", "\n"), "\n")
	if len(lines) == 0 || lines[0] != "---" {
		return nil
	}
	end := -1
	for index, line := range lines[1:] {
		if line == "---" {
			end = index + 1
			break
		}
	}
	if end < 0 {
		return errors.New("frontmatter is unterminated")
	}
	frontmatter := map[string]any{}
	if err := yaml.Unmarshal([]byte(strings.Join(lines[1:end], "\n")), &frontmatter); err != nil {
		return errors.New("frontmatter is malformed")
	}
	for key := range frontmatter {
		if strings.EqualFold(key, "mcpServers") {
			return errors.New("MCP-backed skills are not portable")
		}
	}
	return nil
}

func ValidateExtension(extension Extension) error {
	parts := strings.Split(extension.Repo, "/")
	if len(parts) != 2 || !validExtensionPart(parts[0]) || !validExtensionPart(parts[1]) || !strings.HasPrefix(parts[1], "gh-") {
		return fmt.Errorf("extension repo %q is invalid", extension.Repo)
	}
	if !validExtensionRef(extension.Version) {
		return fmt.Errorf("extension version for %q is invalid", extension.Repo)
	}
	return nil
}

func validExtensionRef(value string) bool {
	if value == "" || len(value) > maxExtensionRef || strings.HasPrefix(value, "-") || strings.HasSuffix(value, "/") || strings.HasSuffix(value, ".") || strings.Contains(value, "\\") || strings.Contains(value, "//") || strings.Contains(value, "..") || strings.Contains(value, "@{") || !extensionRefPattern.MatchString(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsSpace(character) || unicode.IsControl(character) {
			return false
		}
	}
	for _, component := range strings.Split(value, "/") {
		if component == "" || component == "." || component == ".." || strings.HasPrefix(component, ".") || strings.HasSuffix(strings.ToLower(component), ".lock") {
			return false
		}
	}
	return true
}

func validExtensionPart(value string) bool {
	return len(value) <= maxExtensionPart && extensionPartPattern.MatchString(value) && value != "." && value != ".." && !strings.HasSuffix(value, ".")
}

func normalize(bundle *Bundle) {
	if bundle.Files == nil {
		bundle.Files = []File{}
	}
	if bundle.Extensions == nil {
		bundle.Extensions = []Extension{}
	}
	sort.Slice(bundle.Files, func(i, j int) bool {
		return bundle.Files[i].Path < bundle.Files[j].Path
	})
	sort.Slice(bundle.Extensions, func(i, j int) bool {
		left := strings.ToLower(bundle.Extensions[i].Repo)
		right := strings.ToLower(bundle.Extensions[j].Repo)
		if left == right {
			return bundle.Extensions[i].Version < bundle.Extensions[j].Version
		}
		return left < right
	})
}

func computeDigest(bundle Bundle) (string, error) {
	type canonicalFile struct {
		Path   string `json:"path"`
		SHA256 string `json:"sha256"`
	}
	type canonicalBundle struct {
		Schema     int             `json:"schema"`
		Files      []canonicalFile `json:"files"`
		Extensions []Extension     `json:"extensions"`
	}
	files := make([]canonicalFile, 0, len(bundle.Files))
	for _, file := range bundle.Files {
		files = append(files, canonicalFile{Path: file.Path, SHA256: file.SHA256})
	}
	canonical, err := json.Marshal(canonicalBundle{Schema: bundle.Schema, Files: files, Extensions: bundle.Extensions})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(canonical)
	return hex.EncodeToString(digest[:]), nil
}

func isMachOBinary(content []byte) bool {
	if len(content) < 4 {
		return false
	}
	magic := string(content[:4])
	switch magic {
	case "\xfe\xed\xfa\xce", "\xce\xfa\xed\xfe", "\xfe\xed\xfa\xcf", "\xcf\xfa\xed\xfe", "\xca\xfe\xba\xbe", "\xbe\xba\xfe\xca", "\xca\xfe\xba\xbf", "\xbf\xba\xfe\xca":
		return true
	default:
		return false
	}
}
