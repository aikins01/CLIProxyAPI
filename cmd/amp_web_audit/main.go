package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	pathpkg "path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	webBaselineSchema        = 2
	minimumWebBaselineSchema = 1
	maxDocumentBytes         = 5 << 20
	maxAssetBytes            = 8 << 20
	maxBundleBytes           = 64 << 20
	maxAssetCount            = 800
)

var (
	defaultWebBaselinePath    = filepath.Join("dev", "amp-web-parity-baseline.json")
	appEntryPattern           = regexp.MustCompile(`(?:https?://[^"']+)?(/_app/immutable/entry/app\.[A-Za-z0-9_-]+\.js)`)
	nodePathPattern           = regexp.MustCompile(`\.\./nodes/([0-9]+)\.[A-Za-z0-9_-]+\.js`)
	dependencyMapPattern      = regexp.MustCompile(`(?s)m\.f\|\|\(m\.f=\[(.*?)\]\)\)\)=>`)
	nestedJSImportPathPattern = regexp.MustCompile(`^(\./[A-Za-z0-9_.-]+\.js|(?:\.\./)+(?:chunks|nodes)/[A-Za-z0-9_.-]+\.js)$`)
	nestedDependencyPattern   = regexp.MustCompile(`__vite__mapDeps\(\[([0-9,\s]*)\]\)`)
)

type webBaseline struct {
	Schema    int           `json:"schema"`
	SourceURL string        `json:"source_url"`
	Routes    []string      `json:"routes"`
	Contracts []webContract `json:"contracts"`
}

type webContract struct {
	Name             string     `json:"name"`
	Scope            string     `json:"scope"`
	RequiredAll      []string   `json:"required_all,omitempty"`
	RequiredAny      [][]string `json:"required_any,omitempty"`
	RequiredTogether [][]string `json:"required_together,omitempty"`
}

type webAsset struct {
	Path string
	Body []byte
}

type webFinding struct {
	Kind     string   `json:"kind"`
	Contract string   `json:"contract,omitempty"`
	Scope    string   `json:"scope,omitempty"`
	Missing  []string `json:"missing"`
}

type webReport struct {
	Schema     int          `json:"schema"`
	Source     string       `json:"source"`
	AssetCount int          `json:"asset_count"`
	Bytes      int          `json:"bytes"`
	Findings   []webFinding `json:"findings"`
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("amp_web_audit", flag.ContinueOnError)
	flags.SetOutput(stderr)
	baselinePath := flags.String("baseline", defaultWebBaselinePath, "path to the Amp web parity baseline JSON")
	sourceURL := flags.String("url", "", "Amp web URL to audit instead of the baseline source URL")
	assetDir := flags.String("asset-dir", "", "scan local JavaScript assets instead of fetching Amp web")
	strict := flags.Bool("strict", false, "exit non-zero when a required route or contract marker is missing")
	jsonOutput := flags.Bool("json", false, "print the audit report as JSON")
	if err := flags.Parse(args); err != nil {
		return 2
	}

	baseline, err := readWebBaseline(*baselinePath)
	if err != nil {
		fmt.Fprintf(stderr, "read web baseline: %v\n", err)
		return 1
	}
	if err := validateWebBaseline(baseline); err != nil {
		fmt.Fprintf(stderr, "validate web baseline: %v\n", err)
		return 1
	}

	source := strings.TrimSpace(*sourceURL)
	if source == "" {
		source = baseline.SourceURL
	}
	var assets []webAsset
	if strings.TrimSpace(*assetDir) != "" {
		assets, err = readWebAssetDir(strings.TrimSpace(*assetDir))
		source = strings.TrimSpace(*assetDir)
	} else {
		assets, err = fetchWebAssets(http.DefaultClient, source, baseline.Routes)
	}
	if err != nil {
		fmt.Fprintf(stderr, "scan Amp web assets: %v\n", err)
		return 1
	}

	report := auditWebAssets(source, assets, baseline)
	if *jsonOutput {
		encoder := json.NewEncoder(stdout)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(report); err != nil {
			fmt.Fprintf(stderr, "write report: %v\n", err)
			return 1
		}
	} else {
		printWebReport(stdout, report)
	}
	if *strict && len(report.Findings) > 0 {
		return 2
	}
	return 0
}

func readWebBaseline(path string) (webBaseline, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return webBaseline{}, err
	}
	var baseline webBaseline
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&baseline); err != nil {
		return webBaseline{}, err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return webBaseline{}, errors.New("baseline contains trailing JSON")
	}
	return baseline, nil
}

func validateWebBaseline(baseline webBaseline) error {
	if baseline.Schema < minimumWebBaselineSchema || baseline.Schema > webBaselineSchema {
		return fmt.Errorf("schema %d is unsupported; want %d through %d", baseline.Schema, minimumWebBaselineSchema, webBaselineSchema)
	}
	parsed, err := url.Parse(baseline.SourceURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return fmt.Errorf("source_url %q is not an absolute URL", baseline.SourceURL)
	}
	if len(baseline.Routes) == 0 {
		return errors.New("routes must not be empty")
	}
	routes := map[string]struct{}{}
	for _, route := range baseline.Routes {
		if !strings.HasPrefix(route, "/") {
			return fmt.Errorf("route %q must start with /", route)
		}
		if _, exists := routes[route]; exists {
			return fmt.Errorf("duplicate route %q", route)
		}
		routes[route] = struct{}{}
	}
	if len(baseline.Contracts) == 0 {
		return errors.New("contracts must not be empty")
	}
	names := map[string]struct{}{}
	for _, contract := range baseline.Contracts {
		if strings.TrimSpace(contract.Name) == "" || strings.TrimSpace(contract.Scope) == "" {
			return errors.New("contract name and scope must not be empty")
		}
		if _, exists := names[contract.Name]; exists {
			return fmt.Errorf("duplicate contract %q", contract.Name)
		}
		names[contract.Name] = struct{}{}
		if len(contract.RequiredAll) == 0 && len(contract.RequiredAny) == 0 && len(contract.RequiredTogether) == 0 {
			return fmt.Errorf("contract %q has no required markers", contract.Name)
		}
		for _, marker := range contract.RequiredAll {
			if marker == "" {
				return fmt.Errorf("contract %q contains an empty required_all marker", contract.Name)
			}
		}
		for _, group := range contract.RequiredAny {
			if len(group) == 0 {
				return fmt.Errorf("contract %q contains an empty required_any group", contract.Name)
			}
			for _, marker := range group {
				if marker == "" {
					return fmt.Errorf("contract %q contains an empty required_any marker", contract.Name)
				}
			}
		}
		for _, group := range contract.RequiredTogether {
			if baseline.Schema < 2 {
				return fmt.Errorf("contract %q requires schema 2 for required_together", contract.Name)
			}
			if len(group) == 0 {
				return fmt.Errorf("contract %q contains an empty required_together group", contract.Name)
			}
			for _, marker := range group {
				if marker == "" {
					return fmt.Errorf("contract %q contains an empty required_together marker", contract.Name)
				}
			}
		}
	}
	return nil
}

func fetchWebAssets(client *http.Client, source string, routes []string) ([]webAsset, error) {
	base, err := url.Parse(source)
	if err != nil {
		return nil, err
	}
	document, err := fetchLimited(client, base, maxDocumentBytes)
	if err != nil {
		return nil, err
	}
	entryMatch := appEntryPattern.FindSubmatch(document)
	if len(entryMatch) != 2 {
		return nil, errors.New("Amp web page does not reference a Svelte app entry")
	}
	entryURL, err := base.Parse(string(entryMatch[1]))
	if err != nil {
		return nil, err
	}
	entry, err := fetchLimited(client, entryURL, maxAssetBytes)
	if err != nil {
		return nil, err
	}

	dependencyPaths, err := parseDependencyMap(entry)
	if err != nil {
		return nil, err
	}
	paths, err := selectedRouteAssetPaths(entry, routes, dependencyPaths)
	if err != nil {
		return nil, err
	}

	entryPath := entryURL.Path
	delete(paths, entryPath)
	seenPaths := map[string]struct{}{entryPath: {}}
	for path := range paths {
		seenPaths[path] = struct{}{}
	}
	if len(seenPaths) > maxAssetCount {
		return nil, fmt.Errorf("selected route asset count %d exceeds limit %d", len(seenPaths), maxAssetCount)
	}

	assets := make([]webAsset, 0, len(paths)+1)
	assets = append(assets, webAsset{Path: entryURL.Path, Body: entry})
	remainingBundleBytes, err := remainingWebBundleBytes(len(entry), maxBundleBytes)
	if err != nil {
		return nil, err
	}
	for len(paths) > 0 {
		fetched, err := fetchAssetPathsLimited(client, base, paths, remainingBundleBytes)
		if err != nil {
			return nil, err
		}
		nextPaths := map[string]struct{}{}
		for _, asset := range fetched {
			remainingBundleBytes -= len(asset.Body)
			nestedPaths, err := nestedWebAssetPaths(asset, dependencyPaths)
			if err != nil {
				return nil, err
			}
			for path := range nestedPaths {
				if _, exists := seenPaths[path]; exists {
					continue
				}
				seenPaths[path] = struct{}{}
				if len(seenPaths) > maxAssetCount {
					return nil, fmt.Errorf("selected route asset count %d exceeds limit %d", len(seenPaths), maxAssetCount)
				}
				nextPaths[path] = struct{}{}
			}
		}
		assets = append(assets, fetched...)
		paths = nextPaths
	}
	sort.Slice(assets, func(i, j int) bool { return assets[i].Path < assets[j].Path })
	return assets, nil
}

func selectedRouteAssetPaths(entry []byte, routes, dependencyPaths []string) (map[string]struct{}, error) {
	nodePaths := map[int]string{}
	for _, match := range nodePathPattern.FindAllSubmatch(entry, -1) {
		index, err := strconv.Atoi(string(match[1]))
		if err == nil {
			nodePaths[index] = "/_app/immutable/nodes/" + strings.TrimPrefix(string(match[0]), "../nodes/")
		}
	}
	paths := map[string]struct{}{}
	for _, route := range routes {
		pattern := regexp.MustCompile(regexp.QuoteMeta(strconv.Quote(route)) + `\s*:\s*\[\s*(-?[0-9]+)(?:\s*,\s*\[([^\]]*)\])?(?:\s*,\s*\[([^\]]*)\])?`)
		match := pattern.FindSubmatch(entry)
		if len(match) < 2 {
			return nil, fmt.Errorf("required Amp web route %q is missing", route)
		}
		routeNodeIndexes := []string{string(match[1])}
		for _, rawIndexes := range match[2:] {
			for _, rawIndex := range regexp.MustCompile(`-?[0-9]+`).FindAllString(string(rawIndexes), -1) {
				routeNodeIndexes = append(routeNodeIndexes, rawIndex)
			}
		}
		for _, rawIndex := range routeNodeIndexes {
			routeNodeIndex, err := strconv.Atoi(rawIndex)
			if err != nil {
				return nil, err
			}
			nodeIndex := routeNodeIndex
			if routeNodeIndex < 0 {
				nodeIndex = ^routeNodeIndex
			}
			nodePath := nodePaths[nodeIndex]
			if nodePath == "" {
				if routeNodeIndex >= 0 {
					return nil, fmt.Errorf("route %q references missing node %d", route, nodeIndex)
				}
				continue
			}
			paths[nodePath] = struct{}{}
			for _, dependencyIndex := range routeDependencyIndexes(entry, nodeIndex) {
				if dependencyIndex < 0 || dependencyIndex >= len(dependencyPaths) {
					return nil, fmt.Errorf("route %q references dependency index %d outside map", route, dependencyIndex)
				}
				path := dependencyPaths[dependencyIndex]
				if strings.HasSuffix(path, ".js") {
					paths[path] = struct{}{}
				}
			}
		}
	}
	return paths, nil
}

func nestedWebAssetPaths(asset webAsset, dependencyPaths []string) (map[string]struct{}, error) {
	assetURL, err := url.Parse(asset.Path)
	if err != nil {
		return nil, err
	}
	paths := map[string]struct{}{}
	code := javascriptCodeOnly(asset.Body)
	for _, specifier := range nestedJSImportSpecifiers(asset.Body, code) {
		nestedURL, err := assetURL.Parse(specifier)
		if err != nil {
			return nil, err
		}
		path, err := canonicalWebAssetPath(nestedURL.String())
		if err != nil {
			return nil, fmt.Errorf("resolve nested JavaScript import %q from %q: %w", specifier, asset.Path, err)
		}
		paths[path] = struct{}{}
	}
	for _, match := range nestedDependencyPattern.FindAllSubmatch(code, -1) {
		for _, rawIndex := range strings.Split(string(match[1]), ",") {
			rawIndex = strings.TrimSpace(rawIndex)
			if rawIndex == "" {
				continue
			}
			dependencyIndex, err := strconv.Atoi(rawIndex)
			if err != nil {
				return nil, err
			}
			if dependencyIndex < 0 || dependencyIndex >= len(dependencyPaths) {
				return nil, fmt.Errorf("asset %q references dependency index %d outside map", asset.Path, dependencyIndex)
			}
			path := dependencyPaths[dependencyIndex]
			if strings.HasSuffix(path, ".js") {
				paths[path] = struct{}{}
			}
		}
	}
	return paths, nil
}

func nestedJSImportSpecifiers(body, code []byte) []string {
	var specifiers []string
	for index := 0; index < len(code); {
		if !isJavaScriptIdentifierByte(code[index]) {
			index++
			continue
		}
		start := index
		for index < len(code) && isJavaScriptIdentifierByte(code[index]) {
			index++
		}
		keyword := string(code[start:index])
		if keyword != "import" && keyword != "from" || start > 0 && code[start-1] == '.' {
			continue
		}
		next := skipJavaScriptTrivia(body, index)
		dynamicImport := false
		if keyword == "import" && next < len(body) && body[next] == '(' {
			dynamicImport = true
			next = skipJavaScriptTrivia(body, next+1)
		}
		specifier, end, ok := javascriptStringValueAt(body, next)
		if !ok || !nestedJSImportPathPattern.MatchString(specifier) {
			continue
		}
		if dynamicImport {
			next = skipJavaScriptTrivia(body, end)
			if next >= len(body) || body[next] != ')' && body[next] != ',' {
				continue
			}
		}
		specifiers = append(specifiers, specifier)
		index = end
	}
	return specifiers
}

func javascriptCodeOnly(body []byte) []byte {
	code := append([]byte(nil), body...)
	for index := 0; index < len(body); {
		if body[index] == '`' {
			index = maskJavaScriptTemplate(body, code, index)
			continue
		}
		end, ok := skipJavaScriptNonCode(body, index)
		if !ok {
			end, ok = skipJavaScriptRegex(body, code, index)
			if !ok {
				index++
				continue
			}
		}
		maskJavaScriptBytes(code, index, end)
		index = end
	}
	return code
}

func maskJavaScriptTemplate(body, code []byte, index int) int {
	maskJavaScriptBytes(code, index, index+1)
	index++
	for index < len(body) {
		switch {
		case body[index] == '\\':
			end := min(index+2, len(body))
			maskJavaScriptBytes(code, index, end)
			index = end
		case body[index] == '`':
			maskJavaScriptBytes(code, index, index+1)
			return index + 1
		case body[index] == '$' && index+1 < len(body) && body[index+1] == '{':
			maskJavaScriptBytes(code, index, index+2)
			index = maskJavaScriptTemplateExpression(body, code, index+2)
		default:
			maskJavaScriptBytes(code, index, index+1)
			index++
		}
	}
	return index
}

func maskJavaScriptTemplateExpression(body, code []byte, index int) int {
	depth := 1
	for index < len(body) {
		if body[index] == '`' {
			index = maskJavaScriptTemplate(body, code, index)
			continue
		}
		if end, ok := skipJavaScriptNonCode(body, index); ok {
			maskJavaScriptBytes(code, index, end)
			index = end
			continue
		}
		if end, ok := skipJavaScriptRegex(body, code, index); ok {
			maskJavaScriptBytes(code, index, end)
			index = end
			continue
		}
		switch body[index] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				maskJavaScriptBytes(code, index, index+1)
				return index + 1
			}
		}
		index++
	}
	return index
}

func maskJavaScriptBytes(code []byte, start, end int) {
	for index := start; index < end; index++ {
		if code[index] != '\n' && code[index] != '\r' {
			code[index] = ' '
		}
	}
}

func skipJavaScriptNonCode(body []byte, index int) (int, bool) {
	if index >= len(body) {
		return index, false
	}
	if body[index] == '\'' || body[index] == '"' {
		return skipJavaScriptQuoted(body, index), true
	}
	if body[index] != '/' || index+1 >= len(body) {
		return index, false
	}
	if body[index+1] == '/' {
		index += 2
		for index < len(body) && body[index] != '\n' && body[index] != '\r' {
			index++
		}
		return index, true
	}
	if body[index+1] == '*' {
		index += 2
		for index+1 < len(body) && (body[index] != '*' || body[index+1] != '/') {
			index++
		}
		if index+1 < len(body) {
			index += 2
		}
		return index, true
	}
	return index, false
}

func skipJavaScriptRegex(body, code []byte, index int) (int, bool) {
	if index >= len(body) || body[index] != '/' || !javascriptRegexCanStart(code, index) {
		return index, false
	}
	inClass := false
	for end := index + 1; end < len(body); end++ {
		switch body[end] {
		case '\\':
			end++
		case '[':
			inClass = true
		case ']':
			inClass = false
		case '/', '\n', '\r':
			if body[end] != '/' || inClass {
				if body[end] == '\n' || body[end] == '\r' {
					return index, false
				}
				continue
			}
			end++
			for end < len(body) && isJavaScriptIdentifierByte(body[end]) {
				end++
			}
			return end, true
		}
	}
	return index, false
}

func javascriptRegexCanStart(code []byte, index int) bool {
	for index > 0 {
		index--
		switch code[index] {
		case ' ', '\t', '\n', '\r':
			continue
		case '+', '-':
			return index == 0 || code[index-1] != code[index]
		case '(', '[', '{', '=', ':', ',', ';', '!', '?', '&', '|', '*', '/', '%', '^', '~', '<', '>':
			return true
		default:
			end := index + 1
			for index > 0 && isJavaScriptIdentifierByte(code[index-1]) {
				index--
			}
			switch string(code[index:end]) {
			case "await", "case", "delete", "do", "else", "in", "instanceof", "of", "return", "throw", "typeof", "void", "yield":
				return true
			default:
				return false
			}
		}
	}
	return true
}

func skipJavaScriptQuoted(body []byte, index int) int {
	quote := body[index]
	index++
	for index < len(body) {
		if body[index] == '\\' {
			index += 2
			continue
		}
		index++
		if body[index-1] == quote {
			break
		}
	}
	return min(index, len(body))
}

func skipJavaScriptWhitespace(body []byte, index int) int {
	for index < len(body) && (body[index] == ' ' || body[index] == '\t' || body[index] == '\n' || body[index] == '\r') {
		index++
	}
	return index
}

func skipJavaScriptTrivia(body []byte, index int) int {
	for {
		index = skipJavaScriptWhitespace(body, index)
		if index+1 >= len(body) || body[index] != '/' || body[index+1] != '/' && body[index+1] != '*' {
			return index
		}
		index, _ = skipJavaScriptNonCode(body, index)
	}
}

func javascriptStringValueAt(body []byte, index int) (string, int, bool) {
	if index >= len(body) || body[index] != '\'' && body[index] != '"' {
		return "", index, false
	}
	end := skipJavaScriptQuoted(body, index)
	if end <= index+1 || end > len(body) || body[end-1] != body[index] {
		return "", end, false
	}
	value := body[index+1 : end-1]
	if bytes.IndexByte(value, '\\') >= 0 {
		return "", end, false
	}
	return string(value), end, true
}

func isJavaScriptIdentifierByte(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9' || value == '_' || value == '$'
}

func parseDependencyMap(entry []byte) ([]string, error) {
	match := dependencyMapPattern.FindSubmatch(entry)
	if len(match) != 2 {
		return nil, errors.New("Amp web app entry has no Vite dependency map")
	}
	var paths []string
	if err := json.Unmarshal(append(append([]byte{'['}, match[1]...), ']'), &paths); err != nil {
		return nil, fmt.Errorf("decode Vite dependency map: %w", err)
	}
	for index, dependencyPath := range paths {
		path, err := canonicalWebAssetPath(dependencyPath)
		if err != nil {
			return nil, fmt.Errorf("Vite dependency map path %q: %w", dependencyPath, err)
		}
		paths[index] = path
	}
	return paths, nil
}

func canonicalWebAssetPath(value string) (string, error) {
	parsed, err := url.Parse(value)
	if err != nil {
		return "", err
	}
	if value != parsed.Path || !strings.HasPrefix(parsed.Path, "/_app/immutable/") || pathpkg.Clean(parsed.Path) != parsed.Path {
		return "", errors.New("path is outside canonical /_app/immutable assets")
	}
	return parsed.Path, nil
}

func routeDependencyIndexes(entry []byte, nodeIndex int) []int {
	pattern := regexp.MustCompile(`import\("\.\./nodes/` + strconv.Itoa(nodeIndex) + `\.[^"?]+\.js"\),\s*__vite__mapDeps\(\[([0-9,\s]*)\]\)`)
	match := pattern.FindSubmatch(entry)
	if len(match) != 2 {
		return nil
	}
	parts := strings.Split(string(match[1]), ",")
	indexes := make([]int, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		index, err := strconv.Atoi(part)
		if err == nil {
			indexes = append(indexes, index)
		}
	}
	return indexes
}

func remainingWebBundleBytes(entrySize, bundleLimit int) (int, error) {
	if entrySize < 0 || bundleLimit < 0 || entrySize > bundleLimit {
		return 0, fmt.Errorf("selected route assets exceed %d bytes", bundleLimit)
	}
	return bundleLimit - entrySize, nil
}

func fetchAssetPathsLimited(client *http.Client, base *url.URL, paths map[string]struct{}, bundleLimit int) ([]webAsset, error) {
	type result struct {
		asset webAsset
		err   error
	}
	jobs := make(chan string)
	results := make(chan result)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	workerCount := 12
	if len(paths) < workerCount {
		workerCount = len(paths)
	}
	var workers sync.WaitGroup
	for range workerCount {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				var path string
				var ok bool
				select {
				case <-ctx.Done():
					return
				case path, ok = <-jobs:
					if !ok {
						return
					}
				}
				assetURL, err := base.Parse(path)
				if err != nil {
					select {
					case results <- result{err: err}:
					case <-ctx.Done():
					}
					continue
				}
				body, err := fetchLimitedContext(ctx, client, assetURL, maxAssetBytes)
				select {
				case results <- result{asset: webAsset{Path: path, Body: body}, err: err}:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	go func() {
		for path := range paths {
			select {
			case jobs <- path:
			case <-ctx.Done():
				close(jobs)
				workers.Wait()
				close(results)
				return
			}
		}
		close(jobs)
		workers.Wait()
		close(results)
	}()

	assets := make([]webAsset, 0, len(paths))
	total := 0
	var firstErr error
	for result := range results {
		if result.err != nil {
			if firstErr == nil {
				firstErr = result.err
				cancel()
			}
			continue
		}
		if firstErr != nil {
			continue
		}
		if len(result.asset.Body) > bundleLimit-total {
			firstErr = fmt.Errorf("selected route assets exceed %d bytes", bundleLimit)
			cancel()
			continue
		}
		total += len(result.asset.Body)
		assets = append(assets, result.asset)
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return assets, nil
}

func fetchLimited(client *http.Client, target *url.URL, limit int64) ([]byte, error) {
	return fetchLimitedContext(context.Background(), client, target, limit)
}

func fetchLimitedContext(ctx context.Context, client *http.Client, target *url.URL, limit int64) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Accept", "text/html,application/javascript;q=0.9,*/*;q=0.8")
	request.Header.Set("User-Agent", "CLIProxyAPI Amp web parity audit")
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("GET %s returned %s", target, response.Status)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("GET %s exceeded %d bytes", target, limit)
	}
	return body, nil
}

func readWebAssetDir(dir string) ([]webAsset, error) {
	assets := []webAsset{}
	total := 0
	err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(strings.ToLower(entry.Name()), ".js") {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if len(body) > maxAssetBytes {
			return fmt.Errorf("asset %s exceeds %d bytes", path, maxAssetBytes)
		}
		total += len(body)
		if total > maxBundleBytes {
			return fmt.Errorf("asset directory exceeds %d bytes", maxBundleBytes)
		}
		relative, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		assets = append(assets, webAsset{Path: filepath.ToSlash(relative), Body: body})
		if len(assets) > maxAssetCount {
			return fmt.Errorf("asset directory exceeds %d JavaScript files", maxAssetCount)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(assets) == 0 {
		return nil, errors.New("asset directory contains no JavaScript files")
	}
	sort.Slice(assets, func(i, j int) bool { return assets[i].Path < assets[j].Path })
	return assets, nil
}

func auditWebAssets(source string, assets []webAsset, baseline webBaseline) webReport {
	report := webReport{Schema: webBaselineSchema, Source: source, AssetCount: len(assets), Findings: []webFinding{}}
	for _, asset := range assets {
		report.Bytes += len(asset.Body)
	}
	for _, route := range baseline.Routes {
		if !assetsContain(assets, strconv.Quote(route)) {
			report.Findings = append(report.Findings, webFinding{Kind: "route", Missing: []string{route}})
		}
	}
	for _, contract := range baseline.Contracts {
		missing := []string{}
		for _, marker := range contract.RequiredAll {
			if !assetsContain(assets, marker) {
				missing = append(missing, marker)
			}
		}
		for _, group := range contract.RequiredAny {
			found := false
			for _, marker := range group {
				if assetsContain(assets, marker) {
					found = true
					break
				}
			}
			if !found {
				missing = append(missing, strings.Join(group, " | "))
			}
		}
		for _, group := range contract.RequiredTogether {
			if !assetsContainTogether(assets, group) {
				missing = append(missing, strings.Join(group, " & "))
			}
		}
		if len(missing) > 0 {
			report.Findings = append(report.Findings, webFinding{Kind: "contract", Contract: contract.Name, Scope: contract.Scope, Missing: missing})
		}
	}
	return report
}

func assetsContainTogether(assets []webAsset, markers []string) bool {
	for _, asset := range assets {
		found := true
		for _, marker := range markers {
			if !bytes.Contains(asset.Body, []byte(marker)) {
				found = false
				break
			}
		}
		if found {
			return true
		}
	}
	return false
}

func assetsContain(assets []webAsset, marker string) bool {
	needle := []byte(marker)
	for _, asset := range assets {
		if bytes.Contains(asset.Body, needle) {
			return true
		}
	}
	return false
}

func printWebReport(output io.Writer, report webReport) {
	if len(report.Findings) == 0 {
		fmt.Fprintf(output, "Amp upstream web parity passed: %d assets, %d bytes\n", report.AssetCount, report.Bytes)
		return
	}
	fmt.Fprintf(output, "Amp upstream web parity found %d issue(s) across %d assets:\n", len(report.Findings), report.AssetCount)
	for _, finding := range report.Findings {
		if finding.Kind == "route" {
			fmt.Fprintf(output, "- missing route: %s\n", strings.Join(finding.Missing, ", "))
			continue
		}
		fmt.Fprintf(output, "- %s (%s): missing %s\n", finding.Contract, finding.Scope, strings.Join(finding.Missing, ", "))
	}
}
