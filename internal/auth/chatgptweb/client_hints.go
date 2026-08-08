package chatgptweb

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// ClientHints are the low-entropy Chromium client-hint headers a real
// browser sends on every request alongside its User-Agent. A UA without
// matching client hints is an automation tell, so they are derived from the
// captured browser UA to keep the whole header set consistent.
type ClientHints struct {
	Brands   string
	Mobile   string
	Platform string
}

// ClientHintsForUA derives Chromium client hints from a browser User-Agent,
// falling back to the default Chromium UA like userAgentOrDefault.
func ClientHintsForUA(userAgent string) ClientHints {
	ua := userAgentOrDefault(userAgent)
	brand := "Google Chrome"
	engineMajor, ok := browserProductMajor(ua, "Chrome/")
	if !ok {
		engineMajor = "0"
	}
	brandMajor := engineMajor
	edgeMajor, edge := browserProductMajor(ua, "Edg/")
	if !edge {
		edgeMajor, edge = browserProductMajor(ua, "EdgA/")
	}
	if edge {
		brand = "Microsoft Edge"
		brandMajor = edgeMajor
	} else if operaMajor, opera := browserProductMajor(ua, "OPR/"); opera {
		brand = "Opera"
		brandMajor = operaMajor
	}
	hints := ClientHints{
		Brands: chromiumBrandList(engineMajor, brand, brandMajor),
		Mobile: "?0",
	}
	switch {
	case strings.Contains(ua, "Windows"):
		hints.Platform = `"Windows"`
	case strings.Contains(ua, "Android"):
		hints.Platform = `"Android"`
		if strings.Contains(" "+ua+" ", " Mobile ") {
			hints.Mobile = "?1"
		}
	case strings.Contains(ua, "Macintosh") || strings.Contains(ua, "Mac OS X"):
		hints.Platform = `"macOS"`
	case strings.Contains(ua, "CrOS"):
		hints.Platform = `"Chrome OS"`
	default:
		hints.Platform = `"Linux"`
	}
	return hints
}

func chromiumBrandList(engineMajor, productBrand, productMajor string) string {
	major, err := strconv.Atoi(engineMajor)
	if err != nil || major < 0 {
		major = 0
	}
	greaseChars := [...]string{" ", "(", ":", "-", ".", "/", ")", ";", "=", "?", "_"}
	greaseVersions := [...]string{"8", "99", "24"}
	brands := [...]string{
		fmt.Sprintf(`"Not%sA%sBrand";v="%s"`, greaseChars[major%len(greaseChars)], greaseChars[(major+1)%len(greaseChars)], greaseVersions[major%len(greaseVersions)]),
		fmt.Sprintf(`"Chromium";v="%s"`, engineMajor),
		fmt.Sprintf(`"%s";v="%s"`, productBrand, productMajor),
	}
	orders := [...][3]int{
		{0, 1, 2},
		{0, 2, 1},
		{1, 0, 2},
		{1, 2, 0},
		{2, 0, 1},
		{2, 1, 0},
	}
	ordered := make([]string, len(brands))
	for source, destination := range orders[major%len(orders)] {
		ordered[destination] = brands[source]
	}
	return strings.Join(ordered, ", ")
}

func browserProductMajor(userAgent, prefix string) (string, bool) {
	for _, product := range strings.Fields(userAgent) {
		version, ok := strings.CutPrefix(product, prefix)
		if !ok {
			continue
		}
		major, _, _ := strings.Cut(version, ".")
		if major == "" {
			return "", false
		}
		for _, ch := range major {
			if ch < '0' || ch > '9' {
				return "", false
			}
		}
		return major, true
	}
	return "", false
}

func (h ClientHints) apply(req *http.Request) {
	req.Header.Set("Sec-CH-UA", h.Brands)
	req.Header.Set("Sec-CH-UA-Mobile", h.Mobile)
	req.Header.Set("Sec-CH-UA-Platform", h.Platform)
}

// ApplyBrowserHeaders sets the client-hint and fetch-metadata headers a real
// browser sends with the web app's same-origin XHR calls to the backend.
func ApplyBrowserHeaders(req *http.Request, userAgent string) {
	ClientHintsForUA(userAgent).apply(req)
	req.Header.Set("Priority", "u=1, i")
	req.Header.Set("Sec-Fetch-Dest", "empty")
	req.Header.Set("Sec-Fetch-Mode", "cors")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
}

// applyNavigationHeaders sets the client-hint and fetch-metadata headers of
// a browser page navigation rather than an app XHR call.
func applyNavigationHeaders(req *http.Request, userAgent string) {
	ClientHintsForUA(userAgent).apply(req)
	req.Header.Set("Sec-Fetch-Dest", "document")
	req.Header.Set("Sec-Fetch-Mode", "navigate")
	req.Header.Set("Sec-Fetch-Site", "none")
	req.Header.Set("Sec-Fetch-User", "?1")
}
