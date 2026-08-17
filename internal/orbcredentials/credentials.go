package orbcredentials

import (
	"bytes"
	"crypto"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/crypto/ssh"
)

const (
	EndpointPath        = "/ampcode/local-broker/orb-credentials.json"
	SupportHeader       = "X-Cliproxy-Orb-Credentials"
	RevisionHeader      = "X-Cliproxy-Orb-Credentials-Revision"
	AbsentRevision      = "absent"
	RevokedRevision     = "revoked"
	RepairRevision      = "repair-required"
	Schema              = 1
	MaxBodyBytes        = 1 << 20
	MaxDecodedBytes     = 512 << 10
	MaxGitHubTokenBytes = 16 << 10
	MaxIdentities       = 16
	MaxPrivateKeyBytes  = 64 << 10
	MaxPublicKeyBytes   = 16 << 10
	MaxKnownHostsBytes  = 256 << 10
)

var identityNamePattern = regexp.MustCompile(`^id_[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

type Snapshot struct {
	Schema int               `json:"schema"`
	GitHub *GitHubCredential `json:"github"`
	SSH    *SSHCredentials   `json:"ssh"`
}

type GitHubCredential struct {
	Token string `json:"token"`
}

type SSHCredentials struct {
	Identities []SSHIdentity `json:"identities"`
	KnownHosts string        `json:"knownHosts"`
}

type SSHIdentity struct {
	Name       string `json:"name"`
	PrivateKey string `json:"privateKey"`
	PublicKey  string `json:"publicKey"`
}

type Decoded struct {
	GitHubToken string
	SSH         *DecodedSSH
}

type DecodedSSH struct {
	Identities []DecodedSSHIdentity
	KnownHosts []byte
}

type DecodedSSHIdentity struct {
	Name       string
	PrivateKey []byte
	PublicKey  []byte
}

func New(githubToken string, identities []DecodedSSHIdentity, knownHosts []byte) (Snapshot, error) {
	snapshot := Snapshot{Schema: Schema}
	if githubToken != "" {
		snapshot.GitHub = &GitHubCredential{Token: githubToken}
	}
	if len(identities) > 0 {
		snapshot.SSH = &SSHCredentials{
			Identities: make([]SSHIdentity, 0, len(identities)),
			KnownHosts: base64.StdEncoding.EncodeToString(knownHosts),
		}
		for _, identity := range identities {
			snapshot.SSH.Identities = append(snapshot.SSH.Identities, SSHIdentity{
				Name:       identity.Name,
				PrivateKey: base64.StdEncoding.EncodeToString(identity.PrivateKey),
				PublicKey:  base64.StdEncoding.EncodeToString(identity.PublicKey),
			})
		}
	}
	normalized, _, err := Validate(snapshot)
	return normalized, err
}

func Decode(raw []byte) (Snapshot, Decoded, error) {
	if len(raw) == 0 || len(raw) > MaxBodyBytes {
		return Snapshot{}, Decoded{}, errors.New("credential snapshot body is invalid")
	}
	if err := rejectDuplicateJSONFields(raw); err != nil {
		return Snapshot{}, Decoded{}, errors.New("credential snapshot contains invalid JSON")
	}
	var snapshot Snapshot
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&snapshot); err != nil {
		return Snapshot{}, Decoded{}, errors.New("credential snapshot contains invalid JSON")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return Snapshot{}, Decoded{}, errors.New("credential snapshot contains trailing JSON")
	}
	return Validate(snapshot)
}

func Validate(snapshot Snapshot) (Snapshot, Decoded, error) {
	if snapshot.Schema != Schema {
		return Snapshot{}, Decoded{}, fmt.Errorf("credential schema must be %d", Schema)
	}
	if snapshot.GitHub == nil && snapshot.SSH == nil {
		return Snapshot{}, Decoded{}, errors.New("credential snapshot is empty")
	}
	decoded := Decoded{}
	total := 0
	if snapshot.GitHub != nil {
		token := snapshot.GitHub.Token
		if !validGitHubToken(token) {
			return Snapshot{}, Decoded{}, errors.New("GitHub credential is invalid")
		}
		total += len(token)
		decoded.GitHubToken = token
	}
	if snapshot.SSH != nil {
		if len(snapshot.SSH.Identities) == 0 || len(snapshot.SSH.Identities) > MaxIdentities {
			return Snapshot{}, Decoded{}, errors.New("SSH credential identity list is invalid")
		}
		decoded.SSH = &DecodedSSH{Identities: make([]DecodedSSHIdentity, 0, len(snapshot.SSH.Identities))}
		seen := make(map[string]bool, len(snapshot.SSH.Identities))
		for index, identity := range snapshot.SSH.Identities {
			if !identityNamePattern.MatchString(identity.Name) || strings.HasSuffix(strings.ToLower(identity.Name), ".pub") {
				return Snapshot{}, Decoded{}, fmt.Errorf("SSH identity %d has an invalid name", index+1)
			}
			folded := strings.ToLower(identity.Name)
			if seen[folded] {
				return Snapshot{}, Decoded{}, errors.New("SSH credential contains duplicate identities")
			}
			seen[folded] = true
			privateKey, err := decodeStrictBase64(identity.PrivateKey, MaxPrivateKeyBytes)
			if err != nil || len(privateKey) == 0 {
				return Snapshot{}, Decoded{}, fmt.Errorf("SSH identity %d private key is invalid", index+1)
			}
			publicKey, err := decodeStrictBase64(identity.PublicKey, MaxPublicKeyBytes)
			if err != nil {
				return Snapshot{}, Decoded{}, fmt.Errorf("SSH identity %d public key is invalid", index+1)
			}
			if err := validateSSHIdentity(privateKey, publicKey); err != nil {
				return Snapshot{}, Decoded{}, fmt.Errorf("SSH identity %d is invalid", index+1)
			}
			total += len(privateKey) + len(publicKey)
			decoded.SSH.Identities = append(decoded.SSH.Identities, DecodedSSHIdentity{
				Name:       identity.Name,
				PrivateKey: privateKey,
				PublicKey:  publicKey,
			})
		}
		knownHosts, err := decodeStrictBase64(snapshot.SSH.KnownHosts, MaxKnownHostsBytes)
		if err != nil || validateKnownHosts(knownHosts) != nil {
			return Snapshot{}, Decoded{}, errors.New("SSH known_hosts is invalid")
		}
		total += len(knownHosts)
		decoded.SSH.KnownHosts = knownHosts
	}
	if total > MaxDecodedBytes {
		return Snapshot{}, Decoded{}, errors.New("credential snapshot exceeds decoded size limit")
	}
	if snapshot.SSH != nil {
		sort.Slice(snapshot.SSH.Identities, func(i, j int) bool {
			left := strings.ToLower(snapshot.SSH.Identities[i].Name)
			right := strings.ToLower(snapshot.SSH.Identities[j].Name)
			if left == right {
				return snapshot.SSH.Identities[i].Name < snapshot.SSH.Identities[j].Name
			}
			return left < right
		})
		sort.Slice(decoded.SSH.Identities, func(i, j int) bool {
			return strings.ToLower(decoded.SSH.Identities[i].Name) < strings.ToLower(decoded.SSH.Identities[j].Name)
		})
	}
	return snapshot, decoded, nil
}

func validGitHubToken(token string) bool {
	if token == "" || len(token) > MaxGitHubTokenBytes || strings.TrimSpace(token) != token || !utf8.ValidString(token) {
		return false
	}
	for _, character := range token {
		if unicode.IsSpace(character) || unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func decodeStrictBase64(value string, limit int) ([]byte, error) {
	if value == "" {
		return nil, nil
	}
	decoded, err := base64.StdEncoding.Strict().DecodeString(value)
	if err != nil || len(decoded) > limit || base64.StdEncoding.EncodeToString(decoded) != value {
		return nil, errors.New("invalid encoded credential")
	}
	return decoded, nil
}

func validateSSHIdentity(privateKey, publicKey []byte) error {
	parsed, err := ssh.ParseRawPrivateKey(privateKey)
	if err != nil {
		return errors.New("invalid private identity")
	}
	signer, ok := parsed.(crypto.Signer)
	if !ok {
		return errors.New("unsupported private identity")
	}
	privatePublic, err := ssh.NewPublicKey(signer.Public())
	if err != nil {
		return errors.New("invalid private identity")
	}
	if len(publicKey) == 0 {
		return nil
	}
	parsedPublic, _, _, rest, err := ssh.ParseAuthorizedKey(publicKey)
	if err != nil || len(bytes.TrimSpace(rest)) != 0 || !bytes.Equal(privatePublic.Marshal(), parsedPublic.Marshal()) {
		return errors.New("public identity does not match")
	}
	return nil
}

func validateKnownHosts(content []byte) error {
	remaining := content
	for len(bytes.TrimSpace(remaining)) > 0 {
		_, hosts, publicKey, _, rest, err := ssh.ParseKnownHosts(remaining)
		if err != nil || len(hosts) == 0 || publicKey == nil || len(rest) >= len(remaining) {
			return errors.New("invalid known_hosts")
		}
		remaining = rest
	}
	return nil
}

func rejectDuplicateJSONFields(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	var walk func(int) error
	walk = func(depth int) error {
		if depth > 8 {
			return errors.New("JSON is too deeply nested")
		}
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delimiter, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delimiter {
		case '{':
			seen := map[string]bool{}
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return err
				}
				key, ok := keyToken.(string)
				if !ok || seen[strings.ToLower(key)] {
					return errors.New("duplicate JSON field")
				}
				seen[strings.ToLower(key)] = true
				if err := walk(depth + 1); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		case '[':
			for decoder.More() {
				if err := walk(depth + 1); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		default:
			return errors.New("invalid JSON delimiter")
		}
	}
	if err := walk(0); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON")
	}
	return nil
}
