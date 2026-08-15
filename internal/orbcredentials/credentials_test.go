package orbcredentials

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"strings"
	"testing"

	"golang.org/x/crypto/ssh"
)

func TestSnapshotRoundTripAndValidation(t *testing.T) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	privateBlock, err := ssh.MarshalPrivateKey(private, "")
	if err != nil {
		t.Fatal(err)
	}
	privatePEM := pem.EncodeToMemory(privateBlock)
	sshPublic, err := ssh.NewPublicKey(public)
	if err != nil {
		t.Fatal(err)
	}
	publicLine := ssh.MarshalAuthorizedKey(sshPublic)
	knownHosts := []byte("github.com " + strings.TrimSpace(string(publicLine)) + "\n")
	snapshot, err := New("github-token", []DecodedSSHIdentity{{Name: "id_ed25519", PrivateKey: privatePEM, PublicKey: publicLine}}, knownHosts)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	normalized, decoded, err := Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	if normalized.GitHub == nil || decoded.GitHubToken != "github-token" || decoded.SSH == nil || len(decoded.SSH.Identities) != 1 {
		t.Fatalf("decoded snapshot = %#v %#v", normalized, decoded)
	}
	if string(decoded.SSH.Identities[0].PrivateKey) != string(privatePEM) || string(decoded.SSH.KnownHosts) != string(knownHosts) {
		t.Fatal("decoded SSH material did not round trip")
	}
}

func TestSnapshotRejectsUnsafeInputWithoutEchoingSecrets(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{name: "empty snapshot", raw: `{"schema":1,"github":null,"ssh":null}`},
		{name: "duplicate field", raw: `{"schema":1,"github":{"token":"sentinel"},"GitHub":null,"ssh":null}`},
		{name: "unknown field", raw: `{"schema":1,"github":null,"ssh":null,"token":"sentinel"}`},
		{name: "whitespace token", raw: `{"schema":1,"github":{"token":"sentinel token"},"ssh":null}`},
		{name: "invalid base64", raw: `{"schema":1,"github":null,"ssh":{"identities":[{"name":"id_test","privateKey":"sentinel","publicKey":""}],"knownHosts":""}}`},
		{name: "empty ssh", raw: `{"schema":1,"github":null,"ssh":{"identities":[],"knownHosts":""}}`},
		{name: "trailing", raw: `{"schema":1,"github":null,"ssh":null}{"sentinel":true}`},
	}
	for _, test := range tests {
		_, _, err := Decode([]byte(test.raw))
		if err == nil {
			t.Fatalf("%s: expected rejection", test.name)
		}
		if strings.Contains(err.Error(), "sentinel") {
			t.Fatalf("%s: error leaked input: %v", test.name, err)
		}
	}
	if _, err := New("", nil, nil); err == nil {
		t.Fatal("empty credential snapshot was created")
	}
}

func TestSnapshotRejectsKnownHostsWithoutIdentity(t *testing.T) {
	public, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	sshPublic, err := ssh.NewPublicKey(public)
	if err != nil {
		t.Fatal(err)
	}
	knownHosts := []byte("github.com " + strings.TrimSpace(string(ssh.MarshalAuthorizedKey(sshPublic))) + "\n")
	snapshot := Snapshot{
		Schema: Schema,
		SSH: &SSHCredentials{
			Identities: []SSHIdentity{},
			KnownHosts: base64.StdEncoding.EncodeToString(knownHosts),
		},
	}
	if _, _, err := Validate(snapshot); err == nil {
		t.Fatal("known_hosts-only credential snapshot was accepted")
	}
	githubOnly, err := New("github-token", nil, knownHosts)
	if err != nil {
		t.Fatal(err)
	}
	if githubOnly.GitHub == nil || githubOnly.SSH != nil {
		t.Fatalf("GitHub snapshot retained unusable SSH: %#v", githubOnly)
	}
}

func TestSnapshotRejectsMismatchedPublicKey(t *testing.T) {
	_, first, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	secondPublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	privateBlock, err := ssh.MarshalPrivateKey(first, "")
	if err != nil {
		t.Fatal(err)
	}
	privatePEM := pem.EncodeToMemory(privateBlock)
	publicKey, err := ssh.NewPublicKey(secondPublic)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := Snapshot{
		Schema: Schema,
		SSH: &SSHCredentials{
			Identities: []SSHIdentity{{
				Name:       "id_ed25519",
				PrivateKey: base64.StdEncoding.EncodeToString(privatePEM),
				PublicKey:  base64.StdEncoding.EncodeToString(ssh.MarshalAuthorizedKey(publicKey)),
			}},
			KnownHosts: "",
		},
	}
	if _, _, err := Validate(snapshot); err == nil {
		t.Fatal("expected mismatched public key rejection")
	}
}
