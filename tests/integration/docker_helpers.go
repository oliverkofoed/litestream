//go:build integration

package integration

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func RequireDocker(t *testing.T) {
	t.Helper()
	if err := exec.Command("docker", "version").Run(); err != nil {
		t.Skip("Docker is not available, skipping test")
	}
}

func StartMinioTestContainer(t *testing.T) (string, string) {
	t.Helper()

	name := fmt.Sprintf("litestream-minio-%d", time.Now().UnixNano())
	exec.Command("docker", "rm", "-f", name).Run()

	args := []string{
		"run", "-d",
		"--name", name,
		"-p", "0:9000",
		"-e", "MINIO_ROOT_USER=minioadmin",
		"-e", "MINIO_ROOT_PASSWORD=minioadmin",
		"-e", "MINIO_DOMAIN=s3-accesspoint.127.0.0.1.nip.io",
		"minio/minio", "server", "/data",
	}
	containerID := runDockerCommand(t, args...)
	portInfo := runDockerCommand(t, "port", name, "9000/tcp")
	hostPort := parseDockerPort(t, portInfo)

	time.Sleep(5 * time.Second)

	t.Logf("Started MinIO container %s (%s) on port %s", name, containerID[:12], hostPort)
	return name, fmt.Sprintf("http://localhost:%s", hostPort)
}

func StopMinioTestContainer(t *testing.T, name string) {
	t.Helper()
	if name == "" {
		return
	}
	if os.Getenv("SOAK_KEEP_TEMP") != "" {
		t.Logf("SOAK_KEEP_TEMP set, preserving MinIO container: %s", name)
		return
	}
	exec.Command("docker", "rm", "-f", name).Run()
}

func runDockerCommand(t *testing.T, args ...string) string {
	t.Helper()
	cmd := exec.Command("docker", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s failed: %v\nOutput: %s", strings.Join(args, " "), err, string(output))
	}
	return strings.TrimSpace(string(output))
}

func parseDockerPort(t *testing.T, portInfo string) string {
	t.Helper()
	idx := strings.LastIndex(portInfo, ":")
	if idx == -1 || idx == len(portInfo)-1 {
		t.Fatalf("unexpected docker port output: %s", portInfo)
	}
	return portInfo[idx+1:]
}

// StartMinioTestContainerSSL starts a MinIO container with SSL enabled using self-signed certificates.
// Returns the container name, HTTPS endpoint, and the path to the CA certificate.
func StartMinioTestContainerSSL(t *testing.T) (string, string, string) {
	t.Helper()

	// Create temporary directory for certificates
	certDir, err := os.MkdirTemp("", "minio-certs-*")
	if err != nil {
		t.Fatalf("create cert dir: %v", err)
	}
	t.Cleanup(func() {
		os.RemoveAll(certDir)
	})

	// Generate self-signed certificate for storage.service
	caCertPath := generateSelfSignedCert(t, certDir, "storage.service")

	name := fmt.Sprintf("litestream-minio-ssl-%d", time.Now().UnixNano())
	exec.Command("docker", "rm", "-f", name).Run()

	// Use nip.io for DNS resolution (same pattern as non-SSL test)
	domain := "s3-accesspoint.127.0.0.1.nip.io"

	args := []string{
		"run", "-d",
		"--name", name,
		"--hostname", "storage.service",
		"-p", "0:9000",
		"-e", "MINIO_ROOT_USER=minioadmin",
		"-e", "MINIO_ROOT_PASSWORD=minioadmin",
		"-e", fmt.Sprintf("MINIO_DOMAIN=%s", domain),
		"-v", fmt.Sprintf("%s:/root/.minio/certs", certDir),
		"minio/minio", "server", "/data",
	}
	containerID := runDockerCommand(t, args...)

	// Get the mapped port
	portInfo := runDockerCommand(t, "port", name, "9000/tcp")
	hostPort := parseDockerPort(t, portInfo)

	// Wait for MinIO to start
	time.Sleep(5 * time.Second)

	t.Logf("Started MinIO SSL container %s (%s) on port %s with domain %s", name, containerID[:12], hostPort, domain)
	// Use nip.io domain for endpoint (resolves to 127.0.0.1)
	return name, fmt.Sprintf("https://%s:%s", domain, hostPort), caCertPath
}

// generateSelfSignedCert creates a self-signed certificate for the given hostname.
func generateSelfSignedCert(t *testing.T, certDir, hostname string) string {
	t.Helper()

	// Use OpenSSL to generate certificate
	keyPath := fmt.Sprintf("%s/private.key", certDir)
	certPath := fmt.Sprintf("%s/public.crt", certDir)

	// Generate private key
	keyCmd := exec.Command("openssl", "genrsa", "-out", keyPath, "2048")
	if output, err := keyCmd.CombinedOutput(); err != nil {
		t.Fatalf("generate key: %v\nOutput: %s", err, string(output))
	}

	// Generate certificate
	certCmd := exec.Command("openssl", "req", "-new", "-x509",
		"-key", keyPath,
		"-out", certPath,
		"-days", "365",
		"-subj", fmt.Sprintf("/CN=%s", hostname),
		"-addext", fmt.Sprintf("subjectAltName=DNS:%s,DNS:localhost,IP:127.0.0.1", hostname),
	)
	if output, err := certCmd.CombinedOutput(); err != nil {
		t.Fatalf("generate cert: %v\nOutput: %s", err, string(output))
	}

	t.Logf("Generated self-signed certificate for %s", hostname)
	return certPath
}
