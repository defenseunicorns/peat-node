# Installing peat-node from package repositories

Tagged stable releases publish signed APT and DNF repository metadata for
x86_64 and ARM64 to GitHub Pages:

- APT: `https://defenseunicorns.github.io/peat-node/apt`
- DNF/YUM: `https://defenseunicorns.github.io/peat-node/rpm/stable`
- Signing key: `https://defenseunicorns.github.io/peat-node/peat-node-archive-key.gpg`

The package installs both `/usr/bin/peat-node` and the `/usr/bin/peat` operator
CLI, creates the `peat` system user and `/var/lib/peat-node`, installs
`/etc/peat-node/peat-node.env`, and enables and starts `peat-node.service`. The
generic configuration listens only on
`127.0.0.1:50051`. Configure deployment identity and credentials before
exposing the service beyond the host.

## Debian and Ubuntu

Install the repository key without adding it to the deprecated global APT key
store:

```bash
sudo install -d -m 0755 /etc/apt/keyrings
curl -fsSL https://defenseunicorns.github.io/peat-node/peat-node-archive-key.gpg \
  | sudo tee /etc/apt/keyrings/peat-node.gpg >/dev/null
sudo chmod 0644 /etc/apt/keyrings/peat-node.gpg
```

Add the stable repository and install:

```bash
cat <<'EOF' | sudo tee /etc/apt/sources.list.d/peat-node.list
deb [signed-by=/etc/apt/keyrings/peat-node.gpg] https://defenseunicorns.github.io/peat-node/apt stable main
EOF
sudo apt update
sudo apt install peat-node
```

APT selects `amd64` or `arm64` from the host architecture. Normal
`sudo apt upgrade` operations subsequently upgrade peat-node.

## Fedora, RHEL, and Rocky Linux

Add `/etc/yum.repos.d/peat-node.repo`:

```bash
cat <<'EOF' | sudo tee /etc/yum.repos.d/peat-node.repo
[peat-node]
name=Peat Node
baseurl=https://defenseunicorns.github.io/peat-node/rpm/stable/$basearch
enabled=1
repo_gpgcheck=1
gpgcheck=0
gpgkey=https://defenseunicorns.github.io/peat-node/peat-node-archive-key.asc
EOF
sudo dnf makecache
sudo dnf install peat-node
```

`yum install peat-node` is equivalent on hosts where YUM fronts DNF. `$basearch`
selects `x86_64` or `aarch64`. Repository metadata is signed; its authenticated
checksums cover the RPM payloads. Individual RPM files are not separately
signed, hence `gpgcheck=0` and `repo_gpgcheck=1`.

## Configure and operate the service

Review `/etc/peat-node/peat-node.env` before joining a formation. At minimum,
set the deployment-specific application ID and shared key:

```ini
PEAT_NODE_LISTEN=tcp://127.0.0.1:50051
PEAT_NODE_DATA_DIR=/var/lib/peat-node
PEAT_NODE_APP_ID=replace-with-formation-id
PEAT_NODE_SHARED_KEY=replace-with-base64-encoded-32-byte-key
PEAT_NODE_AUTO_SYNC=true
```

Generate a cryptographically random Base64-encoded 32-byte shared key with
OpenSSL:

```bash
openssl rand -base64 32
```

Use the same result as `PEAT_NODE_SHARED_KEY` on every node in the formation.
Treat it as a secret: do not commit it or paste it into logs.

The environment file is preserved across RPM upgrades. Debian package upgrades
also treat files under `/etc` as administrator configuration. Apply changes and
inspect the service with:

```bash
sudo systemctl restart peat-node
sudo systemctl status peat-node
sudo journalctl -u peat-node -f
```

The unit derives a stable default node ID from the hostname (`peat-%H`) and runs
with `User=peat`, `Group=peat`, `ProtectSystem=strict`, and write access limited
to `/var/lib/peat-node`.

## Repository administrator setup

The release workflow publishes only stable tags. Prerelease tags remain GitHub
Release assets and do not replace the stable APT or DNF repositories.

Before the first stable release:

1. In **Settings → Pages**, select **GitHub Actions** as the deployment source.
2. Create a dedicated signing key whose public identity clearly names the Peat
   package repository. Keep this key separate from developer signing keys.
3. Add the ASCII-armored private key as the Actions secret
   `PACKAGE_REPOSITORY_GPG_PRIVATE_KEY`.
4. Add its passphrase as `PACKAGE_REPOSITORY_GPG_PASSPHRASE`. Omit this secret
   for a key without a passphrase, although a protected key is recommended.
5. Ensure the `github-pages` environment permits the release workflow to deploy.

Example key creation and export:

```bash
gpg --quick-generate-key \
  'Peat Node Package Repository <opensource@defenseunicorns.com>' \
  rsa4096 sign 2y
gpg --armor --export-secret-keys \
  'Peat Node Package Repository' > peat-node-package-repository.private.asc
gpg --armor --export \
  'Peat Node Package Repository' > peat-node-package-repository.public.asc
```

Store the private export only in the GitHub Actions secret and an approved
backup. Publish key rotation before signing repositories with the replacement
key; hosts trust exactly the key installed in `/etc/apt/keyrings` or referenced
by the DNF repository configuration.
