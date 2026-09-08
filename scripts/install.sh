#!/usr/bin/env bash
# Installs the poolmgrd binary from a GitHub release of liquidmetal-dev/battery,
# and (unless --binary-only) creates a dedicated system user, a default config,
# and a systemd unit, then enables and starts the service.
set -euo pipefail

REPO="liquidmetal-dev/battery"
BIN_NAME="poolmgrd"
SERVICE_NAME="poolmgrd.service"

BIN_DIR="/usr/local/bin"
CONFIG_DIR="/etc/poolmgrd"
DATA_DIR="/var/lib/poolmgrd"
SYSTEMD_DIR="/etc/systemd/system"
VERSION=""
BINARY_ONLY=0

usage() {
    cat <<EOF
Usage: $(basename "$0") --version <version> [options]

Downloads and installs the poolmgrd binary from a GitHub release of ${REPO},
creates a dedicated system user, a default config, and a systemd unit, then
enables and starts the service.

Required:
  -v, --version VERSION   Release tag to install (e.g. v0.1.0).

Options:
  -b, --binary-only       Only download and install the binary; skip user,
                           config, and systemd unit setup.
  -d, --bin-dir DIR       Directory to install the binary into (default: ${BIN_DIR}).
  -c, --config-dir DIR    Directory for the config file (default: ${CONFIG_DIR}).
  -s, --data-dir DIR      Directory for the sqlite database (default: ${DATA_DIR}).
  -h, --help              Show this help text.
EOF
}

err() {
    echo "error: $*" >&2
}

die() {
    err "$@"
    exit 1
}

while [ $# -gt 0 ]; do
    case "$1" in
        -v|--version)
            [ $# -ge 2 ] || die "--version requires an argument"
            VERSION="$2"
            shift 2
            ;;
        -b|--binary-only)
            BINARY_ONLY=1
            shift
            ;;
        -d|--bin-dir)
            [ $# -ge 2 ] || die "--bin-dir requires an argument"
            BIN_DIR="$2"
            shift 2
            ;;
        -c|--config-dir)
            [ $# -ge 2 ] || die "--config-dir requires an argument"
            CONFIG_DIR="$2"
            shift 2
            ;;
        -s|--data-dir)
            [ $# -ge 2 ] || die "--data-dir requires an argument"
            DATA_DIR="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            usage >&2
            die "unknown argument: $1"
            ;;
    esac
done

if [ -z "$VERSION" ]; then
    usage >&2
    die "--version is required"
fi

if [ "$(id -u)" -ne 0 ]; then
    die "this script must be run as root"
fi

if [ "$(uname -s)" != "Linux" ]; then
    die "poolmgrd releases are only published for Linux"
fi

case "$(uname -m)" in
    x86_64)
        ARCH="amd64"
        ;;
    aarch64|arm64)
        ARCH="arm64"
        ;;
    *)
        die "unsupported architecture: $(uname -m)"
        ;;
esac

for cmd in tar sha256sum install; do
    command -v "$cmd" >/dev/null 2>&1 || die "required command not found: $cmd"
done

if command -v curl >/dev/null 2>&1; then
    DOWNLOADER="curl"
elif command -v wget >/dev/null 2>&1; then
    DOWNLOADER="wget"
else
    die "either curl or wget is required"
fi

download() {
    # download <url> <output-path>
    local url="$1"
    local out="$2"
    if [ "$DOWNLOADER" = "curl" ]; then
        curl --fail --location --show-error --silent --output "$out" "$url"
    else
        wget --quiet --output-document="$out" "$url"
    fi
}

VERSION_NO_V="${VERSION#v}"
ARCHIVE_NAME="${BIN_NAME}_${VERSION_NO_V}_linux_${ARCH}.tar.gz"
RELEASE_URL="https://github.com/${REPO}/releases/download/${VERSION}"

WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

echo "Downloading ${ARCHIVE_NAME} (version ${VERSION})..."
download "${RELEASE_URL}/${ARCHIVE_NAME}" "${WORK_DIR}/${ARCHIVE_NAME}" \
    || die "failed to download ${RELEASE_URL}/${ARCHIVE_NAME}"
[ -s "${WORK_DIR}/${ARCHIVE_NAME}" ] || die "downloaded archive is empty"

echo "Verifying checksum..."
download "${RELEASE_URL}/checksums.txt" "${WORK_DIR}/checksums.txt" \
    || die "failed to download checksums.txt from ${RELEASE_URL}"
(
    cd "$WORK_DIR"
    grep -F "  ${ARCHIVE_NAME}" checksums.txt > checksum.line \
        || die "no checksum entry found for ${ARCHIVE_NAME}"
    sha256sum --check checksum.line
) || die "checksum verification failed"

echo "Extracting..."
tar -xzf "${WORK_DIR}/${ARCHIVE_NAME}" -C "$WORK_DIR"
[ -f "${WORK_DIR}/${BIN_NAME}" ] || die "extracted archive does not contain ${BIN_NAME}"

echo "Installing binary to ${BIN_DIR}/${BIN_NAME}..."
install -d -m 0755 "$BIN_DIR"
install -m 0755 "${WORK_DIR}/${BIN_NAME}" "${BIN_DIR}/${BIN_NAME}"

if [ "$BINARY_ONLY" -eq 1 ]; then
    echo "Installed ${BIN_NAME} ${VERSION} to ${BIN_DIR}/${BIN_NAME}"
    exit 0
fi

if ! getent group "$BIN_NAME" >/dev/null 2>&1; then
    echo "Creating group ${BIN_NAME}..."
    groupadd --system "$BIN_NAME"
fi

if ! getent passwd "$BIN_NAME" >/dev/null 2>&1; then
    echo "Creating user ${BIN_NAME}..."
    useradd --system --no-create-home --shell /usr/sbin/nologin \
        --gid "$BIN_NAME" "$BIN_NAME"
fi

echo "Creating config directory ${CONFIG_DIR}..."
install -d -o "$BIN_NAME" -g "$BIN_NAME" -m 0750 "$CONFIG_DIR"

echo "Creating data directory ${DATA_DIR}..."
install -d -o "$BIN_NAME" -g "$BIN_NAME" -m 0750 "$DATA_DIR"

CONFIG_FILE="${CONFIG_DIR}/config.json"
if [ -f "$CONFIG_FILE" ]; then
    echo "Config file ${CONFIG_FILE} already exists, leaving it untouched."
else
    echo "Writing default config to ${CONFIG_FILE}..."
    cat > "$CONFIG_FILE" <<EOF
{
  "hosts": [],
  "metrics_addr": ":9090"
}
EOF
    chown "$BIN_NAME:$BIN_NAME" "$CONFIG_FILE"
    chmod 0640 "$CONFIG_FILE"
fi

UNIT_FILE="${SYSTEMD_DIR}/${SERVICE_NAME}"
echo "Writing systemd unit to ${UNIT_FILE}..."
cat > "$UNIT_FILE" <<EOF
[Unit]
Description=poolmgrd - battery pool manager daemon
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${BIN_NAME}
Group=${BIN_NAME}
ExecStart=${BIN_DIR}/${BIN_NAME} -config ${CONFIG_DIR}/config.json -db ${DATA_DIR}/poolmgr.db
Restart=on-failure
RestartSec=5
WorkingDirectory=${DATA_DIR}
NoNewPrivileges=true
ProtectSystem=strict
ReadWritePaths=${DATA_DIR}
ProtectHome=true

[Install]
WantedBy=multi-user.target
EOF

echo "Reloading systemd and starting ${SERVICE_NAME}..."
systemctl daemon-reload
if systemctl is-active --quiet "$SERVICE_NAME"; then
    systemctl restart "$SERVICE_NAME"
else
    systemctl enable --now "$SERVICE_NAME"
fi

echo
echo "poolmgrd ${VERSION} installed and running."
echo "  Binary: ${BIN_DIR}/${BIN_NAME}"
echo "  Config: ${CONFIG_FILE}"
echo "  Data:   ${DATA_DIR}"
echo "  Check status with: systemctl status ${SERVICE_NAME}"
