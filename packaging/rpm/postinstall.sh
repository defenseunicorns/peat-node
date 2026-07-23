if ! getent group peat >/dev/null; then
    groupadd --system peat
fi
if ! getent passwd peat >/dev/null; then
    useradd --system --gid peat --home-dir /var/lib/peat-node \
        --no-create-home --shell /sbin/nologin peat
fi
install -d -o peat -g peat -m 0750 /var/lib/peat-node

if command -v systemctl >/dev/null 2>&1; then
    systemctl daemon-reload >/dev/null 2>&1 || true
    systemctl enable --now peat-node.service >/dev/null 2>&1 || true
fi
