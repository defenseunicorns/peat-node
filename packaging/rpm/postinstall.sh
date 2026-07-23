if ! getent group peat >/dev/null; then
    groupadd --system peat
fi
if ! getent passwd peat >/dev/null; then
    useradd --system --gid peat --home-dir /var/lib/peat-node \
        --no-create-home --shell /sbin/nologin peat
fi
install -d -o peat -g peat -m 0750 /var/lib/peat-node
chown -R -h peat:peat /var/lib/peat-node

config=/etc/peat-node/peat-node.env
config_template=/usr/share/peat-node/peat-node.env.example
if [ ! -s "$config" ]; then
    install -d -o root -g root -m 0755 /etc/peat-node
    install -o root -g peat -m 0640 "$config_template" "$config"
fi

if command -v systemctl >/dev/null 2>&1; then
    systemctl daemon-reload >/dev/null 2>&1 || true
    if [ "$1" -eq 1 ]; then
        systemctl enable --now peat-node.service >/dev/null 2>&1 || true
    fi
fi
