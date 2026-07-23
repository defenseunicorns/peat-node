if command -v systemctl >/dev/null 2>&1; then
    systemctl daemon-reload >/dev/null 2>&1 || true
    if [ "$1" -eq 1 ]; then
        systemctl try-restart peat-node.service >/dev/null 2>&1 || true
    fi
fi
