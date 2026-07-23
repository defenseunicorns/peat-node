if [ "$1" -eq 0 ] && command -v systemctl >/dev/null 2>&1; then
    systemctl disable --now peat-node.service >/dev/null 2>&1 || true
fi
