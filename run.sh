#!/bin/bash
# Arranca la app y permite acceso desde la red local (móvil, etc.)

cd "$(dirname "$0")"

# Si UFW está activo, permitir puerto 5000
if command -v ufw >/dev/null 2>&1; then
    status=$(sudo ufw status 2>/dev/null)
    if echo "$status" | grep -q "Status: active"; then
        if ! echo "$status" | grep -q "5000"; then
            echo ">>> Abriendo puerto 5000 en el firewall (ufw)..."
            sudo ufw allow 5000/tcp 2>/dev/null && echo "    OK: puerto 5000 permitido"
        fi
    fi
fi

# Si firewalld está activo
if command -v firewall-cmd >/dev/null 2>&1 && systemctl is-active firewalld >/dev/null 2>&1; then
    if ! sudo firewall-cmd --list-ports 2>/dev/null | grep -q 5000; then
        echo ">>> Abriendo puerto 5000 en firewalld..."
        sudo firewall-cmd --permanent --add-port=5000/tcp 2>/dev/null
        sudo firewall-cmd --reload 2>/dev/null && echo "    OK: puerto 5000 permitido"
    fi
fi

echo ""
exec python app.py
