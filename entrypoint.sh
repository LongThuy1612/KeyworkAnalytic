#!/bin/bash
# Private virtual display (+ noVNC for manual logins), then the service command.
#   DISPLAY_NUM  must differ from the host's :99 (network_mode: host shares the
#                abstract X socket namespace)
#   VNC_PORT / NOVNC_PORT  x11vnc and web-VNC ports (NOVNC_PORT=0 disables)
set -e

DISPLAY_NUM="${DISPLAY_NUM:-101}"
export DISPLAY=":${DISPLAY_NUM}"

rm -f "/tmp/.X${DISPLAY_NUM}-lock" "/tmp/.X11-unix/X${DISPLAY_NUM}" || true
Xvfb "$DISPLAY" -screen 0 1280x1024x24 -ac +extension GLX +render -noreset -maxclients 2048 &
sleep 2
fluxbox >/dev/null 2>&1 &

if [ "${NOVNC_PORT:-0}" != "0" ]; then
  x11vnc -display "$DISPLAY" -nopw -listen 127.0.0.1 -rfbport "${VNC_PORT:-5911}" -forever -shared -xkb >/dev/null 2>&1 &
  ln -sf /usr/share/novnc/vnc.html /usr/share/novnc/index.html 2>/dev/null || true
  websockify --web /usr/share/novnc "${NOVNC_PORT}" "127.0.0.1:${VNC_PORT:-5911}" >/dev/null 2>&1 &
  echo "==> noVNC: http://<host>:${NOVNC_PORT}/vnc.html"
fi

exec "$@"
