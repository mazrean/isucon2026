#!/bin/sh
# Adds 192.168.0.0/16 via the DHCP gateway with larger initcwnd/initrwnd (all inter-host traffic goes via the router anyway).
set -eu
set -- $(ip -4 -o route show default | awk '{for(i=1;i<NF;i++){if($i=="via")g=$(i+1);if($i=="dev")d=$(i+1)}} END{print g, d}')
[ -n "${1:-}" ] && [ -n "${2:-}" ] || { echo "no default route" >&2; exit 1; }
ip route replace 192.168.0.0/16 via "$1" dev "$2" initcwnd 32 initrwnd 64
ip route show 192.168.0.0/16
