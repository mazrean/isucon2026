#!/usr/bin/env bash
# usage: scripts/fw-local.sh open|close|status <port>  -- 疑似ベンチ用にローカル IP からの対象ポートのみ SG で許可/取り消し
set -euo pipefail
ACT=$1; PORT=${2:?port required}
case $PORT in 22|443|8080) echo "port $PORT は変更禁止 (マニュアル)" >&2; exit 1;; esac
LOG=$(dirname "$0")/../logs/fw-local.log
if [[ $ACT == close ]]; then MYIP=$(awk -v p="$PORT" '$2=="OPEN"&&$4==p{print $5}' "$LOG" | tail -1); else MYIP=$(curl -s https://checkip.amazonaws.com)/32; fi
SGS=$(aws ec2 describe-instances --filters Name=instance-state-name,Values=running Name=tag:aws:cloudformation:stack-name,Values="${STACK:?set STACK}" \
  --query 'Reservations[].Instances[].SecurityGroups[].GroupId' --output text | tr '\t' '\n' | sort -u)
for sg in $SGS; do
  case $ACT in
  open)  aws ec2 authorize-security-group-ingress --group-id "$sg" --protocol tcp --port "$PORT" --cidr "$MYIP" >/dev/null 2>&1 && echo "$(date +%FT%T) OPEN $sg $PORT $MYIP" >>"$LOG" || echo "already/failed $sg";;
  close) aws ec2 revoke-security-group-ingress --group-id "$sg" --protocol tcp --port "$PORT" --cidr "$MYIP" >/dev/null 2>&1 && echo "$(date +%FT%T) CLOSE $sg $PORT $MYIP" >>"$LOG" || echo "none/failed $sg";;
  status) aws ec2 describe-security-groups --group-ids "$sg" --query 'SecurityGroups[].IpPermissions' --output json;;
  esac
done
