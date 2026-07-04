#!/usr/bin/env bash
set -euo pipefail

URL=${1:-http://127.0.0.1:8081/api/aggregate/process}
DURATION_SEC=${2:-30}
PAYLOAD=${3:-./data/payload_256kb.json}
QPS=${4:-70}

if [ ! -f "${PAYLOAD}" ]; then
  echo "payload file not found: ${PAYLOAD}"
  exit 1
fi

TMP=$(mktemp)
trap 'rm -f "${TMP}"' EXIT

echo "start test: qps=${QPS}, duration=${DURATION_SEC}s, total=$((QPS * DURATION_SEC))"
for sec in $(seq 1 "${DURATION_SEC}"); do
  seq "${QPS}" | xargs -P "${QPS}" -I{} bash -lc '
    curl -s -o /dev/null -w "code=%{http_code} time=%{time_total}\n" \
      -H "Content-Type: application/json" \
      --data-binary @"'"${PAYLOAD}"'" \
      "'"${URL}"'"
  ' >> "${TMP}"

  echo "sent second ${sec}/${DURATION_SEC}"
  sleep 1
done

awk '
BEGIN {ok=0; fail=0; sum=0; n=0}
{
  split($1,a,"="); split($2,b,"=");
  code=a[2]+0; t=b[2]+0;
  n++; sum+=t;
  if (code==200) ok++; else fail++;
}
END {
  if (n==0) n=1;
  printf("RESULT total=%d ok=%d fail=%d avg=%.4fs\n", ok+fail, ok, fail, sum/n);
}
' "${TMP}"

echo "---- metrics ----"
curl -s http://127.0.0.1:8081/api/aggregate/metrics | sed 's/,/\n/g'
