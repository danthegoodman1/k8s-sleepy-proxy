#!/usr/bin/env bash
# Test-only platform identity, separate from application certificate publication.
# Caller owns the parent temporary directory and its cleanup. Nothing is printed.
generate_native_tls_fixture() {
  local directory="$1"
  local extra_dns="$2"
  [[ "${extra_dns}" =~ ^[A-Za-z0-9.-]+$ ]] || return 2
  (
    umask 077
    mkdir "${directory}"
    cat > "${directory}/openssl.cnf" <<CONFIG
[req]
distinguished_name=subject
x509_extensions=server
prompt=no
[subject]
CN=localhost
[server]
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature
extendedKeyUsage=serverAuth
subjectAltName=DNS:localhost,IP:127.0.0.1,DNS:${extra_dns}
CONFIG
    openssl req -new -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 \
      -nodes -days 2 -config "${directory}/openssl.cnf" \
      -keyout "${directory}/cp.key" -out "${directory}/cp.crt" \
      > "${directory}/openssl.log" 2>&1
    local key
    key="$(openssl rand -hex 32)"
    printf '{"active_id":"fixture","keys":[{"id":"fixture","key_hex":"%s"}]}\n' \
      "${key}" > "${directory}/sealing.json"
    for role in operator proxy sidecar; do
      local token
      token="$(openssl rand -hex 32)" || exit
      printf '%s' "${token}" > "${directory}/${role}.token"
    done
  )
}

# Retain only public runtime identity. Never persist Pod env values or Secrets.
capture_native_tls_pods() {
  local task_kubeconfig="$1"
  local task_namespace="$2"
  local output="$3"
  KUBECONFIG="${task_kubeconfig}" kubectl -n "${task_namespace}" get pods -o json | \
    python3 -c '
import json,sys
pods=json.load(sys.stdin)["items"]
result=[]
for pod in pods:
    labels=pod["metadata"].get("labels",{})
    name=labels.get("app.kubernetes.io/name", "")
    if name not in ("sleepypods-control-plane","sleepypods-frontline"):
        continue
    statuses={item["name"]:item for item in pod.get("status",{}).get("containerStatuses",[])}
    result.append({"name":pod["metadata"]["name"],"uid":pod["metadata"]["uid"],
        "namespace":pod["metadata"]["namespace"],"node":pod["spec"].get("nodeName"),
        "containers":[{"name":c["name"],"image":c["image"],
            "image_id":statuses.get(c["name"],{}).get("imageID"),
            "container_id":statuses.get(c["name"],{}).get("containerID")}
            for c in pod["spec"]["containers"]]})
json.dump(result,sys.stdout,indent=2); print()
' > "${output}" || return
  cat "${output}"
}
