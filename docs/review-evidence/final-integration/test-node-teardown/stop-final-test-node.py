#!/usr/bin/env python3
"""Reversibly stop only this task's verified disposable node; delete nothing."""
from pathlib import Path
import datetime
import json
import os
import subprocess

root = Path(__file__).resolve().parents[2]
out = root / 'docs/review-evidence/final-integration/test-node-teardown'
out.mkdir(exist_ok=True)
env = os.environ.copy()
env.pop('DOCKER_CONTEXT', None)
env['DOCKER_CONFIG'] = str(root / '.generated/docker-public-config')
env['DOCKER_HOST'] = 'unix:///Users/dangoodman/.docker/run/docker.sock'
node_id = '18064c76de5e22f067c09ef2d69c239c8ae58e98eacc42f4a1513b2d5163c74d'
cluster = 'sleepypods-review-remediation'
expected_created = '2026-09-07T21:06:58.786693677Z'

def run(args, timeout=20):
    return subprocess.run(args, env=env, text=True, capture_output=True,
                          timeout=timeout, check=True).stdout

def require(condition, message):
    if not condition:
        raise SystemExit(message)

def inspect_nodes():
    ids = run(['docker', 'ps', '-aq']).split()
    raw = json.loads(run(['docker', 'inspect', *ids])) if ids else []
    return [dict(id=x['Id'], name=x['Name'], created=x['Created'],
                 labels=x['Config'].get('Labels'), mounts=x['Mounts'],
                 running=x['State']['Running'], status=x['State']['Status'])
            for x in raw]

record = dict(started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
              action='Stop the exact task node; preserve container and volume.',
              rationale='Reversible final teardown avoids the earlier declined diagnostic deletions. No Kubernetes objects, PVs, RBAC, containers or volumes are deleted.',
              node_id=node_id)
before = inspect_nodes()
(out / 'containers-before.json').write_text(json.dumps(before, indent=2)+'\n')
node = next(x for x in before if x['id'] == node_id)
require(node['name'] == '/' + cluster + '-control-plane', 'Node name changed')
require(node['created'] == expected_created, 'Node creation changed')
require(node['labels']['io.x-k8s.kind.cluster'] == cluster, 'Cluster label changed')
require(node['labels']['io.x-k8s.kind.role'] == 'control-plane', 'Node role changed')
require(node['running'], 'Node is already stopped')
kubeconfig = str(root / '.generated/implementation-evidence/kubeconfig')
kube = ['kubectl', '--kubeconfig', kubeconfig, '--context', 'kind-'+cluster,
        '--request-timeout=10s']
inventory = json.loads(run([*kube, 'get', 'namespaces,persistentvolumes', '-o', 'json']))
(out / 'kubernetes-before.json').write_text(json.dumps(inventory, indent=2)+'\n')
expected = json.loads((root / '.generated/implementation-evidence/final-cleanup-resource-inventory-latest.json').read_text())
def identities(value):
    return sorted((x['kind'], x['metadata']['name'], x['metadata']['uid'])
                  for x in value['items'])
require(identities(inventory) == identities(expected), 'Resource inventory changed; investigate before stop')
record['command'] = ['docker', 'stop', '--time', '15', node_id]
record['output'] = run(record['command'], timeout=45).strip()
after = inspect_nodes()
(out / 'containers-after.json').write_text(json.dumps(after, indent=2)+'\n')
require({x['id'] for x in before} == {x['id'] for x in after}, 'Container inventory changed')
for x in before:
    y = next(y for y in after if y['id'] == x['id'])
    if x['id'] == node_id:
        require(not y['running'] and y['mounts'] == x['mounts'], 'Node stop or mount preservation failed')
    else:
        require(y == x, f'Unexpected unrelated container change: {x["name"]}')
record.update(exit_code=0, node_stopped=True, containers_deleted=0,
              volumes_deleted=0, other_container_metadata_unchanged=True,
              finished_utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
(out / 'result.json').write_text(json.dumps(record, indent=2)+'\n')
print(json.dumps(record, indent=2))
