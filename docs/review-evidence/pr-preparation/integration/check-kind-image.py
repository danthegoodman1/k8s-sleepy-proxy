from pathlib import Path
import subprocess,tempfile,json,os
root=Path.cwd()
helper=root/'scripts/lib/kind-image.sh'
shell=r'''
set -euo pipefail
source "$1"
kind() {
  printf 'kind' >> "$CALLS"
  printf '\t%s' "$@" >> "$CALLS"
  printf '\n' >> "$CALLS"
  if [[ "$1 $2" == 'load docker-image' ]]; then
    [[ "$CASE" == direct ]]
    return $?
  fi
  [[ "$1 $2" == 'load image-archive' ]] || return 99
  [[ -s "$3" ]] || return 98
  if [[ "$CASE" == import_error ]]; then return 31; fi
}
docker() {
  printf 'docker' >> "$CALLS"
  printf '\t%s' "$@" >> "$CALLS"
  printf '\n' >> "$CALLS"
  if [[ "$1" == version ]]; then
    printf 'linux/arm64\n'
    return 0
  fi
  [[ "$1" == save && "$2" == --platform && "$3" == linux/arm64 && "$4" == image:test && "$5" == --output ]] || return 97
  printf '%s\n' "$6" > "$ARCHIVE_PATH"
  printf 'owned archive\n' > "$6"
  if [[ "$CASE" == save_error ]]; then return 23; fi
}
kind_load_image 'test-cluster' 'image:test'
'''
results=[]
for case,expected in [('direct',0),('fallback',0),('save_error',23),('import_error',31)]:
    with tempfile.TemporaryDirectory(prefix='kind-image-mocks-') as td:
        p=Path(td)
        env=os.environ|{'CASE':case,'CALLS':str(p/'calls'),'ARCHIVE_PATH':str(p/'archive'),'TMPDIR':td}
        result=subprocess.run(['bash','-c',shell,'mock-kind-image',str(helper)],env=env,capture_output=True,text=True,timeout=5)
        calls=(p/'calls').read_text().splitlines()
        assert result.returncode==expected,(case,result.returncode,result.stderr,calls)
        assert calls[0]=='kind\tload\tdocker-image\timage:test\t--name\ttest-cluster',calls
        if case=='direct':
            assert len(calls)==1,calls
        else:
            archive=Path((p/'archive').read_text().strip())
            assert not archive.exists(),(case,'archive leaked')
            assert calls[1]=="docker\tversion\t--format\t{{.Server.Os}}/{{.Server.Arch}}",calls
            assert calls[2]==f'docker\tsave\t--platform\tlinux/arm64\timage:test\t--output\t{archive}',calls
            if case=='save_error': assert len(calls)==3,calls
            else: assert calls[3]==f'kind\tload\timage-archive\t{archive}\t--name\ttest-cluster',calls
        results.append({'case':case,'exit':result.returncode,'commands':calls,'archive_cleanup_verified':case!='direct'})
paths=subprocess.check_output(['git','diff','--cached','--name-only'],text=True).splitlines()
scripts=[p for p in paths if p.endswith('.sh')]
for path in scripts:
    subprocess.run(['bash','-n',path],check=True,timeout=5)
print(json.dumps({'mock_cases':results,'syntax_checked':scripts,'real_docker_or_kind_calls':0},indent=2))
