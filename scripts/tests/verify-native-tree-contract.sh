#!/usr/bin/env bash
# Adversarial source-free test. No private runtime sources are loaded.
set -Eeuo pipefail
verifier="$(realpath "${1:-$(dirname "${BASH_SOURCE[0]}")/../verify-native-tree.sh}")"
root="$(mktemp -d "${TMPDIR:-/tmp}/durable-tree-fixture.XXXXXXXX")"
trap 'rm -rf -- "$root"' EXIT
cd "$root"
git init -q
git config user.name 'tree-guard-smoke'
git config user.email 'tree-guard-smoke@example.invalid'
mkdir -p src
printf 'sample\n' > src/fixture.txt
printf 'another\n' > ordinary.txt
printf 'filter=not-enabled\n' > unused.txt
printf 'src/fixture.txt filter=poison\n' > .gitattributes
printf '*.junk\n' > .gitignore
git add .
git commit -qm fixture
marker="$root/clean-filter-was-executed"
git config filter.poison.clean "sh -c 'touch \"$marker\"; cat'"
passed=0
pass() { passed=$((passed+1)); echo "PASS $1"; }
refuse() {
  local name="$1"; shift
  rm -f -- "$marker" # observe only the verifier, not fixture setup commands
  if bash "$verifier" "$root" > "$root/.git/guard-output" 2>&1; then
    echo "FAIL: $name accepted by verifier" >&2; exit 1
  fi
  [[ ! -e "$marker" ]] || { echo "FAIL: filter executed for $name" >&2; exit 1; }
  pass "$name"
}
bash "$verifier" "$root" > "$root/.git/guard-output"
[[ ! -e "$marker" ]] || { echo 'Clean filter executed on clean fixture' >&2; exit 1; }
pass clean-and-no-filter-exec
printf 'tamper\n' >> src/fixture.txt
refuse changed-raw-file
git show HEAD:src/fixture.txt > src/fixture.txt
chmod +x src/fixture.txt
refuse unexpected-executable-bit
chmod -x src/fixture.txt
mv src/fixture.txt src/fixture.backup
refuse missing-tracked-file
mv src/fixture.backup src/fixture.txt
printf 'malicious\n' > not-tracked.junk
refuse ignored-untracked-file
rm not-tracked.junk
printf 'new\n' > ordinary.txt
git add ordinary.txt
refuse staged-index-mismatch
git show HEAD:ordinary.txt > ordinary.txt
git reset -q -- ordinary.txt
ln -s ordinary.txt symlink.junk
refuse ignored-untracked-symlink
rm symlink.junk
rm ordinary.txt
ln -s src/fixture.txt ordinary.txt
refuse symlink-replacement
rm ordinary.txt
git show HEAD:ordinary.txt > ordinary.txt
rm -f -- "$marker"
bash "$verifier" "$root" > "$root/.git/guard-output"
[[ ! -e "$marker" ]] || { echo 'Clean filter executed during recovered fixture' >&2; exit 1; }
pass recovered-clean-tree
echo "SOURCE_FREE_RAW_TREE_GUARD_TESTS_PASSED=$passed"
