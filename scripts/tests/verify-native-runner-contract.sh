#!/usr/bin/env bash
# Source-free contract test for the SHA-pinned durable verifier.
# It never checks out, copies or executes the private Durable Objects runtime.
set -Eeuo pipefail
verifier="${1:-$(dirname "${BASH_SOURCE[0]}")/../verify-durable-native.sh}"
verifier="$(realpath "$verifier")"
bash -n "$verifier"
tree_guard="$(dirname "$verifier")/verify-native-tree.sh"
bash -n "$tree_guard"
# Independent adversarial source-free check, before mock CMake execution.
bash "$(dirname "${BASH_SOURCE[0]}")/verify-native-tree-contract.sh" "$tree_guard"
for tool in cmake ctest g++ clang++ git mktemp; do
  command -v "$tool" >/dev/null || { echo "missing tool: $tool" >&2; exit 69; }
done
work="$(mktemp -d "${TMPDIR:-/tmp}/durable-verifier-contract.XXXXXXXX")"
trap 'rm -rf "$work"' EXIT
repo="$work/mock-repo"
mkdir -p "$repo/scripts"
cp "$verifier" "$repo/scripts/verify-durable-native.sh"
cp "$tree_guard" "$repo/scripts/verify-native-tree.sh"
cat > "$repo/CMakeLists.txt" <<'CMAKE'
cmake_minimum_required(VERSION 3.20)
project(source_free_verifier_contract LANGUAGES CXX)
include(CTest)
file(WRITE "${CMAKE_BINARY_DIR}/mock.cpp" "int main(){return 0;}\n")
add_executable(mock "${CMAKE_BINARY_DIR}/mock.cpp")
foreach(name IN ITEMS
    sqlite_durable_storage_test
    durable_isolate_queues
    durable_persisted_fence_guard
    durable_tenant_fault_quarantine
    durable_sqlite_vfs_fault)
  add_test(NAME "${name}" COMMAND mock)
endforeach()
CMAKE
(
  cd "$repo"
  git init -q
  git config user.name 'source-free verifier test'
  git config user.email 'source-free-verifier@example.invalid'
  git add .
  git commit -qm mock
)
sha="$(git -C "$repo" rev-parse HEAD)"
verify() {
  EXPECTED_HEAD_SHA="$sha" DURABLE_REPORT_DIR="$2" \
    bash "$repo/scripts/verify-durable-native.sh" > "$3" 2>&1
}
expect_failure() {
  local label="$1" expected="$2" actual
  shift 2
  set +e
  "$@"
  actual=$?
  set -e
  if [[ "$actual" != "$expected" ]]; then
    echo "$label expected exit $expected, got $actual" >&2
    exit 1
  fi
  echo "PASS negative:$label exit=$actual"
}
expect_failure malformed-sha 64 env EXPECTED_HEAD_SHA=abc \
  bash "$repo/scripts/verify-durable-native.sh"
touch "$repo/dirty"
expect_failure dirty-checkout 66 verify dirty "$work/dirty-report" "$work/dirty.log"
rm "$repo/dirty"
expect_failure report-inside-source 66 verify inside "$repo/report" "$work/inside.log"
verify positive "$work/positive-report" "$work/positive.log" || {
  cat "$work/positive.log" >&2
  exit 1
}
[[ "$(cat "$work/positive-report/overall-status.txt")" == passed ]]
[[ "$(grep -c '^PASS ' "$work/positive-report/steps.txt")" == 7 ]]
[[ "$(grep -c '^DISCOVERED ' "$work/positive-report/steps.txt")" == 7 ]]
[[ "$(grep -c 'TESTS=5$' "$work/positive-report/steps.txt")" == 7 ]]
[[ "$(grep -c '^EXACT_HEAD_NATIVE_TESTS_PASSED=' "$work/positive-report/steps.txt")" == 1 ]]
echo 'PASS positive: all seven mock profiles executed five CTest suites'
# Remove one required suite but keep syntactically valid CMake source.
sed -i 's/durable_sqlite_vfs_fault)/)/' "$repo/CMakeLists.txt"
(
  cd "$repo"
  git add CMakeLists.txt
  git commit -qm 'remove required CTest suite'
)
sha="$(git -C "$repo" rev-parse HEAD)"
expect_failure missing-suite 1 verify missing "$work/missing-report" "$work/missing.log"
[[ "$(cat "$work/missing-report/overall-status.txt")" == failed ]]
! grep -q '^EXACT_HEAD_NATIVE_TESTS_PASSED=' "$work/missing-report/steps.txt"
echo 'PASS negative: missing suite cannot produce success receipt'
echo 'SOURCE_FREE_VERIFIER_CONTRACT_PASSED'
