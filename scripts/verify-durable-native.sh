#!/usr/bin/env bash
# Exact-tree, source-local native verification. No source export or external token.
set -Eeuo pipefail
[[ "${EXPECTED_HEAD_SHA:-}" =~ ^[0-9a-fA-F]{40}$ ]] || { echo 'Set EXPECTED_HEAD_SHA to a 40-character SHA' >&2; exit 64; }
root="$(git -C "$(dirname "${BASH_SOURCE[0]}")/.." rev-parse --show-toplevel)"
cd "$root"
actual="$(git rev-parse HEAD)"
[[ "$actual" == "$EXPECTED_HEAD_SHA" ]] || { echo "Wrong HEAD: $actual" >&2; exit 65; }
[[ -z "$(git status --porcelain --untracked-files=all)" ]] || { echo 'Refusing dirty checkout' >&2; exit 66; }
actual_tree="$(git rev-parse 'HEAD^{tree}')"
# A vetted cross-repository mirror has a different commit SHA but must keep
# the source tree bit-for-bit identical. This is a separate, fail-closed gate:
# EXPECTED_HEAD_SHA authenticates the *carrier commit*; this authenticates
# the reviewed source tree (including executable bits and workflow files).
if [[ -n "${EXPECTED_SOURCE_TREE_SHA:-}" ]]; then
  [[ "$EXPECTED_SOURCE_TREE_SHA" =~ ^[0-9a-fA-F]{40}$ ]] || { echo 'Malformed EXPECTED_SOURCE_TREE_SHA' >&2; exit 67; }
  [[ "${actual_tree,,}" == "${EXPECTED_SOURCE_TREE_SHA,,}" ]] || {
    echo "Source tree mismatch: actual $actual_tree expected $EXPECTED_SOURCE_TREE_SHA" >&2
    exit 68
  }
fi
for tool in cmake ctest g++ clang++ git; do command -v "$tool" >/dev/null || { echo "Missing $tool" >&2; exit 69; }; done
report="${DURABLE_REPORT_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/ores-durable-verify.XXXXXXXX")}" 
mkdir -p "$report"; report="$(cd "$report" && pwd -P)"
[[ "$report" != "$root" && "$report" != "$root/"* ]] || { echo 'Report directory must be outside checkout' >&2; exit 66; }
{
  echo "HEAD=$actual"
  echo "TREE=$actual_tree"
  echo "REVIEWED_SOURCE_TREE=${EXPECTED_SOURCE_TREE_SHA:-not-supplied}"
  echo "UTC_STARTED=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  cmake --version | head -1
  g++ --version | head -1
  clang++ --version | head -1
  git ls-tree -r HEAD -- native CMakeLists.txt tests src/native .github/workflows
} > "$report/provenance.txt"
cat "$report/provenance.txt"
finish() { rc=$?; echo "EXIT_STATUS=$rc" >> "$report/provenance.txt"; echo "UTC_FINISHED=$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$report/provenance.txt"; if ((rc == 0)); then echo passed > "$report/overall-status.txt"; else echo failed > "$report/overall-status.txt"; fi; echo "Reports at $report"; }
trap finish EXIT
# Every CTest configuration must discover the complete native regression set.
# CTest may otherwise return success even when a build registers zero tests.
expected_tests=(
  sqlite_durable_storage_test
  durable_isolate_queues
  durable_persisted_fence_guard
  durable_tenant_fault_quarantine
  durable_sqlite_vfs_fault
)
run_ctest() {
  local tag="$1" compiler="$2" type="$3" sanitizer="$4"
  local flags='-Wall -Wextra -Wpedantic -Werror -UNDEBUG' link=''
  case "$sanitizer" in
    none) ;;
    asan-ubsan) flags="$flags -O1 -g -fno-omit-frame-pointer -fsanitize=address,undefined"; link='-fsanitize=address,undefined' ;;
    tsan) flags="$flags -O1 -g -fno-omit-frame-pointer -fsanitize=thread"; link='-fsanitize=thread' ;;
    *) echo "Invalid sanitizer: $sanitizer" >&2; return 64 ;;
  esac
  local build="$report/build-$tag"
  echo "START $tag" | tee -a "$report/steps.txt"
  cmake -S "$root" -B "$build" -DCMAKE_BUILD_TYPE="$type" -DBUILD_TESTING=ON -DCMAKE_CXX_COMPILER="$compiler" "-DCMAKE_CXX_FLAGS=$flags" "-DCMAKE_EXE_LINKER_FLAGS=$link" > "$report/$tag.configure.log" 2>&1 || { cat "$report/$tag.configure.log"; return 1; }
  cmake --build "$build" --parallel 2 > "$report/$tag.build.log" 2>&1 || { cat "$report/$tag.build.log"; return 1; }
  ctest --test-dir "$build" --show-only > "$report/$tag.discovery.log" 2>&1 || { cat "$report/$tag.discovery.log"; return 1; }
  local count expected
  count="$(grep -Ec '^[[:space:]]*Test[[:space:]]*#[0-9]+:' "$report/$tag.discovery.log" || true)"
  if [[ "$count" -ne "${#expected_tests[@]}" ]] ||
     ! grep -Eq "Total Tests: ${#expected_tests[@]}$" "$report/$tag.discovery.log"; then
    echo "CTest manifest mismatch ($tag): discovered $count, expected ${#expected_tests[@]}" >&2
    cat "$report/$tag.discovery.log" >&2
    return 1
  fi
  for expected in "${expected_tests[@]}"; do
    if ! grep -Eq "^[[:space:]]*Test[[:space:]]*#[0-9]+:[[:space:]]+${expected}$" "$report/$tag.discovery.log"; then
      echo "CTest missing required regression ($tag): $expected" >&2
      cat "$report/$tag.discovery.log" >&2
      return 1
    fi
  done
  echo "DISCOVERED $tag TESTS=$count" | tee -a "$report/steps.txt"
  (export ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1 TSAN_OPTIONS=halt_on_error=1; ctest --test-dir "$build" --output-on-failure --verbose) > "$report/$tag.ctest.log" 2>&1 || { cat "$report/$tag.ctest.log"; return 1; }
  echo "PASS $tag HEAD=$actual" | tee -a "$report/steps.txt"
}
run_ctest gcc-debug g++ Debug none
run_ctest gcc-release g++ Release none
run_ctest clang-debug clang++ Debug none
run_ctest clang-release clang++ Release none
run_ctest gcc-asan-ubsan g++ Debug asan-ubsan
run_ctest clang-asan-ubsan clang++ Debug asan-ubsan
run_ctest gcc-tsan g++ Debug tsan
echo "EXACT_HEAD_NATIVE_TESTS_PASSED=$actual" | tee -a "$report/steps.txt"
