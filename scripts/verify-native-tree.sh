#!/usr/bin/env bash
# Verify HEAD, index and raw worktree without git status/diff or clean/smudge filters.
set -Eeuo pipefail
export LC_ALL=C GIT_OPTIONAL_LOCKS=0
root="$(cd "${1:-.}" && pwd -P)"
[[ "$(git -c core.fsmonitor=false -C "$root" rev-parse --show-toplevel)" == "$root" ]] || {
  echo 'Expected repository top-level directory' >&2; exit 70;
}
work="$(mktemp -d "${TMPDIR:-/tmp}/durable-tree-guard.XXXXXXXX")"
trap 'rm -rf -- "$work"' EXIT
# Neither command updates the index or compares worktree content via filters.
git -c core.fsmonitor=false -C "$root" ls-tree -r -z --full-tree HEAD > "$work/tree"
git -c core.fsmonitor=false -C "$root" ls-files --stage -z > "$work/index"
git -c core.fsmonitor=false -C "$root" ls-files --others -z > "$work/others"
if [[ -s "$work/others" ]]; then
  echo 'Untracked or ignored worktree entries are forbidden' >&2; exit 71
fi
exec 3< "$work/tree"
exec 4< "$work/index"
count=0
while IFS= read -r -d '' tree_entry <&3; do
  IFS= read -r -d '' index_entry <&4 || { echo 'Index has fewer entries than HEAD' >&2; exit 72; }
  [[ "$tree_entry" == *$'\t'* && "$index_entry" == *$'\t'* ]] || {
    echo 'Malformed Git tree/index record' >&2; exit 72;
  }
  tree_meta="${tree_entry%%$'\t'*}"
  path="${tree_entry#*$'\t'}"
  index_meta="${index_entry%%$'\t'*}"
  index_path="${index_entry#*$'\t'}"
  [[ "$tree_meta" =~ ^(100644|100755)[[:space:]]blob[[:space:]]([0-9a-f]{40}|[0-9a-f]{64})$ ]] || {
    echo "Unsupported Git entry mode or object: $path" >&2; exit 72;
  }
  mode="${BASH_REMATCH[1]}"
  oid="${BASH_REMATCH[2]}"
  [[ "$path" == "$index_path" && "$index_meta" == "$mode $oid 0" ]] || {
    echo "Index differs from HEAD: $path" >&2; exit 73;
  }
  # No symlinks, submodules or ancestor symlink traversal in native build input.
  [[ -f "$root/$path" && ! -L "$root/$path" ]] || {
    echo "Tracked source is missing, a symlink or not a file: $path" >&2; exit 74;
  }
  ancestor="${path%/*}"
  if [[ "$ancestor" != "$path" ]]; then
    while :; do
      [[ -d "$root/$ancestor" && ! -L "$root/$ancestor" ]] || {
        echo "Source parent is not an actual directory: $ancestor" >&2; exit 74;
      }
      [[ "$ancestor" == */* ]] || break
      ancestor="${ancestor%/*}"
    done
  fi
  if [[ "$mode" == 100755 ]]; then
    [[ -x "$root/$path" ]] || { echo "Executable bit lost: $path" >&2; exit 75; }
  else
    [[ ! -x "$root/$path" ]] || { echo "Unexpected executable bit: $path" >&2; exit 75; }
  fi
  # --no-filters is critical. git status/diff or normal hash-object can run
  # user-controlled filter.<name>.clean commands during a preflight.
  raw_oid="$(git -c core.fsmonitor=false -C "$root" hash-object --no-filters -- "$root/$path")" || exit 76
  [[ "$raw_oid" == "$oid" ]] || { echo "Raw file bytes differ from HEAD: $path" >&2; exit 76; }
  count=$((count+1))
done
if IFS= read -r -d '' extra <&4; then
  echo 'Index has extra entries not in HEAD' >&2; exit 73
fi
[[ "$count" -gt 0 ]] || { echo 'Refusing empty source tree' >&2; exit 77; }
echo "RAW_GIT_TREE_INDEX_WORKTREE_VERIFIED files=$count"
