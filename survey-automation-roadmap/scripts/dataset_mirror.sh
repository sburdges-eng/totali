#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

EXTERNAL_ROOT_DEFAULT="/Volumes/KmiDi-external/survey-automation-roadmap/datasets"
EXTERNAL_ROOT="${EXTERNAL_DATA_ROOT:-$EXTERNAL_ROOT_DEFAULT}"
LOCAL_MIRROR_ROOT="${LOCAL_MIRROR_ROOT:-$REPO_ROOT/.local-datasets}"

DATA_ITEMS=("TOTaLi" "data survey.zip")

require_external_root() {
  if [[ "$EXTERNAL_ROOT" == /Volumes/* ]]; then
    mount_name="${EXTERNAL_ROOT#/Volumes/}"
    mount_name="${mount_name%%/*}"
    mount_root="/Volumes/$mount_name"
    if [[ ! -d "$mount_root" ]]; then
      echo "External drive mount not found: $mount_root" >&2
      exit 1
    fi
  fi
  mkdir -p "$EXTERNAL_ROOT"
}

sync_item() {
  local src="$1"
  local dst="$2"

  if [[ -d "$src" ]]; then
    mkdir -p "$dst"
    rsync -a --delete "$src/" "$dst/"
  elif [[ -f "$src" ]]; then
    mkdir -p "$(dirname "$dst")"
    rsync -a "$src" "$dst"
  else
    echo "Skip missing source: $src"
  fi
}

bootstrap_external() {
  require_external_root
  for item in "${DATA_ITEMS[@]}"; do
    local_path="$REPO_ROOT/$item"
    external_path="$EXTERNAL_ROOT/$item"

    if [[ -L "$local_path" ]]; then
      rm "$local_path"
    fi

    if [[ -e "$local_path" ]]; then
      if [[ -e "$external_path" ]]; then
        echo "Syncing local -> external for existing target: $item"
        sync_item "$local_path" "$external_path"
        if [[ -d "$local_path" ]]; then
          rm -rf "$local_path"
        else
          rm -f "$local_path"
        fi
      else
        echo "Moving local -> external: $item"
        mv "$local_path" "$external_path"
      fi
    fi

    if [[ -e "$external_path" ]]; then
      ln -sfn "$external_path" "$local_path"
      echo "Linked $local_path -> $external_path"
    fi
  done
}

sync_external_to_local() {
  require_external_root
  mkdir -p "$LOCAL_MIRROR_ROOT"

  for item in "${DATA_ITEMS[@]}"; do
    external_path="$EXTERNAL_ROOT/$item"
    local_copy="$LOCAL_MIRROR_ROOT/$item"
    echo "Sync external -> local mirror: $item"
    sync_item "$external_path" "$local_copy"
  done
}

sync_local_to_external() {
  require_external_root

  for item in "${DATA_ITEMS[@]}"; do
    local_copy="$LOCAL_MIRROR_ROOT/$item"
    external_path="$EXTERNAL_ROOT/$item"
    echo "Sync local mirror -> external: $item"
    sync_item "$local_copy" "$external_path"
  done
}

switch_to_external() {
  require_external_root
  for item in "${DATA_ITEMS[@]}"; do
    local_path="$REPO_ROOT/$item"
    external_path="$EXTERNAL_ROOT/$item"
    if [[ -e "$external_path" ]]; then
      ln -sfn "$external_path" "$local_path"
      echo "Linked $local_path -> $external_path"
    else
      echo "External item missing, not linked: $external_path"
    fi
  done
}

switch_to_local() {
  mkdir -p "$LOCAL_MIRROR_ROOT"
  for item in "${DATA_ITEMS[@]}"; do
    local_path="$REPO_ROOT/$item"
    local_copy="$LOCAL_MIRROR_ROOT/$item"
    if [[ -e "$local_copy" ]]; then
      ln -sfn "$local_copy" "$local_path"
      echo "Linked $local_path -> $local_copy"
    else
      echo "Local mirror item missing, not linked: $local_copy"
    fi
  done
}

status_report() {
  echo "Repo root: $REPO_ROOT"
  echo "External root: $EXTERNAL_ROOT"
  echo "Local mirror root: $LOCAL_MIRROR_ROOT"

  for item in "${DATA_ITEMS[@]}"; do
    local_path="$REPO_ROOT/$item"
    external_path="$EXTERNAL_ROOT/$item"
    local_copy="$LOCAL_MIRROR_ROOT/$item"

    echo ""
    echo "Item: $item"
    if [[ -L "$local_path" ]]; then
      echo "  local: symlink -> $(readlink "$local_path")"
    elif [[ -e "$local_path" ]]; then
      echo "  local: present (not symlink)"
    else
      echo "  local: missing"
    fi

    [[ -e "$external_path" ]] && echo "  external: present" || echo "  external: missing"
    [[ -e "$local_copy" ]] && echo "  local-mirror: present" || echo "  local-mirror: missing"
  done
}

usage() {
  cat <<USAGE
Usage: $0 <command>

Commands:
  bootstrap-external       Move dataset items to external root and symlink from repo
  sync-external-to-local   Mirror external dataset items into local mirror directory
  sync-local-to-external   Mirror local mirror dataset items back to external root
  switch-to-external       Point repo dataset symlinks to external root
  switch-to-local          Point repo dataset symlinks to local mirror copies
  status                   Print current dataset location state
USAGE
}

if [[ $# -ne 1 ]]; then
  usage
  exit 1
fi

case "$1" in
  bootstrap-external) bootstrap_external ;;
  sync-external-to-local) sync_external_to_local ;;
  sync-local-to-external) sync_local_to_external ;;
  switch-to-external) switch_to_external ;;
  switch-to-local) switch_to_local ;;
  status) status_report ;;
  *)
    usage
    exit 1
    ;;
esac
