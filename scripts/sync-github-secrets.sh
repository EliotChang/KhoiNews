#!/usr/bin/env bash

set -eo pipefail

SCRIPT_NAME="$(basename "$0")"

ENV_FILE=".env"
REPO=""
EFFECTIVE_REPO=""
DRY_RUN=false
PRUNE=true
CONFIRM_PRUNE=false

updated_count=0
deleted_count=0
skipped_count=0

ENV_KEYS=()
ENV_VALUES=()
REMOTE_KEYS=()

print_usage() {
  cat <<EOF
Usage:
  $SCRIPT_NAME [options]

Options:
  --env-file <path>        Path to env file (default: .env)
  --repo <owner/name>      GitHub repo override (default: current gh repo)
  --dry-run                Preview changes without applying
  --no-prune               Do not delete repo secrets missing from env file
  --confirm-prune          Required to allow deletions when prune is enabled
  --help                   Show this help message

Examples:
  $SCRIPT_NAME --dry-run --confirm-prune
  $SCRIPT_NAME --confirm-prune
  $SCRIPT_NAME --repo figment/news --env-file .env.production --confirm-prune
EOF
}

is_valid_secret_name() {
  local key="$1"
  [[ "$key" =~ ^[A-Z_][A-Z0-9_]*$ ]]
}

contains_key() {
  local target="$1"
  shift
  local item
  for item in "$@"; do
    if [[ "$item" == "$target" ]]; then
      return 0
    fi
  done
  return 1
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --env-file)
        ENV_FILE="${2:-}"
        shift 2
        ;;
      --repo)
        REPO="${2:-}"
        shift 2
        ;;
      --dry-run)
        DRY_RUN=true
        shift
        ;;
      --no-prune)
        PRUNE=false
        shift
        ;;
      --confirm-prune)
        CONFIRM_PRUNE=true
        shift
        ;;
      --help|-h)
        print_usage
        exit 0
        ;;
      *)
        echo "Unknown argument: $1" >&2
        print_usage
        exit 1
        ;;
    esac
  done
}

parse_env_file() {
  if [[ ! -f "$ENV_FILE" ]]; then
    echo "Env file not found: $ENV_FILE" >&2
    exit 1
  fi

  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%$'\r'}"
    [[ -z "$line" ]] && continue
    [[ "$line" =~ ^[[:space:]]*# ]] && continue

    line="${line#export }"
    if [[ "$line" != *"="* ]]; then
      ((skipped_count += 1))
      echo "Skipping invalid env line (missing '='): $line"
      continue
    fi

    local key="${line%%=*}"
    local value="${line#*=}"

    key="$(printf '%s' "$key" | sed -E 's/^[[:space:]]+//; s/[[:space:]]+$//')"

    if [[ -z "$key" ]]; then
      ((skipped_count += 1))
      echo "Skipping env line with empty key"
      continue
    fi

    if ! is_valid_secret_name "$key"; then
      ((skipped_count += 1))
      echo "Skipping invalid secret key '$key' (expected ^[A-Z_][A-Z0-9_]*$)"
      continue
    fi

    if contains_key "$key" "${ENV_KEYS[@]}"; then
      local idx=0
      local existing
      for existing in "${ENV_KEYS[@]}"; do
        if [[ "$existing" == "$key" ]]; then
          ENV_VALUES[$idx]="$value"
          break
        fi
        idx=$((idx + 1))
      done
      continue
    fi

    ENV_KEYS+=("$key")
    ENV_VALUES+=("$value")
  done < "$ENV_FILE"

  if [[ ${#ENV_KEYS[@]} -eq 0 ]]; then
    echo "No valid env keys found in $ENV_FILE" >&2
    exit 1
  fi
}

repo_arg() {
  if [[ -n "$EFFECTIVE_REPO" ]]; then
    printf -- "--repo=%s" "$EFFECTIVE_REPO"
    return 0
  fi
  return 1
}

resolve_repo() {
  if [[ -n "$REPO" ]]; then
    EFFECTIVE_REPO="$REPO"
    return
  fi

  if ! EFFECTIVE_REPO="$(gh repo view --json nameWithOwner -q .nameWithOwner 2>/dev/null)"; then
    echo "Unable to resolve repository. Pass --repo <owner/name>." >&2
    exit 1
  fi
}

sync_secrets() {
  local repo_opt
  repo_opt="$(repo_arg)"

  echo "Syncing ${#ENV_KEYS[@]} secret(s) from $ENV_FILE into $EFFECTIVE_REPO"
  if $DRY_RUN; then
    echo "Dry-run mode: no GitHub changes will be applied"
  fi

  local idx=0
  while [[ $idx -lt ${#ENV_KEYS[@]} ]]; do
    local key="${ENV_KEYS[$idx]}"
    local value="${ENV_VALUES[$idx]}"
    if $DRY_RUN; then
      echo "[dry-run] set secret: $key"
    else
      printf '%s' "$value" | gh secret set "$key" "$repo_opt"
    fi
    ((updated_count += 1))
    idx=$((idx + 1))
  done
}

collect_remote_secret_names() {
  local repo_opt
  repo_opt="$(repo_arg)"

  local list_output
  if ! list_output="$(gh secret list "$repo_opt" 2>/dev/null)"; then
    echo "Failed to list GitHub secrets. Ensure gh is authenticated and repo access is configured." >&2
    exit 1
  fi

  local line
  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    local key
    key="$(printf '%s' "$line" | awk '{print $1}')"
    [[ -z "$key" ]] && continue
    REMOTE_KEYS+=("$key")
  done <<< "$list_output"
}

prune_missing_secrets() {
  if ! $PRUNE; then
    echo "Prune disabled (--no-prune)"
    return
  fi

  if ! $CONFIRM_PRUNE; then
    echo "Prune requested but --confirm-prune was not provided; refusing to delete secrets." >&2
    exit 1
  fi

  collect_remote_secret_names

  local repo_opt
  repo_opt="$(repo_arg)"

  local key
  for key in "${REMOTE_KEYS[@]}"; do
    if contains_key "$key" "${ENV_KEYS[@]}"; then
      continue
    fi

    if $DRY_RUN; then
      echo "[dry-run] delete secret: $key"
    else
      gh secret delete "$key" "$repo_opt"
    fi
    ((deleted_count += 1))
  done
}

print_summary() {
  echo
  echo "Summary"
  echo "  Upserted: $updated_count"
  echo "  Deleted:  $deleted_count"
  echo "  Skipped:  $skipped_count"
}

main() {
  parse_args "$@"
  resolve_repo

  parse_env_file
  sync_secrets
  prune_missing_secrets
  print_summary
}

main "$@"
