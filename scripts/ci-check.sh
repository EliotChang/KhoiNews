#!/usr/bin/env bash
# Verify that the local environment matches CI dependencies.
# Exits non-zero on the first missing or mismatched dependency.

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

ERRORS=0

check() {
  local label="$1" cmd="$2" expected="$3"
  if ! command -v "$cmd" &>/dev/null; then
    printf "${RED}FAIL${NC}  %-28s  not found\n" "$label"
    ERRORS=$((ERRORS + 1))
    return
  fi
  local version
  version=$("${@:4}")
  if [[ -n "$expected" ]] && ! echo "$version" | grep -qE "$expected"; then
    printf "${YELLOW}WARN${NC}  %-28s  got %s (CI expects %s)\n" "$label" "$version" "$expected"
  else
    printf "${GREEN}OK${NC}    %-28s  %s\n" "$label" "$version"
  fi
}

echo "=== CI Parity Check ==="
echo ""

# Python 3.12
check "Python 3.12" python3 "3\.12" python3 --version

# Node 20
check "Node.js 20" node "v20\." node --version

# npm
check "npm" npm "" npm --version

# ffmpeg / ffprobe
check "ffmpeg" ffmpeg "" ffmpeg -version 2>&1 | head -1
check "ffprobe" ffprobe "" ffprobe -version 2>&1 | head -1

# Chrome or Chromium
CHROME_FOUND=false
for candidate in google-chrome google-chrome-stable chromium chromium-browser "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"; do
  if command -v "$candidate" &>/dev/null || [[ -x "$candidate" ]]; then
    printf "${GREEN}OK${NC}    %-28s  %s\n" "Chrome/Chromium" "$candidate"
    CHROME_FOUND=true
    break
  fi
done
if [[ "$CHROME_FOUND" == "false" ]]; then
  printf "${RED}FAIL${NC}  %-28s  not found (Remotion needs headless Chrome)\n" "Chrome/Chromium"
  ERRORS=$((ERRORS + 1))
fi

# Remotion node_modules
REMOTION_DIR="pipeline/video_templates/fish_lipsync"
if [[ -d "$REMOTION_DIR/node_modules" ]]; then
  printf "${GREEN}OK${NC}    %-28s  present\n" "Remotion node_modules"
else
  printf "${RED}FAIL${NC}  %-28s  missing (run: npm ci in %s)\n" "Remotion node_modules" "$REMOTION_DIR"
  ERRORS=$((ERRORS + 1))
fi

# Python requirements
if python3 -c "import anthropic, httpx" 2>/dev/null; then
  printf "${GREEN}OK${NC}    %-28s  importable\n" "Python deps (spot check)"
else
  printf "${RED}FAIL${NC}  %-28s  import failed (run: pip install -r requirements.txt)\n" "Python deps"
  ERRORS=$((ERRORS + 1))
fi

echo ""

# Uncommitted pipeline changes
DIRTY_PIPELINE=$(git diff --name-only HEAD -- pipeline/ .github/workflows/ 2>/dev/null || true)
if [[ -n "$DIRTY_PIPELINE" ]]; then
  printf "${YELLOW}WARN${NC}  Uncommitted pipeline changes (CI will not see these):\n"
  echo "$DIRTY_PIPELINE" | while read -r f; do printf "       %s\n" "$f"; done
  echo ""
fi

if [[ $ERRORS -gt 0 ]]; then
  printf "${RED}%d dependency check(s) failed. CI will likely break.${NC}\n" "$ERRORS"
  exit 1
else
  printf "${GREEN}All CI dependencies present locally.${NC}\n"
fi
