#!/bin/bash
#
# Clear Disk Space
# This script cleans up temporary files, old logs, and package cache
#
# Usage: ./clear_disk_space.sh [--target-host <host>] [--retention-days <days>]
#

set -e

# Default values
RETENTION_DAYS=7
TARGET_HOST="localhost"
DRY_RUN="false"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --target-host|--target_host)
            TARGET_HOST="$2"
            shift 2
            ;;
        --retention-days|--retention_days)
            RETENTION_DAYS="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN="true"
            shift
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

echo "=========================================="
echo "Clear Disk Space Utility"
echo "=========================================="
echo "Target: $TARGET_HOST"
echo "Retention: $RETENTION_DAYS days"
echo "Dry Run: $DRY_RUN"
echo "=========================================="

# Function to run command locally or via SSH
run_cmd() {
    if [[ "$TARGET_HOST" == "localhost" ]] || [[ "$TARGET_HOST" == "127.0.0.1" ]]; then
        eval "$1"
    else
        ssh "$TARGET_HOST" "$1"
    fi
}

# Check current disk usage
echo ""
echo "Current disk usage:"
run_cmd "df -h / /tmp /var 2>/dev/null || df -h /"

# Calculate space before cleanup
BEFORE=$(run_cmd "df / --output=used | tail -1" | tr -d ' ')

echo ""
echo "Starting cleanup..."

# 1. Clean old log files
echo ""
echo "[1/6] Cleaning old log files (older than $RETENTION_DAYS days)..."
if [[ "$DRY_RUN" == "true" ]]; then
    run_cmd "find /var/log -type f -name '*.log' -mtime +$RETENTION_DAYS 2>/dev/null | head -20" || true
    run_cmd "find /var/log -type f -name '*.gz' -mtime +$RETENTION_DAYS 2>/dev/null | head -20" || true
else
    run_cmd "sudo find /var/log -type f -name '*.log.*' -mtime +$RETENTION_DAYS -delete 2>/dev/null" || true
    run_cmd "sudo find /var/log -type f -name '*.gz' -mtime +$RETENTION_DAYS -delete 2>/dev/null" || true
fi

# 2. Clean temp files
echo ""
echo "[2/6] Cleaning temporary files..."
if [[ "$DRY_RUN" == "true" ]]; then
    run_cmd "du -sh /tmp 2>/dev/null" || true
else
    run_cmd "sudo rm -rf /tmp/* 2>/dev/null" || true
    run_cmd "sudo rm -rf /var/tmp/* 2>/dev/null" || true
fi

# 3. Clean package cache (apt)
echo ""
echo "[3/6] Cleaning package cache..."
if [[ "$DRY_RUN" == "true" ]]; then
    run_cmd "du -sh /var/cache/apt/archives 2>/dev/null" || true
else
    run_cmd "sudo apt-get clean 2>/dev/null" || true
    run_cmd "sudo apt-get autoremove -y 2>/dev/null" || true
fi

# 4. Clean journal logs
echo ""
echo "[4/6] Cleaning journal logs..."
if [[ "$DRY_RUN" == "true" ]]; then
    run_cmd "journalctl --disk-usage 2>/dev/null" || true
else
    run_cmd "sudo journalctl --vacuum-time=${RETENTION_DAYS}d 2>/dev/null" || true
fi

# 5. Clean Docker (if installed)
echo ""
echo "[5/6] Cleaning Docker resources..."
if [[ "$DRY_RUN" == "true" ]]; then
    run_cmd "docker system df 2>/dev/null" || echo "Docker not installed"
else
    run_cmd "docker system prune -f 2>/dev/null" || true
fi

# 6. Clean old kernels (be careful!)
echo ""
echo "[6/6] Checking for old kernels..."
run_cmd "dpkg -l 'linux-*' 2>/dev/null | grep -E '^ii' | wc -l" || true

# Calculate space after cleanup
AFTER=$(run_cmd "df / --output=used | tail -1" | tr -d ' ')

# Show results
echo ""
echo "=========================================="
echo "Cleanup Complete!"
echo "=========================================="
echo ""
echo "Disk usage after cleanup:"
run_cmd "df -h / /tmp /var 2>/dev/null || df -h /"

if [[ -n "$BEFORE" ]] && [[ -n "$AFTER" ]]; then
    FREED=$((BEFORE - AFTER))
    if [[ $FREED -gt 0 ]]; then
        echo ""
        echo "Space freed: approximately $((FREED / 1024)) MB"
    fi
fi

echo ""
echo "=========================================="
echo "SUCCESS: Disk cleanup completed"
echo "=========================================="
