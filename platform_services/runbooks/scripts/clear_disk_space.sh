#!/bin/bash
# METADATA
# service: infrastructure
# action: cleanup
# component: disk
# keywords: [disk, space, full, cleanup, logs, temp, storage, filesystem]
# risk: low
# requires_approval: false
# estimated_time: 5m
# rollback: none
# version: 1.0.0
# author: sre-team
# last_updated: 2024-01-20

set -euo pipefail

# Configuration
LOG_RETENTION_DAYS=${LOG_RETENTION_DAYS:-7}
TEMP_RETENTION_DAYS=${TEMP_RETENTION_DAYS:-3}
MIN_FREE_SPACE_GB=${MIN_FREE_SPACE_GB:-10}

echo "============================================"
echo "DISK SPACE CLEANUP SCRIPT"
echo "============================================"

# PRE-CHECK: Current disk usage
echo "[PRE-CHECK] Current disk usage:"
df -h / /var /tmp 2>/dev/null || df -h /

BEFORE_USAGE=$(df / | awk 'NR==2 {print $5}' | tr -d '%')
echo "[PRE-CHECK] Root partition usage: ${BEFORE_USAGE}%"

# PRE-CHECK: Largest directories
echo "[PRE-CHECK] Top 5 largest directories in /var:"
du -sh /var/* 2>/dev/null | sort -rh | head -5 || true

# ACTION: Clean old log files
echo "[ACTION] Cleaning log files older than ${LOG_RETENTION_DAYS} days..."
find /var/log -type f -name "*.log" -mtime +${LOG_RETENTION_DAYS} -exec rm -f {} \; 2>/dev/null || true
find /var/log -type f -name "*.log.*" -mtime +${LOG_RETENTION_DAYS} -exec rm -f {} \; 2>/dev/null || true
find /var/log -type f -name "*.gz" -mtime +${LOG_RETENTION_DAYS} -exec rm -f {} \; 2>/dev/null || true

# ACTION: Clean temp files
echo "[ACTION] Cleaning temp files older than ${TEMP_RETENTION_DAYS} days..."
find /tmp -type f -mtime +${TEMP_RETENTION_DAYS} -exec rm -f {} \; 2>/dev/null || true
find /var/tmp -type f -mtime +${TEMP_RETENTION_DAYS} -exec rm -f {} \; 2>/dev/null || true

# ACTION: Clean package cache
echo "[ACTION] Cleaning package manager cache..."
if command -v apt-get &> /dev/null; then
    apt-get clean 2>/dev/null || true
    apt-get autoremove -y 2>/dev/null || true
elif command -v yum &> /dev/null; then
    yum clean all 2>/dev/null || true
fi

# ACTION: Clean journal logs
echo "[ACTION] Vacuuming journal logs..."
if command -v journalctl &> /dev/null; then
    journalctl --vacuum-time=${LOG_RETENTION_DAYS}d 2>/dev/null || true
fi

# ACTION: Clean Docker (if present)
if command -v docker &> /dev/null; then
    echo "[ACTION] Cleaning Docker unused resources..."
    docker system prune -f 2>/dev/null || true
fi

# POST-CHECK: New disk usage
echo "[POST-CHECK] Disk usage after cleanup:"
df -h / /var /tmp 2>/dev/null || df -h /

AFTER_USAGE=$(df / | awk 'NR==2 {print $5}' | tr -d '%')
FREED=$((BEFORE_USAGE - AFTER_USAGE))

echo "============================================"
echo "DISK CLEANUP COMPLETED"
echo "============================================"
echo "Usage Before: ${BEFORE_USAGE}%"
echo "Usage After: ${AFTER_USAGE}%"
echo "Space Freed: ${FREED}%"
echo "============================================"

# Validation
if [ ${AFTER_USAGE} -gt 90 ]; then
    echo "[WARNING] Disk still above 90%, manual intervention may be needed"
    exit 1
fi

exit 0
