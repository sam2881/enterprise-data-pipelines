#!/bin/bash
# ==============================================================================
# METADATA
# ==============================================================================
# name: Check and Restart Service
# description: Generic service health check and restart script
# service: generic
# component: service
# action: restart
# risk: low
# requires_approval: false
# estimated_time_minutes: 5
# tags: [service, restart, health, check, generic]
# error_patterns:
#   - "service.*down"
#   - "health.*check.*failed"
#   - "not.*responding"
# keywords: [service, restart, health, check, systemd, process]
# pre_checks: [check_service_exists]
# post_checks: [verify_service_running]
# ==============================================================================

set -euo pipefail

# Configuration
SERVICE_NAME="${1:-}"
HEALTH_ENDPOINT="${2:-}"
MAX_RETRIES=3
RETRY_DELAY=10

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

usage() {
    echo "Usage: $0 <service_name> [health_endpoint]"
    echo "Example: $0 nginx http://localhost/health"
    exit 1
}

check_service_exists() {
    if ! systemctl list-units --type=service | grep -q "$SERVICE_NAME"; then
        log_error "Service '$SERVICE_NAME' not found"
        exit 1
    fi
    log_info "Service '$SERVICE_NAME' exists"
}

get_service_status() {
    systemctl is-active "$SERVICE_NAME" 2>/dev/null || echo "inactive"
}

check_health_endpoint() {
    if [ -n "$HEALTH_ENDPOINT" ]; then
        if curl -sf --max-time 5 "$HEALTH_ENDPOINT" > /dev/null 2>&1; then
            return 0
        else
            return 1
        fi
    fi
    return 0
}

restart_service() {
    log_info "Restarting service '$SERVICE_NAME'..."

    # Graceful stop
    if systemctl stop "$SERVICE_NAME" --timeout=30; then
        log_info "Service stopped successfully"
    else
        log_warn "Graceful stop failed, forcing..."
        systemctl kill "$SERVICE_NAME" --signal=SIGKILL || true
    fi

    sleep 2

    # Start service
    if systemctl start "$SERVICE_NAME"; then
        log_info "Service started successfully"
        return 0
    else
        log_error "Failed to start service"
        return 1
    fi
}

wait_for_service() {
    local retries=0
    while [ $retries -lt $MAX_RETRIES ]; do
        sleep $RETRY_DELAY

        local status=$(get_service_status)
        if [ "$status" = "active" ]; then
            if check_health_endpoint; then
                log_info "Service is healthy"
                return 0
            else
                log_warn "Service running but health check failed"
            fi
        fi

        retries=$((retries + 1))
        log_warn "Waiting for service... (attempt $retries/$MAX_RETRIES)"
    done

    log_error "Service failed to become healthy after $MAX_RETRIES attempts"
    return 1
}

main() {
    if [ -z "$SERVICE_NAME" ]; then
        usage
    fi

    log_info "=== Service Check and Restart Script ==="
    log_info "Service: $SERVICE_NAME"
    log_info "Health Endpoint: ${HEALTH_ENDPOINT:-'N/A'}"

    # Pre-check
    log_info "=== PRE-CHECK ==="
    check_service_exists

    local current_status=$(get_service_status)
    log_info "Current status: $current_status"

    # Check if restart needed
    if [ "$current_status" = "active" ] && check_health_endpoint; then
        log_info "Service is healthy, no restart needed"
        exit 0
    fi

    # Restart
    log_info "=== RESTARTING SERVICE ==="
    if restart_service; then
        log_info "=== WAITING FOR SERVICE ==="
        if wait_for_service; then
            log_info "=== REMEDIATION SUCCESSFUL ==="
            exit 0
        fi
    fi

    log_error "=== REMEDIATION FAILED ==="
    exit 1
}

main "$@"
