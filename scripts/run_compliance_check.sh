#!/bin/bash
# ==============================================================================
# AI Agent Platform - Compliance Check Runner
# ==============================================================================
#
# Usage:
#   ./scripts/run_compliance_check.sh              # Run all checks
#   ./scripts/run_compliance_check.sh --verbose    # Verbose output
#   ./scripts/run_compliance_check.sh --json       # JSON output
#   ./scripts/run_compliance_check.sh --standard SOC2  # Specific standard
#
# Standards available:
#   SOC2         - SOC 2 Type II
#   ISO42001     - ISO 42001 (AI Management System)
#   NIST-AI-RMF  - NIST AI Risk Management Framework
#   EU-AI-Act    - EU AI Act (Articles 9-15)
#   MITRE-ATLAS  - MITRE ATLAS (Adversarial Threats)
#   all          - All standards (default)
#
# ==============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

echo "=============================================="
echo "AI Agent Platform - Compliance Checker"
echo "=============================================="
echo ""

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "ERROR: Python3 is not installed"
    exit 1
fi

# Run the compliance checker
python3 tests/compliance/compliance_checker.py "$@"

EXIT_CODE=$?

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "Compliance check completed successfully."
else
    echo "WARNING: Critical compliance issues detected!"
fi

exit $EXIT_CODE
