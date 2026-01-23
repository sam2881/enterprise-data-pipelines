#!/bin/bash
# =============================================================================
# Setup Pre-commit Hooks for AI Agent Platform
# =============================================================================
# This script installs and configures pre-commit hooks to prevent
# accidental commits of secrets and enforce code quality.
#
# Usage: ./scripts/setup-pre-commit.sh
# =============================================================================

set -e

echo "=============================================="
echo "Setting up pre-commit hooks..."
echo "=============================================="

# Check if pre-commit is installed
if ! command -v pre-commit &> /dev/null; then
    echo "Installing pre-commit..."
    pip install pre-commit
fi

# Check if detect-secrets is installed
if ! command -v detect-secrets &> /dev/null; then
    echo "Installing detect-secrets..."
    pip install detect-secrets
fi

# Install pre-commit hooks
echo "Installing pre-commit hooks..."
pre-commit install

# Install commit-msg hook for conventional commits (optional)
pre-commit install --hook-type commit-msg || true

# Generate initial secrets baseline if not exists
if [ ! -f ".secrets.baseline" ]; then
    echo "Generating secrets baseline..."
    detect-secrets scan > .secrets.baseline || true
fi

# Run pre-commit on all files to verify setup
echo ""
echo "Running pre-commit on all files (this may take a moment)..."
pre-commit run --all-files || true

echo ""
echo "=============================================="
echo "Pre-commit setup complete!"
echo "=============================================="
echo ""
echo "What was installed:"
echo "  - detect-secrets: Scans for API keys and passwords"
echo "  - gitleaks: Scans for known secret patterns"
echo "  - black: Python code formatter"
echo "  - isort: Python import sorter"
echo "  - flake8: Python linter"
echo "  - mypy: Python type checker"
echo "  - eslint: JavaScript/TypeScript linter"
echo "  - markdownlint: Markdown linter"
echo ""
echo "The hooks will run automatically on every commit."
echo "To skip hooks (emergency only): git commit --no-verify"
echo ""
echo "To run manually: pre-commit run --all-files"
