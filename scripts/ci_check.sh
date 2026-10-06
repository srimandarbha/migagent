#!/usr/bin/env bash
set -euo pipefail

echo "=========================================================="
echo "   MFA CI Quality & Production Readiness Gate"
echo "=========================================================="

echo "[1/4] Checking python compilation across all modules..."
python3 -m compileall engine persistence simulator scripts tests

echo ""
echo "[2/4] Validating declarative policies against contracts..."
python3 -c "
from pathlib import Path
from engine.rules.policy_validator import validate_all_policies
errs = validate_all_policies(Path('policies'), Path('skills'))
if errs:
    print('Policy validation failures:', errs)
    exit(1)
print('All policies valid against contracts, facts, and skills.')
"

echo ""
echo "[3/4] Validating MTV source-backed corpus integrity..."
python3 scripts/validate_source_corpus.py

echo ""
echo "[4/4] Running automated test suite (unit, integration, kafka)..."
pytest -q

echo ""
echo "=========================================================="
echo "   All CI gates PASSED successfully!"
echo "=========================================================="
