# CLAUDEFIX · Public Store Pricing & FX Collector

Automated public store pricing collector for [CLAUDEFIX](https://claudefix.store).

## Overview
This standalone public repository executes scheduled GitHub Actions workflows to scrape publicly available store pricing:
- **Apple App Store**: Region-specific in-app pricing for Claude Pro and Claude Max
- **Google Play Store**: In-app purchase range per country
- **Exchange Rates**: Daily USD snapshot from Fawaz Ahmed Currency API

## Workflow & Sync Architecture
1. **Daily Scheduled Run**: Runs at 03:17 UTC every day via GitHub Actions.
2. **Snapshot Retention**: Preserves latest known pricing if any region's store front is temporarily unavailable.
3. **Automated Cross-Repo Sync**: When data changes, updates are automatically synced via Deploy Key to the private `overdev-team/claudefix` repository.

## Local Usage
```bash
# Run price collector
python3 scripts/update_prices.py

# Update exchange rates
python3 scripts/update_fx.py

# Validate JSON schema and contracts
python3 scripts/update_prices.py --check
```
