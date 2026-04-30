# Design polish patch 2026-04-29

## Scope
- templates/index.html only
- No API route, backend service, or payload shape changes

## Changes
- Improved left account/campaign/adgroup selection readability with accent-line active states
- Refined current selection summary and workflow tab hierarchy
- Unified card, input, button, badge, table spacing and borders
- Added tabular numeric rendering for counts/costs/log metrics
- Reworked action log card styling for success/partial/fail states
- Added copy button for action log detail items without extra API calls

## Performance note
The patch is CSS-first and does not add additional API calls. The log detail copy feature reads text already rendered in the browser.

## Validation
- Extracted inline scripts from templates/index.html
- Replaced Jinja placeholders for JS syntax check
- node --check passed
