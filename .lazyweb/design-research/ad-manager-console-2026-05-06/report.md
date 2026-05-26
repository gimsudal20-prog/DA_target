# Design Research: Ad Manager Console

## TL;DR
The app should feel closer to Meta Ads Manager / Google Ads / Microsoft Clarity Advertising: dense, table-first, filter-first, and operational. The current design still has too much onboarding-card chrome for a tool used repeatedly by operators.

## Recommendations
1. Put the work surface first: hide large guide/shortcut cards by default and let 조회/등록/변경 tabs lead.
2. Treat query/export buttons as a compact toolbar, not a boxed card section.
3. Prefer flat panels, thin borders, and sticky table/tool headers over heavy cards and gradient backgrounds.
4. Keep action hierarchy clear: lookup buttons blue outline, export buttons green outline, destructive actions red.
5. Preserve dense left navigation: account, campaign, adgroup selection is the operator's main context.

## Reference Patterns
- Meta Ads Manager: campaign/ad set/ad hierarchy, filters above a dense table, export and columns actions near the table.
- Google Ads: dashboards and reports combine scorecards, filters, tables, and date/account scope controls.
- Microsoft Clarity Advertising Dashboard: campaign detail views expose filters, status, metrics, and downloadable reports from a compact table view.

## Sources
- Meta Ads Manager navigation overview: https://sumdigital.com/blog/navigating-meta-ads-manager
- Meta Ads Manager 2026 guide: https://mhigrowthengine.com/blog/how-to-use-meta-ads-manager/
- Google Ads dashboard documentation: https://support.google.com/google-ads/answer/6379084?hl=en-EN
- Microsoft Clarity Advertising Dashboard: https://learn.microsoft.com/zh-cn/clarity/advertising-dashboard/ad-campaign-details

## Implementation Direction
For this app, the best reference fit is not a marketing dashboard with big KPI cards. It is an operator console: compact sidebar, slim target picker, top command/search area, tabbed task modes, then toolbar + table/results.
