# NYC Visitor Year-over-Year Change Map

An interactive Shiny application showing year-over-year changes in visitor patterns to Manhattan destinations.

## About

This app visualizes how visitor patterns to Manhattan have changed between 2024 and 2025, using mobility data to understand the impact of policy changes on travel behavior.

### Features

- **Interactive Map**: Explore visitor changes across different geographic levels (county, tract, CBG)
- **Flexible Destinations**: Select from CPZ area, Manhattan county, or individual census block groups
- **Time Comparison**: Compare same months across years (e.g., Jan-Mar 2024 vs Jan-Mar 2025)
- **Color-coded Visualization**: Red areas show decreased visitors, blue areas show increased visitors
- **Summary Statistics**: View aggregate metrics for your selected analysis

### Data Sources

- **Mobility Data**: Advan/SafeGraph visitor patterns
- **Geographic Data**: U.S. Census Bureau boundaries via tidycensus
- **Policy Areas**: NYC Congestion Pricing Zone boundaries

### Usage

1. Select destination areas in Manhattan
2. Choose the number of months to include in the comparison
3. Select geographic aggregation level
4. Click "Update Map" to generate the visualization

### Technical Details

- Built with R Shiny, Leaflet for mapping
- Data processing with DuckDB for performance
- Filtered for Manhattan destinations to optimize deployment size
- Extreme values (>100% change) filtered for better visualization

### Deployment

This app is designed for deployment on shinyapps.io via GitHub integration.

**App Size**: ~33MB (within shinyapps.io limits)
**Data Coverage**: Manhattan destinations, NYC metro area origins
**Time Period**: 2024-2025