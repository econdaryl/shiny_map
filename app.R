library(shiny)
library(leaflet)
library(sf)
library(dplyr)
library(DBI)
library(duckdb)
library(lubridate)
library(RColorBrewer)
library(tidyr)

# Initialize DuckDB connection - now with data filtering for performance
load_data <- function(data_type = "visitor") {
  con_duck <- dbConnect(duckdb::duckdb())
  
  # Install required DuckDB extensions
  tryCatch({
    dbExecute(con_duck, "INSTALL icu;")
    dbExecute(con_duck, "LOAD icu;")
  }, error = function(e) {
    message("Note: Could not install/load icu extension: ", e$message)
  })
  
  # Get CPZ CBGs first to filter destinations
  cpz_cbg_list <- tryCatch({
    cpz <- st_read("data/cpz/cpz_map.shp", quiet = TRUE)
    nyc_cbgs <- st_read("data/nyc_cbgs.gpkg", quiet = TRUE)
    cpz <- st_transform(cpz, st_crs(nyc_cbgs))
    cpz_cbgs <- st_filter(nyc_cbgs, cpz)
    cpz_cbgs$GEOID
  }, error = function(e) {
    # Fallback to hardcoded CPZ CBGs
    c("360610001001", "360610002001", "360610004001", "360610006001", 
      "360610008001", "360610010001", "360610012001", "360610014001")
  })
  
  # Create CPZ CBG filter for DuckDB
  cpz_cbg_filter <- paste0("'", cpz_cbg_list, "'", collapse = ",")
  
  # Load CBG-CBSA mapping
  dbExecute(con_duck, "CREATE OR REPLACE VIEW cbg_cbsa_map AS SELECT * FROM 'data/cbg_cbsa_map.parquet';")
  
  # Load visitor network data with heavy filtering for performance
  dbExecute(con_duck, "CREATE OR REPLACE TABLE edgelist AS SELECT * FROM 'data/visitor_network_home*.parquet';")
  
  # Apply aggressive filtering and aggregation: pre-aggregate at tract/county level
  # Use simpler SQL without extensions that might not be available
  filter_query <- paste0("SELECT *
  FROM edgelist 
    INNER JOIN cbg_cbsa_map ON cbg_from = cbg
    WHERE cbsa = 35620
      AND cbg_to IN (", cpz_cbg_filter, ");")
  
  edgelist <- dbGetQuery(con_duck, filter_query)
  
  # Now do the filtering and aggregation in R to avoid DuckDB extension issues
  edgelist_processed <- edgelist %>%
    mutate(
      year = year(date),
      month = month(date),
      county_from = substr(cbg_from, 1, 5),
      tract_from = substr(cbg_from, 1, 11),
      county_to = substr(cbg_to, 1, 5)
    ) %>%
    filter(
      year %in% c(2024, 2025),
      month >= 1, month <= 5  # First 5 months only
    ) %>%
    # Pre-aggregate by tract and county to reduce data size
    group_by(year, month, tract_from, county_from, county_to, date) %>%
    summarise(visitors = sum(visitors, na.rm = TRUE), .groups = "drop")
  
  dbDisconnect(con_duck)
  message(paste("Loaded and processed", nrow(edgelist_processed), "rows (filtered for performance)"))
  return(edgelist_processed)
}

# Get CPZ CBGs by spatial intersection - now using local data
get_cpz_cbgs <- function() {
  tryCatch({
    # Load CPZ shapefile
    cpz <- st_read("data/cpz/cpz_map.shp", quiet = TRUE)
    
    # Load NYC CBG geometries from local file (no API call)
    nyc_cbgs <- st_read("data/nyc_cbgs.gpkg", quiet = TRUE)
    
    # Transform to same CRS
    cpz <- st_transform(cpz, st_crs(nyc_cbgs))
    
    # Find CBGs that intersect with CPZ
    cpz_cbgs <- st_filter(nyc_cbgs, cpz)
    
    return(cpz_cbgs$GEOID)
  }, error = function(e) {
    # Return hardcoded CPZ CBGs if files not available
    return(c("360610001001", "360610002001", "360610004001", "360610006001", 
             "360610008001", "360610010001", "360610012001", "360610014001"))
  })
}

# Get destination options - simplified for Manhattan only
get_destination_options <- function(edgelist) {
  # Manhattan CBGs only (since we filtered the data)
  manhattan_cbgs <- edgelist %>%
    distinct(cbg_to) %>%
    arrange(cbg_to) %>%
    pull(cbg_to)
  
  cbg_choices <- setNames(manhattan_cbgs, paste0("CBG ", manhattan_cbgs))
  
  # Manhattan County
  county_choices <- c("36061" = "New York County (Manhattan)")
  
  # CPZ
  cpz_choice <- c("CPZ" = "cpz")
  
  # Combine options
  destination_choices <- list(
    "Special Areas" = c(cpz_choice),
    "Manhattan County" = county_choices,
    "Individual Manhattan CBGs" = cbg_choices
  )
  
  return(destination_choices)
}

# Resolve destination selection to CBG list
resolve_destinations <- function(selections, edgelist, cpz_cbgs) {
  destination_cbgs <- c()
  
  for (selection in selections) {
    if (selection == "cpz") {
      # CPZ CBGs
      destination_cbgs <- c(destination_cbgs, cpz_cbgs)
    } else if (selection == "36061") {
      # Manhattan - get all CBGs
      manhattan_cbgs <- edgelist %>%
        filter(county_to == "36061") %>%
        distinct(cbg_to) %>%
        pull(cbg_to)
      destination_cbgs <- c(destination_cbgs, manhattan_cbgs)
    } else {
      # Individual CBG
      destination_cbgs <- c(destination_cbgs, selection)
    }
  }
  
  return(unique(destination_cbgs))
}

# Get NYC Metro Division geographies from local files - only tract and county
get_nyc_metro_geographies <- function(geo_level) {
  tryCatch({
    if (geo_level == "tract") {
      geoms <- st_read("data/nyc_metro_tracts.gpkg", quiet = TRUE) %>%
        select(GEOID, geometry)
    } else if (geo_level == "county") {
      geoms <- st_read("data/nyc_metro_counties.gpkg", quiet = TRUE) %>%
        select(GEOID, geometry)
    } else {
      return(NULL)
    }
    
    return(geoms)
  }, error = function(e) {
    warning(paste("Could not load local geography data:", e$message))
    return(NULL)
  })
}

# Calculate year-over-year changes - now only tract and county levels
calculate_yoy_change <- function(edgelist, destinations, max_months, agg_level) {
  
  # Filter data for the selected parameters (data is already aggregated by tract/county)
  filtered_data <- edgelist %>%
    filter(month <= max_months)  # CPZ filtering already applied in load_data
  
  message(paste("Filtered data has", nrow(filtered_data), "rows for months <=", max_months))
  
  # Aggregate by geography level (only tract and county supported)
  if (agg_level == "tract") {
    agg_data <- filtered_data %>%
      group_by(tract_from, year) %>%
      summarise(total_visitors = sum(visitors, na.rm = TRUE), .groups = "drop") %>%
      rename(geo_id = tract_from)
  } else if (agg_level == "county") {
    agg_data <- filtered_data %>%
      group_by(county_from, year) %>%
      summarise(total_visitors = sum(visitors, na.rm = TRUE), .groups = "drop") %>%
      rename(geo_id = county_from)
  }
  
  message(paste("Aggregated data has", nrow(agg_data), "rows at", agg_level, "level"))
  
  # Calculate year-over-year change - filter extreme values
  yoy_data <- agg_data %>%
    pivot_wider(names_from = year, values_from = total_visitors, values_fill = 0) %>%
    mutate(
      yoy_change = `2025` - `2024`,
      yoy_pct_change = ifelse(`2024` > 0, ((`2025` - `2024`) / `2024`) * 100, NA)
    ) %>%
    filter(!is.na(yoy_pct_change) & is.finite(yoy_pct_change) & abs(yoy_pct_change) <= 100)
  
  message(paste("Final YoY data has", nrow(yoy_data), "rows"))
  
  return(yoy_data)
}

# UI
ui <- fluidPage(
  titlePanel("NYC Visitor Year-over-Year Change Map - CPZ Focus"),
  
  sidebarLayout(
    sidebarPanel(
      h4("Controls"),
      
      selectInput("destinations", 
                  "Select Manhattan Destination(s):",
                  choices = list("Special Areas" = c("CPZ" = "cpz")),  # Initialize with CPZ
                  multiple = TRUE,
                  selected = "cpz"),
      
      sliderInput("max_months",
                  "Include first X months of each year:",
                  min = 1, max = 5, value = 3, step = 1),
      
      helpText("Note: This compares the same months in 2024 vs 2025 (e.g., Jan-Mar 2024 vs Jan-Mar 2025). Data is limited to first 5 months for performance."),
      
      selectInput("agg_level",
                  "Aggregate visitors by:",
                  choices = list("Census Tract" = "tract", 
                                "County" = "county"),
                  selected = "tract"),
      
      actionButton("update_map", "Update Map", class = "btn-primary"),
      
      hr(),
      
      h4("Summary Statistics"),
      verbatimTextOutput("summary_stats"),
      
      hr(),
      
      p("This app shows year-over-year changes in visitor patterns to the Congestion Pricing Zone (CPZ). 
        Data includes visitors from the entire NYC metropolitan area, limited to first 5 months for performance."),
      p("Red areas indicate decreased visitors, blue areas indicate increased visitors."),
      p(strong("Click 'Update Map' to load and display the data."), style = "color: #0066cc;")
    ),
    
    mainPanel(
      leafletOutput("visitor_map", height = "600px")
    )
  )
)

# Server
server <- function(input, output, session) {
  
  # Load CPZ CBGs first (lightweight)
  cpz_cbgs <- reactive({
    withProgress(message = 'Loading CPZ boundaries...', value = 0.3, {
      get_cpz_cbgs()
    })
  })
  
  # Load data only when needed (lazy loading)
  edgelist_data <- reactive({
    # Only load when update button is clicked
    req(input$update_map > 0)
    withProgress(message = 'Loading visitor data...', value = 0.5, {
      load_data("visitor")
    })
  })
  
  # Set up initial destination choices without loading heavy data
  observe({
    # Set up choices without requiring heavy data load
    destination_choices <- list(
      "Special Areas" = c("CPZ" = "cpz"),
      "Manhattan County" = c("36061" = "New York County (Manhattan)"),
      "Individual Manhattan CBGs" = c("Loading..." = "loading")
    )
    
    updateSelectInput(session, "destinations",
                     choices = destination_choices,
                     selected = "cpz")
  })
  
  # Reactive data processing - only triggers on explicit button click
  processed_data <- eventReactive(input$update_map, {
    req(input$max_months, input$agg_level)
    
    withProgress(message = 'Processing data...', value = 0.3, {
      # Since data is already filtered to CPZ destinations, just process it
      yoy_data <- calculate_yoy_change(edgelist_data(), 
                                      NULL,  # destinations no longer needed
                                      input$max_months, 
                                      input$agg_level)
      
      setProgress(0.7, message = 'Loading geographic boundaries...')
      
      # Get geographic boundaries
      geoms <- get_nyc_metro_geographies(input$agg_level)
      
      if (!is.null(geoms) && nrow(yoy_data) > 0) {
        # Join data with geometries
        map_data <- geoms %>%
          left_join(yoy_data, by = c("GEOID" = "geo_id")) %>%
          filter(!is.na(yoy_pct_change))
        
        message(paste("Map data created with", nrow(map_data), "features"))
      } else {
        map_data <- NULL
        message("No map data created - missing geometries or yoy_data")
      }
      
      setProgress(1)
      return(list(yoy_data = yoy_data, map_data = map_data))
    })
  }, ignoreNULL = FALSE)
  
  # Render map
  output$visitor_map <- renderLeaflet({
    leaflet() %>%
      addTiles() %>%
      setView(lng = -74.0, lat = 40.7, zoom = 10)
  })
  
  observe({
    req(processed_data())
    
    map_data <- processed_data()$map_data
    
    if (!is.null(map_data) && nrow(map_data) > 0) {
      
      # Create diverging color palette: red for negative, white for zero, blue for positive
      pal <- colorNumeric(
        palette = RColorBrewer::brewer.pal(11, "RdBu"),
        domain = c(-100, 100),  # Fixed domain from -100% to +100%
        reverse = TRUE  # Red for negative values (left), blue for positive (right)
      )
      
      # Create popup text
      popup_text <- paste0(
        "<strong>", input$agg_level, ": </strong>", map_data$GEOID, "<br>",
        "<strong>2024 Visitors: </strong>", format(map_data$`2024`, big.mark = ","), "<br>",
        "<strong>2025 Visitors: </strong>", format(map_data$`2025`, big.mark = ","), "<br>",
        "<strong>YoY Change: </strong>", round(map_data$yoy_pct_change, 1), "%"
      )
      
      leafletProxy("visitor_map") %>%
        clearShapes() %>%
        clearControls() %>%
        addPolygons(
          data = map_data,
          fillColor = ~pal(yoy_pct_change),
          weight = 1,
          opacity = 1,
          color = "white",
          dashArray = "3",
          fillOpacity = 0.7,
          popup = popup_text,
          highlight = highlightOptions(
            weight = 2,
            color = "#666",
            dashArray = "",
            fillOpacity = 0.9,
            bringToFront = TRUE
          )
        ) %>%
        addLegend(
          pal = pal, 
          values = c(-100, 100),  # Use fixed range for consistent legend
          opacity = 0.7, 
          title = "YoY % Change",
          position = "bottomright",
          labFormat = labelFormat(suffix = "%", between = " to ")
        )
    }
  })
  
  # Summary statistics
  output$summary_stats <- renderText({
    req(processed_data())
    
    yoy_data <- processed_data()$yoy_data
    
    if (nrow(yoy_data) > 0) {
      paste(
        paste("Analysis: CPZ Destinations"),
        paste("Geographic units analyzed:", nrow(yoy_data)),
        paste("Average YoY change:", round(mean(yoy_data$yoy_pct_change, na.rm = TRUE), 1), "%"),
        paste("Median YoY change:", round(median(yoy_data$yoy_pct_change, na.rm = TRUE), 1), "%"),
        paste("Range:", round(min(yoy_data$yoy_pct_change, na.rm = TRUE), 1), "% to", 
              round(max(yoy_data$yoy_pct_change, na.rm = TRUE), 1), "%"),
        sep = "\n"
      )
    } else {
      "No data available for the selected parameters."
    }
  })
}

# Run the app
shinyApp(ui = ui, server = server)