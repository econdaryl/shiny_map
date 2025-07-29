library(shiny)
library(leaflet)
library(sf)
library(dplyr)
library(DBI)
library(duckdb)
library(lubridate)
library(tidycensus)
library(RColorBrewer)
library(tidyr)

# Initialize DuckDB connection
load_data <- function(data_type = "visitor") {
  con_duck <- dbConnect(duckdb::duckdb())
  
  # NYC counties (Bronx, Kings, New York, Queens, Richmond)
  nyc_fips <- data.frame(fips=c("36005", "36047", "36061", "36081", "36085"))
  duckdb_register(con_duck, "nyc_fips", nyc_fips, overwrite = TRUE)
  
  # Load CBG-CBSA mapping
  dbExecute(con_duck, "CREATE OR REPLACE VIEW cbg_cbsa_map AS SELECT * FROM 'data/cbg_cbsa_map.parquet';")
  
  # Load visitor network data (Manhattan destinations only)
  dbExecute(con_duck, "CREATE OR REPLACE TABLE edgelist AS SELECT * FROM 'data/visitor_network_home*.parquet';")
  
  edgelist <- dbGetQuery(con_duck, "SELECT * FROM edgelist 
    INNER JOIN cbg_cbsa_map ON cbg_from = cbg
    WHERE cbsa = 35620
      AND EXTRACT(year FROM date) IN (2024, 2025);")
  
  edgelist_processed <- edgelist %>% 
    mutate(
      year = year(date),
      month = month(date),
      county_from = substr(cbg_from, 1, 5),
      tract_from = substr(cbg_from, 1, 11),
      county_to = substr(cbg_to, 1, 5)
    )
  
  dbDisconnect(con_duck)
  return(edgelist_processed)
}

# Get CPZ CBGs by spatial intersection
get_cpz_cbgs <- function() {
  tryCatch({
    # Load CPZ shapefile
    cpz <- st_read("data/cpz/cpz_map.shp", quiet = TRUE)
    
    # Get NYC CBG geometries from tidycensus
    nyc_counties <- c("005", "047", "061", "081", "085")  # NYC county codes
    
    nyc_cbgs <- get_acs(
      geography = "block group",
      variables = "B00001_001",  # Total population
      state = "NY",
      county = nyc_counties,
      year = 2018,
      geometry = TRUE
    ) %>%
      select(GEOID, geometry) %>% 
      filter(!is.na(GEOID)) %>%
      distinct(GEOID, .keep_all = TRUE) %>%  # Remove any duplicates
      st_transform(crs=32618)
    
    # Transform to same CRS
    cpz <- st_transform(cpz, st_crs(nyc_cbgs))
    
    # Find CBGs that intersect with CPZ
    cpz_cbgs <- st_filter(nyc_cbgs, cpz)
    
    return(cpz_cbgs$GEOID)
  }, error = function(e) {
    # Return hardcoded CPZ CBGs if shapefile not available
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

# Get NYC Metro Division geographies from tidycensus
get_nyc_metro_geographies <- function(geo_level) {
  # NYC Metro Division counties
  nyc_metro_fips <- c("34003", "34013", "34017", "34019", "34023", "34025", "34027", 
                      "34029", "34031", "34035", "34037", "34039", "34041", "36005", 
                      "36047", "36059", "36061", "36079", "36081", "36085", "36087", 
                      "36103", "36119")
  
  if (geo_level == "cbg") {
    return(NULL)
  } else if (geo_level == "tract") {
    geoms <- get_acs(geography = "tract", variables = "B00001_001", 
                     state = c("NY", "NJ"), year = 2018, geometry = TRUE) %>%
      mutate(county_fips = substr(GEOID, 1, 5)) %>%
      filter(county_fips %in% nyc_metro_fips) %>%
      select(GEOID, geometry)
  } else if (geo_level == "county") {
    geoms <- get_acs(geography = "county", variables = "B00001_001", 
                     state = c("NY", "NJ"), year = 2018, geometry = TRUE) %>%
      filter(GEOID %in% nyc_metro_fips) %>%
      select(GEOID, geometry)
  }
  
  return(geoms)
}

# Calculate year-over-year changes
calculate_yoy_change <- function(edgelist, destinations, max_months, agg_level) {
  
  # Filter data for the selected parameters
  filtered_data <- edgelist %>%
    filter(
      cbg_to %in% destinations,
      month <= max_months
    )
  
  # Aggregate by geography level
  if (agg_level == "cbg") {
    agg_data <- filtered_data %>%
      group_by(cbg_from, year) %>%
      summarise(total_visitors = sum(visitors, na.rm = TRUE), .groups = "drop") %>%
      rename(geo_id = cbg_from)
  } else if (agg_level == "tract") {
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
  
  # Calculate year-over-year change - filter extreme values
  yoy_data <- agg_data %>%
    pivot_wider(names_from = year, values_from = total_visitors, values_fill = 0) %>%
    mutate(
      yoy_change = `2025` - `2024`,
      yoy_pct_change = ifelse(`2024` > 0, ((`2025` - `2024`) / `2024`) * 100, NA)
    ) %>%
    filter(!is.na(yoy_pct_change) & is.finite(yoy_pct_change) & abs(yoy_pct_change) <= 100)
  
  return(yoy_data)
}

# UI
ui <- fluidPage(
  titlePanel("NYC Visitor Year-over-Year Change Map - Manhattan Destinations"),
  
  sidebarLayout(
    sidebarPanel(
      h4("Controls"),
      
      selectInput("destinations", 
                  "Select Manhattan Destination(s):",
                  choices = NULL,  # Will be populated server-side
                  multiple = TRUE,
                  selected = NULL),
      
      sliderInput("max_months",
                  "Include first X months of each year:",
                  min = 1, max = 12, value = 3, step = 1),
      
      helpText("Note: This compares the same months in 2024 vs 2025 (e.g., Jan-Mar 2024 vs Jan-Mar 2025)"),
      
      selectInput("agg_level",
                  "Aggregate visitors by:",
                  choices = list("Census Block Group" = "cbg",
                                "Census Tract" = "tract", 
                                "County" = "county"),
                  selected = "tract"),
      
      actionButton("update_map", "Update Map", class = "btn-primary"),
      
      hr(),
      
      h4("Summary Statistics"),
      verbatimTextOutput("summary_stats"),
      
      hr(),
      
      p("This app shows year-over-year changes in visitor patterns to Manhattan destinations. 
        Data includes visitors from the entire NYC metropolitan area."),
      p("Red areas indicate decreased visitors, blue areas indicate increased visitors.")
    ),
    
    mainPanel(
      leafletOutput("visitor_map", height = "600px")
    )
  )
)

# Server
server <- function(input, output, session) {
  
  # Load data on startup
  edgelist_data <- reactive({
    withProgress(message = 'Loading visitor data...', value = 0.5, {
      load_data("visitor")
    })
  })
  
  # Load CPZ CBGs
  cpz_cbgs <- reactive({
    withProgress(message = 'Loading CPZ boundaries...', value = 0.3, {
      get_cpz_cbgs()
    })
  })
  
  # Update destination choices
  observe({
    req(edgelist_data())
    destination_choices <- get_destination_options(edgelist_data())
    
    # Default to CPZ
    default_selection <- "cpz"
    
    updateSelectInput(session, "destinations",
                     choices = destination_choices,
                     selected = default_selection)
  })
  
  # Reactive data processing
  processed_data <- eventReactive(input$update_map, {
    req(input$destinations, input$max_months, input$agg_level, cpz_cbgs())
    
    withProgress(message = 'Processing data...', value = 0.3, {
      # Resolve destinations to CBG list
      destination_cbgs <- resolve_destinations(input$destinations, edgelist_data(), cpz_cbgs())
      
      yoy_data <- calculate_yoy_change(edgelist_data(), 
                                      destination_cbgs, 
                                      input$max_months, 
                                      input$agg_level)
      
      setProgress(0.7, message = 'Loading geographic boundaries...')
      
      # Get geographic boundaries
      geoms <- get_nyc_metro_geographies(input$agg_level)
      
      if (!is.null(geoms)) {
        # Join data with geometries
        map_data <- geoms %>%
          left_join(yoy_data, by = c("GEOID" = "geo_id")) %>%
          filter(!is.na(yoy_pct_change))
      } else {
        # For CBG level, we'll need to get geometries differently
        map_data <- NULL
      }
      
      setProgress(1)
      return(list(yoy_data = yoy_data, map_data = map_data, destinations = destination_cbgs))
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
    destinations <- processed_data()$destinations
    
    if (nrow(yoy_data) > 0) {
      paste(
        paste("Manhattan destination CBGs:", length(destinations)),
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