#' Pull goods trade
#' 
#' Pull US trade in goods data from the Census Bureau
#'
#' Census API total goods data begin in January 2013
#' Users may want to set scientific notation off: options(scipen = 999)
#' 
#' Dependencies: dplyr, tidyr, readr, jsonlite, lubridate, pull_country_codes
#'
#' @param first_yr integer; year of the first obs, optional,  defaults to 2013, first
#' year of available data
#'
#' @param last_yr integer; year of the final obs, optional, defaults to current year
#'
#' @param api_key string; Census Bureau API key obtained here:
#' http://api.census.gov/data/key_signup.html
#'
#' @param ctry string; Countries of interest (optional; defaults to all countries)
#' Input a two letter ISO code for each country:
#' https://www.census.gov/foreign-trade/schedules/c/country.txt
#' 
#' @param by_year boolean; defaults to FALSE, TRUE allows users to download
#' data by year on a weak connection
#'
#' @return tibble with total monthly U.S. goods exports and imports, by country
#'
#' @examples
#' bilateral trade with all countries by country, all years available:
#' pull_total_goods(api_key = "12345")
#' bilateral trade with all countries by country, specific first and last years:
#' pull_total_goods("2016", "2018", api_key = "12345")
#' bilateral trade with one country:
#' pull_total_goods("2016", "2018", api_key = "12345", ctry = "DE")
#' bilateral trade with several countries:
#' pull_total_goods("2016", "2018", api_key = "12345", ctry = c("CA", "MX"))
pull_goods_trade <- function(first_yr = "2013",
                             last_yr = lubridate::year(Sys.Date()),
                             api_key,
                             ctry = "",
                             by_year = FALSE) {

  # Vars to pull ----------------------------------------------------
  
  import_vars <- paste("CTY_CODE", "CTY_NAME",
                       "GEN_VAL_MO", "GEN_VAL_YR",
                       sep = ",")
  
  export_vars <- paste("CTY_CODE", "CTY_NAME",
                       "ALL_VAL_MO", "ALL_VAL_YR",
                       sep = ",")
  
  # Date range ----------------------------------------------------------
  
  first <- as.Date(paste0(first_yr, "0101"), format = "%Y%m%d")
  last <- as.Date(paste0(last_yr, "1201"), format = "%Y%m%d")
  
  # Country index from Census -----------------------------------------------
  
  country_list <- census_country_codes()
  
  if ((is.null(ctry) | is.na(ctry) | ctry == "" | missing(ctry))) {
    ctry_isos <- country_list %>%
      filter(iso_code == "US") %>%
      mutate(
        code_pattern = ""
      ) %>%
      select(code_pattern)} else {
        ctry_isos <- country_list %>%
          filter(iso_code %in% paste(ctry, sep = ",")) %>%
          select(code) %>%
          mutate(
            code_pattern = paste0("&CTY_CODE=", code)
          )
      }
  
  ctry_code <- paste0(ctry_isos$code_pattern, collapse = "")
  
  # Census call --------------------------------------------------------
  
  census_call <- function(direction, pull_vars, start_year, end_year, api_key) {
    
    year_month = paste0("from+", start_year, "-01+to+", end_year, "-12")
    
    resurl <- tryCatch({
      
      message(paste("Pulling Census",
                    direction, "data", "starting", start_year, "..."),
              sep = " ")
      
      test <- paste0("https://api.census.gov/data/timeseries/intltrade/",
                     direction, "/",
                     "hs?get=", pull_vars,
                     "&time=", year_month,
                     ctry_code,
                     "&key=", api_key)
      
    }, 
    error = function(cond) {
      message("Census call returned error:")
      message(cond)
      message(". If call timed out, try re-running.")
    },
    warning = function(cond) {
      message("Census call returned warning:")
      message(cond)
    }
    )
    
    df <- as.data.frame(jsonlite::fromJSON(resurl))
    
    df_names <- df[1, ] %>%
      tolower()
    
    names(df) <- df_names
    
    df_format <- df %>%
      filter(!(cty_code == "CTY_CODE")) %>%
      mutate(across(3:4, as.numeric)) %>%
      mutate(date = as.Date(paste0(time, "-01"), format = "%Y-%m-%d")) %>%
      select(-time) %>%
      relocate(date) %>%
      arrange(date, cty_code)
    
    return(df_format)
    
  }
  
  # Split API calls if by_year selected -----------------------------------
  
  if (by_year == TRUE) {
    
    # API calls split by year
    
    year_list <- year(seq(first, last, "years"))
    
    list_imports <- lapply(year_list, function(x) {
      
      temp <- NULL
      trytemp <- 1
      
      while (is.null(temp) && trytemp <= 5) { 
        
        trytemp <- trytemp + 1
        
        try(
          
          temp <- census_call("imports",
                              import_vars,
                              start_year = x,
                              end_year = x,
                              api_key)
        )
      }
      
      temp
      
    })
    
    imp_df <- bind_rows(list_imports, .id = "column_label")
    
    list_exports <- lapply(year_list, function(x) {
      
      temp <- NULL
      trytemp <- 1
      
      while (is.null(temp) && trytemp <= 5) { 
        
        trytemp <- trytemp + 1
        
        try(
          
          temp <- census_call("exports",
                              export_vars,
                              start_year = x,
                              end_year = x,
                              api_key)
        )
      }
      
      temp
      
    })
    
    exp_df <- bind_rows(list_exports, .id = "column_label")
    
  } else {
    
    # API calls not split; faster
    
    imp_df <- NULL
    tryimp <- 1
    
    while (is.null(imp_df) && tryimp <= 5) { 
      
      tryimp <- tryimp + 1
      
      try(
        
        imp_df <- census_call("imports",
                              import_vars,
                              start_year = first_yr,
                              end_year = last_yr,
                              api_key)
      )
    }
    
    exp_df <- NULL
    tryexp <- 1
    
    while (is.null(exp_df) && tryexp <= 5) { 
      
      tryexp <- tryexp + 1
      
      try(
        
        exp_df <- census_call("exports",
                              export_vars,
                              start_year = first_yr,
                              end_year = last_yr,
                              api_key)
      )
    }
    
  }
  
  # Combine -----------------------------------------------------------------
  
  if (by_year == TRUE) {
  
    total_trade <- exp_df %>%
      full_join(imp_df, by = c("column_label", "date", "cty_code", "cty_name")) %>%
      rename(
        exp_all_val_mo = all_val_mo,
        exp_all_val_yr = all_val_yr,
        imp_gen_val_mo = gen_val_mo,
        imp_gen_val_yr = gen_val_yr
      ) %>%
      select(date, cty_code, cty_name, starts_with("exp"), starts_with("imp")) %>%
      arrange(date, cty_code)
      
  } else {
    
    total_trade <- exp_df %>%
      full_join(imp_df, by = c("date", "cty_code", "cty_name")) %>%
      rename(
        exp_all_val_mo = all_val_mo,
        exp_all_val_yr = all_val_yr,
        imp_gen_val_mo = gen_val_mo,
        imp_gen_val_yr = gen_val_yr
      ) %>%
      select(date, cty_code, cty_name, starts_with("exp"), starts_with("imp")) %>%
      arrange(date, cty_code)

  }

  total_trade

}
