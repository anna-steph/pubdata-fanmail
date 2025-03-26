#' Pull trade in goods
#' 
#' Pull US trade in goods data from the Census Bureau
#'
#' Census API total goods data begin in January 2013
#' Use options(scipen = 999) to avoid scientific notation
#' 
#' Dependencies: dplyr, tidyr, readr, jsonlite, lubridate, pull_country_codes
#'
#' @param date_start string; earliest date of data, formatted "2024-01-01"
#' @param date_end string; last date of data, formatted "2025-01-01"
#'
#' @param api_key string; Census Bureau API key obtained here:
#' http://api.census.gov/data/key_signup.html
#'
#' @param ctry string; Countries of interest (optional; defaults to all countries)
#' Input a two letter ISO code for each country:
#' https://www.census.gov/foreign-trade/schedules/c/country.txt
#'
#' @return df with total monthly U.S. goods exports and imports, by country
#' @examples
#' pull_trade_goods("2013-01-01", "2021-01-01", "[api_key]")
#' pull_trade_goods("2013-01-01", "2021-01-01", "[api_key]", ctry = "DE")
#' pull_trade_goods("2013-01-01", "2021-01-01", "[api_key]", ctry = c("CA", "MX"))
pull_trade_goods <- function(date_start, date_end,
                             api_key,
                             ctry = "") {

  # Vars to pull ----------------------------------------------------
  
  import_vars <- paste("CTY_CODE", "CTY_NAME",
                       "GEN_VAL_MO", "GEN_VAL_YR",
                       sep = ",")
  
  export_vars <- paste("CTY_CODE", "CTY_NAME",
                       "ALL_VAL_MO", "ALL_VAL_YR",
                       sep = ",")
  
  # Date range ----------------------------------------------------------
  
  period_start <- stringr::str_extract(date_start, "\\d{4}-\\d{2}")
  period_end <- stringr::str_extract(date_end, "\\d{4}-\\d{2}")
  
  period  <- paste0("from+", period_start,
                    "+to+", period_end)
  
  # Country index from Census -----------------------------------------------
  
  # assumes R/census_country_codes.R already sourced
  country_list <- census_country_codes()

  if (is.null(ctry) | isTRUE(ctry == "") | missing(ctry)) {

    ctry_isos <- country_list %>%
      filter(iso_code == "US") %>%
      mutate(
        code_pattern = ""
      ) %>%
      select(code_pattern)

  } else {

    ctry_isos <- country_list %>%
      filter(iso_code %in% paste(ctry, sep = ",")) %>%
      select(code) %>%
      mutate(
        code_pattern = paste0("&CTY_CODE=", code)
      )
  }

  ctry_code <- paste0(ctry_isos$code_pattern, collapse = "")

  # Census call --------------------------------------------------------
  
  census_call <- function(direction, pull_vars, period, api_key) {
    
    resurl <- tryCatch({

      message(paste0("Pulling ", ctry, " ", direction, " from ",
              format(as.Date(date_start, "%Y-%m-%d"), "%B %Y"), " to ",
              format(as.Date(date_end, "%Y-%m-%d"), "%B %Y")))
      
      test <- paste0("https://api.census.gov/data/timeseries/intltrade/",
                     direction, "/",
                     "hs?get=", pull_vars,
                     "&time=", period,
                     ctry_code,
                     "&key=", api_key)
      
      # note: as pulled, resurl contains dup cols for cty_code; remove V6
      df <- as.data.frame(jsonlite::fromJSON(resurl)) %>%
        select(-any_of(c("V6")))

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

  # Pull
    
  imp_df <- NULL
  tryimp <- 1

  while (is.null(imp_df) && tryimp <= 5) {

    tryimp <- tryimp + 1

    try(

      imp_df <- census_call("imports",
                            import_vars,
                            period,
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
                            period,
                            api_key)
    )
  }

  # Combine -----------------------------------------------------------------

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

  message("Returning ", ctry, " trade in goods from ",
          format(as.Date(date_start, "%Y-%m-%d"), "%B %Y"), " to ",
          format(as.Date(date_end, "%Y-%m-%d"), "%B %Y"))

  return(total_trade)

}
