#' Pull trade by NAICS code
#' 
#' Pulls US trade in goods data from the Census Bureau by NAICS code
#' 
#' Dependencies: dplyr, tidyr, stringr, jsonlite, lubridate, pull_country_codes
#'
#' Notes: Census API goods data begin in January 2013
#' Use options(scipen = 999) to avoid scientific notation
#' NAICS-level trade data can be quite detailed. If the api call times out,
#' cut down your requested period length and retry.
#'
#' @param date_start string; date of earliest obs (optional; defaults to 2013)
#'
#' @param date_end string; date of most recent obs (optional; defaults to current year)
#'
#' @param api_key string; Census Bureau API key obtained here:
#' http://api.census.gov/data/key_signup.html
#'
#' @param ctry string; countries of interest (optional; defaults to all countries)
#' Input a two letter ISO code for each country:
#' https://www.census.gov/foreign-trade/schedules/c/country.txt
#'
#' @param naics_level string; NAICS level of interest, 1:6 can be used
#' Defaults to 3-digit level. See:
#' https://www.census.gov/eos/www/naics/faqs/faqs.html
#' https://www.census.gov/programs-surveys/economic-census/guidance/understanding-naics.html
#'
#' @return df
pull_trade_naics <- function(date_start, date_end, api_key, ctry, naics_level) {

  # Specify vars to pull -----------------------------------------------------

  # Set date range

  period_start <- stringr::str_extract(date_start, "\\d{4}-\\d{2}")
  period_end <- stringr::str_extract(date_end, "\\d{4}-\\d{2}")

  period  <- paste0("from+", period_start, "+to+", period_end)

  import_vars <- paste("CTY_CODE", "CTY_NAME",
                       "NAICS", "NAICS_LDESC",
                       "GEN_VAL_MO", "GEN_VAL_YR",
                       sep = ",")

  export_vars <- paste("CTY_CODE", "CTY_NAME",
                       "NAICS", "NAICS_LDESC",
                       "ALL_VAL_MO", "ALL_VAL_YR",
                       sep = ",")
  
  if (missing(naics_level)) {
    naics <- "NA3"
  } else {
    naics <- paste0("NA", naics_level)
  }

  # Country name lookup from Census ------------------------------------------

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
  
  # Data pull ----------------------------------------------------------------

  census_call <- function(direction, pull_vars, period, ctry_code,  api_key, naics) {

    resurl <- tryCatch({

      message(paste0("Pulling ", ctry, " ", direction, " from ",
              format(as.Date(date_start, "%Y-%m-%d"), "%B %Y"), " to ",
              format(as.Date(date_end, "%Y-%m-%d"), "%B %Y")))

      resurl <- paste0("https://api.census.gov/data/timeseries/intltrade/",
                       direction, "/naics?get=", pull_vars, "&COMM_LVL=",
                       naics, "&time=", period, ctry_code,
                       "&key=", api_key)

      # note: as pulled, resurl contains dup cols for cty_code; remove V9
      df <- as.data.frame(jsonlite::fromJSON(resurl))%>%
        select(-any_of(c("V9")))

    },
    error = function(cond) {
      message("Census call returned error:")
      message(cond)
      message(". If call timed out, try re-running.")
    },
    warning = function(cond) {
      message("Census call returned warning:")
      message(cond)
    })

    df_names <- df[1, ] %>%
      tolower()

    names(df) <- df_names
    
    df_format <- df %>%
      filter(!(cty_code == "CTY_CODE")) %>%
      mutate(across(5:6, as.numeric)) %>%
      mutate(date = as.Date(paste0(time, "-01"), format = "%Y-%m-%d")) %>%
      mutate() %>%
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
                            ctry_code,
                            api_key,
                            naics)
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
                            ctry_code,
                            api_key,
                            naics)
    )
  }
  
  # Combine -----------------------------------------------------------------
  
  naics_trade <- exp_df %>%
    full_join(imp_df, by = c("date",
                             "cty_code",
                             "cty_name",
                             "naics",
                             "naics_ldesc",
                             "comm_lvl")) %>%
    rename(
      exp_all_val_mo = all_val_mo,
      exp_all_val_yr = all_val_yr,
      imp_gen_val_mo = gen_val_mo,
      imp_gen_val_yr = gen_val_yr
    ) %>%
    arrange(date, cty_code) %>%
    relocate(comm_lvl, .after = cty_name)
  
  return(naics_trade)
  
}
