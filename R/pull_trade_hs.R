#' Pull trade by HS code
#' 
#' Pulls US trade in goods data from the Census Bureau by HS code
#' 
#' Dependencies: dplyr, tidyr, stringr, jsonlite, lubridate, pull_country_codes
#'
#' Notes: Census API goods data begin in January 2013
#' Use options(scipen = 999) to avoid scientific notation
#' HS code-level trade data can be quite detailed. If the api call times out,
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
#' @param hs_level string; HS level of interest, 2, 4, 6 and 10 can be used
#' Defaults to 2-digit level. See:
#' https://www.trade.gov/harmonized-system-hs-codes
#' 
#' @param hs_code string; HS code of interest; should either fit specified hs_level
#' or use wildcard "2*"
#'
#' @return df
pull_trade_hs <- function(date_start, date_end, api_key, ctry, hs_level) {
  
  # Specify vars to pull -----------------------------------------------------
  
  # Set date range
  
  period_start <- stringr::str_extract(date_start, "\\d{4}-\\d{2}")
  period_end <- stringr::str_extract(date_end, "\\d{4}-\\d{2}")
  
  period  <- paste0("from+", period_start, "+to+", period_end)
  
  # see Appendix A for api endpoints
  # https://www.census.gov/foreign-trade/reference/guides/Guide_to_International_Trade_Datasets.pdf

  import_vars <- paste("CTY_CODE", "CTY_NAME",
                       "I_COMMODITY", "I_COMMODITY_SDESC", "I_COMMODITY_LDESC",
                       "GEN_VAL_MO", "GEN_VAL_YR",
                       sep = ",")

  export_vars <- paste("CTY_CODE", "CTY_NAME",
                       "E_COMMODITY", "E_COMMODITY_SDESC", "E_COMMODITY_LDESC",
                       "ALL_VAL_MO", "ALL_VAL_YR",
                       sep = ",")

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

  census_call <- function(direction, pull_vars, period, ctry_code,  api_key, hs) {

    resurl <- tryCatch({

      message(paste0("Pulling ", ctry, " ", direction, " from ",
                     format(as.Date(date_start, "%Y-%m-%d"), "%B %Y"), " to ",
                     format(as.Date(date_end, "%Y-%m-%d"), "%B %Y")))

      resurl <- paste0("https://api.census.gov/data/timeseries/intltrade/",
                       direction, "/hs?get=", pull_vars, "&COMM_LVL=HS",
                       hs, "&time=", period, ctry_code,
                       "&key=", api_key)

      # note: as pulled, resurl contains dup cols for cty_code; remove V9
      df <- as.data.frame(jsonlite::fromJSON(resurl)) %>%
        select(-ncol(.))

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
      mutate(across(6:7, as.numeric)) %>%
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
                            hs_level)
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
                            hs_level)
    )
  }

  # Combine -----------------------------------------------------------------

  hs_trade <- exp_df %>%
    full_join(imp_df, by = c("date",
                             "cty_code",
                             "cty_name",
                             "e_commodity" = "i_commodity",
                             "e_commodity_ldesc" = "i_commodity_ldesc",
                             "e_commodity_sdesc" = "i_commodity_sdesc",
                             "comm_lvl")) %>%
    rename(
      exp_all_val_mo = all_val_mo,
      exp_all_val_yr = all_val_yr,
      imp_gen_val_mo = gen_val_mo,
      imp_gen_val_yr = gen_val_yr
    ) %>%
    arrange(date, cty_code) %>%
    relocate(comm_lvl, .after = cty_name)

  return(hs_trade)

}
