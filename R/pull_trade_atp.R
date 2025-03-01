#' Pull trade atp
#'
#' Pull trade in U.S. advanced technology products (ATP) by country
#' 
#' Dependencies: dplyr, tidyr, stringr, jsonlite, lubridate, pull_country_codes
#' 
#' Notes: must use options(scipen = 999) to avoid scientific notation
#'
#' @param date_start string; earliest date of data, formatted "2024-01-01"
#' @param date_end string; last date of data, formatted "2025-01-01"
#'
#' @param api_key string; Census Bureau API key, obtained here:
#' http://api.census.gov/data/key_signup.html
#'
#' @param ctry string; Country or countries of interest, using two letter ISO codes:
#' https://www.census.gov/foreign-trade/schedules/c/country.txt
#'
#' @return df with total monthly U.S. ATP exports and imports, by country
#' @examples
#' pull_trade_atp("2013-01-01", "2021-01-01", "[api_key]", ctry = "DE")
#' pull_trade_atp("2013-01-01", "2021-01-01", "[api_key]", ctry = c("CA", "MX"))
pull_trade_atp <- function(date_start, date_end, api_key, ctry) {

  # Specify vars to pull -----------------------------------------------------

  # Set date range
  
  period_start <- stringr::str_extract(date_start, "\\d{4}-\\d{2}")
  period_end <- stringr::str_extract(date_end, "\\d{4}-\\d{2}")
  
  period  <- paste0("from+", period_start,
                    "+to+", period_end)

  import_vars <- paste("CTY_CODE", "CTY_NAME", "HITECH",
                       "HITECH_DESC", "GEN_VAL_MO", "GEN_VAL_YR",
                       sep = ",")

  export_vars <- paste("CTY_CODE", "CTY_NAME", "HITECH",
                       "HITECH_DESC", "ALL_VAL_MO", "ALL_VAL_YR",
                       sep = ",")

  # Country name lookup from Census ------------------------------------------

  country_list <- census_country_codes()

  ctry_isos <- country_list %>%
    filter(iso_code %in% paste(ctry, sep = ",")) %>%
    select(code) %>%
    mutate(
      code_pattern = paste0("&CTY_CODE=", code)
    )

  ctry_code <- paste0(ctry_isos$code_pattern, collapse = "")

  # Data pull ----------------------------------------------------------------
  
  census_call <- function(direction, pull_vars, period, ctry_code, api_key) {

    resurl <-
      paste0("https://api.census.gov/data/timeseries/intltrade/", direction
                        , "/hitech?get=", pull_vars, "&time=", period
                        , ctry_code, "&key=", api_key)
    
    # note: as pulled, resurl contains identical cols for cty_code; remove V8
    df <- as.data.frame(jsonlite::fromJSON(resurl)) %>%
      select(-any_of(c("V8")))

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

    message("Pulled ", ctry, " ", direction)
    return(df_format)

  }

  imp_df <- census_call("imports", 
                        pull_vars = import_vars,
                        period, ctry_code, api_key)
  
  exp_df <- census_call("exports",
                        pull_vars = export_vars,
                        period, ctry_code, api_key)
  
  # Combine -----------------------------------------------------------------

  atp_trade <- exp_df %>%
    full_join(imp_df, by = c("date", "cty_code", "cty_name"
                             , "hitech", "hitech_desc")) %>%
    mutate(
      hightech_desc = stringr::str_to_title(
        stringr::str_extract(hitech_desc, "(.+)(?=\\s\\[)"))
    ) %>%
    select(date, cty_code, cty_name, hitech,
           hitech_desc = hightech_desc,
           exp_all_val_mo = all_val_mo,
           exp_all_val_yr = all_val_yr,
           imp_gen_val_mo = gen_val_mo,
           imp_gen_val_yr = gen_val_yr) %>%
    arrange(cty_name, date)

  message("Returning ", ctry, " ATP trade from ",
          format(as.Date(date_start, "%Y-%m-%d"), "%B %Y"), " to ",
          format(as.Date(date_end, "%Y-%m-%d"), "%B %Y"))

  return(atp_trade)

}
