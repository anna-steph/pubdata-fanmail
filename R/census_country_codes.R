#' Census country codes
#' 
#' Pull a list of countries and three-digit ISO country codes from Census
#' 
#' Taken from:
#' https://www.census.gov/foreign-trade/schedules/c/country.txt
#' 
#' Dependencies: dplyr, tidyr, readr
#' 
#' @param census_txt string; the URL for Census' list of country codes
#' 
#' @return df list of country codes
census_country_codes <- function(census_txt = "https://www.census.gov/foreign-trade/schedules/c/country.txt") {
  
  country_list <- as.data.frame(
    
    tryCatch({
      
      message(paste0("Pulling Census country codes..."))
      
      readr::read_delim(
        census_txt
        , delim = "|"
        , skip = 5
        , col_names = c("code", "name", "iso_code")
        , n_max = 240
        , trim_ws = TRUE,
        show_col_types = FALSE)
    },
    error = function(cond) {
      message("Census call returned error:")
      message(cond)
    },
    warning = function(cond) {
      message("Census call returned warning:")
      message(cond)
    }
    ))
  
  clean_list <- country_list %>%
    mutate(
      clean_iso = dplyr::if_else(
        code == 7920, "NA", trimws(stringr::str_extract(iso_code, "[A-Z][A-Z]"))
      ),
      clean_name = dplyr::if_else(
        code == 7660, "Congo, Democratic Republic of the Congo", name
      )
    ) %>%
    select(-c(iso_code, name)) %>%
    rename(iso_code = clean_iso, name = clean_name)
  
  return(clean_list)
  
}
