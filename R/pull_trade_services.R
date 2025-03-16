#' BEA services lookup
#' 
#' Pull service industry lookup from BEA api for trade in services data
#' 
#' Lookup contains two fields: Key and Desc (description)
#' 
#' Dependencies: jsonlite
#' 
#' Notes:
#' For help on the BEA international services trade data API:
#' https://apps.bea.gov/api/_pdf/bea_web_service_api_user_guide.pdf
#' 
#' For BEA services trade category definitions:
#' https://www.bea.gov/resources/methodologies/international/pdf/iea-concepts-methods.pdf
#'
#' @param api_key string; Bureau of Economic Analysis API key obtained here:
#' https://apps.bea.gov/API/signup/index.cfm
#' 
#' @return df lookup of industry categories in trade in services data
bea_services_lookup <- function(api_key) {

  service_param <- jsonlite::fromJSON(paste0("https://apps.bea.gov/api/data/?&UserID=",
                                             api_key,
                                             "&method=GetParameterValues",
                                             "&DataSetName=IntlServTrade&ParameterName=TypeOfService&ResultFormat=json"))

  services_lookup <- as.data.frame(service_param$BEAAPI$Results$ParamValue)

  return(services_lookup)

}


#' BEA country lookup
#' 
#' Pull country lookup from BEA API for trade in services data
#' 
#' Dependencies: jsonlite
#' 
#' Notes:
#' For help on the BEA international services trade data API:
#' https://apps.bea.gov/api/_pdf/bea_web_service_api_user_guide.pdf
#'
#' @param api_key string; Bureau of Economic Analysis API key obtained here:
#' https://apps.bea.gov/API/signup/index.cfm
#' 
#' @return df lookup of countries  in trade in services data
bea_country_lookup <- function(api_key) {

  # Country and service lookups -----------------------------------------------

  country_param <- jsonlite::fromJSON(paste0("https://apps.bea.gov/api/data/?&UserID=",
                                             api_key,
                                             "&method=GetParameterValues",
                                             "&DataSetName=IntlServTrade&ParameterName=AreaOrCountry&ResultFormat=json"))

  country_lookup <- as.data.frame(country_param$BEAAPI$Results$ParamValue)

  return(country_lookup)  

}


#' Pull services trade
#' 
#' Pull bilateral U.S. trade in services data
#' 
#' Source: Bureau of Economic Analysis (BEA)
#'
#' Dependencies: dplyr, tidyr, jsonlite, stringr
#'
#' Note:
#' The BEA API requires exactly one country or exactly one type of trade
#' Can't pull all trade types for all countries
#' 
#' For help on the BEA international services trade data API:
#' https://apps.bea.gov/api/_pdf/bea_web_service_api_user_guide.pdf
#' 
#' For BEA services trade category definitions:
#' https://www.bea.gov/resources/methodologies/international/pdf/iea-concepts-methods.pdf
#'
#' @param ctry string; Countries of interest (optional; defaults to all countries)
#' See bea_country_lookup below for full list of countries
#' format as "Canada" or c("Canada", "Mexico")
#' 
#' @param serv string; Services of interest (options: defaults to all services)
#'
#' @param api_key string; Bureau of Economic Analysis API key obtained here:
#' https://apps.bea.gov/API/signup/index.cfm
#'
#' @param first_yr string; year of first obs
#'
#' @param last_yr string; year of final obs
#'
#' @return df with total annual U.S. services exports and imports by country
pull_trade_services <- function(ctry, serv, api_key, first_yr, last_yr) {

  if (isTRUE(is.numeric(first_yr)) | isTRUE(is.numeric(last_yr))) {
    first_yr <- as.character(first_yr)
    last_yr <- as.character(last_yr)
  }

  year_list <- paste(seq(from = first_yr, to = last_yr, by = 1), sep = " ", collapse = ",")

  # Country and service lookups -----------------------------------------------

  services_lookup <- bea_services_lookup(api_key = api_key)
  country_lookup <- bea_country_lookup(api_key = api_key)

  # Specify country or countries of interest

  if (missing(ctry)) {
    country_keys <- "All"
  } else {
    search_params <- paste0("(", tolower(paste(ctry, sep = "", collapse = "|")), ")")
    country_of_interest <- country_lookup %>%
      filter(stringr::str_detect(trimws(tolower(Desc)), search_params))
    country_keys <- paste(country_of_interest$Key, collapse = ",")
  }

  if (missing(serv)) {
    service_keys <- "All" # see BEA API guide
  } else {
    ind_search_params <- paste0("(", tolower(paste(serv, sep = "", collapse = "|")), ")")
    service_of_interest <- services_lookup %>%
      filter(stringr::str_detect(trimws(tolower(Desc)), ind_search_params))
    service_keys <- paste(service_of_interest$Key, collapse = ",")
  }

  data_req <- paste0("https://apps.bea.gov/api/data?UserID=",
                     api_key,
                     "&method=GetData&datasetname=INTLSERVTRADE&TypeOfService=",
                     service_keys,
                     "&TradeDirection=All&Affiliation=AllAffiliations&AreaOrCountry=",
                     country_keys,
                     "&Year=",
                     year_list,
                     "&ResultFormat=json")

  call_bea <- function(url) {

    download <- tryCatch({

      message("Pulling ", country_keys, " services trade from ",
              first_yr, " to ", last_yr)

      data_pull <- jsonlite::fromJSON(url)
    }, 
    error = function(cond) {
      message("BEA call returned error:")
      message(cond)
      message(". If call timed out, try re-running.")
    },
    warning = function(cond) {
      message("BEA call returned warning:")
      message(cond)
    })

    serv_data  <- as.data.frame(download$BEAAPI$Results$Data)
    return(serv_data)

  }

  services <- call_bea(data_req)

  if (nrow(services) == 0) {

    error_message <- jsonlite::fromJSON(data_req)$BEAAPI$Error$APIErrorDescription
    message(paste0("Error: ", error_message))
    services <- services_lookup

  } else {

    services <- services %>%
      mutate(
        Year = as.numeric(Year),
        TimePeriod = as.numeric(TimePeriod),
        UNIT_MULT = as.numeric(UNIT_MULT),
        DataValue = dplyr::if_else(DataValue == "", NA_integer_, as.numeric(DataValue))
      )

  }

  return(services)

}
