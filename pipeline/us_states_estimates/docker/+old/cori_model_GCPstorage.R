##### Packages ##### 
suppressPackageStartupMessages(library(tidyr))
suppressPackageStartupMessages(library(dplyr))
suppressPackageStartupMessages(library(readr))
suppressPackageStartupMessages(library(lubridate))
suppressPackageStartupMessages(library(EpiEstim))
suppressPackageStartupMessages(library(googleAuthR))     
suppressPackageStartupMessages(library(googleCloudStorageR))


##### Global parameters #####
bucket_name = "us-states-rt-estimation"
ma_window   <- 7
cori_window <- 1     
wend        <- TRUE  
SI_mean     <- 6
SI_mean_std <- 2.5
SI_mean_min <- max(SI_mean-3,0)
SI_mean_max <- SI_mean+3
SI_std      <- 3
SI_std_std  <- 2
SI_std_min  <- max(SI_std-2,0)
SI_std_max  <- SI_std+2


#####  Helper functions ##### 
na_to_0 <- function(vec){
  if(any(is.na(vec))){ vec[is.na(vec)] = 0 }
  vec
}
neg_to_0 <- function(vec){
  if(any(vec < 0)){ vec[vec < 0] = 0 }
  vec
}


#####  Read data and clean ##### 
raw <- gcs_get_object("data/covidtracking_cases_clean.csv", bucket=bucket_name)
df  <- raw %>% 
       mutate(date=ymd(date)) %>% mutate(state=as.factor(state)) %>%
       group_by(state) %>% arrange(state, date) %>%
       rename(time=date) %>% ungroup()
df$positive_diff_smooth <- na_to_0(df$positive_diff_smooth) 
df$positive_diff_smooth <- neg_to_0(df$positive_diff_smooth) 

# Subet to just state of interest
mystate <- commandArgs(trailingOnly = TRUE)
mystate <- as.character(mystate)
df <- df %>% filter(state==mystate)

#####  Loop through each state ##### 
fulldf <- data.frame()
for(statename in levels(droplevels(df$state))) {
  
  # Subset to just state and calculate moving averages
  state_df  <- df %>% filter(state == statename)

  # Other clean up
  idat <- state_df %>%
          complete(time = seq.Date(min(time), max(time), by='day')) %>%
          mutate_at(.vars = c('positive','death','positive_diff','death_diff',
                              'positive_diff_smooth','death_diff_smooth'), 
                    .funs = function(xx){ifelse(is.na(xx), 0, xx)}) %>%
          arrange(time) %>%
          rename(dates=time, I=positive_diff_smooth)
  idat$time <- seq(1, nrow(idat))
  
  # Get incidence time series
  ts <- seq(2, nrow(idat)-cori_window+1)
  te <- ts+(cori_window-1)
  
  # Use Cori method
  estimate_R(
    incid = idat %>% select(dates, I),
    method = "uncertain_si",
    config = make_config(
      list(
        mean_si = SI_mean,
        std_mean_si = SI_mean_std,
        min_mean_si = SI_mean_min,
        max_mean_si = SI_mean_max,
        std_si = SI_std,
        std_std_si = SI_std_std,
        min_std_si = SI_std_min,
        max_std_si = SI_std_max,
        n1 = 100,
        n2 = 200, 
        t_start = ts, 
        t_end = te
      )
    )
  ) -> outs
  
  # Save out
  outdf <- outs$R %>%
           mutate(time = if(wend == TRUE) t_end else ceiling((t_end+t_start)/2)) %>%
           select('time', 'Mean(R)', 'Quantile.0.025(R)', 'Quantile.0.975(R)') %>%
           merge(idat, by='time') %>%
           rename('RR_pred_cori'='Mean(R)', 
                  'RR_CI_upper_cori'='Quantile.0.975(R)', 
                  'RR_CI_lower_cori'='Quantile.0.025(R)',
                  'date'='dates') %>%
           select(state, date, RR_pred_cori, RR_CI_upper_cori, RR_CI_lower_cori)
  fulldf <- fulldf %>% bind_rows(outdf)
}


## Authenticate on GCE for google cloud services
googleAuthR::gar_gce_auth()

# Save a temp file to upload
#tmp <- tempfile(fileext = ".csv")
#on.exit(unlink(tmp))
#write.csv(fulldf, file=tmp, row.names=FALSE)

# Save out
write_csv(fulldf, "data/cori_estimates.csv")

## Upload
#gcs_upload(tmp, 
#           bucket = bucket_name, 
#           name = paste0("data/cori_estimates_", mystate, ".csv"))