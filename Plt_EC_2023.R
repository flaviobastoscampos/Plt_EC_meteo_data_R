# ET Main/Agritech Project - Plantaditsch dataset # 
# Script file:    Plt_EC_2023.R
# Title:          Process Eddy Covariance and Meteorological Data of a Vineyard Station
# Author:         Flávio Bastos Campos
# Date:           06/12/2022
# Updates:        08/03/2023, 30/06/2023, 04/07/2023, 30/11/2023
# Version:        4.3
#
# Description:    
# This script processes data from the Plantaditsch vineyard station, focusing on Eddy Covariance, 
# and Meteorological data to study water and energy flux dynamics. 
# It includes monthly aggregation, energy balance correction, and organizes data for use in REddyProcWeb.
#
#
# READ ME ------------------------------------------------
# Notes:
# - Define directories in (1) according to your machine.
# - Create the following directories and place the input data files in input_data_dir:
# 
# input_data_dir: where the two input data files are (Plt_HW_2022_h.rds and Plt_HW_irrig_2022_d.rds);
# derived_data_dir: where the derived data files will be exported to;
# plots_dir: where the plots will be saved.
#
# Directories:
# Defines paths for loading data, code, and outputs to streamline workflow.
#
# Libraries & Functions:
# Dynamically checks and loads required libraries and custom functions.
#
# Timestamps:
# Builds timestamps in solar time for dataset alignment.
#
# Data Import:
# Loads EC and meteorological datasets, performs initial checks, and prepares data for processing.
#
# Data Cleaning & Processing:
# - Adjusts timestamps for legal vs. solar time.
# - Filters, renames, and reorders columns.
# - Replaces infinite values with NA and adjusts for -9999 conventions.
#
# Key Checks and Adjustments:
# Ensures time alignment, minute intervals, and data quality.
# Conditional corrections if the logger uses legal time.
#
# Export:
# Exports cleaned datasets as CSV and RDS for further analysis.
# 
# Sections:
# 1. Data Preparation: Monthly sums, EBR computation, and quality checks.
# 2. Export and Formatting: Formats time series for REddyProcWeb and saves final files.
# 3. Post-Processing: Calculates VPD, GPP, NEE, and saves results.
#
# Goals:
# - Standardize EC data format for 2023 to ensure consistency and reliable analysis.
# - Correct and unify energy flux measurements for valid cross-year comparisons.
# - Aggregates and corrects monthly energy fluxes (A and E components) and calculates 
#   the Energy Balance Ratio (EBR) from Eddy Covariance data.
# - Exports monthly aggregated EBR data and prepares time series for REddyProcWeb.
# - Includes transformations, quality checks, and outputs for further analysis.
# 
# Cautions:
# - **Timezones**: Confirm if data is recorded in legal time; adjust if necessary.
# - **Outlier Handling**: Convert extreme values (-Inf/Inf) to NA.
# - **Backup**: Create backups before running this script, especially when overwriting directories.
# - Align timestamps with 30-minute intervals to prevent inconsistencies.
# - Replace missing/infinite values as needed to avoid calculation errors.
# - Confirm column order and headers align with REddyProcWeb requirements.
# - Ensure data meets input requirements before uploading to REddyProcWeb.
#
# Requirements:
# - Working directories: `clean_data_dir` and `organized_data_dir`.
# - Relevant packages: see Libs&functions.R


# Directories  -------------------------

data_dir <- "/home/bastosca/Desktop/SEND/Ahmed/load_data"
code_directory <- "/home/bastosca/Desktop/SEND/Ahmed/code"
clean_data_dir <- "/home/bastosca/Desktop/SEND/Ahmed/clean_data"
organized_data_dir <- "/home/bastosca/Desktop/SEND/Ahmed/Organized_datasets"
from_gf_data_dir <- "/home/bastosca/Desktop/SEND/Ahmed/load_data/from_gf"


# Libraries and functions -----------------------------------------------

# Load from source
# Check the EC_data_directory
if (!dir.exists(code_directory)) {
  warning("Please indicate a directory to load the libraries and functions needed.")
  stop("Directory does not exist: ", code_directory)
} else {
  message("Directory found: ", code_directory)
  message("libraries and functions loaded.")
  setwd(code_directory)
  source("Libs&functions.R")
}


# timestamp  -------------------------

# build a timestamp for benchmarking
# EC and meteo are already being recorded in solar time

# we have data from Jan-May 2023 in Plt:
t_stamp <- seq.POSIXt(as.POSIXct("2023-01-01 00:00:00", tz= "GMT"),
                      as.POSIXct("2023-12-31 23:59:00", tz= "GMT"), 
                      units= "hour", by= "30 mins")

t_stamp_tibble <- tibble(date= t_stamp)



# Importing data  -------------------------

# 1) EC and meteo data  -------------------------

# EC is already set record in legal time.

# read csv file:
setwd(data_dir)
data <- read.csv("Eddy_Plantaditsch_2023_22September.csv", header=T,
                 sep=",", dec=".", stringsAsFactors=F)
data <- as_tibble(data[1:(nrow(data)-0),]) 

# convert column into numeric, if needed
# cols.num <- names(data)
# data[cols.num] <- sapply(data[cols.num], as.numeric) 

# adding the POSIXct timestamp to the tibble
data <- data %>%
  mutate(date= as.POSIXct(data$Date...Time, format="%d.%m.%Y %H:%M", tz="GMT"))

# rename columns
names(data)[names(data) == names(data)] <- 
  c("old_timestamps", "H_raw", "LE_raw", "E_raw", "C_raw", "Ustar",         
    "tau", "var.u", "var.v", "var.w", "var.t", "var.c", "var.h", 
    "avg.u", "avg.v", "avg.w", "avg.t", "avg.c", "avg.h",
    "wv_hor", "wd_hor", "alpha", "beta", "gamma",      
    "c.lag", "h.lag", "L", "zeta", "corr_avg.t",
    "corr_H", "corr_LE", "corr_E", "corr_C",
    "foot_peak", "X50_fetch", "X90_fetch", 
    "H_LE", "sttest1", "sttest2", "itchu", "itchw", "itcht",      
    "var.u_rot.", "var.v_rot.", "var.vhor", "var.wd",
    "u_c", "v_c", "w_c", "t_c", "c_c", "h_c", "u_l", "v_l",        
    "w_l", "t_l", "c_l", "h_l", "recs",
    "p_air", "t_air", "rh_air", "e_air", "rho", "cl_set", "hl_set",     
    "z_meas", "z_veg", "date")

# removing old_timestamps
data <- data %>% select(-old_timestamps)

# re-order columns
col_order <-   c("date", "H_raw", "LE_raw", "E_raw", "C_raw", "Ustar",         
                 "tau", "var.u", "var.v", "var.w", "var.t", "var.c", "var.h", 
                 "avg.u", "avg.v", "avg.w", "avg.t", "avg.c", "avg.h",
                 "wv_hor", "wd_hor", "alpha", "beta", "gamma",      
                 "c.lag", "h.lag", "L", "zeta", "corr_avg.t",
                 "corr_H", "corr_LE", "corr_E", "corr_C",        # corrected fluxes to use
                 "foot_peak", "X50_fetch", "X90_fetch", 
                 "H_LE", "sttest1", "sttest2", "itchu", "itchw", "itcht",      
                 "var.u_rot.", "var.v_rot.", "var.vhor", "var.wd",
                 "u_c", "v_c", "w_c", "t_c", "c_c", "h_c", "u_l", "v_l",        
                 "w_l", "t_l", "c_l", "h_l", "recs",
                 "p_air", "t_air", "rh_air", "e_air", "rho", "cl_set", "hl_set",     
                 "z_meas", "z_veg")

data <- data[,col_order]


# Check-ups and corrections ---------------------

# (1) fix minute
nrow(data)
levels(as.factor(minute(data$date)))
data <- data %>% filter(minute(date)==0 | minute(date)==30)
nrow(data)

# (2) for rows with wrong/double timestamps
data$date[duplicated(data$date)]

# (3) substitute -Inf/Inf values by NA
is.na(data) <- sapply(data, is.infinite)         # remove Inf/-Inf

# (4) check minutes and others
levels(as.factor(minute(data$date)))
levels(as.factor(hour(data$date)))
levels(as.factor(day(data$date)))
levels(as.factor(month(data$date)))
levels(as.factor(year(data$date)))
# all ok.

# (5) Manual check if the system was logging with CEST adjustments
data_t <- data %>% 
  filter(date>=as.POSIXct("2023-03-24 00:00:00", tz="GMT") & 
           date<=as.POSIXct("2023-03-26 20:00:00", tz="GMT"))

# view(data_t[,c("date", "LE_raw")])
# ok. It's always in Solar time.

# If it logs in Legal time:
# split the timeseries in before and after the clock change
# # solar time (ST)
# data_Plt_ST <- data_Plt %>% 
#   filter(date>=as.POSIXct("2023-02-01 00:00:00", tz="GMT") & 
#            date<=as.POSIXct("2023-03-26 01:30:00", tz="GMT"))
# 
# # legal time (LT)
# data_Plt_LT <- data_Plt %>% 
#   filter(date>=as.POSIXct("2023-03-26 03:00:00", tz="GMT") & 
#            date<=as.POSIXct("2023-05-10 23:59:00", tz="GMT"))
# 
# data_Plt_LT$date <- data_Plt_LT$date - (1*60*60)   # forcing the clock back to solar time
# 
# data_Plt_ST$date[1]
# data_Plt_ST$date[nrow(data_Plt_ST)]
# data_Plt_LT$date[1]
# data_Plt_LT$date[nrow(data_Plt_LT)]
# 
# data_Plt <- rbind(data_Plt_ST, data_Plt_LT)
# 
# # get time components again after adjusting to solar time
# data_Plt <- data_Plt %>% mutate(Year= year(date),
#                                 month= month(date),
#                                 day= day(date),
#                                 hour= hour(date),
#                                 minute= minute(date))

# (6) separate underestimated LE data from April 2023 onwards
# data <- data %>% mutate(
#   corr_LE= ifelse(date>=as.POSIXct("2023-01-01 00:00:00", tz="GMT") &
#                     date<=as.POSIXct("2023-04-26 00:00:00", tz="GMT"), 
#                   corr_LE,
#                   NA))

# For .csv version (tag with -9999)
data_for_csv <- data %>% replace(is.na(.), -9999)        # NA -> -9999

# keeping a tibble with 365 * 48 entries
data_for_csv <- t_stamp_tibble %>% left_join(data_for_csv, join_by(date))

# export csv and txt
setwd(clean_data_dir)
write.csv2(data_for_csv, file= "Plt_EC_2023_22September.csv")

write.table(data_for_csv, file = "x_Plt_EC_2023_22September.txt", sep = ",", dec=".",
            na="-9999", quote=F, row.names=F, col.names=T)

# save
setwd(clean_data_dir)
saveRDS(data, file= "Plt_EC_2023_m1_m9.rds")


# 2) meteo, radiation, precipitation and soil data   -------------------------

# reading meteo from the .dat file from the CR3000
setwd(data_dir)
data_meteo <- read.table('CR3000_Meteo_Rn_G_usoil_2023_Sept.dat',
                         sep=',', dec=".", header=T, skip=1)
data_meteo <- as_tibble(data_meteo[3:nrow(data_meteo),])

# getting the timestamp from the original TIMESTAMP
data_meteo_timestamp <- data_meteo$TIMESTAMP

# convert column into numeric
cols.num <- names(data_meteo)
data_meteo[cols.num] <- sapply(data_meteo[cols.num], as.numeric) 

# adding the timestamp back to the tibble
data_meteo <- data_meteo %>% select(-TIMESTAMP)

date_vec <- as.POSIXct(data_meteo_timestamp, format="%Y-%m-%d %H:%M:%S", tz="GMT")
data_meteo <- as_tibble(data_meteo) %>% mutate(date= date_vec)

# re-order columns
# get all names but "date"
list_names= names(data_meteo)[!(names(data_meteo) %in% c("date"))]

# write the customised order
col_order <- c("date", list_names)

# set it in order
data_meteo <- data_meteo[,col_order]

# fix names
data_meteo <- 
  data_meteo[,c("date", "RECORD", "batt_Avg",                
                "Rinc_SW_CM3_Avg", "Rout_SW_CM3_Avg", "Rinc_LW_CG3_Avg", "Rout_LW_CG3_Avg",
                "Rn_CNR1_Avg", "T_CNR1_PT100_Avg",        
                "rain_Tot", "T_air_Avg", "RH_air_Avg", 
                "CS616_period_ref_Avg", "CS616_period_trench_Avg",
                "usoil_ref_Avg", "usoil_trench_Avg", 
                "soil_heat_1_Avg", "soil_heat_2_Avg",
                "PAR_tot_BF5_Avg", "PAR_diff_BF5_Avg",    # NAs
                "UpRed_NDVI_Avg", "DownRed_NDVI_Avg", "UpNIR_NDVI_Avg", "DownNIR_NDVI_Avg", # NAs       
                "NDVI_Avg", "UpRed_PRI_Avg", "DownRed_PRI_Avg", "UpNIR_PRI_Avg", 
                "DownNIR_PRI_Avg", "PRI_Avg", "par_Avg")]      # NAs

names(data_meteo)[names(data_meteo) == names(data_meteo)] <- 
  c("date", "RECORD", "batt_Avg",                
    "SWin", "SWout", "LWin", "LWout",
    "Rn_Wm2", "T_CNR1",        
    "Precip", "Tair", "rH", 
    "CS616_period_ref_Avg", "CS616_period_trench_Avg",
    "usoil_10cm", "usoil_50cm", 
    "soil_heat_not_connected", "G_Wm2",
    "PAR_tot_BF5_Avg", "PAR_diff_BF5_Avg",    # NAs
    "UpRed_NDVI_Avg", "DownRed_NDVI_Avg", "UpNIR_NDVI_Avg", "DownNIR_NDVI_Avg", # NAs       
    "NDVI_Avg", "UpRed_PRI_Avg", "DownRed_PRI_Avg", "UpNIR_PRI_Avg",     # NAs
    "DownNIR_PRI_Avg", "PRI_Avg", "par_Avg")    # NAs



# Check-ups and corrections ---------------------

# (1) fix minute
nrow(data_meteo)
levels(as.factor(minute(data_meteo$date)))
data_meteo <- data_meteo %>% filter(minute(date)==0 | minute(date)==30)
nrow(data_meteo)

# (2) for rows with wrong/double timestamps
data_meteo$date[duplicated(data_meteo$date)]

# (3) substitute -Inf/Inf values by NA
is.na(data_meteo) <- sapply(data_meteo, is.infinite)         # remove Inf/-Inf

# (4) check minutes and others
levels(as.factor(minute(data_meteo$date)))
levels(as.factor(hour(data_meteo$date)))
levels(as.factor(day(data_meteo$date)))
levels(as.factor(month(data_meteo$date)))
levels(as.factor(year(data_meteo$date)))
# all ok.

# keep only 2023 for now
data_meteo <- data_meteo %>% 
  filter(date>=as.POSIXct("2023-01-01 00:00:00", tz="GMT") & 
           date<=as.POSIXct("2023-09-29 20:00:00", tz="GMT"))


# (5) Manual check if the system was logging with CEST adjustments
data_meteo_t <- data_meteo %>% 
  filter(date>=as.POSIXct("2023-03-24 00:00:00", tz="GMT") & 
           date<=as.POSIXct("2023-03-26 20:00:00", tz="GMT"))

# view(data_meteo_t[,c("date", "Tair")])
# ok. It's always in Solar time.

# If it logs in Legal time:
# split the timeseries in before and after the clock change
# # solar time (ST)
# data_meteo_Plt_ST <- data_meteo_Plt %>% 
#   filter(date>=as.POSIXct("2023-02-01 00:00:00", tz="GMT") & 
#            date<=as.POSIXct("2023-03-26 01:30:00", tz="GMT"))
# 
# # legal time (LT)
# data_meteo_Plt_LT <- data_meteo_Plt %>% 
#   filter(date>=as.POSIXct("2023-03-26 03:00:00", tz="GMT") & 
#            date<=as.POSIXct("2023-05-10 23:59:00", tz="GMT"))
# 
# data_meteo_Plt_LT$date <- data_meteo_Plt_LT$date - (1*60*60)   # forcing the clock back to solar time
# 
# data_meteo_Plt_ST$date[1]
# data_meteo_Plt_ST$date[nrow(data_meteo_Plt_ST)]
# data_meteo_Plt_LT$date[1]
# data_meteo_Plt_LT$date[nrow(data_meteo_Plt_LT)]
# 
# data_meteo_Plt <- rbind(data_meteo_Plt_ST, data_meteo_Plt_LT)
# 
# # get time components again after adjusting to solar time
# data_meteo_Plt <- data_meteo_Plt %>% mutate(Year= year(date),
#                                 month= month(date),
#                                 day= day(date),
#                                 hour= hour(date),
#                                 minute= minute(date))


# (6) remove meaningless G data from April 2023 onwards
data_meteo <- data_meteo %>% mutate(
  G_Wm2= ifelse(date>=as.POSIXct("2023-01-01 00:00:00", tz="GMT") &
                  date<=as.POSIXct("2023-04-26 00:00:00", tz="GMT"), 
                G_Wm2,
                NA))

# For .csv version (tag with -9999)
data_meteo_for_csv <- data_meteo %>% replace(is.na(.), -9999)        # NA -> -9999

# keeping a tibble with 365 * 48 entries
data_meteo_for_csv <- t_stamp_tibble %>% left_join(data_meteo_for_csv, join_by(date))

# export csv and txt
setwd(clean_data_dir)
write.csv2(data_meteo_for_csv, file= "Plt_meteo_2023_22September.csv")

write.table(data_meteo_for_csv, file = "x_Plt_meteo_2023_22September.txt", sep = ",", dec=".",
            na="-9999", quote=F, row.names=F, col.names=T)

# save
setwd(clean_data_dir)
saveRDS(data_meteo, file= "Plt_meteo_2023_m1_m9.rds")



# 3) EC + meteo -------------------------

# load
setwd(clean_data_dir)
data <- readRDS("Plt_EC_2023_m1_m9.rds")
data_meteo <- readRDS("Plt_meteo_2023_m1_m9.rds")

# merge EC and meteo
data <- t_stamp_tibble %>% left_join(data, join_by(date))
data <- data %>% left_join(data_meteo, join_by(date))

# substitute -Inf/Inf values by NA
is.na(data) <- sapply(data, is.infinite)         # remove Inf/-Inf

# get DoY and VPD
data <- data %>% mutate(DoY= yday(date),
                        VPD_KPa= VPD(rH, Tair))       # calculated VPD(KPa)

# save
setwd(clean_data_dir)
saveRDS(data, file= "Plt_EC_meteo_2023_m1_m9.rds")


# 4) *  gf (hh) 21-22-23 -----------------------

# imputing only G values based on env variables
# using hh scale

# Load previously treated datasets
setwd(organized_data_dir)
data_2021 <- readRDS("Plt_EC_meteo_2021.rds")
data_2022 <- readRDS("Plt_EC_meteo_2022.rds")

# Load current year's dataset
setwd(clean_data_dir)
data_2023 <- readRDS("Plt_EC_meteo_2023_m1_m9.rds")

# 2021
input_vars_2021 <- c("date", "H_f_Wm2", "Rg_f_Wm2", "G_Wm2", "Tair", "ws", "usoil_10cm")
data_2021 <- data_2021[,input_vars_2021]
colnames(data_2021) <- c("date", "H", "Rg", "G", "Tair", "ws", "usoil")

# 2022
input_vars_2022 <- c("date", "H_raw", "SWin", "G_Wm2", "Tair", "ws", "usoil_10cm")
data_2022 <- data_2022 %>% mutate(ws= (avg.w^2 + avg.u^2 + avg.v^2)^0.5)        # m/s
data_2022 <- data_2022[,input_vars_2022]
colnames(data_2022) <- c("date", "H", "Rg", "G", "Tair", "ws", "usoil")

# compute ws for 2023
data_2023 <- data_2023 %>% mutate(ws= (avg.w^2 + avg.u^2 + avg.v^2)^0.5)        # m/s
input_vars_2023 <- c("date", "H_raw", "SWin", "G_Wm2", "Tair", "ws", "usoil_10cm")
data_2023 <- data_2023[,input_vars_2023]
colnames(data_2023) <- c("date", "H", "Rg", "G", "Tair", "ws", "usoil")

# merge
data <- rbind(data_2021, data_2022, data_2023)

# get proper period of observed data with NAs
data_cut <- data %>% filter(
  date>=as.POSIXct("2021-05-01 00:00:00", tz="GMT") &
    date<=as.POSIXct("2023-09-22 23:59:00", tz="GMT"))

# keep timestamps of data
date_vec <- data_cut$date

# get columns of interest
data_cut <- data_cut[,c("H", "Rg", "G", "Tair", "ws", "usoil")] %>% 
  as.data.frame()

skim(data_cut)$n_missing

# start the gap-filling of G 
# Sys.time()
# imp_data_cut <- missForest(data_cut)   # to run the imputation
# imp_data_cut$OOBerror                  # NRMSE= 0.3319787 for this run
# Sys.time()
# 
# imp_data_cut <- as_tibble(imp_data_cut$ximp)
# 
# colnames(imp_data_cut) <- c("H_gf", "Rg_gf", "G_gf",
#                             "Tair_gf", "ws_gf", "usoil_10cm_gf")
# 
# imp_data_cut <- imp_data_cut %>% mutate(date= date_vec)
setwd(from_gf_data_dir)
# saveRDS(imp_data_cut, file = "imp_data_cut_temp_beforeEBC_hh.rds")

# load it
imp_data_cut <- readRDS("imp_data_cut_temp_beforeEBC_hh.rds")


# left_join the imp_data_cut columns into my complete dataset
names(data)[names(data)=="G_Wm2"] <- "G_Wm2_gaps"
names(data)[names(data)=="Tair"] <- "Tair_gaps"
names(data)[names(data)=="ws"] <- "ws_gaps"
names(data)[names(data)=="rH"] <- "rH_gaps"
names(data)[names(data)=="usoil"] <- "usoil_10cm_gaps"
# names(data)[names(data)=="LE"] <- "LE_gaps"
names(data)[names(data)=="H"] <- "H_gaps"
names(data)[names(data)=="Rg"] <- "Rg_gaps"

data_hh_imp <- data %>% left_join(
  imp_data_cut[,c("H_gf", "Rg_gf", "G_gf",
                  "Tair_gf", "ws_gf", "usoil_10cm_gf", "date")], 
  by= "date")

# Save
setwd(clean_data_dir)
saveRDS(data_hh_imp, file = "data_hh_Plt21_22_23_imp_beforeEBC.rds")



# 5) EBC ----------------------------

# EBC over 1 month, using original values, hh scale
# load complete datafile
setwd(clean_data_dir)
data_w_raw <- readRDS("Plt_EC_meteo_2023_m1_m9.rds")

# load imputed variables
setwd(clean_data_dir)
data_imp <- readRDS("data_hh_Plt21_22_23_imp_beforeEBC.rds")     # filter only 2023
data_imp <- data_imp[,c("H_gf", "Rg_gf", "G_gf",
                        "Tair_gf", "ws_gf", "usoil_10cm_gf", "date")]

# merge
data_w_raw <- data_w_raw %>% left_join(data_imp, join_by(date))

# Energy Balance Closure (Wohlfahrt 2019 - method 4)
# remove NAs only to obtain the EBR
# continue using data_w_raw because it has NA, not -9999 flags
data_w_non_EBC <- data_w_raw

# substitute -Inf/Inf values in SWout by NA
is.na(data_w_non_EBC) <- sapply(data_w_non_EBC, is.infinite)         # remove Inf/-Inf

# switch -9999 flags for NA: General filter
nrow_ini <- nrow(data_w_non_EBC)
data_w_non_EBC <- data_w_non_EBC %>% replace(.==-9999, NA)   # data[data <= -2000] <- NA
nrow_end <- nrow(data_w_non_EBC)
nrow_end/nrow_ini

# get date values
data_w_non_EBC <- data_w_non_EBC %>% mutate(Year= year(date),
                                            month= month(date),
                                            day= day(date),
                                            A_Wm2= Rn_Wm2 - G_gf,
                                            E_Wm2= LE_raw + H_raw,       # correct definition: + sign
                                            error= A_Wm2 - E_Wm2)        # correct definition: - sign)

# visual check
plot(data_w_non_EBC$date, (month(data_w_non_EBC$date))/0.025, col="black", type="l", ylim=c(-250,600))
points(data_w_non_EBC$date, (data_w_non_EBC$H_raw), col="darkorange")
points(data_w_non_EBC$date, (data_w_non_EBC$LE_raw), col="blue")
lines(data_w_non_EBC$date, (data_w_non_EBC$Tair)*5, col="darkred")
points(data_w_non_EBC$date, (month(data_w_non_EBC$date))/0.025, col="black", type="l")
lines(data_w_non_EBC$date, (data_w_non_EBC$G_Wm2)*1, col="magenta")


# need to filter and remove the whole row if NA is present in Rn, LE, H or G
# in 2023, the EBC will be done only until April (last value 2023-04-26 00:00:00)
nrow(data_w_non_EBC)
data_w_non_EBC_f <- data_w_non_EBC %>% filter(Rn_Wm2 > -100 & G_gf > -100)   # using G_gf here
data_w_non_EBC_f <- data_w_non_EBC_f %>% filter(LE_raw > -100 & H_raw > -100)
data_w_non_EBC_f <- data_w_non_EBC_f %>% mutate(i= 1)          # insert counter
nrow(data_w_non_EBC_f)
nrow(data_w_non_EBC_f)/nrow(data_w_non_EBC)

# get monthly sums
# export EBR
setwd(clean_data_dir)
saveRDS(data_w_non_EBC_f, file = "data_w_non_EBC_f_Plt2023.rds")

# Compute EBR
data_w_non_EBC_f_m <- data_w_non_EBC_f %>% group_by(Year, month) %>% summarise(
  A_Wm2= sum(A_Wm2),
  E_Wm2= sum(E_Wm2),
  EBR= sum(E_Wm2)/sum(A_Wm2),
  N= sum(i))

# check the Years and months available in the dataset
levels(as.factor(data_w_non_EBC_f_m$Year))
levels(as.factor(data_w_non_EBC_f_m$month))
tib <- data_w_non_EBC_f %>% 
  group_by(Year, month) %>% 
  summarise(tib_test= mean(Tair, na.rm=T))

# Check how many month-values are there in the data
nrow(data_w_non_EBC_f_m)

# Add the month-values into the unfiltered file: data_w_non_EBC 
EBR_vector <- data_w_non_EBC_f_m$EBR          # a vector with '# month-values' entries

# export EBR
setwd(clean_data_dir)
saveRDS(EBR_vector, file = "EBR_vector__Plt2023.rds")
saveRDS(data_w_non_EBC_f_m, file = "EBR_tibble_Plt2023.rds")

EBC_tag <- vector()
EBC_tag[1] <- 1

for (j in c(2:nrow(data_w_non_EBC))) {
  month_change= ifelse(data_w_non_EBC$month[j] != data_w_non_EBC$month[j-1], 1, 0)
  x= ifelse(month_change==0, 0, 1)
  EBC_tag[j]= EBC_tag[j-1] + x
}

# insert EBR from EBR_vector into data_w_non_EBC file using the EBC_tag as row index
data_EBC_1m <- data_w_non_EBC %>% mutate(
  EBR=  round(EBR_vector[EBC_tag], 4))

# check the EBC tags and the EBR values
levels(as.factor(EBC_tag))
levels(as.factor(data_EBC_1m$EBR))

# compute corrected values
data_EBC_1m <- data_EBC_1m %>% mutate(
  A= Rn_Wm2 - G_gf,                          # in Wm2, G_gf here
  E= LE_raw + H_raw,                         # in Wm2
  error= A - E,                              # in Wm2
  LE_corr= LE_raw/EBR,                       # in Wm2
  H_corr= H_raw/EBR,                         # in Wm2
  # LE_f_corr= LE_raw/EBR,                     # in Wm2
  # H_f_corr= H_raw/EBR,                       # in Wm2
  E_Wm2_corr= LE_corr + H_corr)              # in Wm2


# Quality check
par(mfrow=c(3,1), mai = c(0.25, 0.55, 0.25, 0.25))    # (bottom, left, upper, right)
plot(data_EBC_1m$date, data_EBC_1m$LE_corr, col="darkgreen", ylim=c(-150, 600))
plot(data_EBC_1m$date, data_EBC_1m$LE_raw, col="blue", ylim=c(-150, 600))
plot(data_EBC_1m$date, data_EBC_1m$EBR, col="red", type="l", ylim=c(0.2, 3))
abline(h=1)

data_w_non_EBC_f_m$EBR

# substitute -Inf/Inf values by NA
is.na(data_EBC_1m) <- sapply(data_EBC_1m, is.infinite)         # remove Inf/-Inf

# For .csv version (tag with -9999)
data_EBC_1m_9999 <- data_EBC_1m %>% replace(is.na(.), -9999)        # NA -> -9999

# Export
setwd(clean_data_dir)
write.csv2(data_EBC_1m_9999, file= "data_EBC_1m_Plt2023.csv")

saveRDS(data_EBC_1m, file = "data_EBC_1m_Plt2023.rds")


# 5.2) for REddyProcWeb -----------------------

# format files to use REddyProcWeb tool
setwd(clean_data_dir)
data <- readRDS("data_EBC_1m_Plt2023.rds")

# format files to use REddyProcWeb tool
# important: start with the 00:30 of the 1st day of interest
# and finish with the 00:00 of the one-day-after the last day of interest
t_stamp_merging <- seq.POSIXt(as.POSIXct("2023-01-01 00:30:00", tz= "GMT"),
                              as.POSIXct("2023-10-01 00:01:00", tz= "GMT"), 
                              units= "hour", by= "30 mins")

t_stamp_merging_tibble <- tibble(date= t_stamp_merging)

data <- t_stamp_merging_tibble %>% left_join(data, join_by(date))   # set data into the complete timestamp

data <- data %>% mutate(
  DoY= yday(date),
  Year= year(date),
  month= month(date),
  day= day(date),
  hour= hour(date),
  min= minute(date),
  VPD_KPa= VPD(rH, Tair),
  VPD_hPa= 10*VPD_KPa,                  # VPD in hPa
  Hour_dec= get_Hour_decimal(minute(date)),
  hours= hour(date))

data <- data %>% mutate(
  Hour= hours + Hour_dec)  # add 0.5 to match the REddyProc system

# organise columns
data <- data[,c("Year", "DoY", "Hour", "corr_C", "corr_LE", "corr_H", 
                "SWin", "Tair", "rH", "VPD_hPa", "Ustar", "date")]

colnames(data) <- c("Year", "DoY", "Hour", "NEE", "LE", "H",
                    "Rg", "Tair", "rH", "VPD", "Ustar", "date")

# first checks
skim(data)
nrow(data)/48
plot(data$date)

# check for equidistant timestamps
counter <- data %>% group_by(DoY) %>% count()
plot(counter$DoY, counter$n)
abline(h=48)

# visual test of trends
# data <- data %>% filter(month(data$date)==4 | month(data$date)==3)
par(mfrow=c(3,2), mai = c(0.25, 0.55, 0.25, 0.25))    # (bottom, left, upper, right)
plot(data$date, data$Rg, type="l", col="red")

plot(data$date, data$LE, type="l", col="blue", ylim=c(-100,500))
points(data$date, data$H, type="l", col="darkorange", ylim=c(-100,500))

plot(data$date, data$NEE, type="l", col="brown", ylim=c(-40,60))

plot(data$date, data$rH, type="l", col="blue")

plot(data$date, data$Tair, type="l", col="black", ylim=c(-5,45))
points(data$date, (40/1000)*(data$Rg), type="l", col="red")


# NA -> -9999
data <- data %>% replace(is.na(.), -9999)

# check first and last lines
data[1:3,]
data[(nrow(data)-2):nrow(data),]
data[(nrow(data)-2):nrow(data),c("date", "Hour", "DoY")]

# Preparing the 1st and 2nd rows for the tibble --- complete sequence
first_line <- c("Year", "DoY", "Hour", "NEE", "LE", "H", "Rg", "Tair", "rH", "VPD", "Ustar", "date")
second_line <- c("-", "-", "-", "umolm-2s-1", "Wm-2", "Wm-2", "Wm-2", "degC", "%", "hPa", "ms-1", "-")

# convert column into character and bind
cols.num <- names(data)
data[cols.num] <- sapply(data[cols.num], as.character) 
data <- as_tibble(rbind(first_line, second_line, data))


data_to_partition_NEE <- data[,c("Year", "DoY", "Hour", "NEE", "LE", "H",
                                 "Rg", "Tair", "rH", "VPD", "Ustar")]

# export
setwd(clean_data_dir)
write.table(data_to_partition_NEE, file = "inputFile_Plt2023.txt", sep = "\t",
            quote=F, row.names=F, col.names=F)
write.xlsx(data_to_partition_NEE, "inputFile_Plt2023.xlsx", colNames=T, sheetName="input", showNA=T)

# information for the REddyProcWeb
# reference for input format: https://bgc.iwww.mpg.de/5624918/Input-Format
# Packages:
# install.packages("date")

# to upload: https://bgc.iwww.mpg.de/5622399/REddyProc
# Location: Kaltern
# SiteID: Plantaditsch
# latitude: ~ +46.2
# longitude: ~ +11.2
# timezone: (GMT+2) -> 2.0
# temperature (Tair or Tsoil) are available: Y for Tair

# ok.



# 5.3) read output from REddyProcWeb  -------------------------

# library("data.table")
# get_file <- fread('https://www.bgc-jena.mpg.de/REddyProc/work/444829477/output.txt',
#                   header=T, sep="\t", dec=".")

setwd(organized_data_dir)
data <- read.table('REddyProc_Plt_2023_September.txt',
                   sep='\t', dec=".", header=T, skip=0)

# keep headers for units
data <- as_tibble(data[1:nrow(data),])
headers <- data[1,]

# data
data <- as_tibble(data[2:nrow(data),])

# get timestamps
data$date <- as.POSIXct(data$Date.Time, tz= "GMT") 

# to avoid double columns
list_names= names(data)[!(names(data) %in% 
                            c("Year", "DoY", "Hour", "Tair", "rH", "Ustar", "Date.Time"))]

data <- data[,list_names]

# convert columns into numeric
# get a list of column names of which values should not (naturally) be == 0
cols.num= names(data)[!(names(data) %in% c("date"))]
data[cols.num] <- sapply(data[cols.num], as.numeric)

# load
setwd(clean_data_dir)
data_Plt <- readRDS("data_EBC_1m_Plt2023.rds")

# confirm if we need this also in 2023:
# to avoid double columns in the final datafile
list_names= names(data_Plt)[!(names(data_Plt) %in% c("NEE", "LE", "H"))]
data_Plt <- data_Plt[,list_names]

# merge
data <- data_Plt %>% left_join(data, join_by(date))   # set data into the complete timestamp

# I want to differentiate values of LE before and after April 2023 
# (when alternative pump started working)
data <- data %>% mutate(
  LE_f_0104= ifelse(date<=as.POSIXct("2023-04-26 00:00:00", tz="GMT"), 
                    LE_f,
                    NA))

# save
setwd(clean_data_dir)
saveRDS(headers, file= "Plt_NEEpart_headers.rds")
saveRDS(data, file= "Plt_EC_meteo_EBC_Fpart_2023.rds")

plot(data$date, data$Rn_Wm2, type="l", col="darkorange")
points(data$date, data$LE_f, type="l", col="blue")
points(data$date, data$H_f, type="l", col="darkgreen")
points(data$date, data$G_gf, type="l", col="red")


plot(data$date, data$LE_f, type="l", col="darkorange")      # from ReddyProc
points(data$date, data$LE_raw, type="l", col="blue")        # from Eddyflux computations
points(data$date, data$LE_orig, type="l", col="magenta")    # = original values, with -9999 
points(data$date, data$LE, type="l", col="green")           # =LE_orig
points(data$date, data$LE_f_0104, type="l", col="yellow")   # =LE_f but until April 26th



# 6) computations ----------------------------

# 6.1) initial computations ----------------------------

# load
setwd(clean_data_dir)
data <- readRDS("Plt_EC_meteo_EBC_Fpart_2023.rds")

altitude <- 325

# to later match 2021 and 2022 data
data$G_Wm2 <- data$G_gf

# compute ws
data <- data %>% mutate(ws= (avg.w^2 + avg.u^2 + avg.v^2)^0.5)        # m/s

data <- data %>% mutate(
  NEE_f_gChh= (MM_C*1800*(10^-6))*NEE_f,         # gC m-2 hh-1
  
  absH= ea(rH_f, Tair_f)*MM_H2O/(R_gases*(Tair_f+273.15)),     # g/m³
  P_KPa= Press(240),            # altitude 240 m for the vineyard
  P_hPa= 10*P_KPa,              # 1.0*P_mbar
  
  Rn_MJ= Rn_Wm2*(1.8*10^-3),    # MJ m-2 hh-1          # transformations confirmed
  G_MJ= G_Wm2*(1.8*10^-3),      # MJ m-2 hh-1           # G_gf here
  LE_f_MJ= LE_f*(1.8*10^-3),    # MJ m-2 hh-1
  H_f_MJ= H_f*(1.8*10^-3),# MJ m-2 hh-1
  
  #  R_pot= fCalcPotRadiation(DoY= DoY, Hour= Hour, LatDeg= 46.5869, LongDeg= 11.4337, 
  #                           TimeZone= +1, useSolartime= T), # potential radiation in W m-2
  
  GPP_nt= GPP_f,      # GPP_f is umolC m-2 s-1
  GPP_dt= GPP_DT,     # umolC m-2 s-1
  Reco_nt= Reco,      # Reco is umolC m-2 s-1
  Reco_dt= Reco_DT,   # Reco is umolC m-2 s-1
  
  GPP_nt_gChh= (MM_C*1800*(10^-6))*GPP_nt,    # gC m-2 hh-1
  GPP_dt_gChh= (MM_C*1800*(10^-6))*GPP_dt,    # gC m-2 hh-1
  Reco_nt_gChh= (MM_C*1800*(10^-6))*Reco_nt,    # gC m-2 hh-1
  Reco_dt_gChh= (MM_C*1800*(10^-6))*Reco_dt,    # gC m-2 hh-1
  
  VPD_KPa= VPD(rH_f, Tair_f),       # calculated VPD(KPa)
  VPD_hPa= 0.01*VPD_KPa,        # to transform calculated VPD(KPa) into hPa (EdyyProc format)
  delta= delta(Tair_f),
  rho= rho(P_hPa, Tair_f),        # Kg m-3
  
  lambda= lambda(Tair_f),         # MJ Kg-1
  MM_lambda= MM_H2O*lambda,     # molar mass * lambda
  
  ET_EC_mmol_f= round( ((LE_f)/(MM_lambda)),4 ), # enter LE_Wm2 for ET in mmol m-2 s-1
  x_conv= 18*1.8*0.001/rho,     # calculation step
  ET_EC_mmhh_f= round(x_conv*ET_EC_mmol_f, 4),  # ET_EC in mm hh-1, NOT CORRECTED by EBR yet
  
  ET_EC_mmol_corr= round( ((corr_LE)/(MM_lambda)),4 ), # enter LE_Wm2 for ET in mmol m-2 s-1
  x_conv= 18*1.8*0.001/rho,     # calculation step
  ET_EC_mmhh_corr= round(x_conv*ET_EC_mmol_corr, 4),  # ET_EC in mm hh-1, NOT CORRECTED by EBR yet
  
  # the LE_fqc or LE_qc is the quality flag. See: f_: Original values and gaps filled with
  # mean of selected datapoints (condition depending on gap filling method) in ""EddyProcWeb.
  ET_PM_mmhh= ET(Rn_MJ, G_MJ, Tair, P_hPa, rH, ws))   # ET_PM in mm hh-1


data <- data[, c("date", "DoY", "Tair",
                 "Rg", "G_Wm2", "H", "LE", "Rg_f", "H_f", "LE_f",
                 "NEE", "NEE_f", "NEE_f_gChh",
                 "NEE_fqc", "LE_fqc",
                 "SWin", "SWout", "LWin", "LWout", "Rn_Wm2", "T_CNR1", "rh_air",
                 "Rn_MJ", "G_MJ", "LE_f_MJ", "H_f_MJ", "PotRad", 
                 "GPP_nt", "GPP_dt", "Reco_nt", "Reco_dt",
                 "GPP_nt_gChh", "GPP_dt_gChh", "Reco_nt_gChh", "Reco_dt_gChh",
                 "ws", "avg.w", "avg.u", "avg.v", "Ustar",
                 "rH", "absH", "P_KPa", "P_hPa","VPD_KPa", "VPD_hPa",
                 "Precip", "usoil_10cm", "usoil_50cm",
                 "delta", "rho", "lambda",
                 "ET_EC_mmol_f", "ET_EC_mmhh_f",
                 "ET_EC_mmol_corr", "ET_EC_mmhh_corr", "ET_PM_mmhh")]

names(data)[names(data)=="NEE_fqc"] <- "NEE_QC"     # previously was NEE_fqc.
names(data)[names(data)=="LE_fqc"] <- "LE_QC"
names(data)[names(data)=="T_CNR1"] <- "Tair_CNR1"
names(data)[names(data)=="rh_air"] <- "rH_CNR1"
# NEE_uStar_fqc means the estimate on the original unbootstrapped data,
# it corresponds either to not performing uStar filtering or to uStar threshold of the non-bootstrapped data.

# computations
data <- data %>% mutate(A_Wm2= Rn_Wm2 - G_Wm2,
                        E_Wm2= LE_f + H_f,   # correct definition: + sign
                        error= A_Wm2 - E_Wm2)    # correct definition: - sign

data <- data %>% mutate(
  SWin_MJ= SWin*(1.8*10^-3),                                 # from Wm-2 to MJm-2hh-1
  E_MJ= E_Wm2*(1.8*10^-3),
  A_MJ= A_Wm2*(1.8*10^-3),
  albedo= SWout/SWin,
  
  LE_MJm2s= LE_f_MJ/1800,       # FIXED! # convert from MJm-2hh-1 into MJm-2s-1
  Rn_MJm2s= Rn_MJ/1800,
  G_MJm2s= G_MJ/1800,
  
  Ram= ws/(Ustar^2),
  Rb= 6.2/(Ustar^0.667),
  ga= 1/(Ram + Rb),           # Thom (1975); Verma (1987); Knauer et al. (2018)
  g_aM= (Ustar^2)/ws,         # Van Der Tol et al. (2003), given by Thom (1975)
  Gs= (LE_MJm2s*ga*psych) / ( (delta*(Rn_MJm2s - G_MJm2s)) + 
                                (rho*cp*ga*VPD_KPa - LE_MJm2s*(delta+psych))),
  GPP= GPP_dt_gChh,
  GPP.VPD= (VPD_KPa*GPP_dt_gChh),
  GPP.VPD.sq= ((VPD_KPa^0.5)*GPP_dt_gChh),    
  
  WUE= GPP/ET_EC_mmhh_f,                # Beer et al. (2009), Nelson et al. (2018)
  IWUE= GPP.VPD/ET_EC_mmhh_f,           # Beer et al. (2009)
  uWUE= GPP.VPD.sq/ET_EC_mmhh_f)        # Zhou et al. (2016)


# compute Number of hh-Periods since last Precipitation Event: NP
tib <- tibble(Precip= data$Precip)
tib %>% summarise(NA_number= sum(is.na(Precip)))  # Count missing values
tib <- tib %>% replace(is.na(.), 0.0)             # substitute NA by 0
tib %>% summarise(NA_number= sum(is.na(Precip)))  # check for NAs
tib <- as_tibble(tib)

np <- 0
NP <- vector()
for (i in 1:nrow(tib)) {
  ifelse(tib$Precip[i]==0, np <- np+1, np <- 0)
  NP[i] <- np }

data <- data %>% mutate(NP= NP)

# substitute -Inf/Inf values by NA
is.na(data) <- sapply(data, is.infinite)         # remove Inf/-Inf

# switch -9999 flags for NA: General filter
nrow_ini <- nrow(data)
data <- data %>% replace(.==-9999, NA)   # data[data <= -2000] <- NA
nrow_end <- nrow(data)
nrow_end/nrow_ini

# subset the dataset
data <- data[, c("date", "DoY", "Tair",
                 "Rg", "G_Wm2", "H", "LE", "Rg_f", "H_f", "LE_f",
                 "NEE", "NEE_f", "NEE_f_gChh",
                 "NEE_QC", "LE_QC",
                 "SWin", "SWout", "LWin", "LWout", "Rn_Wm2", "Tair_CNR1", "rH_CNR1",
                 "Rn_MJ", "G_MJ", "LE_f_MJ", "H_f_MJ", "PotRad", 
                 "GPP_nt", "GPP_dt", "Reco_nt", "Reco_dt",
                 "GPP_nt_gChh", "GPP_dt_gChh", "Reco_nt_gChh", "Reco_dt_gChh",
                 "ws", "avg.w", "avg.u", "avg.v", "Ustar",
                 "rH", "absH", "P_KPa", "P_hPa","VPD_KPa", "VPD_hPa",
                 "Precip", "usoil_10cm", "usoil_50cm",
                 "delta", "rho", "lambda",
                 "ET_EC_mmol_f", "ET_EC_mmhh_f",
                 "ET_EC_mmol_corr", "ET_EC_mmhh_corr", "ET_PM_mmhh",
                 "A_Wm2", "E_Wm2", "error", "SWin_MJ", "E_MJ", "A_MJ", 
                 "albedo", "LE_MJm2s", "Rn_MJm2s", "G_MJm2s",
                 "Ram", "Rb", "ga", "g_aM", "Gs", 
                 "GPP.VPD", "GPP.VPD.sq", "WUE", "IWUE", "uWUE", "NP")]

# For .csv version (tag with -9999)
data_9999 <- data %>% replace(is.na(.), -9999)        # NA -> -9999

# get date values
data <- data %>% mutate(Year= year(date),
                        month= month(date),
                        day= day(date))

# Export
setwd(clean_data_dir)
saveRDS(data, file= "Plt_EC_meteo_gf_EBC_Fpart_comp_2023.rds")



# 7) h values -------------------------------

setwd(clean_data_dir)
data_EBC_1m <- readRDS("Plt_EC_meteo_gf_EBC_Fpart_comp_2023.rds")
data_EBC_1m <- data_EBC_1m %>% mutate(hou= hour(date))


# 8.1) * EBC - data_EBC_1m
# get hourly values in data_EBC_1m
data_EBC_h <- data_EBC_1m %>% group_by(Year, month, day, hou) %>% summarise(
  GPP_nt_gCh= sum(GPP_nt_gChh, na.rm=T),
  GPP_dt_gCh= sum(GPP_dt_gChh, na.rm=T),
  NEE_f_gCh= sum(NEE_f_gChh, na.rm=T),         # from NEE_gChh= (MM_C*1800*(10^-6))*NEE_uStar_f_Wm2
  Reco_nt_gCh= sum(Reco_nt_gChh, na.rm=T),
  Reco_dt_gCh= sum(Reco_dt_gChh, na.rm=T),
  ET_EC_mmh_f= sum(ET_EC_mmhh_f, na.rm=T),
  ET_EC_mmh_corr= sum(ET_EC_mmhh_corr, na.rm=T),
  ET_PM_mmh= sum(ET_PM_mmhh, na.rm=T),
  
  Rn_Wm2= mean(Rn_Wm2, na.rm=T),               # value in W/m² averaged over an h
  Rg_f= mean(Rg_f, na.rm=T),
  LE_f_corr= mean(LE_f, na.rm=T),         # the gap-filled fluxes 
  H_f_corr= mean(H_f, na.rm=T),
  G_Wm2= mean(G_Wm2, na.rm=T),
  SWin= mean(SWin, na.rm=T),
  SWout= mean(SWout, na.rm=T),
  
  Rn_MJ= sum(Rn_MJ, na.rm=T),                  # value in MJ/m²h integrated over an h
  LE_MJ= sum(LE_f_MJ, na.rm=T),
  H_MJ= sum(H_f_MJ, na.rm=T),
  G_MJ= sum(G_MJ, na.rm=T),
  SWin_MJ= sum(SWin_MJ, na.rm=T),
  albedo= 1-(Rn_MJ/SWin_MJ),
  
  VPD_KPa= mean(VPD_KPa, na.rm=T),
  Precip= sum(Precip, na.rm=T),
  Tair= mean(Tair, na.rm=T),
  rH= mean(rH, na.rm=T),
  absH= mean(absH, na.rm=T),
  ws= mean(ws, na.rm=T),
  Ustar= mean(Ustar, na.rm=T),
  ga= mean(ga, na.rm=T),                      # value in W/m² averaged over an h
  g_aM= mean(g_aM, na.rm=T),
  Gs= mean(Gs, na.rm=T),
  
  usoil_10cm= mean(usoil_10cm, na.rm=T),
  usoil_50cm= mean(usoil_50cm, na.rm=T))

# re-build the timestamp
data_EBC_h <- as_tibble(data_EBC_h) %>% mutate(index_row= c(1:nrow(data_EBC_h)))

data_EBC_h <- as_tibble(data_EBC_h) %>% 
  mutate(date_char= paste((paste(Year, month, day, sep="-")), (paste(hou, "00", "00", sep=":")), sep=" "))

date_vec <- as.POSIXct(data_EBC_h$date_char, format="%Y-%m-%d %H:%M:%S", tz="GMT")
data_EBC_h <- as_tibble(data_EBC_h) %>% mutate(date= date_vec)


# QF are missing in h and d datasets


# when values==0 receive NA
# get a list of column names of which values should not (naturally) be == 0
list_names= names(data_EBC_h)[!(names(data_EBC_h) %in% 
                                  c("hou", "Precip", "date"))]     # and maybe the QF

# values==0 receive NA for all columns of list_names
data_EBC_h <- mutate_at(data_EBC_h, list_names, funs(ifelse(.==0, NA, .)))

# remove columns
data_EBC_h <- data_EBC_h %>% select(-date_char, -index_row)

# Export
setwd(clean_data_dir)
saveRDS(data_EBC_h, file = "data_EBC_h_Plt2023.rds")



# 6) d values -------------------------------

setwd(clean_data_dir)
data_EBC_1m <- readRDS("Plt_EC_meteo_gf_EBC_Fpart_comp_2023.rds")

# 6.1) * EBC - data_EBC_1m
# get daily values from data_EBC_1m
data_EBC_d <- data_EBC_1m %>% group_by(Year, month, day) %>% summarise(
  NEE_f_gCd= sum(NEE_f_gChh, na.rm=T),
  GPP_nt_gCd= sum(GPP_nt_gChh, na.rm=T),
  GPP_dt_gCd= sum(GPP_dt_gChh, na.rm=T),
  Reco_nt_gCd= sum(Reco_nt_gChh, na.rm=T),
  Reco_dt_gCd= sum(Reco_dt_gChh, na.rm=T),
  
  ET_EC_mmd= sum(ET_EC_mmhh_f, na.rm=T),
  # ET_PM_mmd= sum(ET_PM_mmhh, na.rm=T),
  
  LE_f= mean(LE_f, na.rm=T),        # all W/m²
  H_f= mean(H_f, na.rm=T),
  Rn_Wm2= mean(Rn_Wm2, na.rm=T),    # is this automatically gap-filled
  G_Wm2= mean(G_Wm2, na.rm=T),      # ?????
  
  Rg_f= mean(Rg_f, na.rm=T),        # in W/m²
  PotRad= mean(PotRad, na.rm=T),    # in W/m²
  Rn_MJ= sum(Rn_MJ, na.rm=T),       # value in MJ/m²d (integrated over a d)
  SWin_MJ= sum(SWin_MJ, na.rm=T),
  albedo= 1-(Rn_MJ/SWin_MJ),
  
  Precip= sum(Precip, na.rm=T),
  Tair= mean(Tair, na.rm=T),
  rH= mean(rH, na.rm=T),
  absH= mean(absH, na.rm=T),
  VPD_KPa= mean(VPD_KPa, na.rm=T),
  
  ws= mean(ws, na.rm=T),
  Ustar= mean(Ustar, na.rm=T),
  ga= mean(ga, na.rm=T),             # value in W/m² (averaged over a d)
  g_aM= mean(g_aM, na.rm=T),
  Gs= mean(Gs, na.rm=T),
  
  usoil_10cm= mean(usoil_10cm, na.rm=T),
  usoil_50cm= mean(usoil_50cm, na.rm=T))

# re-build the timestamp
data_EBC_d <- as_tibble(data_EBC_d) %>% mutate(index_row= c(1:nrow(data_EBC_d)))

data_EBC_d <- as_tibble(data_EBC_d) %>% 
  mutate(date_char= paste((paste(Year, month, day, sep="-")), (paste("00", "00", "00", sep=":")), sep=" "))

date_vec <- as.POSIXct(data_EBC_d$date_char, format="%Y-%m-%d %H:%M:%S", tz="GMT")
data_EBC_d <- as_tibble(data_EBC_d) %>% mutate(date= date_vec,
                                               DoY= yday(date))


# QF are missing in h and d datasets


# when values==0 receive NA
# get a list of column names of which values should not (naturally) be == 0
list_names= names(data_EBC_d)[!(names(data_EBC_d) %in% 
                                  c("hou", "Precip", "date", "DoY"))]     # and maybe the QF

# values==0 receive NA for all columns of list_names
data_EBC_d <- mutate_at(data_EBC_d, list_names, funs(ifelse(.==0, NA, .)))

# remove columns
data_EBC_d <- data_EBC_d %>% select(-date_char, -index_row)

# Export
setwd(clean_data_dir)
saveRDS(data_EBC_d, file = "data_EBC_d_Plt2023.rds")







