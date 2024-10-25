# Script file:    Libs&functions.R
# Title:          Libraries & functions
# Author:         Flavio Bastos Campos
# Date:           2020 - 2023
# Version:        5.4
# Description:    
# This script calculates ET based on meteorological data, including temperature, humidity, wind speed, and pressure, 
# using the Penman-Monteith approach. It defines key constants, functions to compute temperature-dependent 
# parameters (like latent heat and vapor pressure), and provides utilities to calculate the ET and VPD (vapor pressure deficit).
#
# Goals:
# 1.0) Set up directory settings for consistent character display
# 2.0) Load necessary libraries for data manipulation, analysis, and plotting
# 3.0) Define constants
# 4.0) Define functions
#
# Notes:
# - Ensure that all required libraries are installed.
# - Altitude, canopy height, and other constants should be verified and adjusted according to measurement site conditions.
#
#
#
#
# Setup the locale -------------------------------------------------------
Sys.setlocale("LC_ALL", "en_GB.UTF-8")
# https://stackoverflow.com/questions/15438429/axis-labels-are-not-plotted-in-english


# Loading Packages -------------------------------------------------------

library("openxlsx")
library(tibble) 
library(tidyr)
library(lubridate)
require(skimr)        # fast NA values and descriptive statistics
library(egg)          # to use ggarrange function
library(grid)         # to add static text annotations
library(dplyr)        # for data wrangling
library(ggplot2)      # for awesome plotting


# Constants defining ------------------------------------------------------

psych <- 0.066        # KPa.°C-1, psychrometric constant
R_gas <- 287.058      # J. Kg-1.°K-1, air specific gas constant
R_gases <- 8.314      # J. K-1. mol-1, ideal gases constant
cp <- 1.013*0.001     # MJ. Kg-1.°C-1, specific heat of dry air
rs <- 70.0            # KPa.°C-1, bulk surface resistance (P.-M. eq.)
MM_H2O <- 18.01528    # g mol-1, water molar mass
MM_C <- 12.01070      # g mol-1, carbon molar mass


# Functions defining ------------------------------------------------------

# obtain Hour in decimals for the REddyProcWeb tool
get_Hour_decimal <- function(min) {    
  Hour_dec <- ifelse(min==30, 
                     0.5, 
                     ifelse(min==0, 0, NA))
  return(Hour_dec)
}

lambda <- function(Tair){
  lambda <- 2.501 - (2.361*0.001*Tair) # MJ.kg-1
  lambda <- round(lambda, 4)
  return(lambda)
}

es <- function(Tair){
  es <- 0.6108 * exp((17.27*Tair)/(Tair + 237.3)) # KPa.°C-1
  es <- round(es, 4)
  return(es)
}

delta <- function(Tair){
  delta <- (4098*(es(Tair)))/(Tair + 237.3)^2 # KPa.°C-1
  delta <- round(delta, 4)
  return(delta)
}

rho <- function(P_hPa, Tair){
  rho <- (100*P_hPa)/((Tair + 273.15) * R_gas) # Kg.m-3
  rho <- round(rho, 4)
  return(rho)
}

ea <- function(RH, Tair){
  ea <- RH*es(Tair)/100  # KPa
  ea <- round(ea, 4)
  return(ea)
}

ra <- function(ws){
  zm <- 6.0      # (!)    # m, height of ws measurement
  zh <- 6.0      # (!)    # m, height of RH measurement
  h <- 3.0       # (!)    # m, canopy height - 28m in Renon
  d <- 2*h/3      #        # m, the zero plane displacement height
  zom <- 0.123*h  #        # m, the roughness length of the momentum transfer
  zoh <- 0.1*zom  #        # m, the roughness length of the heat and vapor transfer
  K <- 0.41       #        # m, the von Karman’s constant

  ra <- log((zm-d)/zom) * log((zh-d)/zoh) / (ws * K^2)  # s.m-1
  ea <- round(ra, 4)
  return(ra)
}

VPD <- function(RH, Tair){
  VPD <- es(Tair) - ea(RH, Tair)  # KPa
  VPD <- round(VPD, 4)
  return(VPD)
}

Press <- function(altitude){
  Press <- 101.3*((293 - 0.0065*altitude)/293)^5.26 # altitude in m -> Press in KPa
  Press <- round(Press, 4)
  return(Press)
}


ET <- function(Rn_MJ, G_MJ, Tair, P_hPa, RH, ws){
  ET <- ( delta(Tair)* (Rn_MJ - G_MJ) + (rho(P_hPa,Tair) * cp * (VPD(RH,Tair)/ra(ws))) ) /
    ( lambda(Tair)* ( delta(Tair) + (psych * (1 + (rs/ra(ws))))) )  # mmhh-1
  ET <- round(ET, 4)
  return(ET)
}

# computes the RMSE between two vectors of same length
RMSE_fc <- function(Actual, Predicted) {
  diff_x_y <- (Actual - Predicted)^2
  RMSE <- (sum(diff_x_y)/length(Actual))^0.5
  return(RMSE)
}





