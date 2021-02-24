#Import libraries
library('ipumsr')
library('dplyr')
library("ggplot2")
library(data.table)
library(readr)
library(future)
library(haven)
library(vroom)
library(foreach)
library(parallel)
library("doMC")
library(collections)
options(scipen=999)
registerDoMC(cores = 4)

#Read in data
setwd("~/Google Drive/Non-Academic Work/Research/Traina/Code/")

#Read in fdic state level data
fdic_state <- vroom("Datasets/Cleaned/FDIC_state.csv", delim = ",")

#read in ffiec data
ffiec <- vroom("Datasets/Cleaned/ffiec.csv", delim = ",")

#create function that takes in a year and outputs the cpi conversion
#grabbing columns, renaming columns, transposing dataframe, converting to correct data types, 
cpi <- data.frame(cbind(fdic_state$year, fdic_state$cpi_1990))
colnames(cpi) <- c("Year", "cpi_1990")
cpi <- as.data.frame(t(as.matrix(cpi %>% distinct())))
cpi['Year',] <- as.numeric(cpi['Year',])
colnames(cpi) <- as.numeric(cpi['Year',])
cpi <- cpi[2,]

#actual function creation
cpiFun <- function(x) cpi[,as.character(x)]

#Filtering for only state level reports
ffiec_state <- ffiec %>% filter(Report_Level== 10)
#Find aggregate loan amounts for FFIEC banks
ffiec_state$totalLoans <- ffiec_state$TotalAmtSBLoans_100000 + 
  ffiec_state$TotalAmtSBLoans_250000 + ffiec_state$TotalAmtSBLoans_1000000
#group by state and year
ffiec_state_grouped <- ffiec_state %>% 
  group_by(State, Year) %>% 
  summarise(totalSBLoans_1mil = sum(totalLoans))
#filter for the relevant states and remove 2019 because no FDIC data for 2018
ffiec_state_grouped <- ffiec_state_grouped %>% 
  filter(State %in% unique(ffiec_state_grouped$State)[1:56])%>% 
  filter(Year != "2019")
#make cpi column numeric
ffiec_state_grouped$cpi_1990 <- as.numeric(lapply(ffiec_state_grouped$Year, cpiFun))
#convert to 1990 price level
ffiec_state_grouped$totalSBLoans_1mil_cpiadj <- (ffiec_state_grouped$totalSBLoans_1mil/ffiec_state_grouped$cpi_1990 * 100)
#merge ffiec and fdic dataset
ffiec_fdic_state <- ffiec_state_grouped %>% full_join(by = c("State" = "FIPS_state", "Year" = "year", "cpi_1990"), fdic_state)
#calculate proportion of total loans by fdic insured banks in a particular state and year that were from ffiec banks
ffiec_fdic_state$sb1mil_loanRatio <- ffiec_fdic_state$totalSBLoans_1mil_cpiadj/ffiec_fdic_state$all_loans_cpiadj

#write data
system.time(fwrite(ffiec_fdic_state, "Datasets/Cleaned/FFIEC_FDIC.csv", nThread = 10))

#summarise ffiec loan proportions
summary(ffiec_fdic_state$sb1mil_loanRatio)