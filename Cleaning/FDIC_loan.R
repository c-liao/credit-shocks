#read libraries
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
options(scipen=999)
registerDoMC(cores = 4)

#Set to my personal directory - adjust accordingly
setwd("~/Google Drive/Non-Academic Work/Research/Traina/Code/")

#url of fdic academic challenge data
url <- "https://www.fdic.gov/analysis/academic-challenge/files/academic-challenge-data-csv.zip"
#place to download data to
fname <- "Datasets/Imported/FDIC/ACdata.csv.zip"
ret = download.file(url, fname)

#import data
df <- vroom(fname, delim = ",")
#filter for data after 2004
df <- df %>% filter(year > 2004)

#removed duplicate banks so that loans aren't repeated in aggregation
grouped <- df %>% group_by(year) %>% distinct(cert, .keep_all = TRUE)
#create 1990 cpi conversion column
grouped$cpi_1990 = grouped$cpi_2018 * 1.9222

#group by state and year, aggregate all loan amounts
state_loans <- grouped %>% 
  group_by(year, FIPS_state, cpi_1990) %>% 
  summarise(all_loans_cpiadj = sum(all_loans, na.rm = TRUE),
            ag_loans_cpiadj = sum(ag_loans, na.rm = TRUE),
            real_estate_loans_cpiadj = sum(real_estate_loans, na.rm = TRUE),
            ci_loans_cpiadj = sum(ci_loans, na.rm = TRUE),
            consumer_loans_cpiadj = sum(consumer_loans, na.rm = TRUE),
            credit_card_loans_cpiadj = sum(credit_card_loans, na.rm = TRUE),
            dollarloans_ci_LT1M_cpiadj = sum(dollarloans_ci_LT1M, na.rm = TRUE),
            dollarloans_ag_bus_LT500K_cpiadj = sum(dollarloans_ag_bus_LT500K, na.rm = TRUE))
columns <- c("all_loans", "ag_loans", "real_estate_loans", "ci_loans", "consumer_loans", 
             "credit_card_loans", "dollarloans_ci_LT1M", "dollarloans_ag_bus_LT500K")
#create new column names for cpi adjusted values
columns <- paste(columns, "_cpiadj", sep = "")
#rename columns
state_loans[,columns] <- ((state_loans[,columns]/state_loans$cpi_1990) * 100)
#convert fips state codes back to original format
state_loans$FIPS_state <- ifelse(state_loans$FIPS_state < 10, 
                                  paste("0", state_loans$FIPS_state, sep = ""), 
                                 state_loans$FIPS_state)

#write dataset
fwrite(state_loans, "Datasets/Cleaned/FDIC_state.csv")