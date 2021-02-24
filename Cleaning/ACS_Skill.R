#Import libraries
library(data.table)
library(readr)
library(dplyr)
library(haven)
library("doMC")
library(future)
library(vroom)
library(RCurl)
library(googledrive)
plan(multiprocess)
options(scipen=999)
registerDoMC(cores = 4)

#Set to my personal directory - adjust accordingly
setwd("~/Google Drive/Non-Academic Work/Research/Traina/Code/")

ACS_url <- "https://drive.google.com/file/d/1BYhbTeiZRijQgWEsY30wGVstpwrxrt81/view?usp=sharing"
fname <- "Datasets/Imported/ACS/usa_00007.dat"
ret = drive_download(ACS_url, fname, overwrite = TRUE)

#read in ACS data
ddi <- read_ipums_ddi("Datasets/Imported/ACS/usa_00007.xml")
data <- data.frame(read_ipums_micro(ddi))
#disable scientific notation
options(scipen = 999)
#remove unneeded columns
data$EDUC <- NULL
data$EMPSTATD <- NULL

#Set workers as either skilled (1) or unskilled (0) based off whether they have a bachelor's degree or more
data$SKILLED <- ifelse(data$EDUCD %in% c(101, 114, 115, 116), 1, 0)

#create puma (public use micro area) columns for years
data$puma <- ifelse(data$YEAR < 2012, 1e4*data$STATEFIP + data$PUMA, 1e5*data$STATEFIP + data$PUMA)
#filter data for unneeded years
data <- data %>% filter(YEAR > 2000) 

#reformatting FIPS codes for state and county
data$STATEFIP <- ifelse(data$STATEFIP < 10, paste("0", data$STATEFIP, sep = ""), data$STATEFIP)
data$COUNTYFIP <- ifelse(data$COUNTYFIP < 10, paste("00", data$COUNTYFIP, sep = ""), 
                      ifelse(data$COUNTYFIP < 100, paste("0", data$COUNTYFIP, sep = ""), data$COUNTYFIP))
data$FIP <- paste(data$STATEFIP,data$COUNTYFIP, sep = "")

#read in data to find commuting zones based off county
cwcty <- read_dta(file = "Datasets/Imported/Dorn/cw_cty_czone.dta")
#reformat codes with leading zeroes
cwcty$cty_fips <- ifelse(cwcty$cty_fips < 10000, paste("0", cwcty$cty_fips, sep = ""), cwcty$cty_fips)
cwcty$czone <- ifelse(cwcty$czone < 10000, paste("0", cwcty$czone, sep = ""), cwcty$czone)
#add commuting zone codes to dataset
data <- left_join(data, cwcty, by = c("FIP" = "cty_fips"))
data <- data %>% 
  rename(
    czonecounty = czone
    )

#read in data to find commuting zones based off post 2010 PUMA data
cw2010 <- read_dta(file = "Datasets/Imported/Dorn/cw_puma2010_czone.dta")
#leading zeroes
cw2010$czone <- ifelse(cw2010$czone < 10000, paste("0", cw2010$czone, sep = ""), cw2010$czone)
#add commuting zone codes
data <- left_join(data, cw2010, by = c("puma" = "puma2010"))
#rename columns to distinguish between 2010 and 2000 results
data <- data %>% 
  rename(
    czone2010 = czone,
    afactor2010 = afactor,
  )
#read in data to find commuting zones based off post 2010 PUMA data
cw2000 <- read_dta(file = "Datasets/Imported/Dorn/cw_puma2000_czone.dta")
#leading zeroes
cw2000$czone <- ifelse(cw2000$czone < 10000, paste("0", cw2000$czone, sep = ""), cw2000$czone)
#add commuting zone codes
data <- left_join(data, cw2000, by = c("puma" = "puma2000"))
#rename columns to distinguish between 2010 and 2000 results
data <- data %>% 
  rename(
    czone2000 = czone,
    afactor2000 = afactor,
  )

#merge all the afactor data into one
data <- data %>% mutate(afactor = coalesce(afactor2010,afactor2000))
#merge commuting zone data into one
data <- data %>% mutate(czone = coalesce(czone2000,czone2010))
data <- data %>% mutate(czone = coalesce(czone,czonecounty))
#remove irrelevant data
data$czonecounty = NULL
data$czone2000 = NULL
data$czone2010 = NULL
data$afactor2010 = NULL
data$afactor2000 = NULL

#Read in Dorn Skill data
dorn <- read.csv("Datasets/Cleaned/Dorn_Skill.csv")
#join Dorn skill data with ACS data based on occupation
merged <- inner_join(data, dorn, by = c("OCC1990" = "occ1990dd"))
#Reweight data based off commuting zone aggregation
merged$PERWT <- ifelse(is.na(merged$afactor), merged$PERWT, merged$PERWT*merged$afactor)

#Write dataset that now has data for commuting zones and skill level for occupations in ACS survey
system.time(fwrite(merged, "Datasets/Cleaned/ACS_SKILL.csv"))