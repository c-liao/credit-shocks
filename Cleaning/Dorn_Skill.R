#import libraries
library(tidyverse)
library(haven)
library(data.table)
library(readr)
library(dplyr)
library(future)
options(scipen=999)

#Set directory
setwd("~/Google Drive/Non-Academic Work/Research/Traina/Code/")

#read in data
class <- read_dta(file = "Datasets/Imported/Dorn/occ1990dd_task_alm.dta")

#convert to csv
system.time(fwrite(class, "Datasets/Cleaned/Dorn_Skill.csv"))