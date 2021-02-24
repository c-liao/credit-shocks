#read libraries
library(data.table)
library(readr)
library(dplyr)
library(future)
plan(multiprocess)
options(scipen=999)
        
#Set to my personal directory - adjust accordingly
setwd("~/Google Drive/Non-Academic Work/Research/Traina/Code/")

years <- 2:19
df <- data.frame()

#read in first file to get column names
filename <- readxl::read_xls("Datasets/Imported/Disaster/SBA_Disaster_Loan_Data_FY01.xls", sheet = 5, skip = 4)
cols <- colnames(filename)
cols <- cols[-5]

#Importing data
#no url downloading because the excel files are small in size so they can be uploaded to git
for (i in years) {
  print(i)
  if (i<=3) {
    i = paste("0", i, sep = "")
    filename <- paste("Datasets/Imported/Disaster/SBA_Disaster_Loan_Data_FY",i,".xls",sep = "")
    temp <- readxl::read_xls(filename, sheet = 5, skip = 4)
    temp <- temp %>% subset(select = -5)
    colnames(temp) <- cols
  }
  else {
    if (i < 10) {
      i = paste("0", i, sep = "")
    }
    filename <- paste("Datasets/Imported/Disaster/SBA_Disaster_Loan_Data_FY",i,".xlsx",sep = "")
    temp <- readxl::read_xlsx(filename, sheet = 5, skip = 4)
    colnames(temp) <- cols
  }
  temp$Year <- paste("20", i, sep = "")
  df <- rbind(df, temp)
}

#make data numeric
df <- data.frame(lapply(df, function(x) if(is.integer(x)) as.numeric(x) else x))

#export data
system.time(fwrite(df, "Datasets/Cleaned/disaster_loans.csv"))