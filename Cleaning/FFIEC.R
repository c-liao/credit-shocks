#Import libraries
library(data.table)
library(readr)
library(dplyr)
library(haven)
library("doMC")
library(future)
library(vroom)
library(RCurl)
plan(multiprocess)
options(scipen=999)
registerDoMC(cores = 4)

#Set to my personal directory - adjust accordingly
setwd("~/Google Drive/Non-Academic Work/Research/Traina/Code/")

#Width of the columns from 2005 onwards
len1 <- c(5, 10, 1, 4, 1, 1, 2, 3, 5, 4, 1, 1, 1, 3, 3, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10)
#Width of the columns for 2000
len2 <- c(5, 10, 1, 4, 1, 1, 2, 3, 4, 4, 1, 1, 1, 3, 3, 6, 8, 6, 8, 6, 8, 6, 8, 6, 8)

#Types for the columns
#col_t is for 2005 onwards, col_t2 is for 2000
col_t <- paste(unlist(c(rep("c", 2), rep("n", 4), rep("c", 7), rep("n", 12))), collapse = '')
col_t2 <- paste(unlist(c(rep("c", 2), rep("n", 4), rep("c", 7), rep("n", 12))), collapse = '')
#column names
cols <- c("Table_ID", "Respondent_ID", "Agency_Code", "Year","Loan_Type","Action_Type",
          "State", "County","MSA/MD",  "Assessment_Area_Number", "Partial_County_Indicator",
          "Split_County", "Pop_Class", "Income_Group", "Report_Level", "NumSBLoans_100000", 
          "TotalAmtSBLoans_100000", "NumSBLoans_250000", "TotalAmtSBLoans_250000", "NumSBLoans_1000000", 
          "TotalAmtSBLoans_1000000","NumLoansRevenue_1000000", "TotalLoansRevenue_1000000", 
          "NumSBAffiliateLoans", "TotalAmtSBAffiliateLoans")

#function to read in files efficiently
bdown=function(url, file){
  f = CFILE(file, mode="wb")
  a = curlPerform(url = url, writedata = f@ref, noprogress=FALSE)
  close(f)
  return(a)
}

#Download files from the FFIEC website and read them in
system.time(df<-foreach(i = c(0, 5:19)) %do% {
  if (i<10) {
    i <- paste("0", i,sep="")
  }
  fname <- paste("Datasets/Imported/FFIEC/",i,"exp_discl.zip", sep = "")
  url <- paste("https://www.ffiec.gov/cra/xls/",i,"exp_discl.zip", sep = "")
  
  ret = bdown(url, fname)
})

#Import small business loan originations from 2016 to 2019
#Merged the datasets into one dataframe - df
system.time(df <- foreach(i = c(0, 5:19), .combine = 'bind_rows') %do% {
  #add leading zero
  if (i<10) {
    i <- paste("0", i,sep="")
  }
  fname <- paste("Datasets/Imported/FFIEC/",i,"exp_discl.zip", sep = "")
  #vroom package is super fast - also dataset is fixed width
  #dataset from 2000 has different fixed width parameters
  if (i==0) {
    temp <- vroom_fwf(fname, fwf_widths(len2, cols), col_types = col_t, progress = TRUE)
  }
  else {
    temp <- vroom_fwf(fname, fwf_widths(len1, cols), col_types = col_t, progress = TRUE)
  }
  #set column names
  colnames(temp) <- cols
  #return imported dataset
  #Since before 2016, the dataset contained all the tables, filter for just small business loan originations
  temp %>% filter(Table_ID == "D1-1")
})

#Remove duplicates
df <- df %>% distinct()

#Export combined ffiec data from 2000 and 2005-2019
system.time(fwrite(df, "Datasets/Cleaned/ffiec.csv", nThread = 10))