# credit-shocks

For a research project with James Traina on the impact of credit shocks on the college skill premium and the skill distribution of jobs. 
To be updated and reformatted in a more aesthetically pleasing format later

Cleaning folder
1. ACS_Skill.R - merges ACS dataset with Dorn Skill Dataset (also performs PUMA->Commuting Zone conversion)
Produces Datasets/Cleaned/ACS_SKILL.csv
2. Dorn_Skill.R - Converts dorn Skill dataset into a csv
Produces Datasets/Cleaned/Dorn_Skill.csv
3. FDIC_FFIEC.R - Merges FDIC and FFIEC datasets so that they are aggregate on a state-year level. Also finds the proportion of FDIC loans that were from FFIEC institutions
Produces Datasets/Cleaned/FFIEC_FDIC.csv
4. Disaster_Loans.R - Merges 2001-2019 disaster loan data into one dataset
Produces Datasets/Cleaned/disaster_loans.csv
5. FFIEC.R - Merges 2000, 2005-2019 FFIEC loan data into one dataset
Produces Datasets/Cleaned/ffiec.csv

Datasets folder (imported only, see Cleaning section for information on cleaned datasets)
1. ACS - place where the data (just the .dat file) for the ACS will be downloaded from ACS_Skill.R. The rest of the data is already in the file
2. Disaster - place where the disaster loan data is located
3. Dorn - place where all of David Dorn's datasets are located
4. FDIC - place where the FDIC data will be downloaded into. Codebook is already located in it. 
5. FFIEC - place where FFIEC data will be downloaded into. 
