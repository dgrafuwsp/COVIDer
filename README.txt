The purpose of these Python scripts are to update and parse the copious COVID data provided by the NY Times and The Atlantic into state- and county- specific datasets that can be analyzed by students to compare doubling-rates, r, cases per 100 000, death rates, etc. and generally learn to analyze larger datasets without having to parse the larger original data files.

The files that end in .py are the scripts to update and parse the data. Two directories need to exist in the same directory as the Python scripts: /csv_data and /covid_data.

The file that ends in .csv is the census data used to get population for the counties. It can be manually placed in the /csv_data directory.

The /samples directory has the sample output files. The parsing script can be run for any set of counties or states (or US territories).
