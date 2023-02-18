
# Notes

## Tools

csvkit (https://csvkit.readthedocs.io)

## Data Processing

1. unzip
2. check for csv line errors
	`csvclean -n mc_listings.csv > error_check.txt`
3. pull out large columns into separate file...


X. add line_number to csv
	`csvcut -l mc_listings.csv > mc_listings2.csv`