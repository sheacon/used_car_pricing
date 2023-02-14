
# script to remove URLs from listings file
# test result: reduced file size by 25%

import gzip
import time
import csv

file_input = 'mc_listings.csv.gz'
file_output = 'mc_listings_reduced.csv'

start = time.time()

with gzip.open(file_input, 'rt') as f_in, open(file_output, 'w') as f_out:

    csv_reader = csv.reader(f_in)
    csv_writer = csv.writer(f_out)


    # header
    header = next(csv_reader)
    del header[67]
    del header[3]
    csv_writer.writerow(header)


    i = 0

    for line in csv_reader:

        i += 1

        # increment timer
        if i % 1_000 == 0:
            print(f'{i:,}')
            print(round((time.time() - start)/60,2))

        # replace photo links with count
        line[66] = line[66].count('|') + 1

        # remove photo_url and more_info
        del line[67]
        del line[3]
        
        # write line
        csv_writer.writerow(line)




    print(f'{i:,}')
    print(round((time.time() - start)/60,2))
