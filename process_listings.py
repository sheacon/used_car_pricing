
# script initial process file
# test result: reduced file size by 41%

import sys
import time
import csv
import json
from itertools import islice

# settings
#script_name = sys.argv[0]
num_lines = int(sys.argv[1]) # number of lines to process
num_jobs = int(sys.argv[2]) # how many jobs it's split into
this_job = int(sys.argv[3]) # which job number this run is

file_dir = '/data/p_dsi/capstone_projects/shea/'
file_input = 'mc_listings.csv'
file_output = 'processed/mc_listings_'+ str(this_job) +'.csv'

# lines to process calculation
def split_file_lines(num_lines, num_pieces):
    indexes_per_piece = num_lines // num_pieces
    remaining_indexes = num_lines % num_pieces
    ranges = []
    current_index = 0
    for i in range(num_pieces):
        if i < remaining_indexes:
            range_end = current_index + indexes_per_piece + 1
        else:
            range_end = current_index + indexes_per_piece
        ranges.append((current_index, range_end))
        current_index = range_end
    return ranges

line_ranges = split_file_lines(num_lines, num_jobs)

start_line,end_line = line_ranges[this_job]


start = time.time()

with open(file_dir+file_input, 'r') as f_in, open(file_dir+file_output, 'w') as f_out:

    csv_reader = csv.reader(f_in)
    csv_writer = csv.writer(f_out)

    i = 0

    csv_reader = islice(csv_reader, start_line, end_line) # skip x number, stop at y number

    if this_job == 0:

        i += 1

        # header
        header = next(csv_reader)
        
        del header[85] # in_transit_days
        del header[84] # in_transit_at
        del header[83] # in_transit
        del header[67] # photo url
        del header[65] # features
        del header[57] # car_street
        del header[56] # car_address
        del header[53] # inventory_type (all used)
        del header[52] # listing_type (all dealer)
        del header[51] # seller_type (all dealer)
        del header[50] # seller_email
        del header[49] # seller_phone
        del header[48] # country (all US)
        del header[42] # street
        del header[39] # dealer_id
        del header[35] # model_code
        del header[3] # more_info

        csv_writer.writerow(header)

    for line in csv_reader:

        i += 1

        # increment timer
        if i % 1_000 == 0:
            print(f'{i:,}')
            print(round((time.time() - start)/60,2))

        # replace photo links with count
        line[66] = line[66].count('|') + 1

        # replace options with unique options/features combined
        combined = '|'.join(list(set(str(line[64]).split('|') + str(line[65]).split('|'))))
        line[64] = combined


        # translate nest json to compact dict
        if line[86]:
            hvf_items = json.loads(line[86])
            options = {'Standard': {}, 'Optional': {}}
            for item in hvf_items:
                if item['category'] not in options[item['type']]:
                    options[item['type']][item['category']] = []
                options[item['type']][item['category']].append(item['description'])
            line[86] = str(options)

        # remove photo_url, more_info, and features
        del line[85] # in_transit_days
        del line[84] # in_transit_at
        del line[83] # in_transit
        del line[67] # photo url
        del line[65] # features
        del line[57] # car_street
        del line[56] # car_address
        del line[53] # inventory_type (all used)
        del line[52] # listing_type (all dealer)
        del line[51] # seller_type (all dealer)
        del line[50] # seller_email
        del line[49] # seller_phone
        del line[48] # country (all US)
        del line[42] # street
        del line[39] # dealer_id
        del line[35] # model_code
        del line[3] # more_info
        
        # write line
        csv_writer.writerow(line)

        #if i == 245_000: break

# print final line count and processing time
    print(f'{i:,}')
    print(round((time.time() - start),5))
