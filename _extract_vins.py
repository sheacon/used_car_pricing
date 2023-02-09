import gzip
import time

file_input = '/data/p_dsi/capstone_projects/shea/mc_us_used.csv.gz'
file_output = '/scratch/conawws1/vins.txt'

start = time.time()

with gzip.open(file_input, 'r') as f_in, open(file_output, 'w') as f_out:

    i = 0

    while i < 250_000_000: # supposed to be 224m records

        i += 1

        # increment timer
        if i % 100_000 == 0:
            print(f'{i:,}')
            print(round((time.time() - start)/60,2))

        line = f_in.readline()
        if not line: break

        line = str(line).split(',')
        vin = line[1] + '\n'

        f_out.writelines(vin)



    print(f'{i:,}')
    print(round((time.time() - start)/60,2))
