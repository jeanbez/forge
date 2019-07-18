import os
import os.path
import sys


def parse(line):
    tag, value = line.split(':')

    return value.strip()


# Make sure we are using Python3 in order to dump the log in screen and in file
if sys.version_info[0] < 3:
    print('You must use Python 3')

    exit()


for file in os.listdir('experiments-grid-5000'):
    if file.endswith('.time'):
        experiment = os.path.join('experiments-grid-5000', file)

        setup_forwarders = None
        setup_clients = None
        setup_layout = None
        setup_spatiality = None
        setup_size = None
        setup_total = None

        operation = None

        read_min_time = None
        read_max_time = None
        read_mean_time = None

        write_min_time = None
        write_max_time = None
        write_mean_time = None

        with open(experiment, 'r') as f:
            lines = f.readlines()

            for line in lines:
                if 'forwarders' in line:
                    setup_forwarders = int(parse(line))

                    continue

                if 'clients' in line:
                    setup_clients = int(parse(line))

                    continue

                if 'layout' in line:
                    if int(parse(line)) == 0:
                        setup_layout = 'file-per-process'
                    else:
                        setup_layout = 'shared-file'

                    continue

                if 'spatiality' in line:
                    if int(parse(line)) == 0:
                        setup_spatiality = 'contiguous'
                    else:
                        setup_spatiality = 'strided'

                    continue

                if 'request' in line:
                    setup_size = int(parse(line))

                    continue

                if 'total' in line:
                    setup_total = int(parse(line))

                    continue

                if 'WRITE' in line:
                    operation = 'WRITE'

                    continue

                if 'READ' in line:
                    operation = 'READ'

                    continue

                if 'min' in line:
                    if operation == 'WRITE':
                        write_min_time = float(parse(line))

                    if operation == 'READ':
                        read_min_time = float(parse(line))

                if 'max' in line:
                    if operation == 'WRITE':
                        write_max_time = float(parse(line))

                    if operation == 'READ':
                        read_max_time = float(parse(line))

                if 'mean' in line:
                    if operation == 'WRITE':
                        write_mean_time = float(parse(line))

                    if operation == 'READ':
                        read_mean_time = float(parse(line))

            print('{};{};{};{};{};{};{};{};{};{};{};{}\n'.format(
                setup_forwarders,
                setup_clients,
                setup_layout,
                setup_spatiality,
                setup_size,
                setup_total,
                write_min_time,
                write_max_time,
                write_mean_time,
                read_min_time,
                read_max_time,
                read_mean_time
            ))
