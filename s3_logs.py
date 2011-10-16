import os
from os import path
from datetime import datetime, timedelta

QUOTE_LEFT = '["'
QUOTE_RIGHT = ']"'

def get_int(int_str):
    return 0 if int_str == '-' else int(int_str)

def parse_log(lines):
    """
        Log format is taken from here:
        http://s3browser.com/amazon-s3-bucket-logging-server-access-logs.php#amazon_s3_server_access_log_format
    """
    parsed = []
    for line in lines:
        if line:
            fields = []
            tail = line.strip()
            while tail:
                field, _, tail = tail.partition(' ')
                if field[0] in QUOTE_LEFT:
                    delimiter_index = QUOTE_LEFT.index(field[0])
                    quote_right = QUOTE_RIGHT[delimiter_index]
                    if (len(field) > 1) and (field[-1] == quote_right):
                        fields += [field[1:-1]]
                    else:
                        field_right, _, tail = tail.partition(quote_right)
                        tail = tail.strip()
                        fields += ['%s %s' % (field[1:], field_right)]
                else:
                    fields += [field]
            timestamp, _, ts_offset = fields[2].partition(' ')
            timestamp = datetime.strptime(timestamp, '%d/%b/%Y:%H:%M:%S')
            offset_sign = ts_offset[:1]
            offset_hrs = ts_offset[1:3]
            offset_mins = ts_offset[-2:]
            ts_offset = timedelta(seconds=int(offset_hrs)*3600 + int(offset_mins)*60)
            if offset_sign == '+':
                timestamp = timestamp + ts_offset
            elif offset_sign == '-':
                timestamp = timestamp - ts_offset
                
                
            log_entry = {
                'bucket_owner':fields[0],
                'bucket':fields[1],
                'timestamp':timestamp,
                'ip':fields[3],
                'user':fields[4],
                'request_id':fields[5],
                'operation':fields[6],
                'key':fields[7],
                'url':fields[8],
                'http_status':int(fields[9]),
                'error_code':fields[10],
                'transfer_bytes':get_int(fields[11]),
                'object_size':get_int(fields[12]),
                'time_total':get_int(fields[13]),
                'time_amazon':get_int(fields[14]),
                'referrer':fields[15],
                'user_agent':fields[16],
                'version_id':fields[17],
            }
            parsed += [log_entry]
    return parsed

def read_log_file(filename):
    f = open(filename, 'r')
    lines = f.readlines()
    f.close()
    return parse_log(lines)        

operations = {}    
log_files = os.listdir('logs/')
for log_filename in log_files:
    file_path = path.join('logs/', log_filename)
    if path.isfile(file_path):
        log_data = read_log_file(file_path)
        for element in log_data:
            operation = element['operation']
            if operation in operations:
                operations[operation] += element['transfer_bytes']
            else:
                operations[operation] = element['transfer_bytes']
for operation, traffic in operations.iteritems():
    print operation, ': ', traffic