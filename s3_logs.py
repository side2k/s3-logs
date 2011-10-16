from datetime import datetime

QUOTE_LEFT = '["'
QUOTE_RIGHT = ']"'


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
            log_entry = {
                'bucket_owner':fields[0],
                'bucket':fields[1],
                'timestamp':fields[2],
                'ip':fields[3],
                'user':fields[4],
                'request_id':fields[5],
                'operation':fields[6],
                'key':fields[7],
                'url':fields[8],
                'http_status':fields[9],
                'error_code':fields[10],
                'bytes_sent':fields[11],
                'object_size':fields[12],
                'time':fields[13],
                'time_turnaround':fields[14],
                'referrer':fields[15],
                'user_agent':fields[16],
                'version_id':fields[17],
            }
            parsed += [log_entry]
    return parsed
        
f = open('logs/2011-10-14-00-16-56-B066D716F32D84BA', 'r')
lines = f.readlines()
f.close()
parsed_data = parse_log(lines)        
print parsed_data[0]