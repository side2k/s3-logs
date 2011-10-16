import os
from os import path
import subprocess
from datetime import datetime, timedelta

QUOTE_LEFT = '["'
QUOTE_RIGHT = ']"'

CACHE_PATH_DEFAULT = '/tmp/munin_s3_logs.cache/'
CACHE_EXPIRES_DEFAULT = 3600 #seconds

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
    
def parse_ls(lines):
    result = []
    for line in lines:
        if not line.strip():
            continue
        fields = [field for field in line.strip().split(' ') if field]
        result.append({
            'timestamp':datetime.strptime(
                '%s %s' % (fields[0], fields[1]),
                '%Y-%m-%d %H:%M'),
            'size':int(fields[2]),
            'path':fields[3]})
    return result    
    
def get_buckets():
    process = subprocess.Popen(['s3cmd', 'ls'], stdout=subprocess.PIPE)
    buckets = []
    for line in process.stdout.readlines():
        _, _, bucket_name = line.partition('s3://')
        if bucket_name:
            buckets += [bucket_name.strip()]
    return buckets

def get_log_list(bucket_name):
    process = subprocess.Popen(['s3cmd', 'ls', 's3://%s/logs/' % bucket_name], stdout=subprocess.PIPE)
    return parse_ls(process.stdout.readlines())
    
    
def get_cached_data(cache_path = CACHE_PATH_DEFAULT):
    if 's3_cache_expires' in os.environ:
        try:
            cache_expires = int(os.environ['s3_cache_expires'])
        except:
            cache_expires = CACHE_EXPIRES_DEFAULT
    else:
        cache_expires = CACHE_EXPIRES_DEFAULT
    cache_valid = False
    if os.path.exists(cache_path):
        file_stats = os.stat(cache_path)
        cache_update = datetime.fromtimestamp(file_stats.st_mtime)
        update_delta = datetime.now() - cache_update
        cache_valid = \
            (update_delta.days * 86400 +update_delta.seconds) < cache_expires
    else:
        os.mkdir(cache_path)
        os.chmod(cache_path, stat.S_IRUSR | stat.S_IWUSR)
        
    if not cache_valid:
        s3_list = get_file_list()
        cache_file = open(cache_path, 'w')
        cache_file.writelines(s3_list)
        cache_file.close()
        os.chmod(cache_path, stat.S_IRUSR | stat.S_IWUSR)
    else:
        cache_file = open(cache_path, 'r')
        s3_list = cache_file.readlines()
        cache_file.close()
        
    return parse_list(s3_list)

def normalize_name(name):
    normal_first = re.sub(r'^[^A-Za-z_]', r'_', name)
    return re.sub(r'[^A-Za-z0-9_]', r'_', normal_first)
    
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

print get_log_list('asdarh.ru')