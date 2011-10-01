import ConfigParser
import re
import datetime
import os
import sys

from boto.s3.connection import S3Connection

# things you might want to change
#
# the list of fields to print in our table. See the total list
# of options in the comments for log_lines
fields = ['date', 'ip', 'key', 'bytessent', 'totaltime']

# things you can change (but why?)
config_file = 'config.ini'
cache_dir = "cache"


# and now code...
_config = None
def config():
    global _config
    if _config: return _config

    if not os.path.exists(config_file):
        sys.stderr.write("%s does not exist. You can base it on config.ini.sample\n")
        sys.exit(1)

    _config = ConfigParser.ConfigParser({})
    _config.read(config_file)
    return _config

def access_key():
    return config().get("config", "aws_access_key")
    
def secret_key():
    return config().get("config", "aws_secret_key")

def log_bucket_name():
    return config().get("config", "log_bucket")

def log_prefix():
    return config().get("config", "log_prefix")

_conn = None
def s3conn():
    global _conn
    if _conn: return _conn

    _conn = S3Connection(access_key(), secret_key())
    return _conn

def buckets():
    return s3conn().get_all_buckets()

def bucket(name):
    for b in buckets():
        if b.name == name: return b
    return None

def log_bucket():
    return bucket(log_bucket_name())

def bucket_names():
    return [ b.name for b in buckets() ]

def log_keys():
    return log_bucket().list(prefix = log_prefix())

def cacheing_read(key):
    if not os.path.exists(cache_dir):
        os.mkdir(cache_dir)

    keyfile = os.path.join(cache_dir, key.name)
    if os.path.exists(keyfile):
        f = open(keyfile)
        data = f.read();
        f.close()
        return data

    f = open(keyfile, 'w')
    data = key.read()
    key.close()
    f.write(data)
    f.close()
    return data

def log_line_strs():
    for k in log_keys():
        for l in cacheing_read(k).splitlines():
            yield l

line_re = re.compile("(?P<bucketuser>\S+) (?P<bucket>\S+) \[(?P<date>[^\]]+)\] (?P<ip>\S+) (?P<requestor>\S+) (?P<requestid>\S+) (?P<operation>\S+) (?P<key>\S+) \"(?P<requesturi>[^\"]+)\" (?P<status>\S+) (?P<errorcode>\S+) (?P<bytessent>\S+) (?P<objectsize>\S+) (?P<totaltime>\S+) (?P<turnaroundtime>\S+) \"(?P<referrer>[^\"]+)\" \"(?P<useragent>[^\"]+)\"")
def parse_line_str(line):
    global line_re
    m = line_re.match(line)
    if m: return m.groupdict()
    return None

def parse_line(line):
    data = parse_line_str(line)
    data['date'] = datetime.datetime.strptime(data['date'], "%d/%b/%Y:%H:%M:%S +0000")

    def nillify(d, f):
        if d[f] == "-": d[f] = None
    def makeint(d, f):
        if d[f]: d[f] = int(d[f])

    for k in data.iterkeys():
        nillify(data, k)

    makeint(data, 'status')
    makeint(data, 'bytessent')
    makeint(data, 'objectsize')
    makeint(data, 'totaltime')
    makeint(data, 'turnaroundtime')
    return data

def log_lines():
    """
    And finally ew get to the point of this whole exercise!

    This will return an array of dictionaries. Each dictionary
    represents one logged s3 event. Every dictionary will contain the
    fields:

    bucketuser - the user id of the owner of the source bucket
    bucket     - the name of the source bucket
    date       - when the request was processed (a datetime object)
    ip         - the ip of the requestor
    requestor  - the user id of the requestor (if not anonymous)
    requestid  - unique s3 generated request id
    operation  - either SOAP or REST
    key        - None if keyless operation, otherwise the name of the key
    requesturi - the request uri
    status     - the http status code of the response
    errorcode  - None if no error, otherwise the integer code
    bytessent  - Number of bytes sent during response (excluding http overhead)
    objectsize - The size of the object
    totaltime  - The total time taken to service the request (until last byte sent)
    turnaroundtime - The time s3 spent thinking about the request
    referrer   - the http referrer
    useragent  - the http user-agent string
    """
    for s in log_line_strs():
        yield parse_line(s)

def print_table(rows, fields, spacing=2):
    widths = {}
    for f in fields:
        widths[f] = 0

    for row in rows:
        for field in fields:
            row[field] = str(row[field])
            widths[field] = max(widths[field], len(row[field]))

    for f in fields:
        sys.stdout.write(f.ljust(widths[f] + spacing))
    sys.stdout.write("\n")

    for row in rows:
        for f in fields:
            sys.stdout.write(row[f].ljust(widths[f] + spacing))
        sys.stdout.write("\n")

if __name__ == '__main__':
    logs = [log for log in log_lines()]
    logs.sort(key = lambda d: d['date'], reverse=True)

    for log in logs:
        for k in log.iterkeys():
            log[k] = str(log[k])

    print_table(logs, fields)
