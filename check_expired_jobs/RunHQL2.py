#!/usr/bin/env python3
import sys
from traceback import format_exc, extract_tb
import argparse

parser = argparse.ArgumentParser(description='Run HQL scripts on SparkSQL 2.2.0')
parser.add_argument('-f', dest='hql_file', help='source hql file')
parser.add_argument('-a', dest='params', help='paramteres to replace')
parser.add_argument('-t', dest='emails', help='list of emails to notify upon error')
parser.add_argument('-r', dest='retrials', help='number of retrials 0 to 5', default=0, type=int)
parser.add_argument('-c', dest='cred', help='UNUSED, for compatibility with RunHQL')
parser.add_argument('-e', action='store_true', help='UNUSED, for compatibility witt RunHQL')
parser.add_argument('--cluster', action='store_true', help='run in cluster mode rather than client mode')
parser.add_argument('--execute', action='store_true', help='execute queries')
args = parser.parse_args()
retrials = args.retrials
if retrials < 0 or retrials > 5:
    raise Exception("Retrials have to be between 0 and 5")
params = dict()
if args.params:
    params = dict(tuple(param.split('=')) for param in args.params.split(','))

hql_file = args.hql_file


def replace_params(text, params_dict):
    for (k, v) in params_dict.items():
        text = text.replace(r"${%s}" % k, v)
    return text


with open(hql_file) as h:
    raw_source = h.read()
source = replace_params(raw_source, params)
stripped_lines = [raw_line.strip() for raw_line in source.split(';')]
sets = [line[3:].strip() for line in stripped_lines if line.startswith('set ')]
queries = [line for line in stripped_lines if line != '' and not line.startswith('set ')]


def send_error_email(emails, error_msg, hql_file, highPriority=False):
    from smtplib import SMTP
    from email.mime.text import MIMEText
    s = SMTP('localhost')
    msg = MIMEText(error_msg, 'html')
    ME = 'RunHQL2.py@careerbuilder.com'
    msg['From'] = ME
    msg['To'] = emails
    msg['Subject'] = 'RunHQL2.py encountered an error: {hql_file}'.format(hql_file=hql_file)
    if highPriority:
        msg['Importance'] = 'High'
    s.sendmail(ME, emails.split(','), msg.as_string())
    s.quit()


def get_sparkContext(sets):
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import HiveContext
    import json
    sconf = SparkConf()
    for conf in sets:
        conf_key, conf_value = tuple(conf.split('='))
        sconf = sconf.set(conf_key, conf_value)
    sc = SparkContext(conf=sconf)
    sqlContext = HiveContext(sc)
    sqlContext.udf.register('to_json', lambda d: json.dumps(d))
    return (sc, sqlContext)


if args.execute:
    import re

    sc, sqlContext = get_sparkContext(sets)
    udf_pattern = re.compile(r'create\s+temporary\s+function')
    while True:
        try:
            for query in queries:
                if udf_pattern.match(query.lower()):
                    print("IGNORING FUNCTION DEFINITION: ", query)
                    continue
                print(query)
                sqlContext.sql(query).count()
            break
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            exc = str(extract_tb(exc_traceback))
            raw_error_msg = 'Error while executing query (trial {trial}/{total_trials}): \n\n {query} \n\n {exc}'
            error_msg = raw_error_msg.format(query=query, exc=str(format_exc()), trial=args.retrials - retrials,
                                             total_trials=args.retrials)
            print(error_msg)
            retryingCache = 'java.io.FileNotFoundException: File does not exist' in error_msg
            retrying = retryingCache or retrials > 0

            if args.emails:
                send_error_email(args.emails, error_msg.replace('\n', '\n<br />'), hql_file,
                                 highPriority=(not retrying))
            sqlContext.clearCache()
            sc.stop()
            if not retrying:
                raise e
            sc, sqlContext = get_sparkContext(sets)
            if not retryingCache:
                retrials -= 1
    sc.stop()
else:
    command_line = ['spark2-submit',
                    '--master', 'yarn',
                    '--executor-memory', '8g',
                    '--deploy-mode', 'cluster' if args.cluster else 'client'] + \
                   [element for conf in sets for element in ['--conf', '"%s"' % conf]] + \
                   ['--files', hql_file,
                    'RunHQL2.py'] + sys.argv[1:] + ['--execute']

    from subprocess import Popen, PIPE, STDOUT, check_output

    print(command_line)
    pid = Popen(command_line, stdout=sys.stdout, stderr=sys.stderr)
    exit_code = pid.wait()
    # print(exit_code)
    sys.exit(exit_code)
