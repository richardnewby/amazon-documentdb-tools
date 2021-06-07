 #!/usr/bin/python3

from os.path import isfile, dirname, abspath, join
import argparse
import sys
import yaml
from mtools.util import logevent
import csv
import logging

ROOT_DIR = dirname(abspath(__file__))
VERSIONS = ['3.6', '4.0']
DEBUG = False

def all_keys(x):
    k = []
    if dict == type(x):
        for kk in x.keys():
            k.append(kk)
            k = k + all_keys(x[kk])
    elif list == type(x):
        for vv in x:
            k = k + all_keys(vv)
    return k

def dollar_keys(x):
    return list(set([k for k in all_keys(x) if k.startswith('$')]))

keywords = {}
def load_keywords(fname):
    global keywords
    with open(fname) as csv_file:
        reader = csv.DictReader(csv_file, delimiter=',')
        for k in reader.fieldnames[1:]:
            if 'Command' == k:
                continue
            keywords[k] = {}
        for row in reader:
            for k in keywords.keys():
                keywords[k][row['Command']] = row[k]
    return keywords

def check_keys(query, usage_map, ver):
    unsupported = False
    for k in dollar_keys(query):
        if 'No' == keywords[ver][k]:
            usage_map[k] = usage_map.get(k, 0) + 1
            unsupported = True
    return unsupported

def process_aggregate(log_event, usage_map, ver):
    command = yaml.load(" ".join(log_event.split_tokens[log_event.split_tokens.index("command:")+2:log_event.split_tokens.index("planSummary:")]), Loader=yaml.FullLoader)
    logging.debug('Processing AGGREGATE: {}'.format(command))
    p_usage_map = {}
    for p in command["pipeline"]:
        if check_keys(p, p_usage_map, ver):
            logging.debug('Unsupported: {}'.format(p_usage_map))
    for k in p_usage_map.keys():
        usage_map[k] = usage_map.get(k, 0) + 1
    actual_query = '{}.aggregate({})'.format(log_event.namespace, command["pipeline"])
    retval = {"unsupported": (0 < len(p_usage_map.keys())),
              "unsupported_keys": list(p_usage_map.keys()),
              "logevent": log_event,
              "processed": 1,
              "actual_query": actual_query}
    return retval

def process_query(log_event, usage_map, ver):
    p_usage_map = {}
    query = yaml.load(log_event.actual_query, Loader=yaml.FullLoader)
    if check_keys(query, p_usage_map, ver):
        logging.debug('Unsupported: {}'.format(p_usage_map))
    for k in p_usage_map.keys():
        usage_map[k] = usage_map.get(k, 0) + 1
    actual_query = '{}.find({})'.format(log_event.namespace, query)
    retval = {"unsupported": (0 < len(p_usage_map.keys())),
              "unsupported_keys": list(p_usage_map.keys()),
              "logevent": log_event,
              "processed": 1,
              "actual_query": actual_query}
    return retval

def process_find(log_event, usage_map, ver):
    p_usage_map = {}
    query = yaml.load(" ".join(log_event.split_tokens[log_event.split_tokens.index("command:")+2:log_event.split_tokens.index("planSummary:")]), Loader=yaml.FullLoader)
    logging.debug('Processing FIND: {}'.format(query))
    if check_keys(query["filter"], p_usage_map, ver):
        logging.debug('Unsupported: {}'.format(p_usage_map))
    for k in p_usage_map.keys():
        usage_map[k] = usage_map.get(k, 0) + 1
    actual_query = '{}.find({}'.format(log_event.namespace, query["filter"])
    if "projection" in query.keys():
        actual_query = '{}, {}'.format(actual_query, query["projection"])
    actual_query = '{})'.format(actual_query)
    retval = {"unsupported": (0 < len(p_usage_map.keys())),
              "unsupported_keys": list(p_usage_map.keys()),
              "logevent": log_event,
              "processed": 1,
              "actual_query": actual_query}
    return retval

def process_update(log_event, usage_map, ver):
    p_usage_map = {}
    cmd = yaml.load(" ".join(log_event.split_tokens[log_event.split_tokens.index("command:") + 1:log_event.split_tokens.index("planSummary:")]), Loader=yaml.FullLoader)
    logging.debug('Processing UPDATE: {}'.format(cmd))
    if check_keys(cmd, p_usage_map, ver):
        logging.debug('Unsupported: {}'.format(p_usage_map))
    for k in p_usage_map.keys():
        usage_map[k] = usage_map.get(k, 0) + 1
    actual_query = '{}.updateMany({}, {})'.format(log_event.namespace, cmd["q"], cmd["u"])
    retval = {"unsupported": (0 < len(p_usage_map.keys())),
              "unsupported_keys": list(p_usage_map.keys()),
              "logevent": log_event, "processed": 1,
              "actual_query": actual_query}
    return retval

def process_line(log_event, usage_map, ver, cmd_map):
    retval = {"unsupported": False, "processed": 0}

    #print(f'Command: {le.command}, Component: {le.component}, Actual Query: {le.actual_query}')
    if 'COMMAND' == log_event.component:
        if log_event.command in ['find']:
            retval = process_find(log_event, usage_map, ver)
            cmd_map["find"] = cmd_map.get("find", 0) + 1

        if log_event.command in ['aggregate']:
            retval = process_aggregate(log_event, usage_map, ver)
            cmd_map["aggregate"] = cmd_map.get("aggregate", 0) + 1

    elif 'QUERY' == log_event.component:
        retval = process_query(log_event, usage_map, ver)
        cmd_map["query"] = cmd_map.get("query", 0) + 1

    elif 'WRITE' == log_event.component:
        if log_event.operation in ['update']:
            #print("Processing update...")
            retval = process_update(log_event, usage_map, ver)
            cmd_map["update"] = cmd_map.get("update", 0) + 1

 #   if ("actual_query" in retval.keys()):
 #       print(f'BBB  {retval["actual_query"]}')

    return retval

def process_log_file(ver, fname, unsupported_fname, unsupported_query_fname):
    unsupported_file = open(unsupported_fname, "w")
    unsupported_query_file = open(unsupported_query_fname, "w")
    usage_map = {}
    cmd_map = {}
    line_ct = 0
    unsupported_ct = 0
    with open(fname) as log_file:
        for line in log_file:
            log_event = logevent.LogEvent(line)
            if log_event.datetime is None:
              logging.debug("Unable to process line: {}".format(line))
              continue
            pl = process_line(log_event, usage_map, ver, cmd_map)
            line_ct += pl["processed"]
            if pl["unsupported"]:
                unsupported_file.write(pl["logevent"].line_str)
                unsupported_file.write("\n")
                unsupported_query_file.write('{}  // {}\n'.format(pl["actual_query"], pl["unsupported_keys"]))
                unsupported_ct += 1
    unsupported_file.close()

    print('Results:')
    if unsupported_ct > 0:
        print('\t {} out of {} queries unsupported'.format(unsupported_ct, line_ct))
        print('Unsupported operators (and number of queries used):')
        for k,v in sorted(usage_map.items(), key=lambda x: (-x[1],x[0])):
            print('\t{:20}  {}'.format(k, v))
    else:
        print('\t All queries are supported')

    print('Query Types:')
    for k,v in sorted(cmd_map.items(), key=lambda x: (-x[1],x[0])):
        print('\t{:20}  {}'.format(k, v))
    print('Log lines of unsupported operators logged here: {}'.format(unsupported_fname))
    print('Queries of unsupported operators logged here: {}'.format(unsupported_query_fname))

def main():
    """
       parse command line arguments
    """
    parser = argparse.ArgumentParser(
        description="""Examines log files from MongoDB to determine if there are any queries which use operators that
                        are not supported in Amazon DocumentDB.""")

    parser.add_argument('--debug',
                        required=False,
                        action='store_true',
                        dest='debug',
                        help='output debugging information')

    parser.add_argument('--version', '-v',
                        required=True,
                        type=str,
                        dest='version',
                        help='version of Amazon DocumentDB with which you are evaluating compatibility.')

    parser.add_argument('--input-file', '-i',
                        required=True,
                        type=str,
                        dest='input_fname',
                        help='location of the MongoDB log file  to examine')

    parser.add_argument('--output-file', '-o',
                        required=True,
                        type=str,
                        dest='output_fname',
                        help='location of the output compatibility report.')

    args = parser.parse_args()

    log_level = logging.INFO

    if args.debug is True:
        log_level = logging.DEBUG

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    root_handler = logging.StreamHandler(sys.stdout)
    root_handler.setLevel(log_level)
    formatter = logging.Formatter('%(asctime)s: %(message)s')
    root_handler.setFormatter(formatter)
    root_logger.addHandler(root_handler)

    if args.version not in VERSIONS:
        message = 'Version {} not supported'.format(args.version)
        parser.error(message)

    # Get absolute file paths
    input_path = abspath(args.input_fname)
    output_path = abspath(args.output_fname)
    output_query_path = '{}.query'.format(output_path)

    if not isfile(input_path):
        message = 'Input file not found ({})'.format(input_path)
        parser.error(message)

    # Load the keyword csv file
    load_keywords(join(ROOT_DIR, 'dollar.csv'))

    # Process the log file
    process_log_file(args.version, input_path, output_path, output_query_path)

if __name__ == '__main__':
    main()