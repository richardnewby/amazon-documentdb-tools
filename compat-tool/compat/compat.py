 #!/usr/bin/python3

from os.path import isfile, dirname, abspath, join
import sys
import yaml
from mtools.util import logevent
import csv
import json

ROOT_DIR = dirname(abspath(__file__))
dollar_file = join(ROOT_DIR, 'dollar.csv')
versions = ['3.6', '4.0']

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
    p_usage_map = {}
    for p in command["pipeline"]:
        check_keys(p, p_usage_map, ver)
    for k in p_usage_map.keys():
        usage_map[k] = usage_map.get(k, 0) + 1
    actual_query = '{namespace}.aggregate({command})'.format(namespace=log_event.namespace, command=command["pipeline"])
    retval = {"unsupported": (0 < len(p_usage_map.keys())),
              "unsupported_keys": list(p_usage_map.keys()),
              "logevent": log_event,
              "processed": 1,
              "actual_query": actual_query}
    return retval

def process_query(log_event, usage_map, ver):
    p_usage_map = {}
    query = yaml.load(log_event.actual_query, Loader=yaml.FullLoader)
    check_keys(query, p_usage_map, ver)
    for k in p_usage_map.keys():
        usage_map[k] = usage_map.get(k, 0) + 1
    actual_query = '{namespace}.find({query}'.format(namespace=log_event.namespace, query=query["filter"])
    if "projection" in query.keys():
        actual_query = '{actual_query}, {projection}'.format(actual_query=actual_query, projection=query["projection"])
    actual_query = '{})'.format(actual_query)
    retval = {"unsupported": (0 < len(p_usage_map.keys())),
              "unsupported_keys": list(p_usage_map.keys()),
              "logevent": log_event,
              "processed": 1,
              "actual_query": actual_query}
    return retval

def process_find(log_event, usage_map, ver):
    p_usage_map = {}
    query = yaml.load(" ".join(log_event.split_tokens[log_event.split_tokens.index("command:")+2:log_event.split_tokens.index("planSummary:")]), Loader=yaml.FullLoader)
    check_keys(query["filter"], p_usage_map, ver)
    for k in p_usage_map.keys():
        usage_map[k] = usage_map.get(k, 0) + 1
    actual_query = '{namespace}.find({query}'.format(namespace=log_event.namespace, query=query["filter"])
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
    command = yaml.load(" ".join(log_event.split_tokens[log_event.split_tokens.index("command:") + 1:log_event.split_tokens.index("planSummary:")]), Loader=yaml.FullLoader)
    check_keys(command, p_usage_map, ver)
    for k in p_usage_map.keys():
        usage_map[k] = usage_map.get(k, 0) + 1
    actual_query = '{namespace}.updateMany({q}, {u})'.format(namespace=log_event.namespace, q=command["q"], u=command["u"])
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
            #print("Processing COMMAND find...")
            retval = process_find(log_event, usage_map, ver)
            cmd_map["find"] = cmd_map.get("find", 0) + 1

        if log_event.command in ['aggregate']:
            #print("Processing COMMAND aggregate...")
            retval = process_aggregate(log_event, usage_map, ver)
            cmd_map["aggregate"] = cmd_map.get("aggregate", 0) + 1

    elif 'QUERY' == log_event.component:
        #print("Processing query...")
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
 #          print('\n{}'.format(line))
            log_event = logevent.LogEvent(line)
            if log_event.datetime is None:
              continue
            pl = process_line(log_event, usage_map, ver, cmd_map)
            line_ct += pl["processed"]
            if pl["unsupported"]:
                unsupported_file.write(pl["logevent"].line_str)
                unsupported_file.write("\n")
                unsupported_query_file.write('{actual}  // {unsupported}\n'.format(actual=pl["actual_query"], unsupported=pl["unsupported_keys"]))
                unsupported_ct += 1
    unsupported_file.close()

    print('Results:')
    if unsupported_ct > 0:
        print('\t {unsupported_ct} out of {line_ct} queries unsupported'.format(unsupported_ct=unsupported_ct, line_ct=line_ct))
        print('Unsupported operators (and number of queries used):')
        for k,v in sorted(usage_map.items(), key=lambda x: (-x[1],x[0])):
            print('\t{key:20}  {val}'.format(key=k, val=v))
    else:
        print('\t All queries are supported')

    print('Query Types:')
    for k,v in sorted(cmd_map.items(), key=lambda x: (-x[1],x[0])):
        print('\t{key:20}  {val}'.format(key=k, val=v))
    print('Log lines of unsupported operators logged here: {file}'.format(file=unsupported_fname))
    print('Queries of unsupported operators logged here: {file}'.format(file=unsupported_query_fname))

def print_usage():
    print("Usage: compat.py <version> <input_file> <output_file>")
    print("  version : " + ", ".join(versions))
    print("  input_file: location of MongoDB log file")
    print("  output_file: location to write log lines that correspond to unsupported operators")

def main(args):
    if 3 != len(args):
        print('Incorrect number of arguments')
        print_usage()
        sys.exit()
    ver = args[0]
    if ver not in versions:
        print('Version {ver} not supported'.format(ver=ver))
        print_usage()
        sys.exit()
    infname = args[1]
    if not isfile(infname):
        print('Input file not found ({file})'.format(file=infname))
        print_usage()
        sys.exit()
    outfname = args[2]
    outqueryfname = '{file}.query'.format(file=outfname)
    load_keywords(dollar_file)
    process_log_file(ver, infname, outfname, outqueryfname)

if __name__ == '__main__':
    print(ROOT_DIR)
    main(sys.argv[1:])