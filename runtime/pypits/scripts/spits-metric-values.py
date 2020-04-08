#! /usr/bin/python3

import os
import sys
import json
import argparse

from typing import Tuple, List
from libspits import SimpleEndpoint, messaging

connection_timeout= 15

def connect(host_addr: str, port: str) -> SimpleEndpoint:
    global connection_timeout
    try:
        endpoint = SimpleEndpoint(host_addr, port)
        endpoint.Open(connection_timeout)
        return endpoint
        
    except Exception as err:
        print("Impossible connect to `{}:{}`: {}".format(host_addr, port, err))
        raise
    
def spits_metrics_values(host_addr: str, port: str):
    endpoint = connect(host_addr, int(port))
    endpoint.WriteInt64(messaging.msg_cd_query_metrics_list)
    m_list = endpoint.ReadString(connection_timeout)
    m_list = json.loads(m_list)
    m_names = [dict(name=metric['name']) for metric in m_list]
    m_names = dict(metrics=m_names)
    m_names = json.dumps(m_names)
    
    endpoint = connect(host_addr, int(port))
    endpoint.WriteInt64(messaging.msg_cd_query_metrics_last_values)
    endpoint.WriteString(m_names)
    m_values = endpoint.ReadString(connection_timeout)
    m_values = json.loads(m_values)
    print(m_values)
    
    endpoint.Close()

def main(argv):
    parser = argparse.ArgumentParser(description='List last metric values from SPITS job/task manager')
    parser.add_argument('jobid', action='store', help='ID of the job')
    parser.add_argument('address', action='store', help='Address of the manager')
    parser.add_argument('port', action='store', help='Port of the manager')   
    args = parser.parse_args(argv)
    spits_metrics_values(args.address, args.port)
    
if __name__ == '__main__':
    main(sys.argv)
    sys.exit(0)
