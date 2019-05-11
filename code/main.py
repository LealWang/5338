import mongodb_query as mongo
import neo4j_query as neo
import sys
import argparse
parser = argparse.ArgumentParser(description = 'For COMP 5338.')
#define database type
parser.add_argument('-type', type = str, default = None)
#define query type
parser.add_argument('-query',type = str, default = None)
parser.add_argument('-sq1',type = int, default = None)
parser.add_argument('-sq2',type = str, default = None)
parser.add_argument('-aq1',type = str,nargs='+', default = None)
parser.add_argument('-aq2',type = str,nargs='+', default = None)
parser.add_argument('-aq3',type = str,default = None)
parser.add_argument('-aq4',type = str,nargs='+', default = None)
parser.add_argument('-aq5',type = int, default = None)
parser.add_argument('-aq6',type = int, default = None)
args = parser.parse_args()
result = None
if args.type == 'mongo':
    if args.query == 'sq1':
       result = mongo.query_sq1(args.sq1)
    elif args.query == 'sq2':
        result = mongo.query_sq2(args.sq2)
    elif args.query == 'aq1':
        result = mongo.query_aq1(args.aq1)
    elif args.query == 'aq2':
        result = mongo.query_aq2(args.aq2[0],args.aq2[1])
    elif args.query == 'aq4':
        result = mongo.query_aq4(int(args.aq4[0]),int(args.aq4[1]),args.aq4[2])
elif args.type == 'neo':
    if args.query == 'sq1':
       result = neo.query_sq1(args.sq1)
    elif args.query == 'sq2':
        result = neo.query_sq2(args.sq2)
    elif args.query == 'aq3':
        result = neo.query_aq3(args.aq3)
    elif args.query == 'aq5':
        result = neo.query_aq5(args.aq5)
    elif args.query == 'aq6':
        result = neo.query_aq6(args.aq6)
for i in result:
    print(i)