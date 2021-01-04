import os,sys,datetime,re
import logging

from collections import Counter

import pymongo
from pymongo import MongoClient
import requests

from lgdadmin import irucio

logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


MONGO_URI = 'mongodb://' + \
        os.environ['MONGODB_USER'] + ':'   + \
        os.environ['MONGODB_PASSWORD'] + '@'   + \
        os.environ['MONGODB_HOSTNAME'] + ':27017/' + \
        os.environ['MONGODB_DATABASE']


#client = MongoClient(MONGO_URI)
_client = None
def get_client():
    """ Return the client to the db instance
    """
    global _client
    if _client is None:
        _client = MongoClient(MONGO_URI)

    return _client

def get_db():
    db = get_client().flaskdb
    return db

def get_campaign():
    db = get_db()
    if db.campaigns.find_one():
        logger.info("Campaign already defined")
        return db.campaigns.find_one()
    r = {'active':True, 
         label:'test',
         'deadline':datetime.datetime(2020,6,1),
         'limit_date':datetime.datetime(2018,1,1),
         'notes':''}
    id = db.campaigns.insert_one(r)
    logger.info(f'Inserted {id}')
    return db.campaigns.find_one()

def define_campaign(label, deadline=datetime.datetime.now()  +  datetime.timedelta(days=31), 
                           limit_date = datetime.datetime.now() - datetime.timedelta(days=730),
                           notes=None):
    db = get_db()
    cpgn = [x for x in db.campaigns.find({'label':label})]
    if not len(cpgn):
        logger.info('No existing campaign with name {}; continuing'.format(label))
    else:
        if len(cpgn) > 1:
            logger.info("Found multiple campaigns; this shouldn't happen")
            print(cpgn)
            raise RuntimeError("Multiple campaign instances found.")
        else:
            logger.info("Campaign already defined")
            logger.info(cpgn)
            return cpgn

    #if here we need to create one
    r = {'active':True, 
        'label':label,
        'deadline'  :deadline,
        'limit_date':limit_date,
        'notes':'' if notes is None else notes}

    id = db.campaigns.insert_one(r)
    logger.info(f'Inserted {id}')
    return db.campaigns.find_one({'label':label})




def setup_collections():
    db = get_db()
    db.rules.create_index([('id', pymongo.ASCENDING)],unique=True)

    db.ukrse.create_index([('rse', pymongo.ASCENDING)],unique=True)

    db.replicas.create_index([('rse', pymongo.ASCENDING),('scope',pymongo.ASCENDING),('name',pymongo.ASCENDING)],
                                unique=True)
    db.replicas.create_index([('rse', pymongo.ASCENDING),('owner',pymongo.ASCENDING)])
    db.replicas.create_index([('owner', pymongo.ASCENDING),('rse',pymongo.ASCENDING)])

    db.replicas.create_index([('created_at',pymongo.ASCENDING)])

    db.campaigns.create_index([('active', pymongo.ASCENDING)],unique=True)

    db.owners.create_index([('account', pymongo.ASCENDING)],unique=True)
    


def upsert_uk_lgd():
    """ Get and set list of uk localgroup disk RSEs
    """
    results = []
    for rse in irucio.list_uk_localgroupdisks():
        r = irucio.rse_client.list_rse_attributes(rse)
        logger.debug(f'{rse}: {r}')
        r['rse'] = rse
        results.append(r)

    # now add to db
    db = get_client().flaskdb
    coll = db.ukrse

    for r in results:
        result = coll.replace_one({'rse':r['rse']},r,upsert=True)
        logger.debug(result.matched_count)


def get_rses():
    return sorted(x['rse'] for x in get_db().ukrse.find({},{'rse':1,'_id':0}))                                                                                                          

def get_consistency_datasets(rse,dump_date=None):
    """Generator; complete datasets at an RSE, 
        if no date provided (in datetime format), the latest available dump will be taken.
    """
    url = 'https://rucio-hadoop.cern.ch/consistency_datasets'
    query = {'rse':rse}
    if dump_date is not None:
        query['date'] = dump_date.strftime('%d-%m-%Y')
    r = requests.get(url=url,params=query,verify=False)
    logger.debug(f'{r.status_code} : {r.url}')
    if r.status_code !=200:
        raise IOError("Failed to retreive consistency check")

    headers = ('rse','scope','name','owner','size','created_at','last_accessed', 'rule_ids', 'n_replicas', 'last_update')
    fmt     = (str, str, str,str, int, str,str,str,str,int,str)
    for line in r.text.strip().split('\n'):
        row = {k:f(v) for k,v,f in zip(headers,line.split('\t'),fmt)}
        row['rule_ids'] = row['rule_ids'].split(',')
        row['size_rule_ids'] = len(row['rule_ids'])
        row['n_replicas'] = int(row['n_replicas'])
        try:
            row['created_at'   ] = datetime.datetime.fromtimestamp(int(row['created_at'   ])/1000.) 
            row['last_update'  ] = datetime.datetime.fromtimestamp(int(row['last_update'  ])/1000.) 
            row['last_accessed'] = datetime.datetime.fromtimestamp(int(row['last_accessed'])/1000.) if row['last_accessed'] != '""' else None
        except Exception as e:
            print(line)
            raise e
        row['owner'] = list(set(row['owner'].split(',')))
        yield row

def create_datasets_per_rse(fromDumps=True):
    """ Insert data: note that for the moment the dumps and rucio commands
    return different keys! From dumps is recommended!
    """
    #RSES
    rses = sorted([x['rse'] for x in get_db().ukrse.find({},{'rse':1,'_id':0})])                                                                                                          
    logger.debug(rses)

    replicas = get_db().replicas

    if not fromDumps:
        for rse in rses:
            logger.debug(f'Update: {rse}')
            db.replicas.drop({'rse':rse})
            g = irucio.rep_client.list_datasets_per_rse(rse)
            #TODO: might be better to delete all from rse and then insert many?
            for r in g:
                replicas.replace_one({'rse':r['rse'],'scope':r['scope'],'name':r['name']},
                            r, upsert=True)
    else:
        for rse in rses:
            logger.debug(f'Update: {rse}')
            replicas.delete_many({'rse':rse})
            r = get_consistency_datasets(rse=rse,dump_date=None)
            replicas.insert_many(r)



def summary_replicas():
    """Print out some basic summary info
    """
    rses = get_rses()
    max_len_rse = max(len(x) for x in rses)
    print(f'Number of UK LGD RSES: {len(rses)}')

    #counts per rse
    replicas = get_db().replicas
    print(f'Total number of datasets: {replicas.count_documents({})}')
    for rse in rses:
        count = replicas.count_documents({'rse':rse})
        print(f'\t {rse:{max_len_rse}}:  {count}')

    # Users
    #print(get_db().replicas.distinct('scope'))
    print("Scopes:")
    scopes = [x for x in get_db().replicas.distinct('scope')]
    max_len_scopes= max(len(x) for x in scopes)
    scopes = {x:replicas.count_documents({'scope':x}) for x in scopes}
    #scope_counter = Counter({x:replicas.count_documents({'scope':x}) for x in scopes})
    for k,v in sorted(scopes.items(), key=lambda x: int(x[1]) , reverse=True):
        print(f'\t{k:10} {v}')

    print("\n\nOwners:")
    owners = [x for x in get_db().replicas.distinct('owner')]
    max_len_owners = max(len(x) for x in owners)
    owners = {x:replicas.count_documents({'owner':x}) for x in owners}
    for k,v in sorted(owners.items(), key=lambda x: int(x[1]) , reverse=True):
        print(f'\t{k:10} {v}')


def get_outofdate_replicas(campaign_label,rse=None, scope=None, name_pattern=None,projection=None):
    db = get_db()
    campaign = db.campaigns.find_one({'label':campaign_label})
    if campaign is None:
        raise ValueError("Campaign {} does not exist".format(campaign_label))
    query = {'created_at':{'$lt':campaign['limit_date'] }}
    if rse is not None:
        query['res'] = rse
    if scope is not None:
        query['scope'] = scope
    if name_pattern is not None:
        query['name'] = {'$regex':name_pattern, '$options':'i'}

    g = db.replicas.find(filter=query,projection=projection).sort([('rse',pymongo.ASCENDING),('scope',pymongo.ASCENDING)])
    for x in g:
        yield x


def query_replicas(filters,order_by='owner'):
    replicas = get_db().replicas
    if order_by=='owner':
        g = replicas.find(filter=filters).sort([('owner',pymongo.ASCENDING),('rse',pymongo.ASCENDING)])
    else:
        g = replicas.find(filter=filters).sort([('rse',pymongo.ASCENDING),('owner',pymongo.ASCENDING)])
    return (x for x in g)


def update_owners(owners,force_update=False):
    """For a list of account owners, update the DB 
        if force_update is true, replace the existing entry if exists,
        otherwise do nothing
    """
    db_owners = get_db().owners
    updated = 0
    for owner in owners:
        _id = db_owners.find_one({'account':owner})
        logger.debug(f'{_id} ')
        if not force_update and _id is not None:
            continue
        else:
            person = irucio.get_account_info(owner)
            db_owners.replace_one({'account':owner},person,upsert=True )
            updated += 1
    logger.debug(f'Updated {updated} owners')
    pass

def update_all_replicas_with_owners(force_update=False):
    g = get_db().replicas.find({})
    owners = []
    for replica in g:
        owners.extend(replica['owner'])
    owners = list(set(owners))
    update_owners(owners,force_update=force_update)
    print(len(owners))

def upsert_rules():
    """
    """
    db = get_db()
    rule_strings = sorted(set([y for x in db.replicas.find({},{'rule_ids':1,'_id':0}) for y in x['rule_ids']]))
    logger.debug(f'Found {len(rule_strings)} strings')

    rules = db.rules
    for i,rule in enumerate(rule_strings):
        if i%10000 == 0:
            print(f'Loop: {i}')
        try:
            r = irucio.rule_client.get_replication_rule(rule)
        except irucio.exception.RuleNotFound as e:
            print("Missing rule: ",e)
            continue  
        rules.replace_one({'id':r['id']},r, upsert=True)

    return None

