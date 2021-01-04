import glob,os,sys
from collections import defaultdict,namedtuple
import re,pickle

import rucio
from rucio.client               import Client
from rucio.client.didclient     import DIDClient
from rucio.client.replicaclient import ReplicaClient
from rucio.client.rseclient     import RSEClient
from rucio.client.ruleclient    import RuleClient
from rucio.client.accountclient import AccountClient

from rucio.common import exception

DID = namedtuple('DID',['scope','name','ftype','owner','path','dataset'])


did_client   = DIDClient()
rep_client   = ReplicaClient()
rse_client   = RSEClient()
rule_client  = RuleClient()
admin_client = AccountClient()

def list_uk_localgroupdisks():
    """Return a list of known uk localgroup disks
    """
    global rse_client
    r = rse_client.list_rses(rse_expression='spacetoken=ATLASLOCALGROUPDISK&cloud=UK')
    return sorted(x['rse'] for x in r)

def get_rse_usage(rse,filters=None):
    return rse_client.get_rse_usage(rse,filters=filters)


def get_account_info(owner_name):
    """ return the dict of account info.
    If not valid, raises AccountNotFound error
    """
    info = admin_client.get_account(owner_name)
    return info


