#st: short for "states module"
# -*- coding: utf-8 -*-

"""
transport file through sftp  
state module
using paramiko
"""

import json,logging
from salt.exceptions import CommandExecutionError

log = logging.getLogger(__name__)
sftp_state_version = "0.1-dev"


def get(name,sftp_server=None, 
    sftp_port=None, 
    sftp_user=None, 
    sftp_pwd=None, 
    sftp_pkey_file=None,
    source_file=None,
    target_file=None):
    ret = {'name': name,
           'changes': {},
           'result': False,
           'comment': ''}
    try:
        get_file = __salt__['sftp_mod.push_to_sftp'](
            hostname=sftp_server,username=sftp_user,password=sftp_pwd,
            file_on_minion_path=source_file,
            file_on_sftp_path=target_file
        )
    except CommandExecutionError as err:
        ret['comment'] = str(err)
        return ret
    ret['changes'] = get_file
    ret['result'] = not bool(get_file['retcode'])
    if ret['result']:
        ret['comment'] = 'file "{0}" save as {1} ok'.format(source_file,target_file)
    else:
        ret['comment'] = 'file "{0}" save as {1} failed'.format(source_file,target_file)
    return ret

def push(name,sftp_server=None, 
    sftp_port=None, 
    sftp_user=None, 
    sftp_pwd=None, 
    sftp_pkey_file=None,
    source_file=None,
    target_file=None):
    ret = {'name': name,
           'changes': {},
           'result': False,
           'comment': ''}
    try:
        push_file = __salt__['sftp_mod.download_from_sftp'](
            hostname=sftp_server,username=sftp_user,password=sftp_pwd,
            file_on_minion_path=target_file,
            file_on_sftp_path=source_file
        )
    except CommandExecutionError as err:
        ret['comment'] = str(err)
        return ret
    ret['changes'] = push_file
    ret['result'] = not bool(push_file['retcode'])
    if ret['result']:
        ret['comment'] = 'file "{0}" save as {1} ok'.format(source_file,target_file)
    else:
        ret['comment'] = 'file "{0}" save as {1} failed'.format(source_file,target_file)
    return ret    
