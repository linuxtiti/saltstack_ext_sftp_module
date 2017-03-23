# -*- coding: utf-8 -*-
"""
This is a EXECUTION MODULE
using paramiko to sftp.get/put file 
windows is not supported
"""

import socket,logging,traceback

try:
    import paramiko
    from paramiko.ssh_exception import BadHostKeyException
    from paramiko.ssh_exception import AuthenticationException
    from paramiko.ssh_exception import SSHException
    HAS_LIBS = True
except ImportError:
    log.debug('can not import paramiko module')
    HAS_LIBS = False

__virtualname__ = "sftp_mod"
sftp_mod_version = "0.4-dev"
log = logging.getLogger(__name__)
    
def __virtual__():
    '''
    Overwriting the cmd python module makes debugging modules
    with pdb a bit harder so lets do it this way instead.
    '''
    if HAS_LIBS:
        return __virtualname__
    else:
        return False


def display_ver():
    module_name = __name__
    info_ret = {
        "name": module_name,
        "version": sftp_mod_version
    }
    return info_ret

    
def _get_ssh_conn(hostname, port=None, username=None, password=None, pkey=None,test_mode=False):
    ret_dict = dict()
    ssh_conn_code = 1
    err_msg = None
    ssh_conn_obj = paramiko.SSHClient()
    ssh_conn_obj.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh_conn_obj.connect(hostname,port=port,username=username,password=password,pkey=pkey)
        ssh_conn_code = 0
    except BadHostKeyException:
        err_msg = 'host {0} key could not be verified'.format(hostname)
    except AuthenticationException:
        err_msg =  'host {0} authentication failed'.format(hostname)
    except SSHException:
        err_msg = 'unable to establish ssh connection to host {0}'.format(hostname)
    except socket.error:
        err_msg = 'unable to connect host {0} ,socket error'.format(hostname)
    if err_msg:
        log.warning(err_msg)

    ret_dict['status'] = ssh_conn_code
    ret_dict['err']  = err_msg
    if test_mode:
        ssh_conn_obj.close()
        ret_dict['conn_obj'] = None
    else:
        ret_dict['conn_obj'] = ssh_conn_obj
    return ret_dict

    
def _sftp(hostname,
          port=None,
          username=None,
          password=None,
          pkey_file=None,
          local_file_path=None,
          remote_file_path=None,
          sftp_action=None):
    ret = dict()
    load_private_key = False
    err_msg = ""
    private_key = None
    if port is None:
        ssh_port = 22
    else:
        ssh_port = port
    if pkey_file:
        try:
            private_key = paramiko.RSAKey.from_private_key_file(pkey_file)
            load_private_key = True
        except Exception as e:
            err_msg = 'unable to create key obj from {0},errmsg: \'{1}\''.format(str(pkey_file), str(e))
            log.debug(err_msg)
    if load_private_key is False and password is None:
        # no key auth and no password ,unable to ssh host
        ret['retcode'] = 1
        ret['stdout'] = ""
        ret['stderr'] = "unable to ssh host,no password provided, and  " + err_msg
        # __context__['retcode'] = ret['retcode']
        return ret
    trans_obj = paramiko.Transport((hostname, ssh_port))
    trans_conn_code = 1
    err_conn_msg = ""
    try:
        if private_key:
            trans_obj.connect(username=username, pkey=private_key)
        elif password:
            trans_obj.connect(username=username, password=password)
        trans_conn_code = 0
    except SSHException:
        err_conn_msg = 'unable to establish ssh connection to host {0},maybe authentication fails,or socket error'.format(hostname)
        log.debug(err_conn_msg)
    if trans_conn_code == 1:
        ret['pid'] = None
        ret['retcode'] = 1
        ret['stdout'] = ""
        ret['stderr'] = 'unable establish sftp connection to host:{0},due to {1}'.format(hostname,err_conn_msg)
    else:
        file_result_code = 1
        file_err_msg = ""
        try:
            sftp_obj = paramiko.SFTPClient.from_transport(trans_obj)
            if sftp_action == "put":
                sftp_obj.put(local_file_path, remote_file_path)
            if sftp_action == "get":
                sftp_obj.get(remote_file_path, local_file_path)
            file_result_code = 0
            trans_obj.close()
        except Exception as e:
            file_err_msg = str(e)
            traceback.print_exc()
            try:
                trans_obj.close()
            except:
                pass
    ret['pid'] = None
    ret['retcode'] = file_result_code
    ret['stderr'] = file_err_msg
    ret['stdout'] = 'sftp server: {0},local file:{1},remote file:{2}'.format(hostname,local_file_path,
                                                                          remote_file_path)
    return ret


def test_conn(hostname, port=None, username=None, password=None, pkey_file=None):
    #ret = _get_ssh_conn()
    ret = dict()
    load_private_key = False
    err_msg = ""
    private_key = None
    ssh_conn_dict = dict()
    if port is None:
        ssh_port = 22
    else:
        ssh_port = int(port)
    if pkey_file:
        try:
            private_key = paramiko.RSAKey.from_private_key_file(pkey_file)
            load_private_key = True
        except Exception as e:
            err_msg = 'unable to create key obj from {0},errmsg: \'{1}\''.format(str(pkey_file), str(e))
            log.debug(err_msg)
    if load_private_key is False and password is None:
        # no key auth and no password ,unable to ssh host
        ret['retcode'] = 1
        ret['stdout'] = ""
        ret['stderr'] = "unable to ssh host,no password provided, and  " + err_msg
        # __context__['retcode'] = ret['retcode']
        return ret
    if private_key:
        ssh_conn_dict = _get_ssh_conn(hostname, port=ssh_port, username=username, pkey=private_key,test_mode=True)
    elif password:
        ssh_conn_dict = _get_ssh_conn(hostname, port=ssh_port, username=username, password=password,test_mode=True)
    ret['retcode'] = int(ssh_conn_dict['status'])
    if ret['retcode'] == 0:
        ret['stdout'] = "test ssh connection: ok"
    else:
        ret['stdout'] = "test ssh connection: failed"
    ret['stderr'] = ssh_conn_dict.get('err')

    return ret


def push_to_sftp(hostname, 
    port=None, 
    username=None, 
    password=None, 
    pkey_file=None,
    file_on_minion_path=None,
    file_on_sftp_path=None):
    
    ret = dict()
    test_sftp_conn = test_conn(hostname,port,username,password,pkey_file)
    if test_sftp_conn['retcode'] != 0:
        return test_sftp_conn
    ret = _sftp(hostname,port=port,username=username,
        password=password,pkey_file=pkey_file,
        local_file_path=file_on_minion_path,remote_file_path=file_on_sftp_path,
        sftp_action="put")
    
    return ret


def download_from_sftp(hostname, 
    port=None, 
    username=None, 
    password=None, 
    pkey_file=None,
    file_on_minion_path=None,
    file_on_sftp_path=None):
    
    ret = dict()
    test_sftp_conn = test_conn(hostname,port,username,password,pkey_file)
    if test_sftp_conn['retcode'] != 0:
        return test_sftp_conn
    ret = _sftp(hostname,port=port,username=username,
        password=password,pkey_file=pkey_file,
        local_file_path=file_on_minion_path,remote_file_path=file_on_sftp_path,
        sftp_action="get")    
    
    return ret
