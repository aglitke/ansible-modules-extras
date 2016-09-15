#!/usr/bin/python
# This file is part of Ansible
#
# Ansible is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Ansible is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Ansible.  If not, see <http://www.gnu.org/licenses/>.

DOCUMENTATION = '''
---
module: quarry_lvm
author: "Adam Litke (@aglitke)"
version_added: "2.0"
short_description: Quarry Volume Provisioning (LVM backend)
requirements: [ lvm, python-rtslib ]
description:
    - Provision volumes and snapshots from an LVM Volume Group
    - Expose LUNs via ISCSI
options:
    volume_id:
        required: true
        aliases: [ "name" ]
        description:
            - The UUID of the volume to manage

    volume_size:
        required: false
        description:
            - When creating a volume or snapshot, the desired size in bytes

    volume_ancestor:
        required: false
        description:
            - Create a snapshot based on the given volume_id

    state:
        required: false
        default: "present"
        choices: [ "absent", "present" ]
        description:
            - Whether the volume should exist or not

    connection:
        required: false
        choices: [ "absent", "present" ]
        description:
            - Whether an ISCSI connection to the volume should exist or not

    initiator:
        required: false
        description:
            - An iqn to identify an ISCSI initiator that is allowed to connect

    userid:
        required: false
        description:
            - The CHAP userid required for the connection

    password:
        required: false
        description:
            - The CHAP password required for the connection
'''

EXAMPLES = '''
# These examples assume the host already has a VG named quarry_volumes

# Create a new 1G volume
quarry_lvm:
  volume_id: 10a5c927-835d-42c6-beee-b625ccea5db1
  volume_size: 1073741824
  state: present

# Set up a connection to an existing volume
quarry_lvm:
  volume_id: 10a5c927-835d-42c6-beee-b625ccea5db1
  connection: present
  initiator: iqn.1994-05.com.redhat:453b282362e5
  userid: user
  password: pass

# Create and connect a snapshot
quarry_lvm:
  volume_id: 765334a6-a73b-4faa-84f6-70a4603c6209
  volume_size: 1073741824
  volume_ancestor: 10a5c927-835d-42c6-beee-b625ccea5db1
  state: present
  connection: present
  initiator: iqn.1994-05.com.redhat:453b282362e5
  userid: user
  password: pass

# Disconnect and delete the snapshot
quarry_lvm:
  volume_id: 765334a6-a73b-4faa-84f6-70a4603c6209
  state: absent
  connection: absent
  initiator: iqn.1994-05.com.redhat:453b282362e5

# Disconnect a volume
quarry_lvm:
  volume_id: 10a5c927-835d-42c6-beee-b625ccea5db1
  connection: absent
  initiator: iqn.1994-05.com.redhat:453b282362e5

# Delete a volume
quarry_lvm:
  volume_id: 10a5c927-835d-42c6-beee-b625ccea5db1
  state: absent
'''

import rtslib_fb as rtslib

from ansible.module_utils.basic import AnsibleModule

QUARRY_VG = 'quarry_volumes'


def main():
    mod = AnsibleModule(
        argument_spec=dict(
            volume_id=dict(required=True, aliases=['name'], type='str'),
            volume_size=dict(required=False, type='int'),
            volume_ancestor=dict(required=False, type='str'),
            state=dict(required=False, choices=['present', 'absent'],
                       default='present'),
            connection=dict(required=False, choices=['present', 'absent']),
            initiator=dict(required=False, type='str'),
            userid=dict(required=False, type='str'),
            password=dict(required=False, type='str')),
        supports_check_mode=False,
    )

    check_vg(mod)

    final_result = dict(changed=False)
    state = mod.params['state']
    vol_size = mod.params['volume_size']
    vol_id = mod.params['volume_id']
    vol_ancestor = mod.params['volume_ancestor']
    vol = LvmVolume(mod, vol_id)

    try:
        exists = vol.exists()
        if state == 'present':
            if not exists:
                result = vol.create(vol_ancestor, vol_size)
                final_result.update(result)
            result = handle_connection(mod, vol)
            final_result.update(result)
        elif state == 'absent' and exists:
            result = handle_connection(mod, vol)
            final_result.update(result)
            result = vol.delete()
            final_result.update(result)
    except CommandFailure as e:
        mod.fail_json(name=vol.vol_id, msg=str(e), rc=e.rc)

    mod.exit_json(**final_result)


def check_vg(mod):
    rc, out, err = mod.run_command(['vgs', QUARRY_VG])
    if rc != 0:
        mod.fail_json(msg="Volume group %s not found" % QUARRY_VG)


def handle_connection(mod, vol):
    connection = mod.params['connection']
    if connection is None:
        return dict()
    initiator = mod.params['initiator']
    if initiator is None:
        mod.fail_json(name=vol.vol_id,
                      msg="initiator is required when managing connections")
    userid = mod.params['userid']
    password = mod.params['password']

    if connection == 'present':
        return vol.add_connection(initiator, userid, password)
    elif connection == 'absent':
        return vol.remove_connection(initiator)


class LvmVolume(object):
    def __init__(self, module, vol_id):
        self.module = module
        self.vol_id = vol_id

    def _cmd(self, cmd):
        rc, out, err = self.module.run_command(cmd)
        if rc != 0:
            raise CommandFailure(rc, out, err, cmd)
        return out, err

    def exists(self):
        o, e = self._cmd(['lvs', '-o', 'name', QUARRY_VG])
        return self.vol_id in [line.strip() for line in o.splitlines()]

    def create(self, ancestor, size):
        cmd = ['lvcreate', '-an', '-L', '%sb' % size, '-n', self.vol_id]
        if ancestor is not None:
            cmd.extend(['-s', '%s/%s' % (QUARRY_VG, ancestor)])
        else:
            cmd.extend([QUARRY_VG])
        self._cmd(cmd)
        return dict(changed=True,
                    volume_id=self.vol_id,
                    volume_size=size,
                    volume_ancestor=ancestor)

    def delete(self):
        self._cmd(['lvremove', '-f', '%s/%s' % (QUARRY_VG, self.vol_id)])
        return dict(changed=True,
                    volume_id=None)

    def add_connection(self, initiator, userid, password):
        changed = False

        # TODO: Get current state first
        self._cmd(['lvchange', '-ay', '--yes',
                   '%s/%s' % (QUARRY_VG, self.vol_id)])

        fabric = rtslib.FabricModule('iscsi')
        target = tpg = None
        for t in fabric.targets:
            if 'quarry' in t.wwn:
                target = t
                tpg = list(target.tpgs)[0]
                break
        if not target:
            iqn = rtslib.utils.generate_wwn('iqn')
            pre, post = iqn.split(':', 1)
            iqn = "iqn.2016-10.com.ansible.quarry:" + post
            target = rtslib.Target(fabric, wwn=iqn)
            tpg = rtslib.TPG(target, 1)
            rtslib.NetworkPortal(tpg, "0.0.0.0", "3260")
            changed = True

        if not tpg.enable:
            tpg.enable = True
            changed = True

        dev = "/dev/{}/{}".format(QUARRY_VG, self.vol_id)
        try:
            storage = rtslib.BlockStorageObject(self.vol_id)
        except rtslib.RTSLibNotInCFS:
            storage = rtslib.BlockStorageObject(self.vol_id, dev=dev)
            lun_idx = len(list(tpg.luns))
            tpg.lun(lun_idx, storage, self.vol_id)
            changed = True
        else:
            lun = list(storage.attached_luns)[0]
            lun_idx = lun.lun

        node_acl = None
        acls = list(tpg.node_acls)
        for acl in acls:
            if acl.node_wwn == initiator:
                node_acl = acl
                break
        if node_acl is None:
            node_acl = tpg.node_acl(initiator)
            changed = True

        if node_acl.chap_userid != userid or node_acl.chap_password != password:
            node_acl.chap_userid = userid
            node_acl.chap_password = password
            changed = True

        for acl_lun in list(node_acl.mapped_luns):
            if acl_lun.tpg_lun.lun == lun_idx:
                break
        else:
            node_acl.mapped_lun(lun_idx, lun_idx, False)
            changed = True

        return dict(changed=changed,
                    volume_id=self.vol_id,
                    target=target.wwn,
                    initiator=initiator,
                    lun=lun_idx,
                    username=userid,
                    password=password)

    def remove_connection(self, initiator):
        changed = False
        if self.connection_exists(initiator):
            storage = rtslib.BlockStorageObject(self.vol_id)
            storage.delete()
            # TODO: Remove empty ACLs and Targets
            changed = True

        return dict(changed=changed,
                    volume_id=self.vol_id,
                    target=None,
                    initiator=None,
                    lun=None)

    def connection_exists(self, initiator):
        try:
            storage = rtslib.BlockStorageObject(self.vol_id)
        except rtslib.RTSLibNotInCFS:
            pass
        else:
            luns = list(storage.attached_luns)
            if len(luns) == 1:
                mapped_luns = list(luns[0].mapped_luns)
                if len(mapped_luns) == 1:
                    if mapped_luns[0].node_wwn == initiator:
                        return True
        return False


class CommandFailure(Exception):
    def __init__(self, rc, out, err, cmd=[]):
        self.rc = rc
        self.out = out
        self.err = err
        self.cmd = cmd

    def __str__(self):
        return "Command %r Failed (rc=%d) out=%s err=%s" % (self.cmd, self.rc,
                                                            self.out, self.err)


if __name__ == '__main__':
    main()
