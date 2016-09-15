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
module: quarry_lvm_setup
short_description: Set up a quarry LVM backend on a host
description:
    Initializes a quarry Volume Group from a given set of devices
version_added: "2.2"
options:
  devices:
    description:
      - A list of devices that should be included in the Volume Group
    required: false
    default: null
  state:
    description:
      - The Volume Group configuration state
    required: false
    default: present
    choices: [ "present", "absent" ]

author:
    - "Adam Litke (@aglitke)"
'''

EXAMPLES = '''
# Create configuration
- quarry_lvm_setup:
    devices: [ "/dev/sdb", "/dev/sdc" ]

# Add a physical device
- quarry_lvm_setup:
    devices: [ "/dev/sdb", "/dev/sdc", "/dev/sdd" ]

# Destroy configuration
- quarry_lvm_setup:
    state: absent
'''

from ansible.module_utils.basic import AnsibleModule
from ansible.modules.extras.storage.quarry import lvm_utils


QUARRY_VG = 'quarry_volumes'


def main():
    mod = AnsibleModule(
        argument_spec=dict(
            devices=dict(required=False, type='list', default=[]),
            state=dict(required=False, choices=['present', 'absent'],
                       default='present')),
        supports_check_mode=True,
    )

    devices = mod.params['devices']
    state = mod.params['state']

    if mod.check_mode:
        try:
            changes_required = check(devices, state)
        except Exception as e:
            mod.fail_json(msg=e.message)
        else:
            mod.exit_json(changed=changes_required)
        return


def check(devices, state):
    exists = False
    pvs = None
    vgs = lvm_utils.vgs()
    for vg in vgs:
        if vg.name == QUARRY_VG:
            exists = True
            pvs = vg.pv_name

    if state == 'present':
        return not exists or set(devices) != set(pvs)
    elif state == 'absent':
        return exists


if __name__ == '__main__':
    main()
