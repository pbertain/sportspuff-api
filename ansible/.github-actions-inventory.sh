#!/bin/bash
# Generate dynamic inventory for GitHub Actions
# This allows managing hosts in one place

HOST_NAME="${HOST_NAME:-host74.nird.club}"

cat << EOF
{
  "prod": {
    "hosts": ["${HOST_NAME}"]
  },
  "dev": {
    "hosts": ["${HOST_NAME}"]
  },
  "_meta": {
    "hostvars": {
      "${HOST_NAME}": {
        "ansible_user": "ansible"
      }
    }
  }
}
EOF

