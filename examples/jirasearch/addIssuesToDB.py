# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import sqlite3
import pickle
import os
import sys
import localconstants
import datetime

"""
Apply a set of issues from an issues.txt file to the sqlite3 DB used by
indexJira.py for NRT updates.
"""

db = sqlite3.connect(localconstants.DB_PATH)
c = db.cursor()
with open(sys.argv[1], 'rb') as f:
  while True:
    l = f.readline()
    if l == b'':
      break
    i = l.find(b':')
    key = l[:i]
    value = l[i+1:].strip()
    c.execute('REPLACE INTO issues (key, body) VALUES (?, ?)', (key, pickle.dumps(json.loads(value.decode('utf-8')))))
db.commit()
