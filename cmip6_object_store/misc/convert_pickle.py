#!/usr/bin/env python

"""
Converts existing pickle store into abcunits database.


"""

import os
import pickle
import re

from progress.bar import Bar
import cmip6_object_store.cmip6_zarr.results_store as rs

pickle_dir = f"{os.environ['HOME']}/cmip6-object-store/data"
project = "cmip6"


zarrs_pickle = os.path.join(pickle_dir, f"{project}-zarrs.pickle")
errors_pickle = os.path.join(pickle_dir, f"{project}-errors.pickle")
verified_pickle = os.path.join(pickle_dir, f"{project}-verified.pickle")

# not bothering with lock file as this will be run a small number of times
def read_pickle(fn):
    with open(fn, "rb") as f:
        return pickle.load(f)

    
results_store = rs.get_results_store(project)
verified_store = rs.get_verification_store(project)

    
zarrs = read_pickle(zarrs_pickle)
errors = read_pickle(errors_pickle)
verified = read_pickle(verified_pickle)

print(f'{len(zarrs)} successes, {len(errors)} errors, {len(verified)} verification statuses')

stored_successes = results_store.get_successful_runs()
successes_to_store = set(zarrs.keys()) - set(stored_successes)

div = 100
with Bar(f'inserting successes (count in {div}s)', max=1+len(successes_to_store)//div) as bar:
    for i, dataset_id in enumerate(successes_to_store):
        results_store.insert_success(dataset_id)
        if i % div == 0:
            bar.next()

all_messages = results_store.get_all_results()
print(f'inserting (up to) {len(errors)} failures')

with Bar(f'inserting failures (count in {div}s)', max=1+len(errors)//div) as bar:
    for i, (dataset_id, error) in enumerate(errors.items()):
        msg = error.strip().split('\n')[-1]
        m = re.search('<Message>(.*?)</Message>', msg)
        if m:
            msg = m.group(1)
        msg = re.sub('["\']', '', msg)
        if dataset_id not in all_messages or all_messages[dataset_id] not in ('failed', msg):
            try:
                results_store.insert_failure(dataset_id, msg)
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                print(f'Warning could not store error {msg}: {exc}')
                results_store.insert_failure(dataset_id, 'failed')
        if i % div == 0:
            bar.next()

print(f'inserting {len(verified)} verification statuses')
for dataset_id, status in verified.items():
    if status == 'VERIFIED':
        verified_store.insert_success(dataset_id)
    else:
        verified_store.insert_failure(dataset_id, status)

