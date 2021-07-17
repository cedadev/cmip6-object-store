#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Console script for cmip6_object_store."""

import argparse
import os
import shutil
import sys

from cmip6_object_store import CONFIG, logging
from cmip6_object_store.cmip6_zarr.batch import BatchManager
from cmip6_object_store.cmip6_zarr.caringo_store import CaringoStore
from cmip6_object_store.cmip6_zarr.compare import compare_zarrs_with_ncs
from cmip6_object_store.cmip6_zarr.results_store import get_results_store, get_verification_store
from cmip6_object_store.cmip6_zarr.task import TaskManager
from cmip6_object_store.cmip6_zarr.intake_cat import create_intake_catalogue
from cmip6_object_store.cmip6_zarr.utils import (
    get_credentials,
    get_zarr_url,
)

LOGGER = logging.getLogger(__file__)


def _add_arg_parser_run(parser):

    _add_arg_parser_project(parser, description="convert to Zarr in Object Store")

    group = parser.add_mutually_exclusive_group()

    group.add_argument(
        "--slurm-array-member",
        action="store_true",
        required=False,
        help=("Not for interactive use. \n"
              "Batch number will be taken from SLURM_ARRAY_TASK_ID environment variable."),
    )

    group.add_argument(
        "-b",
        "--batches",
        type=str,
        default="all",
        required=False,
        help="Batches to run, default is 'all'. Also accepts comma separated "
        "list of batch numbers and/or ranges specified with a hyphen. E.g: "
        "'1,2,3' or '1-5'.",
    )

    parser.add_argument(
        "-d",
        "--datasets",
        type=str,
        default=None,
        required=False,
        help="Datasets to run. Also accepts comma separated "
        "list of datasets. E.g.: -d cmip6...gn.v20190910,cmip6...v20200202",
    )

    parser.add_argument(
        "-r",
        "--run-mode",
        type=str,
        default="lotus",
        required=False,
        help="Mode to run in, either 'lotus' (default) or 'local'.",
    )


def _range_to_list(range_string, sep):
    start, end = [int(val) for val in range_string.split(sep)]
    return list(range(start, end + 1))


def parse_args_run(args):
    # Parse batches into a single value
    slurm_array_member = args.slurm_array_member
    batches = args.batches
    datasets = args.datasets

    if slurm_array_member:
        batches = [int(os.environ["SLURM_ARRAY_TASK_ID"])]
    
    elif batches == "all":
        batches = None
    else:
        items = batches.split(",")
        batches = []

        for item in items:
            if "-" in item:
                batches.extend(_range_to_list(item, "-"))
            else:
                batches.append(int(item))

        batches = sorted(list(set(batches)))

    if datasets:
        datasets = datasets.split(",")

    return args.project, batches, datasets, args.run_mode


def run_main(args):
    project, batches, datasets, run_mode = parse_args_run(args)

    tm = TaskManager(project, batches=batches, datasets=datasets, run_mode=run_mode)
    tm.run_tasks()


def _add_arg_parser_project(parser, description=""):

    parser.add_argument(
        "-p",
        "--project",
        type=str,
        default=CONFIG["workflow"]["default_project"],
        required=False,
        help=f"Project {description}",
    )


def parse_args_project(args):
    return args.project


def _add_arg_parser_create(parser):
    _add_arg_parser_project(parser)
    parser.add_argument("--all", action="store_true",
                        help="include all datasets in batches (default is to check if already done)")
    
def create_main(args):
    project = parse_args_project(args)
    bm = BatchManager(project, exclude_done=not args.all)
    bm.create_batches()


def _add_arg_parser_intake(parser):
    _add_arg_parser_project(parser, description="to create intake catalog for")
    parser.add_argument("--limit", type=int,
                        help="maximum number of datasets to include")

def intake_main(args):
    project = parse_args_project(args)
    create_intake_catalogue(project, limit=args.limit)
    
    
def _add_arg_parser_clean(parser):

    _add_arg_parser_project(parser, description="to clean out directories for")

    parser.add_argument(
        "-D",
        "--delete-objects",
        action="store_true",
        help="Delete all the objects in the Object Store - DANGER!!!",
    )

    parser.add_argument(
        "-b",
        "--buckets",
        default=[],
        nargs="*",
        help="Identifiers of buckets TO DELETE!",
    )


def parse_args_clean(args):
    return args.project, args.delete_objects, args.buckets


def clean_main(args):
    project, delete_objects, buckets_to_delete = parse_args_clean(args)

    if delete_objects:
        resp = input("DO YOU REALLY WANT TO DELETE THE BUCKETS? [Y/N] ")
        if resp != "Y":
            print("Exiting.")
            sys.exit()

    batch_dir = BatchManager(project)._version_dir
    log_dir = os.path.join(CONFIG["log"]["log_base_dir"], project)
    to_delete = [log_dir, batch_dir]

    for dr in to_delete:
        if os.path.isdir(dr):
            LOGGER.warning(f"Deleting: {dr}")
            shutil.rmtree(dr)

    if buckets_to_delete:
        LOGGER.warning("Starting to delete buckets from Object Store!")
        caringo_store = CaringoStore(creds=get_credentials())

        for bucket in buckets_to_delete:
            LOGGER.warning(f"DELETING BUCKET: {bucket}")
            caringo_store.delete(bucket)


def _add_arg_parser_list(parser):

    _add_arg_parser_project(parser, description="to list directories for")

    parser.add_argument(
        "-c",
        "--count-only",
        action="store_true",
        help="Only show the total count of records processed.",
    )


def parse_args_list(args):
    return args.project, args.count_only


def list_main(args):
    project, count_only = parse_args_list(args)
    results_store = get_results_store(project)
    
    records = results_store.get_successful_runs()

    if not count_only:
        for _, dataset_id in records:
            zarr_url = get_zarr_url(dataset_id, project)
            print(f"Record: {zarr_url}")

    print(f"\nTotal records: {len(records)}")


def verify_main(args):
    project = parse_args_project(args)
    results_store = get_results_store(project)
    verified_store = get_verification_store(project)
    
    successes, out_of = compare_zarrs_with_ncs(project, dataset_id=args.dataset)
    print(f"\nVerified {successes} out of {out_of} datasets.")

    print("\n\nResults of all verifications so far:")

    n_verified_successes = verified_store.count_successes()
    n_total_successes = results_store.count_successes()
    n_total = results_store.count_results()

    print(f"""{n_verified_successes} successfully verified
{n_total_successes} total claimed successes
{n_total} total results including failures""")


def show_errors_main(args):
    project = parse_args_project(args)
    results_store = get_results_store(project)    

    errors = results_store.get_failed_runs()
    
    for dataset_id, error in errors.items():
        print("\n===================================================")
        print(f"{dataset_id}:")
        print("===================================================\n")
        print("\t" + error)

    print(f"\nFound {len(errors)} errors.")


def main():
    """Console script for cmip6_object_store."""
    main_parser = argparse.ArgumentParser()
    main_parser.set_defaults(func=lambda args: main_parser.print_help())
    subparsers = main_parser.add_subparsers()

    run_parser = subparsers.add_parser("run")
    _add_arg_parser_run(run_parser)
    run_parser.set_defaults(func=run_main)

    create_parser = subparsers.add_parser("create-batches")
    _add_arg_parser_create(create_parser)
    create_parser.set_defaults(func=create_main)

    clean_parser = subparsers.add_parser("clean")
    _add_arg_parser_clean(clean_parser)
    clean_parser.set_defaults(func=clean_main)

    list_parser = subparsers.add_parser("list")
    _add_arg_parser_list(list_parser)
    list_parser.set_defaults(func=list_main)

    verify_parser = subparsers.add_parser("verify")
    _add_arg_parser_project(verify_parser)

    verify_parser.add_argument(
        "-d",
        "--dataset",
        type=str,
        default=None,
        required=False,
        help="Single dataset ID to verify (defaults to choosing a sample)"
    )

    verify_parser.set_defaults(func=verify_main)

    intake_parser = subparsers.add_parser("create-intake")
    _add_arg_parser_intake(intake_parser)
    intake_parser.set_defaults(func=intake_main)

    show_errors_parser = subparsers.add_parser("show-errors")
    _add_arg_parser_project(show_errors_parser)
    show_errors_parser.set_defaults(func=show_errors_main)

    args = main_parser.parse_args()
    args.func(args)


if __name__ == "__main__":

    sys.exit(main())  # pragma: no cover
