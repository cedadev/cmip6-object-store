#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Console script for cmip6_object_store."""

import argparse
import sys
import os
import shutil

from cmip6_object_store import CONFIG, logging
from cmip6_object_store.cmip6_zarr.batch import BatchManager
from cmip6_object_store.cmip6_zarr.task import TaskManager

LOGGER = logging.getLogger(__file__)


def _get_arg_parser_run(parser):

    parser.add_argument(
        "-p",
        "--project",
        type=str,
        default='cmip6',
        required=False,
        help="Project to convert to Zarr in Object Store.",
    )

    parser.add_argument(
        "-b",
        "--batches",
        type=str,
        default='all',
        required=False,
        help="Batches to run, default is 'all'. Also accepts comma separated " 
             "list of batch numbers and/or ranges specified with a colon. E.g: "
             "'1,2,3' or '1:5'.",
    )

    parser.add_argument(
        "-r",
        "--run-mode",
        type=str,
        default='lotus',
        required=False,
        help="Mode to run in, either 'lotus' (default) or 'local'.",
    )

    return parser


def _range_to_list(range_string):
    start, end = [int(_) for _ in range_string.split(':')]
    return list(range(start, end + 1))


def parse_args_run(args):
    # Parse batches into a single value
    batches = args.batches

    if batches == 'all':
        batches = None
    else:
        items = batches.split(',')
        batches = []

        for item in items:
            if ':' in item:
                batches.extend(_range_to_list(item))
            else:
                batches.append(int(item))

        batches = sorted(list(set(batches)))

    return args.project, batches, args.run_mode


def run_main(args):
    project, batches, run_mode = parse_args_run(args)
    tm = TaskManager(project, batches=batches, run_mode=run_mode)
    tm.run_tasks()


def _get_arg_parser_create_batches(parser):

    parser.add_argument(
        "-p",
        "--project",
        type=str,
        default='cmip6',
        required=False,
        help="Project to generate batches for.",
    )

    return parser


def parse_args_create(args):
    return args.project


def create_main(args):
    project = parse_args_create(args)
    bm = BatchManager(project)
    bm.create_batches()


def _get_arg_parser_clean(parser):

    parser.add_argument(
        "-p",
        "--project",
        type=str,
        default='cmip6',
        required=False,
        help="Project to clean out directories for.",
    )

    return parser


def parse_args_clean(args):
    return args.project


def clean_main(args):
    project = parse_args_create(args)
    
    batch_dir = BatchManager(project)._version_dir
    log_dir = os.path.join(CONFIG['log']['log_base_dir'], project)
    to_delete = [log_dir, batch_dir]

    for dr in to_delete:
        if os.path.isdir(dr):
            LOGGER.warning(f'Deleting: {dr}')
            shutil.rmtree(dr)

def main():
    """Console script for cmip6_object_store."""
    main_parser = argparse.ArgumentParser()
    subparsers = main_parser.add_subparsers()

    run_parser = subparsers.add_parser("run")
    _get_arg_parser_run(run_parser)
    run_parser.set_defaults(func=run_main)

    create_parser = subparsers.add_parser("create-batches")
    _get_arg_parser_create_batches(create_parser)
    create_parser.set_defaults(func=create_main)

    clean_parser = subparsers.add_parser("clean")
    _get_arg_parser_clean(clean_parser)
    clean_parser.set_defaults(func=clean_main)

    args = main_parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    
    sys.exit(main())  # pragma: no cover
