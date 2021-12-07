#!/usr/bin/env python3

from .sync import sync, cleanup
import argparse
import configparser
import logging
import sys


def read_settings(fn: str = 'blab-dataimporter-googledrive-settings.cfg') \
        -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config.optionxform = str  # type: ignore  # do not convert to lower-case
    config.read(fn)
    return config


parser = argparse.ArgumentParser()

verbosity = parser.add_mutually_exclusive_group()
verbosity.add_argument(
    '--debug', '-d',
    help='show verbose log for debugging',
    action='store_true')
verbosity.add_argument(
    '--quiet', '-q',
    help='print only warnings and errors',
    action='store_true')


def non_negative_float(s: str) -> float:
    try:
        n = float(s)
    except (ValueError, TypeError):
        pass
    else:
        if n >= 0:
            return n
    raise argparse.ArgumentTypeError(
        f"invalid non-negative float value: '{s}'")


subparsers = parser.add_subparsers(dest='cmd', required=True)
parser_sync = subparsers.add_parser('sync')
parser_cleanup = subparsers.add_parser('cleanup')
parser_cleanup.add_argument(
    '--delay', help='deletion delay', type=non_negative_float)

options = parser.parse_args(sys.argv[1:])


logging.basicConfig(level=logging.INFO)
logging.getLogger(__package__).setLevel(
    logging.DEBUG if options.debug else
    logging.WARNING if options.quiet else logging.INFO)


config = dict(read_settings())

if options.cmd == 'sync':
    sync(config)
elif options.cmd == 'cleanup':
    cleanup(config, options.delay)
