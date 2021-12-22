#!/usr/bin/env python3

import argparse
import configparser
import logging
import structlog
import sys
import typing

from .local import LocalStorageDatabase
from .remote import GoogleDriveService as GDService
from .server import serve
from .sync import sync, cleanup


def read_settings(fn: str = 'blab-data-lake-settings.cfg') \
        -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config.optionxform = str  # type: ignore  # do not convert to lower-case
    config.read(fn)
    return config


def parse_args(args: list[str]) -> argparse.Namespace:

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

    def port(s: str) -> int:
        try:
            n = int(s)
        except (ValueError, TypeError):
            pass
        else:
            if 1 <= n <= 65535:
                return n
        raise argparse.ArgumentTypeError(
            f"invalid port value: '{s}'")

    subparsers = parser.add_subparsers(dest='cmd', required=True)
    subparsers.add_parser(
        'sync', help='synchronise contents from Google Drive')
    parser_cleanup = subparsers.add_parser(
        'cleanup',
        help='delete local files that have been deleted or overwritten ' +
        'on Google Drive')
    parser_cleanup = subparsers.add_parser(
        'printlocal',
        help='display a tree of the files downloaded from Google Drive')
    parser_cleanup = subparsers.add_parser(
        'printremote',
        help='display a tree of the files available on Google Drive')
    parser_cleanup.add_argument(
        '--delay', help='deletion delay', type=non_negative_float)
    parser_runserver = subparsers.add_parser('serve', help='start server')
    parser_runserver.add_argument(
        '--port', '-p', help='server port', type=port)

    return parser.parse_args(args)


def setup_logger(level: int) -> None:
    from sys import stdout
    logging.basicConfig(level=logging.INFO, format="%(message)s",
                        stream=stdout,)
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(level))


options = parse_args(sys.argv[1:])
setup_logger(logging.DEBUG if options.debug else
             logging.WARNING if options.quiet else logging.INFO)

config: dict[str, typing.Any] = dict(read_settings())


if options.cmd == 'sync':
    sync(config)
elif options.cmd == 'cleanup':
    cleanup(config, options.delay)
elif options.cmd == 'serve':
    serve(config, options.port)
elif options.cmd == 'printlocal':
    db = LocalStorageDatabase(config['Database'])
    with db.new_session() as session:
        tree = db.get_tree(session)
        if tree:
            tree.print_tree()
elif options.cmd == 'printremote':
    gdservice = GDService(config['GoogleDrive'])
    gdservice.get_tree().print_tree()
