#!/usr/bin/env python3
"""Allows running the program as a module.

``python -m blabgddatalake <command> <arguments>``
"""

import argparse
import logging
import sys

import structlog

from .config import Config
from .local.localdb import LocalStorageDatabase
from .remote.gd import GoogleDriveService as GDService
from .server import serve
from .sync import cleanup, sync


def parse_args(args: list[str]) -> argparse.Namespace:
    """Parse command-line arguments.

    Args:
        args: the arguments to parse

    Returns:
        the parsed arguments
    """
    parser = argparse.ArgumentParser()
    verbosity = parser.add_mutually_exclusive_group()
    verbosity.add_argument('--debug',
                           '-d',
                           help='show verbose log for debugging',
                           action='store_true')
    verbosity.add_argument('--quiet',
                           '-q',
                           help='print only warnings and errors',
                           action='store_true')

    def non_negative_float(s: str) -> float:
        """Convert a string into a non-negative float.

        Args:
            s: the string to convert

        Raises:
            ArgumentTypeError: \
                if the input string does not represent
                a non-negative float

        Returns:
            a float with the value represented by the input string
        """
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
        """Convert a string into an integer between 1 and 65535.

        Args:
            s: the string to convert

        Raises:
            ArgumentTypeError: \
                if the input string does not represent
                an integer number between 1 and 65535

        Returns:
            an int with the value represented by the input string
        """
        try:
            n = int(s)
        except (ValueError, TypeError):
            pass
        else:
            if 1 <= n <= 65535:
                return n
        raise argparse.ArgumentTypeError(f"invalid port value: '{s}'")

    subparsers = parser.add_subparsers(dest='cmd', required=True)
    subparsers.add_parser('sync',
                          help='synchronise contents from Google Drive')
    parser_cleanup = subparsers.add_parser(
        'cleanup',
        help='delete local files that have been deleted or overwritten '
        'on Google Drive')
    parser_cleanup.add_argument('--delay',
                                help='deletion delay',
                                type=non_negative_float)
    subparsers.add_parser(
        'printlocal',
        help='display a tree of the files downloaded from Google Drive')
    subparsers.add_parser(
        'printremote',
        help='display a tree of the files available on Google Drive')
    parser_runserver = subparsers.add_parser('serve', help='start server')
    parser_runserver.add_argument('--port',
                                  '-p',
                                  help='server port',
                                  type=port)

    return parser.parse_args(args)


def setup_logger(level: int) -> None:
    """Define the logging level.

    The root logger level is always set to :attr:`logging.INFO`. The provided
    level defines only the minimum level of the log messages emitted by
    this program, not its dependencies.

    Args:
        level: the logger level
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        stream=sys.stdout,
    )
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(level))


options = parse_args(sys.argv[1:])
setup_logger(logging.DEBUG if options.debug else logging.WARNING if options.
             quiet else logging.INFO)

settings_fn = 'blab-data-lake-settings.cfg'
config = Config.read_settings(settings_fn)

if options.cmd == 'sync':
    sys.exit(sync(config))
elif options.cmd == 'cleanup':
    sys.exit(cleanup(config, options.delay))
elif options.cmd == 'serve':
    sys.exit(serve(config, options.port))
elif options.cmd == 'printlocal':
    db = LocalStorageDatabase(config.database)
    with db.new_session() as session:
        tree = db.get_tree(session)
        if tree:
            tree.print_tree()
        sys.exit(int(not bool(tree)))
elif options.cmd == 'printremote':
    gdservice = GDService(config.google_drive)
    r_tree = gdservice.get_tree()
    if r_tree:
        r_tree.print_tree()
    sys.exit(int(not bool(r_tree)))
