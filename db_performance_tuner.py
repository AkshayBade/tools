#!/usr/bin/env python3
'''
This script is mint to execute database operations which helps optimize database performance.
e.g: periodic Archival to back up table, periodic purging, moving data between partitions.

Created on Aug 18, 2021


Sample Usage: python db_performance_tuner.py -c dev archive -st firds -tt firds_backup --date_condition 20201130
'''

from __future__ import absolute_import, division

import argparse
import pyodbc
from   requests_kerberos        import HTTPKerberosAuth, OPTIONAL

DB_CONFIG = dict(
    dev=dict(SERVER_DRIVER='{SQL Server}', SERVERNAME='', DATABASE='', SCHEMA='')
)


class _SubCommand(object):
    SUBCMD_NAME = None

    SUBCMD_HELP = None

    @classmethod
    def add_arguments(parser):
        raise NotImplementedError()

    @classmethod
    def execute(opts):
        raise NotImplementedError()

    @classmethod
    def bind(cls, subparsers):
        assert cls.SUBCMD_NAME is not None and cls.SUBCMD_HELP is not None

        parser = subparsers.add_parser(cls.SUBCMD_NAME, help=cls.SUBCMD_HELP)
        cls.add_arguments(parser)
        parser.set_defaults(func=cls.execute)


class ArchiveData(_SubCommand):
    SUBCMD_NAME = "archive"

    SUBCMD_HELP = "Archives records from source table to backup table based on date condition."

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument("-st", "--source_table", required=True, type=str, help="table from which data is archived.")
        parser.add_argument("-tt", "--target_table", required=True, type=str, help="table to which data is added.")
        parser.add_argument("--date_condition", required=True, type=str,
                            help="date column up to which data should be archived.")

    @classmethod
    def execute(cls, opts):
        db_config = DB_CONFIG.get(opts.context)
        kerberos_auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
        CONNECTION_STR = 'Driver=' + db_config.get("SERVER_DRIVER") + ';Server=' + db_config.get(
            "SERVERNAME") + ';Database=' + db_config.get(
            "DATABASE") + ';Trusted_Connection=yes;app=' + db_config.get("SCHEMA") + ';auth=' + str(
            kerberos_auth)

        try:
            cnxn = pyodbc.connect(CONNECTION_STR, autocommit=True)
            cursor = cnxn.cursor()

            sql_query = '''DECLARE @row_count int = 50000;
SET ROWCOUNT @row_count;
WHILE (@row_count > 0)
    BEGIN
        DELETE {schema}.{source_table}
        OUTPUT DELETED.*
        INTO {schema}.{target_table}
        WHERE effective_date<=\'{date_condition}\';
        SET @row_count = @@ROWCOUNT;
        EXEC {database}..sp__block_for_free_log 30;
    END'''.format(schema=db_config.get("SCHEMA"), source_table=opts.source_table, target_table=opts.target_table,
                  date_condition=opts.date_condition, database=db_config.get("DATABASE"))

            cursor.execute(sql_query)

        except Exception as e:
            print(str(e))
        finally:
            cnxn.close()
            print("Connection Closed Successfully!!!")


def parseopts():
    parser = argparse.ArgumentParser(description=__doc__, usage="<subcommand> [<subcommand_options> | --help]")

    parser.add_argument("-c", "--context", default="dev", choices=["dev"], help='dev/prod defaults to dev.')

    subparsers = parser.add_subparsers(title="Available sub-commands")
    #XXX bind sub classes here.
    ArchiveData.bind(subparsers)

    # Parse args
    return parser.parse_args()


def main():
    opts = parseopts()
    opts.func(opts)


if __name__ == "__main__":
    main()
