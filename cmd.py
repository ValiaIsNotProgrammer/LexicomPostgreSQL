from db import PostgreSQL
import argparse


class CmdParser():
    def __init__(self, db: PostgreSQL):
        self.parser = argparse.ArgumentParser(description="script with subcommands")
        self.subparsers = self.parser.add_subparsers(title="commands", dest="command")
        self.db = db
        self.__add_commands()

    def __add_commands(self):
        parser_command1 = self.subparsers.add_parser("backup", help="Description for command 1")
        parser_command1.set_defaults(func=self.db.backup)
        parser_command2 = self.subparsers.add_parser("update_statuses", help="Description for command 2")
        parser_command2.set_defaults(func=self.db.update_statuses)
        parser_command3 = self.subparsers.add_parser("generate_tables", help="Description for command 3")
        parser_command3.set_defaults(func=self.db.generate_tables)
        return

    def get_args(self):
        return self.parser.parse_args()
