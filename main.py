from cmd import CmdParser
from db import PostgreSQL


if __name__ == '__main__':
    db = PostgreSQL()
    cmdparser = CmdParser(db)
    args = cmdparser.get_args()
    if hasattr(args, 'func'):
        args.func()
    db.close()


