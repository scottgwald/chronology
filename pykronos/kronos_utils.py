"""
This will one day be a useful command-line utility to interact with Kronos.
"""
from argparse import ArgumentParser


def main(args):
  pass


def parse_args():
  parser = ArgumentParser(description=('A set of command-line utilities to '
                                       'manage Kronos'))

  parser.add_argument('--server', required=True, help='The server URL')
  parser.add_argument('--start', type=float, help=('Start time (seconds '
                                                   'since epoch)'))
  parser.add_argument('--end', type=float, help=('End time (seconds '
                                                 'since epoch)'))

  parser.add_argument('--key', type=str, help='Key to put')
  parser.add_argument('--value', type=str, help='Value to put')


  group = parser.add_mutually_exclusive_group(required=True)
  group.add_argument('--get', action='store_true', help=('Get (use with '
                                                         '--start and --end)'))
  group.add_argument('--put', action='store_true', help=('Put (use with '
                                                         '--key and --value)'))
  group.add_argument('--index', action='store_true', help=('Get server status'))

  return parser.parse_args()

if __name__ == "__main__":
  main(parse_args())

