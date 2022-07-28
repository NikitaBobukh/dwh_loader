import argparse
import database
import time
import traceback

from Components.Utils import get_launch_id
from config.cred.enviroment import Environment


class BaseLoader:
    def __init__(self, name=None, **parameters):
        self.name = name or self.__class__.__name__

        self.argparser = argparse.ArgumentParser()
        self.init_args()
        self.input_parameters = vars(self.argparser.parse_args())
        self.init_parameters = parameters
        self.env = Environment()

        self.worker_type = None

        self.launch_id = self.get_arg('launch_id') or get_launch_id(Environment.gsstorage_creds_json)

        self.bq_creds = Environment.gsstorage_creds_json


    def init_args(self):
        self.argparser.add_argument('--launch_id', type=int, help='Launch ID')
        self.argparser.add_argument('--warehouse', type=str, help='SF Warehouse Name')
        self.argparser.add_argument('--force_run', action='store_true', help='Runs loader ignoring merge checker')
        self.argparser.add_argument('--ignore_cloudwatch', action='store_true', default=None,
                                    help='Do not log this launch in CloudWatch')
        self.argparser.add_argument('--loader_processes_max_count', type=int,
                                    help='Maximum loader processes count at one time')
        self.argparser.add_argument('--hours_passed_threshold', type=float,
                                    help='Threshold of hours passed to start process')

    def get_arg(self, arg):
        input_value = self.input_parameters.get(arg)
        if input_value is not None:
            return input_value

        init_value = self.init_parameters.get(arg)
        if init_value is not None:
            return init_value

        return getattr(self, f'default_{arg.lower()}', None)

    def run(self):
        raise NotImplementedError