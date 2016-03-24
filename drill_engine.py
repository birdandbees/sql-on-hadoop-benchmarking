import warnings
from query_engine import Engine
from query_generator import QueryGenerator
import logging


class DrillEngine(Engine):
    def print_engine_name(self):
        return "Testing Drill engine: "

    def report(self):
        logging.info(self.print_engine_name())
        logging.info(self.query_output)