from query_engine import Engine
import logging


class HiveEngine(Engine):
    def print_engine_name(self):
        return "Testing Hive engine: "

    def report(self):
        logging.info(self.print_engine_name())
        logging.info(self.query_output)





