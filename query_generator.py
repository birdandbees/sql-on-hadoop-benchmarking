

class QueryGenerator(object):
    def __init__(self, def_file, query_dir):
        self.def_file = def_file
        self.query_dir = query_dir
        self.queries = []


    def parse_definition(self):
        with open(self.def_file) as f:
            self.queries = f.readlines()
        return


    def generate_query(self, type, count):
        return








