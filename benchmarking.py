from query_generator import *
from impala_engine import *
from hive_engine import *
from drill_engine import *
import logging
import os


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                   format='%(asctime)s %(name)-8s %(levelname)-5s %(message)s',
                   datefmt='%m-%d %H:%M',
                   filename=__file__ + '.log',
                   filemode='w')

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)



    # Performance testing for hive
    query_generator = QueryGenerator('', 'sql/hive')
    hive_engine = HiveEngine('ls ', '', query_generator)
    # turn off system cache
    hive_engine.setup('echo 1 > /proc/sys/vm/drop_cached ')
    hive_engine.perf_run(6)
    hive_engine.report()

    # Performance testing for impala
    query_generator = QueryGenerator('', 'sql/impala')
    impala_engine = ImpalaEngine('ls ', '', query_generator)
    impala_engine.perf_run(6)
    impala_engine.report()

    # Performance testing for drill
    query_generator = QueryGenerator('', 'sql/drill')
    drill_engine = DrillEngine('ls ', '', query_generator)
    drill_engine.perf_run(6)
    drill_engine.report()


    # Concurrency testing for hive
    query_generator = QueryGenerator('sql/hive/con/test', 'sql/hive')
    query_generator.parse_definition()
    hive_engine = HiveEngine('ls ', '', query_generator)
    elapsed = hive_engine.concurrency_run()
    hive_engine.report()

    # Concurrency testing for impala
    query_generator = QueryGenerator('sql/impala/con/test', 'sql/impala')
    query_generator.parse_definition()
    impala_engine = ImpalaEngine('ls ', '', query_generator)
    impala_engine.concurrency_run()
    impala_engine.report()

    # Concurrency testing for hive
    query_generator = QueryGenerator('sql/drill/con/test', 'sql/drill')
    query_generator.parse_definition()
    drill_engine = DrillEngine('ls ', '', query_generator)
    drill_engine.concurrency_run()
    drill_engine.report()


    hive_engine.teardown('echo 1 > /proc/sys/vm/drop_cached ')