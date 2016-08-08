from utils import run_shell_cmd
from utils import fn_timer
import os
import logging
from multiprocessing import Pool

class Engine(object):



    def __init__(self, cmd, output, query_generator):

        self.cmd = cmd
        self.cmd_output = output
        self.perf_cmd = "/usr/bin/time -f \"%C,%e,%S,%U,%K\"  "
        self.query_generator = query_generator
        self.query_output = []


    @staticmethod
    def setup(cmd):
        '''
        Environment setup before running any tests

        :param cmd:
        :return:
        '''
        output, err = run_shell_cmd(cmd)
        if err:
	    logging.error("Shell command returns error, exiting... :" + err)
            exit(1)

    @staticmethod
    def teardown(cmd):
        '''
        Clean-up after tests
        :param cmd:
        :return:
        '''
        output, err = run_shell_cmd(cmd)
        if err:
            exit(1)
        return



    def perf_run(self, num_of_time, average_results=True):
         self.query_output.append('Performance Query Results:')
         for f in os.listdir(self.query_generator.query_dir):
             if os.path.isfile(os.path.join(self.query_generator.query_dir,f)):
                iterations = 0
                elapse = 0
                self.query_output.append('Running ' + f)
                for i in range(1, num_of_time):

                    run = self.perf_cmd + self.cmd + os.path.join(self.query_generator.query_dir,f) + self.cmd_output
                    output, err = run_shell_cmd(run)
                    if not err:
                        iterations += 1
                        self.query_output.append(output)
                        elapse += float(output.split(',')[-4])

                if average_results:
                    elapse = 0 if iterations == 0 else elapse / iterations
                    self.query_output.append( ' '.join(['average elapse:',  str(elapse)]) )
         return

    def profile(function, *args, **kwargs):
        """ Returns performance statistics (as a string) for the given function.
            credits: http://www.clips.ua.ac.be/tutorials/python-performance-optimization
        """

        def _run():
            function(*args, **kwargs)

        import cProfile as profile
        import pstats
        import os
        import sys;
        sys.modules['__main__'].__profile_run__ = _run
        id = function.__name__ + '()'
        profile.run('__profile_run__()', id)
        p = pstats.Stats(id)
        p.stream = open(id, 'w')
        p.sort_stats('time').print_stats(20)
        p.stream.close()
        s = open(id).read()
        os.remove(id)
        return s

    @fn_timer
    def concurrency_run(self):
        '''
         Currently not support error exit from sub-process
        '''
        self.query_output.append('Concurrency Query Test Stats:')
        pool_size = len(self.query_generator.queries)
        pool = Pool(processes = pool_size)
        results = pool.map_async(run_shell_cmd, self.query_generator.queries, pool_size)
        pool.close()
        pool.join()
        results.get()
        logging.debug(results.get())
        return 'total queries: ' + str(pool_size) + ' ;Concurrency tests returned in '




