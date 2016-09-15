from collections import OrderedDict
from typing import Dict, List, Any

from pyelt.datalayers.dwh import Dwh
from pyelt.helpers.pyelt_logging import Logger, LoggerTypes



class BaseProcess():
    def __init__(self, pipe: 'Pipe'):
        self.pipe = pipe
        self.dwh = pipe.pipeline.dwh
        self.runid = pipe.pipeline.runid
        self.source_system = pipe.source_system
        self.logger = pipe.pipeline.logger
        self.sql_logger = pipe.pipeline.sql_logger  # Logger.create_logger('SQL log', pipe.runid, logger_type=LoggerTypes.SQL)
        self.steps = OrderedDict()


    # def append_step(self, name, func, params = {}, result = None):
    #     self.steps[name] = ProcesStep(name, func, params, result)

    def run(self) -> None:
        for step_name, step in self.steps.items():
            result = step.execute()
            self.logger.log(step_name)


    def execute(self, sql: str, log_message: str='') -> None:
        self.sql_logger.log_simple(sql + '\r\n')
        try:
            rowcount = self.dwh.execute(sql, log_message)
            self.logger.log(log_message, rowcount=rowcount, indent_level=5)
        except Exception as err:
            if 'on_errors' in self.dwh.config and self.dwh.config['on_errors'] == 'throw':
                raise Exception(err, sql, log_message)
            else:
                self.logger.log_error(log_message, sql, err.args[0])

    def execute_read(self, sql: str, log_message: str='') -> List[List[Any]]:
        self.sql_logger.log_simple(sql + '\r\n')
        result = []
        try:
            result = self.dwh.execute_read(sql, log_message)

            self.logger.log(log_message, indent_level=5)
        except Exception as err:
            self.logger.log_error(log_message, sql, err.args[0])
            raise Exception(err)
            # if 'on_errors' in self.dwh.pyelt_config and self.dwh.pyelt_config['on_errors'] == 'throw':
            #     # todo sql in exception opnemen
            #     raise Exception(err)
            # else:
            #     print(err.args)
            #     print(sql)
            #     raise Exception(err)
        finally:
            return result



    def _get_fixed_params(self) -> Dict[str, Any]:
        params = {}
        params['runid'] = self.runid
        params['source_system'] = self.source_system
        params['sor'] = self.pipe.sor.name
        params['rdv'] = self.dwh.rdv.name
        params['dv'] = self.dwh.dv.name
        return params

class ProcesStep:
    def __init__(self, name, func, params = {}, result = None) -> None:
        self.is_active = True
        self.name = name
        self.func = func
        self.params = params

    def execute(self):
        self.result =  self.func(self.params)
        return self.result