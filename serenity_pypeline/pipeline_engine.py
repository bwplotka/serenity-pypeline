import ConfigParser
import json
import Queue

from serenity_pypeline.logger import log
from serenity_pypeline.protos.mesos_pb2 import ResourceUsage
import serenity_pypeline.protos.serenity_pb2

from task import Task


class WorkflowNotFoundException(Exception):
    pass


class PipelineEngine(object):
    CONFIG_PATH = '/etc/serenity-pypeline/serenity-pypeline.conf'

    def __init__(self, workflow_path=None):
        self._workflow_path = workflow_path
        self._options = self._get_options(self._get_config())

        self._workflow_json = self._get_workflow()
        self._first_task, self._workflow = self._create_workflow(self._workflow_json)

        self._initialize_objects()
        log.info("Pypeline initialized with workflow: " +  self._workflow_json['name'])

    def run(self, serializedUsage):
        """
        Runs every task from workflow

        As parameter we expect ResourceUsage.proto from mesos.proto

        :return: Status of workflow execution
        """
        usage = ResourceUsage()
        try:
            usage.ParseFromString(serializedUsage)
        except Exception as e:
            log.error("Exception occurred while parsing usage: " + str(e))
            return 1, e

        queue = Queue.Queue()
        queue.put(self._first_task)
        run_res = (0, None)
        while not queue.empty():
            task = queue.get()

            try:
                if task.input is None:
                    result = task.run(usage=usage)
                else:
                    result = task.run(**task.input)

                for t in task.next_success:
                    t.input = result
                    queue.put(t)

            except Exception as e:
                log.error("Exception occurred while execution tasks: " + str(e))
                run_res = (1, e)
                for t in task.next_error:
                    t.input = {"error": e}
                    queue.put(t)

        return run_res

    def _get_config(self):
        config = ConfigParser.ConfigParser()
        config.read(PipelineEngine.CONFIG_PATH)
        return config

    def _get_options(self, config):
        result = {}

        for section in config.sections():
            result[section] = self._get_option(config, section)

        return result

    def _get_option(self, config, section):
        result = {}

        for option in config.options(section):
            result[option] = config.get(section, option)

        return result

    def _get_workflow(self):
        if self._workflow_path is None:
            self._workflow_path = self._options['workflow']['path']

        with open(self._workflow_path) as wf_file:
            wf = json.load(wf_file)

        if not wf:
            raise WorkflowNotFoundException("Cannot load workflow")

        return wf

    def _create_workflow(self, wf_definition):
        wf = wf_definition['definition']

        start_point = wf[wf['start']]
        start_task = Task(start_point['run'])

        queue = Queue.Queue()
        start_point['class'] = start_task
        start_point['in_queue'] = True

        queue.put(start_point)

        while not queue.empty():
            task = queue.get()

            for s in task['onSuccess']:
                tmp = wf[s]

                if 'in_queue' not in tmp:
                    tmp['in_queue'] = True
                    tmp['class'] = Task(tmp['run'])
                    queue.put(tmp)
                task['class'].add_success(tmp['class'])

            for e in task['onFail']:
                tmp = wf[e]

                if 'in_queue' not in tmp:
                    tmp['in_queue'] = True
                    tmp['class'] = Task(tmp['run]'])
                    queue.put(tmp)
                task['class'].add_error(tmp['class'])

        return start_task, wf

    def _initialize_objects(self):
        queue = Queue.Queue()
        queue.put(self._first_task)

        while not queue.empty():
            task = queue.get()
            [queue.put(t) for t in task.next_error]
            [queue.put(t) for t in task.next_success]

            if not task.is_initialized():
                task.init_class(self._options)
