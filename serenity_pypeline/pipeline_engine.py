import ConfigParser
import json


class WorkflowNotFoundException(Exception):
    pass


class PipelineEngine(object):

    CONFIG_PATH = '/etc/serenity-pypeline/serenity-pypeline.conf'

    def __init__(self):
        config = self._get_config()

        self._options = self._get_options(config)
        self._workflow = self._get_workflow()

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
        wf_path = self._options['workflow']['path']

        with open(wf_path) as wf_file:
            wf = json.load(wf_file)

        if not wf:
            raise WorkflowNotFoundException("Cannot load workflow")

        return wf
