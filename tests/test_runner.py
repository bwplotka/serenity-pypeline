from serenity_pypeline.logger import log
from serenity_pypeline.pipeline_engine import PipelineEngine
from tests.usage import getSerializedUsage
from serenity_pypeline.protos.mesos_pb2 import ResourceUsage

class PypelineTestRunner(object):
    def __init__(self, _test_cases=[]):
        self.test_cases = _test_cases

    def run(self):
        log.info("Starting Serenity Test Runner")
        for test_case in self.test_cases:
            for i in xrange(test_case['iterations']):
                log.info("-------- Running " + test_case['name'] + " iter: " + \
                    str(i) + "/" + str(test_case['iterations']))
                engine = PipelineEngine(test_case['workflow'])

                (result, exception) = engine.run(test_case['input'][i])
                if result != 0:
                    log.error("-------- TEST FAILED: " + str(exception))
                else:
                    log.info("-------- TEST SUCCESS")
        return 0


if __name__ == "__main__":
    test_cases = [
        {
            "name": "simple echo workflow 1",
            "workflow": "./simple_workflow.json",
            "iterations": 2,
            "input": [
                getSerializedUsage(),
                getSerializedUsage()
            ]
        }
    ]

    PypelineTestRunner(test_cases).run()