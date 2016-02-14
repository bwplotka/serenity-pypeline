import time
from serenity_pypeline.logger import log

from serenity_pypeline.pipeline_engine import PipelineEngine
from tests.usage import getSerializedUsage

ITERATIONS = 1
ITERVAL = 1  # seconds

if __name__ == "__main__":
    iterations = ITERATIONS
    fake_input = getSerializedUsage()

    engine = PipelineEngine()
    log.info("-------- Initializied Pypeline Engine. ")
    for i in xrange(iterations):
        log.info("-------- Running Pypeline iteration: " + \
            str(i) + "/" + str(iterations))

        (result, exception) = engine.run(fake_input)

        time.sleep(ITERVAL)