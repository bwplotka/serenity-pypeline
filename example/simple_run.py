from serenity_pypeline.pipeline_engine import PipelineEngine


def run():
    """
    simple run that present what serenity-pypeline is able to do
    :return: status of run
    """

    engine = PipelineEngine()
    return engine.run(useless="kwargs")