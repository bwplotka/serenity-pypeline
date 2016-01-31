from pca_initial import PcaInitial
from pipeline_engine import PipelineEngine

pipelineEngine = PipelineEngine()

pcaInit = PcaInitial(pipelineEngine._get_options(pipelineEngine._get_config()))
pcaInit.run()