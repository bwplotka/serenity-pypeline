from serenity_pypeline.logger import log
from serenity_pypeline.filters.filter import Filter


class Echo(Filter):

    def __init__(self, config):
        super(Echo, self).__init__(config)

    def run(self, **kwargs):
        usage = kwargs['usage']
        log.info(usage)
        return kwargs
