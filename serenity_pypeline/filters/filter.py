from abc import abstractmethod, ABCMeta

class Filter(object):
    __metaclass__ = ABCMeta

    def __init__(self, config):
        self._config = config

    @abstractmethod
    def run(self, **kwargs):
        raise NotImplementedError("Filter run not implemented")
