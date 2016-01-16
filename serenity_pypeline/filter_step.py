from abc import abstractmethod, ABCMeta

__author__ = 'MacRomanowski'


class FilterStep(object):
    __metaclass__ = ABCMeta

    # TODO: this constructor should be enforced to be implemented in derived classes
    def __init__(self, conf):
        pass

    @abstractmethod
    def run(self, **kwargs):
        raise NotImplementedError('user must define run method')
