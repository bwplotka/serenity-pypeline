# Simple class for testing workflows


class Echo(object):

    def __init__(self, *args, **kwargs):
        pass

    def run(self, **kwargs):
        return kwargs
