class NotInitializedError(Exception):
    pass

class Task(object):

    def __init__(self, classpath):
        self._klass = self._get_class(classpath)
        self._obj = None
        self.next_success = []
        self.next_error = []
        self.input = None

    def _get_class(self, classpath):
        classpath, classname = tuple(classpath.split(':'))
        klass = __import__(classpath, fromlist=[classname])
        klass = getattr(klass, classname)
        return klass

    def init_class(self, config):
        self._obj = self._klass(config)

    def run(self, **kwargs):
        if self._obj is None:
            raise NotInitializedError("Class is not initialized")

        return self._obj.run(**kwargs)

    def add_success(self, obj):
        self.next_success.append(obj)

    def add_error(self, obj):
        self.next_error.append(obj)

    def is_initialized(self):
        return not (self._obj is None)
