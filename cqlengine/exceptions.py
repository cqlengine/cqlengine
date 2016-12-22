#cqlengine exceptions
class CQLEngineException(Exception): pass
class ModelException(CQLEngineException): pass
class ValidationError(CQLEngineException): pass

class UndefinedKeyspaceException(CQLEngineException): pass
class IfNotExistsWithCounterColumn(CQLEngineException): pass

class LWTException(CQLEngineException):

    def __init__(self, existing):
        """Lightweight transaction exception.

        This exception will be raised when a write using an `IF` clause could
        not be applied due to existing data violating the condition. The
        existing data is available through the `existing` attribute.

        :param existing: The current state of the data which prevented the write.
        """
        super(LWTException, self).__init__(self)
        self.existing = existing
