__all__=['proxypool','retriever']

class ValidateException(Exception):
# Used in the validate function. Argument:
# ('retry', reason) # Retry with different proxy
# ('continue', warning, sleep_in_seconds) # Retry with the same proxy. Temporary condition
    pass

class ProxyException(Exception):
# Used in the filter proxy engine. Argument: proxy
    pass
