import logging

stdio_handler = logging.StreamHandler()
stdio_handler.setLevel(logging.INFO)
log = logging.getLogger('chef')
log.addHandler(stdio_handler)
log.setLevel(logging.DEBUG)
