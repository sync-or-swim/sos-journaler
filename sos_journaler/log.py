import logging
import os


logger = logging.getLogger("sos-journaler")
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
