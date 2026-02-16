"""Allow running as `python -m chromecast_sink`."""

import sys

from chromecast_sink.cli import main

sys.exit(main())
