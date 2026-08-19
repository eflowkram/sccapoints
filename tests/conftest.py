import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import clubpoints

# Scraping logs every scored run at INFO; that buries test failures.
clubpoints.setup_logging(debug=False)
clubpoints.log.setLevel("WARNING")

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")


@pytest.fixture
def fixture_path():
    def _path(name):
        return os.path.join(FIXTURES, name)

    return _path


@pytest.fixture
def sdr_db():
    """An in-memory database wired to the SDR rules."""
    clubpoints.load_region("sdr")
    clubpoints.NON_POINTS = ("X", "TO", "N")
    connection = clubpoints.db_init(":memory:")
    yield connection
    connection.close()
