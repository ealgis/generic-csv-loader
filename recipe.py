from ealgis_common.db import DataLoaderFactory
from ealgis_common.util import make_logger


logger = make_logger(__name__)


def main():
    tmpdir = "/app/tmp"
    datadir = '/app/data/'
    factory = DataLoaderFactory(db_name="scratch_cluster", clean=False)
    results = []
    for result in results:
        result.dump(tmpdir)


if __name__ == '__main__':
    main()
