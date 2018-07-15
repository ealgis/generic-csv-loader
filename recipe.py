import datetime
import json
import sys

from ealgis_common.loaders import RewrittenCSV, CSVLoader
from ealgis_common.db import DataLoaderFactory
from ealgis_common.util import make_logger

tmpdir = "/app/tmp"

logger = make_logger(__name__)


class GenericCSVException(Exception):
    pass


class _GenericCSVMutator:
    gid_column = 'gid'

    def __init__(self, skip_rows, match_column, mapping):
        self.header_index = None
        self.skip_rows = skip_rows
        self.match_column = match_column
        self.mapping = mapping

    def mutate(self, line, row):
        if line < self.skip_rows:
            return None
        elif line == self.skip_rows:
            # header
            self.header = row.copy()
            self.header_index = row.index(self.match_column)
            if self.gid_column in row:
                raise GenericCSVException("{} column already exists in input data".format(self.gid_column))
            # add a GID column
            return [self.gid_column] + row
        else:
            return [self.mapping[row[self.header_index]]] + row


class GenericCSVLoader:
    match_methods = {
        'str': str
    }

    def __init__(self, config_file):
        with open(config_file, "r") as fd:
            self.config = json.load(fd)
        self.factory = DataLoaderFactory(db_name="datastore", clean=False)
        self.mapping = self.build_geo_gid_mapping()

    def run(self):
        def gid_match(line, row):
            if line < skip:
                return None
            elif line == skip:
                header = row
                logger.debug(header)
                return header
            else:
                return row
            raise Exception()

        target_schema = self.config['target_schema']
        csv_config = self.config['csv']
        linkage = self.config['linkage']
        skip = csv_config['skip']
        mutator = _GenericCSVMutator(csv_config['skip'], linkage['csv_column'], self.mapping)
        with self.factory.make_loader(target_schema) as loader:
            loader.add_dependency(linkage['shape_schema'])
            loader.set_metadata(
                name=self.config['name'],
                family=self.config['family'],
                description=self.config['description'],
                date_published=datetime.datetime.strptime(self.config['date_published'], '%Y-%m-%d').date()
            )
            target_table = self.config['name']

            # normalise the CSV file by reading it in and writing it out again,
            # Postgres is quite pedantic. we also want to add an additional column to it
            with RewrittenCSV(tmpdir, self.config['file'], mutator.mutate, dialect=csv_config['dialect']) as norm:
                instance = CSVLoader(loader.dbschema(), target_table, norm.get(), pkey_column=0)
                instance.load(loader)

            with loader.access_schema(linkage['shape_schema']) as geo_access:
                shape_table = linkage['shape_table']
                geo_source = geo_access.get_geometry_source(shape_table)
                loader.add_geolinkage(
                    geo_access,
                    shape_table, geo_source.gid_column,
                    target_table, _GenericCSVMutator.gid_column)

            return loader.result()

    def build_geo_gid_mapping(self):
        mapping = {}
        linkage = self.config['linkage']
        match_fn = self.match_methods[linkage['match']]
        with self.factory.make_schema_access(linkage['shape_schema']) as shape_access:
            shape_table = linkage['shape_table']
            geo_source = shape_access.get_geometry_source(shape_table)
            geo_cls = shape_access.get_table_class(shape_table)
            geo_column = getattr(geo_cls, linkage['shape_column'])
            gid_column = getattr(geo_cls, geo_source.gid_column)
            for gid, match_val in shape_access.session.query(gid_column, geo_column):
                match_val = match_fn(match_val)
                if match_val in mapping:
                    raise GenericCSVException("Shape mapping column has duplicate keys. Aborting.")
                mapping[match_val] = gid
        return mapping


def main():

    for arg in sys.argv[1:]:
        loader = GenericCSVLoader(sys.argv[1])
        result = loader.run()
        # result.dump(tmpdir)



if __name__ == '__main__':
    main()
