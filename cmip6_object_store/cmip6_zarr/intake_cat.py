import os
from functools import wraps
from time import time

import pandas as pd

from cmip6_object_store import CONFIG, logging
from cmip6_object_store.cmip6_zarr.utils import (
    get_pickle_store,
    get_zarr_url,
    read_zarr,
)

LOGGER = logging.getLogger(__file__)


def timer(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        print(f"func: {f.__name__} args: [{args} {kw}] took: {(te-ts):2.4f} sec")
        return result

    return wrap


class IntakeCatalogue:
    def __init__(self, project):
        self._iconf = CONFIG["intake"]
        self._project = project

    def create(self):
        self._create_json()
        self._create_csv()

    def _create_json(self):
        template_file = self._iconf["json_template"]

        with open(template_file) as reader:
            content = reader.read()

        description = self._iconf["description_template"].format(project=self._project)
        cat_id = self._iconf["id_template"].format(project=self._project)
        csv_catalog = self._iconf["csv_catalog"].format(project=self._project)
        json_catalog = self._iconf["json_catalog"].format(project=self._project)

        content = (
            content.replace("__description__", description)
            .replace("__id__", cat_id)
            .replace("__cat_file__", csv_catalog)
        )

        with open(json_catalog, "w") as writer:
            writer.write(content)

        LOGGER.info(f"Wrote intake JSON catalog: {json_catalog}")

    def _create_csv(self):

        csv_catalog = self._iconf["csv_catalog"].format(project=self._project)

        # if os.path.isfile(csv_catalog):
        #     raise FileExistsError(f'File already exists: {csv_catalog}')

        # Read in Zarr catalogue
        zarr_cat_as_df = self._get_zarr_df()
        zarr_cat_as_df.to_csv(csv_catalog, index=False)

        LOGGER.info(f"Wrote CSV catalog file: {csv_catalog}")

    @timer
    def _get_zarr_df(self):
        # Read in Zarr store pickle and convert to DataFrame, and return
        records = get_pickle_store("zarr", self._project).read()

        headers = [
            "mip_era",
            "activity",
            "institute",
            "model",
            "experiment_id",
            "ensemble_member",
            "mip_table",
            "variable",
            "grid_label",
            "version",
            "temporal_range",
            "zarr_path",
        ]
        rows = []

        for dataset_id, zarr_path in records.items():

            items = dataset_id.split(".")
            temporal_range = self._get_temporal_range(dataset_id)

            zarr_url = get_zarr_url(zarr_path)

            items.extend([temporal_range, zarr_url])
            rows.append(items[:])

            if len(rows) > 1000:
                break

        return pd.DataFrame(rows, columns=headers)

    def _get_temporal_range(self, dataset_id):
        ds = read_zarr(dataset_id, use_cftime=True)
        time_var = ds.time.values

        time_range = "-".join(
            [tm.strftime("%Y%m") for tm in (time_var[0], time_var[-1])]
        )
        ds.close()

        LOGGER.info(f"Found {time_range} for {dataset_id}.")
        return time_range


def create_intake_catalogue(project):
    cat = IntakeCatalogue(project)
    cat.create()


create_intake_catalogue("cmip6")
