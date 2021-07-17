import os
from functools import wraps
from time import time

import numpy as np
import pandas as pd

from cmip6_object_store import CONFIG, logging

from cmip6_object_store.cmip6_zarr.utils import (
    get_archive_path,
    get_zarr_url,
    read_zarr,
)

from cmip6_object_store.cmip6_zarr.results_store import get_results_store


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
        self._results_store = get_results_store(self._project)

    def create(self):
        self._create_json()
        self._create_csv()

    def _create_json(self):
        template_file = self._iconf["json_template"]

        with open(template_file) as reader:
            content = reader.read()

        description = self._iconf["description_template"].format(project=self._project)
        cat_id = self._iconf["id_template"].format(project=self._project)
        csv_catalog_url = self._iconf["csv_catalog_url"].format(project=self._project)
        json_catalog = self._iconf["json_catalog"].format(project=self._project)

        content = (
            content.replace("__description__", description)
            .replace("__id__", cat_id)
            .replace("__cat_file__", csv_catalog_url)
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

        LOGGER.info(
            f"Wrote {len(zarr_cat_as_df)} records to CSV catalog file:\n {csv_catalog}"
        )

    @timer
    def _get_zarr_df(self):
        # Read in Zarr results store and convert to DataFrame, and return
        dataset_ids = self._results_store.get_successful_runs()
        print(f"{len(dataset_ids)} datasets")

        headers = [
            "mip_era",
            "activity_id",
            "institution_id",
            "source_id",
            "experiment_id",
            "member_id",
            "table_id",
            "variable_id",
            "grid_label",
            "version",
            "dcpp_start_year",
            "time_range",
            "zarr_path",
            "nc_path",
        ]

        rows = []
        #LIMIT = 10000000000
        LIMIT = 100

        for row, dataset_id in enumerate(dataset_ids):

            if row == LIMIT:
                break

            items = dataset_id.split(".")
            dcpp_start_year = self._get_dcpp_start_year(dataset_id)
            temporal_range = self._get_temporal_range(dataset_id)

            zarr_url = get_zarr_url(dataset_id, self._project)
            nc_path = get_archive_path(dataset_id, self._project) + "/*.nc"

            items.extend([dcpp_start_year, temporal_range, zarr_url, nc_path])
            rows.append(items[:])

        return pd.DataFrame(rows, columns=headers)

    def _get_dcpp_start_year(self, dataset_id):
        member_id = dataset_id.split(".")[5]

        if not "-" in member_id or not member_id.startswith("s"):
            return np.nan

        return member_id.split("-")[0][1:]

    def _get_temporal_range(self, dataset_id):
        try:
            nc_files = os.listdir(get_archive_path(dataset_id, self._project))
            nc_files = [
                fn for fn in nc_files if not fn.startswith(".") and fn.endswith(".nc")
            ]

            time_ranges = [fn.split(".")[-2].split("_")[-1].split("-") for fn in nc_files]
            start = f"{min(int(tr[0]) for tr in time_ranges)}"
            if len(start) == 4:
                start += "01"

            end = f"{max(int(tr[1]) for tr in time_ranges)}"
            if len(end) == 4:
                end += "12"
                        
            time_range = f"{start}-{end}"
            LOGGER.info(f"Found {time_range} for {dataset_id}")
        except Exception as exc:
            LOGGER.warning(f"FAILED TO GET TEMPORAL RANGE FOR: {dataset_id}: {exc}")
            time_range = ""
        # ds = read_zarr(dataset_id, self._project, use_cftime=True)
        # time_var = ds.time.values

        # time_range = "-".join(
        #     [tm.strftime("%Y%m") for tm in (time_var[0], time_var[-1])]
        # )
        # ds.close()

        return time_range


def create_intake_catalogue(project):
    cat = IntakeCatalogue(project)
    cat.create()


create_intake_catalogue(CONFIG["workflow"]["default_project"])
