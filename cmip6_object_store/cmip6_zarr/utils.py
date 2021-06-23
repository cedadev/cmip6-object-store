import json
import os
import uuid

import s3fs
import xarray as xr

from ..config import CONFIG, get_from_proj_or_workflow



def get_credentials(creds_file=None):

    if not creds_file:
        creds_file = CONFIG["store"]["credentials_file"]

    with open(creds_file) as f:
        creds = json.load(f)

    return creds


def get_uuid():
    _uuid = uuid.uuid4()
    return _uuid


def get_var_id(dataset_id, project):
    var_index = CONFIG[f"project:{project}"]["var_index"]
    return dataset_id.split(".")[var_index]


def create_dir(dr):
    if not os.path.isdir(dr):
        os.makedirs(dr)


def split_string_at(s, sep, indx):
    items = s.split(sep)
    first, last = sep.join(items[:indx]), sep.join(items[indx:])
    return first, last


def to_dataset_id(path, project):

    if ("/") in path:
        path = path.replace("/", ".")
        prefix = get_from_proj_or_workflow("bucket_prefix", project)
        if not path.startswith(prefix):
            raise ValueError(f"path {path} does not start with expected prefix {prefix}")
        path = path[len(prefix):]
    
    items = path.split(".")
    if items[-1].endswith(".nc") or items[-1] == "zarr":
        items = items[:-1]

    n_facets = CONFIG[f"project:{project}"]["n_facets"]
    return ".".join(items[-n_facets:])


def get_zarr_url(dataset_id, project):
    prefix = CONFIG["store"]["endpoint_url"]
    zarr_path = get_zarr_path(dataset_id, project, join=True)
    return f"{prefix}{zarr_path}"


def get_zarr_path(dataset_id, project, join=False):
    split_at = get_from_proj_or_workflow("split_level", project)
    prefix = get_from_proj_or_workflow("bucket_prefix", project)
    parts = dataset_id.split(".")
    bucket = prefix + ".".join(parts[:split_at])
    zarr_file = ".".join(parts[split_at:]) + ".zarr"
    if join:
        return f"{bucket}/{zarr_file}"
    else:
        return (bucket, zarr_file)


def read_zarr(path, project, **kwargs):
    dataset_id = to_dataset_id(path, project)
    zarr_path = get_zarr_path(dataset_id, project, join=True)
    endpoint_url = CONFIG["store"]["endpoint_url"]
    jasmin_s3 = s3fs.S3FileSystem(
        anon=True, client_kwargs={"endpoint_url": endpoint_url}
    )

    s3_store = s3fs.S3Map(root=zarr_path, s3=jasmin_s3)
    ds = xr.open_zarr(store=s3_store, consolidated=True, **kwargs)
    return ds


def get_archive_path(path, project):
    dataset_id = to_dataset_id(path, project)
    archive_dir = CONFIG[f"project:{project}"]["archive_dir"]

    return os.path.join(archive_dir, dataset_id.replace(".", "/"))



