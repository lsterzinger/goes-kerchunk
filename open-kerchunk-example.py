import fsspec
import xarray as xr

fs = fsspec.filesystem(
    'reference',
    fo='./ABI-L2-ACMC-2020-102.json',
    remote_protocol='s3',
    remote_options={'anon': True}
)

ds = xr.open_dataset(fs.get_mapper(""), engine='zarr')