from defs import xr_read_window, polymask
from rasterio import _io, windows
import concurrent.futures
import xarray as xr
import pandas as pd


class MemoryDataset(_io.MemoryDataset, windows.WindowMethodsMixin):
    pass


def junk():
    flist = gpkg_divides.geometry.to_list()
    polys = flist


def get_forcing_dict_newway(
    gpkg_divides,
    folder_prefix,
    file_list,
    var_list,
):
    reng = "rasterio"

    _xds = xr.open_dataset(folder_prefix.joinpath(file_list[0]), engine=reng)
    _template_arr = _xds.U2D.values
    _u2d = MemoryDataset(
        _template_arr,
        transform=_xds.U2D.rio.transform(),
        gcps=None,
        rpcs=None,
        crs=None,
        copy=False,
    )

    df_dict = {}
    for _v in var_list:
        df_dict[_v] = pd.DataFrame(index=gpkg_divides.index)

    ds_list = []
    for _nc_file in file_list:
        _full_nc_file = folder_prefix.joinpath(_nc_file)
        ds_list.append(xr.open_dataset(_full_nc_file, engine=reng))

    for i, feature in enumerate(gpkg_divides.geometry):
        print(f"{i}, {round(i/len(gpkg_divides.geometry), 5)*100}".ljust(40), end="\r")
        mask, _, window = polymask(_u2d)(feature)
        mask = xr.DataArray(mask, dims=["y", "x"])
        winslices = dict(zip(["y", "x"], window.toslices()))
        for j, _xds in enumerate(ds_list):
            cropped = xr_read_window(_xds, winslices, mask=mask)
            for var in var_list:
                time_value = _xds.time.values[0]
                var_mean = cropped[var].mean().values.item()
                df_dict[var].loc[i, time_value] = var_mean

    [ds.close() for ds in ds_list]

    return df_dict


def get_forcing_dict_newway_parallel(
    gpkg_divides,
    folder_prefix,
    file_list,
    var_list,
    para="thread",
    para_n=2,
):
    reng = "rasterio"
    _xds = xr.open_dataset(folder_prefix.joinpath(file_list[0]), engine=reng)
    _template_arr = _xds.U2D.values

    _u2d = MemoryDataset(
        _template_arr,
        transform=_xds.U2D.rio.transform(),
        gcps=None,
        rpcs=None,
        crs=None,
        copy=False,
    )
    ds_list = [xr.open_dataset("data/" + f) for f in file_list]

    if para == "process":
        pool = concurrent.futures.ProcessPoolExecutor
    elif para == "thread":
        pool = concurrent.futures.ThreadPoolExecutor
    else:
        pool = concurrent.futures.ThreadPoolExecutor

    with pool(max_workers=para_n) as executor:
        future_list = []

        df_dict = {}
        for _v in var_list:
            df_dict[_v] = pd.DataFrame(index=gpkg_divides.index)

        for i, m in enumerate(gpkg_divides.geometry):
            print(
                f"{i}, {round(i/len(gpkg_divides.geometry), 5)*100}".ljust(40), end="\r"
            )
            mask, _, window = polymask(_u2d)(m)
            mask = xr.DataArray(mask, dims=["y", "x"])
            winslices = dict(zip(["y", "x"], window.toslices()))
            for f in ds_list:
                cropped = executor.submit(xr_read_window, f, winslices, mask=mask)
                future_list.append(cropped)
                time_value = f.time.values[0]
                for var in var_list:
                    var_mean = cropped.result()[var].mean().values.item()
                    df_dict[var].loc[i, time_value] = var_mean
        for i, _f in enumerate(concurrent.futures.as_completed(future_list)):
            cropped = _f.result()

    [_xds.close() for _xds in ds_list]
    return df_dict


def get_forcing_dict_newway_inverted(
    gpkg_divides,
    folder_prefix,
    file_list,
    var_list,
):
    reng = "rasterio"
    _xds = xr.open_dataset(folder_prefix.joinpath(file_list[0]), engine=reng)
    _template_arr = _xds.U2D.values
    _u2d = MemoryDataset(
        _template_arr,
        transform=_xds.U2D.rio.transform(),
        gcps=None,
        rpcs=None,
        crs=None,
        copy=False,
    )
    ds_list = []
    for _nc_file in file_list:
        _full_nc_file = folder_prefix.joinpath(_nc_file)
        ds_list.append(xr.open_dataset(_full_nc_file, engine=reng))

    df_dict = {}
    for _v in var_list:
        df_dict[_v] = pd.DataFrame(index=gpkg_divides.index, columns=_xds.time.values)

    stats = []
    future_list = []
    mask_win_list = []

    for i, feature in enumerate(gpkg_divides.geometry):
        print(f"{i}, {round(i/len(gpkg_divides.geometry), 5)*100}".ljust(40), end="\r")
        mask, _, window = polymask(_u2d)(feature)
        mask = xr.DataArray(mask, dims=["y", "x"])
        winslices = dict(zip(["y", "x"], window.toslices()))
        mask_win_list.append((mask, winslices))

    for f in ds_list:
        print(f"{i}, {round(i/len(file_list), 2)*100}".ljust(40), end="\r")
        for j, (_m, _w) in enumerate(mask_win_list):
            cropped = xr_read_window(f, _w, mask=_m)
            for var in var_list:
                time_value = f.time.values[0]
                var_mean = cropped[var].mean().values.item()
                df_dict[var].loc[j, time_value] = var_mean
            j += 1

    [f.close() for f in ds_list]
    return df_dict


def get_forcing_dict_newway_inverted_parallel(
    gpkg_divides,
    folder_prefix,
    file_list,
    var_list,
    para="thread",
    para_n=2,
):
    import concurrent.futures

    reng = "rasterio"
    _xds = xr.open_dataset(folder_prefix.joinpath(file_list[0]), engine=reng)
    _template_arr = _xds.U2D.values
    _u2d = MemoryDataset(
        _template_arr,
        transform=_xds.U2D.rio.transform(),
        gcps=None,
        rpcs=None,
        crs=None,
        copy=False,
    )

    ds_list = [xr.open_dataset("data/" + f) for f in file_list]

    stats = []
    future_list = []
    mask_win_list = []

    for i, feature in enumerate(gpkg_divides.geometry):
        print(f"{i}, {round(i/len(gpkg_divides.geometry), 5)*100}".ljust(40), end="\r")
        mask, _, window = polymask(_u2d)(feature)
        mask = xr.DataArray(mask, dims=["y", "x"])
        winslices = dict(zip(["y", "x"], window.toslices()))
        mask_win_list.append((mask, winslices))

    if para == "process":
        pool = concurrent.futures.ProcessPoolExecutor
    elif para == "thread":
        pool = concurrent.futures.ThreadPoolExecutor
    else:
        pool = concurrent.futures.ThreadPoolExecutor

    with pool(max_workers=para_n) as executor:
        df_dict = {}
        for _v in var_list:
            df_dict[_v] = pd.DataFrame(index=gpkg_divides.index)

        for f in ds_list:
            print(f"{i}, {round(i/len(file_list), 2)*100}".ljust(40), end="\r")
            for j, (_m, _w) in enumerate(mask_win_list):
                cropped = executor.submit(xr_read_window, f, _w, mask=_m)
                future_list.append(cropped)
                time_value = f.time.values[0]
                for var in var_list:
                    var_mean = cropped.result()[var].mean().values.item()

                    df_dict[var].loc[j, time_value] = var_mean
                j += 1

        for _f in concurrent.futures.as_completed(future_list):
            cropped = _f.result()

    [f.close() for f in ds_list]
    return df_dict
