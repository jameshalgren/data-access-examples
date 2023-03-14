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

    # Open the first NetCDF file in "file_list" as an xarray dataset.
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

    # Create an empty dictionary called "df_dict".
    # This will eventually contain a Pandas dataframe for each variable in "var_list".
    # The index of each dataframe will be the same as the index of "gpkg_divides".
    df_dict = {}
    for _v in var_list:
        df_dict[_v] = pd.DataFrame(index=gpkg_divides.index)

    # Create a list called "ds_list" to hold each NetCDF file in "file_list" as an xarray dataset.
    ds_list = []
    for _nc_file in file_list:
        _full_nc_file = folder_prefix.joinpath(_nc_file)
        ds_list.append(xr.open_dataset(_full_nc_file, engine=reng))

    # Loop over each geometry in "gpkg_divides".
    for i, feature in enumerate(gpkg_divides.geometry):
        # Print the progress of the loop as a percentage.
        print(f"{i}, {round(i/len(gpkg_divides.geometry), 5)*100}".ljust(40), end="\r")
        
        # Create a mask for the current geometry using the MemoryDataset and the "polymask" function.
        mask, _, window = polymask(_u2d)(feature)
        
        # Convert the mask to a DataArray.
        mask = xr.DataArray(mask, dims=["y", "x"])
        
        # Get the window corresponding to the geometry using "xr_read_window".
        # The window will be used to crop the xarray dataset.
        winslices = dict(zip(["y", "x"], window.toslices()))
        
        # Loop over each xarray dataset in "ds_list".
        for j, _xds in enumerate(ds_list):
            # Crop the xarray dataset using the window and mask.
            cropped = xr_read_window(_xds, winslices, mask=mask)
            
            # Loop over each variable in "var_list".
            for var in var_list:
                # Get the time value for the current xarray dataset.
                time_value = _xds.time.values[0]
                
                # Compute the mean value for the current variable and add it to the corresponding Pandas dataframe.
                var_mean = cropped[var].mean().values.item()
                df_dict[var].loc[i, time_value] = var_mean

    # Close each xarray dataset in "ds_list".
    [ds.close() for ds in ds_list]
    
    # Return the dictionary containing the Pandas dataframes
    return df_dict







def get_forcing_dict_newway_parallel(
    gpkg_divides,  # GeoDataFrame containing polygons to use as masks
    folder_prefix,  # path prefix to the folder containing input data files
    file_list,  # list of input data files
    var_list,  # list of variables to extract
    para="thread",  # type of parallelism: "thread" for threading, "process" for multiprocessing
    para_n=2,  # number of worker threads or processes to use
):
    # Set the raster engine to use for reading input data
    reng = "rasterio"

    # Open the first input file to get the template array for creating in-memory datasets
    _xds = xr.open_dataset(folder_prefix.joinpath(file_list[0]), engine=reng)
    _template_arr = _xds.U2D.values

    # Create an in-memory dataset using the template array
    _u2d = MemoryDataset(
        _template_arr,
        transform=_xds.U2D.rio.transform(),
        gcps=None,
        rpcs=None,
        crs=None,
        copy=False,
    )

    # Open all input files as xarray datasets
    ds_list = [xr.open_dataset("data/" + f) for f in file_list]

    # Determine which type of parallelism to use based on the value of para
    if para == "process":
        pool = concurrent.futures.ProcessPoolExecutor
    elif para == "thread":
        pool = concurrent.futures.ThreadPoolExecutor
    else:
        pool = concurrent.futures.ThreadPoolExecutor

    # Create a thread or process pool with the specified number of workers
    with pool(max_workers=para_n) as executor:
        future_list = []  # list to hold future objects for each crop operation

        # Create a dictionary of DataFrames to hold the extracted data for each variable
        df_dict = {}
        for _v in var_list:
            df_dict[_v] = pd.DataFrame(index=gpkg_divides.index)

        # Loop over each polygon in gpkg_divides and extract the variable data for that polygon
        for i, m in enumerate(gpkg_divides.geometry):
            print(f"{i}, {round(i/len(gpkg_divides.geometry), 5)*100}".ljust(40), end="\r")
            
            # Use polymask to create a mask array for the current polygon
            mask, _, window = polymask(_u2d)(m)
            mask = xr.DataArray(mask, dims=["y", "x"])
            winslices = dict(zip(["y", "x"], window.toslices()))

            # Loop over each input file and extract the variable data for the current polygon and time value
            for f in ds_list:
                cropped = executor.submit(xr_read_window, f, winslices, mask=mask)
                future_list.append(cropped)
                time_value = f.time.values[0]
                for var in var_list:
                    # Compute the mean value of the variable data for the current polygon and time value
                    var_mean = cropped.result()[var].mean().values.item()
                    # Add the mean value to the DataFrame for the current variable and polygon/time value
                    df_dict[var].loc[i, time_value] = var_mean

        # Wait for all crop operations to complete and retrieve the results
        for i, _f in enumerate(concurrent.futures.as_completed(future_list)):
            cropped = _f.result()

    # Close all input files
    [_xds.close() for _xds in ds_list]

    # Return the dictionary containing the Pandas dataframes
    return df_dict





def get_forcing_dict_newway_inverted(
    gpkg_divides,   # Geopackage containing the polygons to be cropped
    folder_prefix,  # Folder where the NetCDF files are stored
    file_list,      # List of NetCDF files to be processed
    var_list,       # List of variables to be extracted from the NetCDF files
):

    reng = "rasterio"
    # Open the first NetCDF file to use as a template for the other files
    _xds = xr.open_dataset(folder_prefix.joinpath(file_list[0]), engine=reng)
    # Get the 2D U variable to use as a template array
    _template_arr = _xds.U2D.values
    # Create a MemoryDataset with the template array and the transform information from the first NetCDF file
    _u2d = MemoryDataset(
        _template_arr,
        transform=_xds.U2D.rio.transform(),
        gcps=None,
        rpcs=None,
        crs=None,
        copy=False,
    )
    # Open all NetCDF files using the Rasterio engine and add them to a list
    ds_list = []
    for _nc_file in file_list:
        _full_nc_file = folder_prefix.joinpath(_nc_file)
        ds_list.append(xr.open_dataset(_full_nc_file, engine=reng))

    # Create a dictionary to store the extracted data
    df_dict = {}
    # For each variable in the var_list, create a DataFrame with the index of gpkg_divides and the columns of the time values of the first NetCDF file
    for _v in var_list:
        df_dict[_v] = pd.DataFrame(index=gpkg_divides.index, columns=_xds.time.values)

    # Create a list to store the masks and windows of each polygon
    mask_win_list = []

    # Loop through the polygons in gpkg_divides
    for i, feature in enumerate(gpkg_divides.geometry):
        print(f"{i}, {round(i/len(gpkg_divides.geometry), 5)*100}".ljust(40), end="\r")
        # Use polymask to create a mask and window for the current polygon
        mask, _, window = polymask(_u2d)(feature)
        # Create a DataArray with the mask
        mask = xr.DataArray(mask, dims=["y", "x"])
        # Create a dictionary with the window slices to extract the data from the NetCDF files
        winslices = dict(zip(["y", "x"], window.toslices()))
        # Add the mask and window to the list
        mask_win_list.append((mask, winslices))

    # Loop through the NetCDF files
    for i, f in enumerate(ds_list):
        print(f"{i}, {round(i/len(file_list), 2)*100}".ljust(40), end="\r")
        # Loop through the masks and windows
        for j, (_m, _w) in enumerate(mask_win_list):
            # Crop the data of the current variable with the current mask and window
            cropped = xr_read_window(f, _w, mask=_m)
            # Loop through the variables in var_list
            for var in var_list:
                # Get the time value of the NetCDF file
                time_value = f.time.values[0]
                # Get the mean of the current variable and store it in the corresponding DataFrame
                var_mean = cropped[var].mean().values.item()
                df_dict[var].loc[j, time_value] = var_mean
            j+=1
            
    # Close all input files
    [f.close() for f in ds_list]
    # Return the dictionary containing the Pandas dataframes
    return df_dict





def get_forcing_dict_newway_inverted_parallel(
    gpkg_divides,   # Geopackage containing the polygons to be cropped
    folder_prefix,  # Path to folder containing netCDF files
    file_list,  # is a list of netCDF files to read data from
    var_list,  # List of variables to extract from netCDF files
    para="thread",  # Type of parallelism to use (either 'thread' or 'process')
    para_n=2,  # Number of workers to use in the pool
):
    import concurrent.futures

    # Open the first netCDF file to use as a template
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

    # Open all netCDF files and store them in a list
    ds_list = [xr.open_dataset("data/" + f) for f in file_list]

    stats = []
    future_list = []
    mask_win_list = []

    # Iterate over each polygon in the GeoDataFrame and extract the corresponding mask and window
    for i, feature in enumerate(gpkg_divides.geometry):
        print(f"{i}, {round(i/len(gpkg_divides.geometry), 5)*100}".ljust(40), end="\r")
        mask, _, window = polymask(_u2d)(feature)
        mask = xr.DataArray(mask, dims=["y", "x"])
        winslices = dict(zip(["y", "x"], window.toslices()))
        mask_win_list.append((mask, winslices))

    # Create a pool of workers depending on the type of parallelism specified
    if para == "process":
        pool = concurrent.futures.ProcessPoolExecutor
    elif para == "thread":
        pool = concurrent.futures.ThreadPoolExecutor
    else:
        pool = concurrent.futures.ThreadPoolExecutor

    with pool(max_workers=para_n) as executor:
        # Create a dictionary to store extracted data for each variable
        df_dict = {}
        for _v in var_list:
            df_dict[_v] = pd.DataFrame(index=gpkg_divides.index)

        # Parallel processing step where each cropped window (represented as a tuple of mask and window) is submitted to a worker pool using the executor.submit() method to extract data from the corresponding netCDF files. The xr_read_window function is used for this purpose. The results of these computations are wrapped in futures, which are added to the future_list variable.
        for f in ds_list:
            print(f"{i}, {round(i/len(file_list), 2)*100}".ljust(40), end="\r")
            for j, (_m, _w) in enumerate(mask_win_list):
                cropped = executor.submit(xr_read_window, f, _w, mask=_m)
                future_list.append(cropped)
                time_value = f.time.values[0]
                # Extract the mean value of each variable for the cropped window
                for var in var_list:
                    var_mean = cropped.result()[var].mean().values.item()
                    df_dict[var].loc[j, time_value] = var_mean
                j += 1

        # Wait for all futures to complete
        for _f in concurrent.futures.as_completed(future_list):
            cropped = _f.result()
    
    # Close all netCDF files
    [f.close() for f in ds_list]
    return df_dict
