from cmip_results import CMIPPlot
import matplotlib.pyplot as plt

def two_plots():
    cmip_plot = CMIPPlot()
    plt.figure()
    # map plot
    cmip_plot.plot('time_boxplot', 'map', 'log')
    plt.xticks(range(1,4), ['S3netcdf4','NetCDF4-python', 'xarray/zarr'])
    plt.title('X-Y map reads from Caringo') 
    plt.grid(which='both', axis='y')
    cmip_plot.save_plot('agu_map.png')

    # time series
    cmip_plot.plot('time_boxplot', 'timeseries', 'log')
    plt.xticks(range(1,4), ['S3netCDF4','NetCDF4-python', 'xarray/zarr'])
    plt.title('Timeseries reads from Caringo') 
    plt.grid(which='both', axis='y')
    cmip_plot.save_plot('agu_timeseries.png')

def one_plot():
    cmip_plot = CMIPPlot()
    plt.figure()
    # map plot
    plt.subplot(1,2,1)
    cmip_plot.plot('time_boxplot', 'map', 'log')
    plt.xticks(range(1,4), ['S3netcdf4','NetCDF4-python', 'xarray/zarr'], rotation=10)
    plt.title('X-Y map reads from Caringo') 
    plt.grid(which='both', axis='y')

    # time series
    ax = plt.subplot(1,2,2)
    cmip_plot.plot('time_boxplot', 'timeseries', 'log')
    plt.xticks(range(1,4), ['S3netCDF4','NetCDF4-python', 'xarray/zarr'], rotation=10)
    plt.title('Timeseries reads from Caringo') 
    plt.grid(which='both', axis='y')
    ax.set_ylabel('')
    cmip_plot.save_plot('agu_both.png')


if __name__ == "__main__":
    one_plot()
    two_plots()