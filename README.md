# era5-gcs-to-zarr

用于直接从 **Google Cloud Storage (GCS)** 读取 ERA5 再分析数据并将其转换为本地 **Zarr 格式** 的 Python 脚本。  
支持并行处理、缓存、断点续传和高效的分块存储，适用于气候研究和机器学习。

Python scripts for reading ERA5 reanalysis data directly from **Google Cloud Storage (GCS)** and converting to local **Zarr format**.  
Supports parallel processing, caching, resume on interruption, and efficient chunked storage for climate research and machine learning.

---

---

依赖安装 / Requirements:

```bash
pip install pandas gcsfs xarray numpy rich psutil
```

使用方法 / Usage:
克隆仓库 / Clone the repository:

```bash
git clone https://github.com/Schw11n/era5-gcs-to-zarr.git
cd era5-gcs-to-zarr
```

修改配置参数（脚本顶部） / Adjust configuration in the script:

PRESSURE_LEVELS: 需要的气压层，例如 ["500", "700"] / Pressure levels to download, e.g. ["500", "700"]

VARIABLES: 需要的变量，例如温度、风场、湿度等 / Variables to fetch, e.g. temperature, wind, humidity

ZARR_PATH: 本地 Zarr 文件保存路径 / Local save path for Zarr file

MAX_WORKERS: 并行进程数 / Number of parallel processes

BATCH_SIZE: 每批处理的天数 / Number of days per batch

经纬度范围: 在读取一天()函数中修改 latitude 和 longitude / Latitude/longitude range in function

运行脚本 / Run the script:

```bash
python era5_gcs_to_zarr.py
```

按提示输入开始日期和结束日期，脚本会自动下载并生成 Zarr 文件。
Enter start and end dates when prompted. The script will download and merge data into a Zarr file.
