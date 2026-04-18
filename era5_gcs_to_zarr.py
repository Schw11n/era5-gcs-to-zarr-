import pandas as pd
import gcsfs
import xarray as xr
from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED
import multiprocessing as mp
import numpy as np
import os
import time
import warnings
import psutil
import tempfile
import shutil
from rich.live import Live
from rich.table import Table

warnings.filterwarnings("ignore", category=UserWarning)

# 缓存目录（请根据实际情况修改）
os.environ["TMPDIR"] = "D:\\era5_tmp"
os.environ["TEMP"] = "D:\\era5_tmp"
os.environ["TMP"] = "D:\\era5_tmp"
tempfile.tempdir = "D:\\era5_tmp"

os.makedirs("D:\\era5_tmp", exist_ok=True)
os.makedirs("D:\\era5_block_cache", exist_ok=True)

def bind_to_core(worker_id):
    """可选：绑定进程到指定CPU核心"""
    try:
        p = psutil.Process(os.getpid())
        p_cores = [0, 2, 4, 6]  # 修改为你的CPU核心编号
        core = p_cores[worker_id % len(p_cores)]
        p.cpu_affinity([core])
    except Exception:
        pass

# 配置参数
PRESSURE_LEVELS = ["500", "700"]
VARIABLES = {
    "u": "u_component_of_wind",
    "v": "v_component_of_wind",
    "w": "vertical_velocity",
    "z": "geopotential",
    "t": "temperature",
    "q": "specific_humidity",
    "cc": "fraction_of_cloud_cover"
}
BASE_PATH = "gcp-public-data-arco-era5/raw/date-variable-pressure_level"
ZARR_PATH = r"E:\BSBS\zarr\era5_uv_500_700.zarr"   # 修改为你的保存路径
MAX_WORKERS = 4
BATCH_SIZE = 4

def get_completed():
    """检查已完成的日期（断点续传）"""
    if not os.path.exists(ZARR_PATH):
        return set()
    try:
        ds = xr.open_zarr(ZARR_PATH, consolidated=False)
        if "time" not in ds.dims:
            ds.close()
            raise ValueError("Zarr缺少time维度")
        n_days = ds.sizes["time"]
        ds.close()
        start_date = pd.Timestamp("2000-01-01")   # 修改为你的起始日期
        return {start_date + pd.Timedelta(days=i) for i in range(n_days)}
    except Exception:
        try:
            shutil.rmtree(ZARR_PATH)
        except Exception:
            pass
        return set()

def read_one_day(i, d, status_dict):
    """下载一天数据并保存为临时Zarr"""
    bind_to_core(i)
    all_keys = [f"{var}_{level}" for level in PRESSURE_LEVELS for var in VARIABLES.keys()]
    status_dict[d] = {"completed": [], "total": len(all_keys), "all_keys": all_keys, "current": None, "current_start": 0.0}

    fs = gcsfs.GCSFileSystem(token="anon", block_cache_dir="D:\\era5_block_cache", cache_timeout=0)
    data_all, lat_vals, lon_vals = {}, None, None

    for level in PRESSURE_LEVELS:
        for var_name, folder in VARIABLES.items():
            key = f"{var_name}_{level}"
            path = f"{BASE_PATH}/{d:%Y/%m/%d}/{folder}/{level}.nc"
            cur = status_dict.get(d, {})
            cur["current"], cur["current_start"] = key, time.time()
            status_dict[d] = cur
            try:
                with fs.open(path, "rb", timeout=60) as f:
                    ds = xr.open_dataset(f)
                da = ds[list(ds.data_vars)[0]]
                # 修改为你的研究区域经纬度
                data = da.sel(latitude=slice(32, 27), longitude=slice(81, 98)).astype("float32").values
                if lat_vals is None:
                    lat_vals = da.sel(latitude=slice(32, 27)).latitude.values
                    lon_vals = da.sel(longitude=slice(81, 98)).longitude.values
                data_all[key] = data
                ds.close()
            except Exception as e:
                print(f"[错误] 下载失败 {path}: {e}")
            cur = status_dict.get(d, {})
            completed_list = list(cur.get("completed", []))
            if key not in completed_list:
                completed_list.append(key)
            cur["completed"] = completed_list
            status_dict[d] = cur

    worker_zarr = f"D:\\era5_tmp\\part_{i:04d}_{d:%Y%m%d}.zarr"
    ds_new = xr.Dataset(
        {k: (["time", "step", "lat", "lon"], v[np.newaxis, ...]) for k, v in data_all.items()},
        coords={"time": [d], "step": np.arange(24), "lat": lat_vals, "lon": lon_vals}
    )
    ds_new.to_zarr(worker_zarr, mode="w", consolidated=False)
    cur = status_dict.get(d, {})
    cur["current"], cur["current_start"] = None, 0.0
    status_dict[d] = cur
    return {"zarr": worker_zarr, "date": d}

def merge_one(path):
    """合并临时Zarr到主文件"""
    ds = xr.open_zarr(path, consolidated=False)
    if os.path.exists(ZARR_PATH):
        ds.to_zarr(ZARR_PATH, mode="a", append_dim="time")
    else:
        ds.to_zarr(ZARR_PATH, mode="w")
    ds.close()
    shutil.rmtree(path, ignore_errors=True)

if __name__ == "__main__":
    mp.freeze_support()
    print("\n" + "=" * 70)
    print(" ERA5 多变量处理（并行读取 + 动态仪表盘） ")
    print("=" * 70)

    def input_date(prompt):
        while True:
            print(prompt)
            s = input().strip()
            try:
                return pd.to_datetime(s, format="%Y-%m-%d")
            except:
                print("格式错误！请重新输入\n")

    start_date = input_date("请输入开始日期 (YYYY-MM-DD):")
    end_date = input_date("请输入结束日期 (YYYY-MM-DD):")
    dates = pd.date_range(start_date, end_date)
    completed = get_completed()
    dates = [d for d in dates if pd.Timestamp(d) not in completed]
    print(f"[任务] 剩余天数: {len(dates)}")
    if not dates:
        exit()

    total_batches = (len(dates) + BATCH_SIZE - 1) // BATCH_SIZE
    pool = ProcessPoolExecutor(max_workers=MAX_WORKERS)
    manager = mp.Manager()
    status_dict = manager.dict()
    completed_days, global_start_time = 0, time.time()

    def get_renderable(batch_dates, batch_idx):
        table = Table(show_header=True, header_style="bold cyan")
        table.add_column("日期")
        table.add_column("进度")
        for d in batch_dates:
            if d in status_dict:
                info = status_dict[d]
                progress = f"{len(info.get('completed', []))}/{info.get('total', 0)}"
            else:
                progress = "0"
            table.add_row(str(d.date()), progress)
        pct = completed_days / len(dates)
        bar = "█" * int(30 * pct) + "░" * (30 - int(30 * pct))
        table.title = f"总进度: {completed_days}/{len(dates)} ({pct:.1%}) {bar}"
        table.caption = f"批次 {batch_idx}/{total_batches}"
        return table

    try:
        with Live(get_renderable([], 0), refresh_per_second=4, screen=False) as live:
            for batch_idx, b in enumerate(range(0, len(dates), BATCH_SIZE), start=1):
                batch = dates[b:b+BATCH_SIZE]
                futures = [pool.submit(read_one_day, i, d, status_dict) for i, d in enumerate(batch)]
                buffer, next_time = {}, min(batch)
                pending = list(futures)
                while pending:
                    done, pending = wait(pending, timeout=0.5, return_when=FIRST_COMPLETED)
                    live.update(get_renderable(batch, batch_idx))
                    for f in done:
                        try:
                            r = f.result()
                            d = r["date"]
                            buffer[d] = r["zarr"]
                            while next_time in buffer:
                                merge_one(buffer[next_time])
                                del buffer[next_time]
                                next_time += pd.Timedelta(days=1)
                            completed_days += 1
                            live.update(get_renderable(batch, batch_idx))
                        except Exception as e:
                            print(f"[任务异常] {e}")
                for d in batch:
                    if d in status_dict:
                        del status_dict[d]
    except KeyboardInterrupt:
        print("\n检测到 Ctrl+C，正在退出")
        pool.shutdown(wait=True)
        exit()
    finally:
        pool.shutdown(wait=True)

    print("\n全部完成")
