# SnowMapper Performance Optimization Report

## Executive Summary

| Module | Runtime | Peak Memory | Peak CPU | Priority |
|--------|---------|-------------|----------|----------|
| `grid_fsm_to_netcdf.py` | 6m 23s | **4.3 GB** | 110% | **HIGH** |
| `compute_basin_stats.py` | 2m 51s | 1.2 GB* | 600%* | DONE |
| `run_forecast_sim.py` | 0m 50s | TBD | TBD | LOW |
| `merge_reproject.py` | 0m 02s | <100 MB | <100% | LOW |
| `zonal_stats.py` | 0m 06s | <100 MB | <100% | LOW |

*6 workers × ~200MB each

---

## 1. grid_fsm_to_netcdf.py (HIGHEST PRIORITY)

### Profile Results
```
Total time:     405.85s (6.8 min)
Peak memory:    4,309 MB (4.3 GB)
Avg memory:     2,386 MB
Peak CPU:       110.9% (single-threaded)
```

### Bottleneck Analysis

**83% of time** spent in `write_ncdf` → `xarray.to_netcdf`:
```
write_ncdf:      335.9s (83%)
├── to_netcdf:   334.9s
│   └── dump_to_store: 334.1s
│       └── set_variables: 334.0s
```

**Memory:** Each variable loads full grid into memory (~1GB each), 4 variables = 4GB peak.

### Recommended Optimizations

#### 1.1 Process Variables Sequentially with Explicit Memory Cleanup
```python
# Current: All grids stay in memory
for var in ["swe", "snd", "rof", "gst"]:
    process_variable(var, ...)  # Grid stays in memory

# Better: Explicit cleanup
import gc
for var in ["swe", "snd", "rof", "gst"]:
    process_variable(var, ...)
    gc.collect()  # Force garbage collection
```
**Expected improvement:** Peak memory 4.3GB → ~1.5GB

#### 1.2 Use Chunked NetCDF Writing
```python
# Current: Write entire grid at once
ds.to_netcdf(...)

# Better: Chunked writing for large grids
encoding = {var_name: {'chunksizes': (100, 100)}}
ds.to_netcdf(..., encoding=encoding)
```
**Expected improvement:** 10-20% faster writes, lower memory

#### 1.3 Use Dask for Out-of-Core Processing
```python
import dask.array as da

# Convert numpy array to dask with chunks
grid_dask = da.from_array(grid_stack, chunks=(100, 500, 500))
ds = xr.Dataset({var_name: (['time', 'y', 'x'], grid_dask)})
ds.to_netcdf(..., compute=True)
```
**Expected improvement:** Memory usage reduced by 50-70%

#### 1.4 Parallel Variable Processing
Currently processes 4 variables sequentially. Could parallelize:
```python
from concurrent.futures import ProcessPoolExecutor

with ProcessPoolExecutor(max_workers=4) as executor:
    futures = [executor.submit(process_variable, var, ...) for var in vars]
```
**Expected improvement:** 2-3x faster (limited by I/O)

---

## 2. compute_basin_stats.py (ALREADY OPTIMIZED)

### Previous Optimizations Applied
1. ✅ Switched from `rio.clip` to `rasterstats` (9x speedup)
2. ✅ Added `ProcessPoolExecutor` with 6 workers (5x speedup)
3. ✅ **Total: 45x speedup** (2+ hours → 3 min)

### Current Profile
```
Runtime:        2m 51s
Peak memory:    ~1.2 GB (6 workers × 200MB)
CPU usage:      600% (6 cores fully utilized)
```

### Remaining Opportunities

#### 2.1 Cache Affine Transform
Currently recalculates for each file:
```python
# Current: Recalculate each time
affine = Affine(x_res, 0, x.min() - x_res/2, ...)

# Better: Calculate once, reuse
# All NC files have same grid, cache the transform
```
**Expected improvement:** 5-10%

#### 2.2 Pre-rasterize Polygons
```python
# rasterstats rasterizes polygons for each file
# Could pre-rasterize once and reuse mask
from rasterio.features import rasterize
polygon_mask = rasterize(polygons.geometry, out_shape=grid.shape, transform=affine)
```
**Expected improvement:** 20-30%

---

## 3. Memory Optimization Summary

| Module | Current Peak | Target | Technique |
|--------|-------------|--------|-----------|
| grid_fsm_to_netcdf | 4.3 GB | <1.5 GB | gc.collect(), chunking |
| compute_basin_stats | 1.2 GB | 1.0 GB | Cache transforms |
| Total pipeline | ~5 GB | <2 GB | Sequential + cleanup |

---

## 4. CPU Optimization Summary

| Module | Current | Parallel? | Potential |
|--------|---------|-----------|-----------|
| grid_fsm_to_netcdf | 1 core | No → Yes | 2-4x with 4 workers |
| compute_basin_stats | 6 cores | Yes | Already optimal |
| run_forecast_sim | 1 core | TopoPyScale handles | N/A |

---

## 5. Quick Wins (Immediate Implementation)

### 5.1 Add gc.collect() to grid_fsm_to_netcdf.py
```python
import gc

def main(mydir):
    ...
    for var, unit in [("swe", "mm"), ("snd", "m"), ("rof", "mm"), ("gst", "k")]:
        process_variable(var, unit, config.dem.epsg, config.dem.dem_resol)
        gc.collect()  # <-- Add this
```

### 5.2 Disable Debug Output in TopoPyScale
The numbered output (0, 1, 2, ..., 19) is debug print statements in sim_fsm.py.
Removing these will slightly improve I/O performance.

### 5.3 Use float32 Consistently
Ensure all arrays use float32 (not float64) to halve memory usage:
```python
data = data.astype('float32')
```

---

## 6. Profiling Commands

```bash
# Profile any module
python profile_pipeline.py <module_name> [args...]

# Profile grid_fsm_to_netcdf
python profile_pipeline.py grid_fsm_to_netcdf ./domains/D2000

# Memory profiling with mprof
mprof run python grid_fsm_to_netcdf.py ./domains/D2000
mprof plot
```

---

## 7. Monitoring Commands

```bash
# Watch memory usage during pipeline
watch -n 1 'ps aux | grep python | head -5'

# Detailed memory breakdown
python -c "import tracemalloc; tracemalloc.start(); exec(open('script.py').read()); print(tracemalloc.get_traced_memory())"
```
