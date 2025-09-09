# Reference: https://confluence.ecmwf.int/display/DAC/ECMWF+open+data%3A+real-time+forecasts+from+IFS+and+AIFS
from ecmwf.opendata import Client
import datetime

# Buat client
# aifs-single yang datanya paling banyak, kalau ifs gak ada.
MODELNAME = "aifs-single"
c = Client(model=MODELNAME)

# Tentukan tanggal 1 tahun lalu
today = datetime.datetime.today()
# today = datetime.datetime(2024, 6, 1)  # untuk tes
date = today - datetime.timedelta(days=365)

DATE_STR = date.strftime("%Y-%m-%d %H:%M:%S")
print(f"Mengunduh data untuk {DATE_STR}...")

# Parameter untuk unduh data
TYPE = "fc"                   # forecast
# STREAM = "oper"             # operational
TIME = "00"                   # run model jam 00 UTC
STEP = list(range(0, 361, 6))  # forecast step dalam jam
PARAMETERS = ['ssrd']         # surface net solar radiation downwards
LEVELTYPE = "sfc"             # surface

# Simpan hasil ke file grib
FILENAME = f'{MODELNAME}-{PARAMETERS[0]}-{DATE_STR}.grib2'

try:
    c.retrieve(
        type=TYPE,
        # stream=STREAM,
        date=DATE_STR,
        # Kalau date gak ada, otomatis ambil yang terbaru
        time=TIME,
        step=STEP,
        param=PARAMETERS,
        levtype=LEVELTYPE,
        target=FILENAME,
    )

except Exception as e:
    print(f"Gagal ambil {DATE_STR}: {e}")
