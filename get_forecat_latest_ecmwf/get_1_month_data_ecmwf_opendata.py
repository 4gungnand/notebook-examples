# Referensi: medium-2t-wind.ipynb
from ecmwf.opendata import Client
import datetime

# Buat client
c = Client(model="aifs-single") # aifs-single yang datanya paling banyak, kalau ifs gak ada.

# Tentukan tanggal
today = datetime.date.today()

date = today
DATE_STR = date.strftime("%Y-%m-%d")
print(f"Mengunduh data untuk {DATE_STR}...")

# Simpan hasil ke file grib
FILENAME = f'medium-2t-wind-{DATE_STR}.grib2'
TYPE = "fc"          # forecast
# STREAM = "oper"    # operational
TIME="00"            # run model jam 00 UTC
STEP=list(range(0,361, 6))  # forecast step dalam jam
PARAMETERS = ['ssrd'] 
LEVELTYPE = "sfc"    # surface

try:
    c.retrieve(
        type=TYPE,         
        # stream=STREAM,     
        # date=DATE_STR,
        time=TIME,        
        step=STEP,
        param=PARAMETERS,
        levtype=LEVELTYPE,    
        target=FILENAME,
    )
    
except Exception as e:
    print(f"Gagal ambil {DATE_STR}: {e}")