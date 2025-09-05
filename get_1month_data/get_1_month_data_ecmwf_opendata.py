# Referensi: medium-2t-wind.ipynb
from ecmwf.opendata import Client
import datetime

# Buat client
c = Client()

# Tentukan tanggal
today = datetime.date.today()
one_month_ago = today - datetime.timedelta(days=30)

# Loop setiap hari dan unduh data
for i in range(31):
    date = one_month_ago + datetime.timedelta(days=i)
    DATE_STR = date.strftime("%Y-%m-%d")
    print(f"Mengunduh data untuk {DATE_STR}...")

    # Simpan hasil ke file grib
    FILENAME = f'medium-2t-wind-{DATE_STR}.grib2'
    TYPE = "fc"          # forecast
    # STREAM = "oper"    # operational
    TIME="00"            # run model jam 00 UTC
    STEP=[0, 3, 6, 9, 12, 15, 18, 21, 24]  # forecast step dalam jam
    # Variabel yang diambil: 10u (u wind 10m), 10v (v wind 10m), 2t (temp 2m), mx2t3 (max temp 2m 3hr), mn2t3 (min temp 2m 3hr)
    PARAMETERS = ['10u', '10v', '2t', 'mx2t3', 'mn2t3'] 
    LEVELTYPE = "sfc"    # surface

    try:
        c.retrieve(
            type=TYPE,         
            # stream=STREAM,     
            date=DATE_STR,
            time=TIME,        
            step=STEP,
            param=PARAMETERS,
            levtype=LEVELTYPE,    
            target=FILENAME,
        )
        
    except Exception as e:
        print(f"Gagal ambil {DATE_STR}: {e}")