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
    FILENAME = 'medium-2t-wind.grib'
    TYPE = "fc"          # forecast
    # STREAM = "oper"      # operational
    TIME="00"            # run model jam 00 UTC
    STEP=6               # jam ke 6 dari run model 
    PARAMETERS = ['10u', '10v','2t', 'mx2t3', 'mn2t3'] 

    try:
        c.retrieve(
            type=TYPE,           # forecast
            # stream=STREAM,    # operational
            date=DATE_STR,
            time=TIME,        # run model jam 00 UTC
            step=STEP,
            param=PARAMETERS,
            levtype="sfc",      # surface
            target=FILENAME
        )
        
    except Exception as e:
        print(f"Gagal ambil {DATE_STR}: {e}")