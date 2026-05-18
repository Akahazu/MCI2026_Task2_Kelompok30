# MCI2026_Task2_Kelompok30

| Name           | NRP        |
| --- | --- |
| Muhammad Zahran Rizki Primanda            | 5025241107        |
| Severinus Fabian Tanuwidjaja            | 5025241110        |


## Pengenalan Dataset
Data diambil dari http://96.9.212.102:8000/orders yang berisikan file JSON yang berisikan rincian belanja dari 100 data pemesanan terakhir dengan struktur sebagai berikut:
```
JSON Response
        │
        ├── total_orders
        │     └── 100
        │
        └── orders[]
              │
              ├── order
              │    │
              │    ├── order_id
              │    ├── user_id
              │    ├── order_number
              │    ├── order_dow
              │    ├── order_hour_of_day
              │    ├── days_since_prior_order
              │    ├── eval_set
              │    │
              │    └── products[]
              │          │
              │          ├── product
              │          │    ├── product_id
              │          │    ├── product_name
              │          │    ├── aisle_id
              │          │    ├── aisle
              │          │    ├── department_id
              │          │    ├── department
              │          │    ├── add_to_cart_order
              │          │    └── reordered
              │          │
              │          ├── product
              │          └── ...
              │
              ├── order
              └── ...
```
<img width="858" height="884" alt="image" src="https://github.com/user-attachments/assets/14f645d7-2b4d-4039-ac5b-99bde3b58d9e" />
<br>

## Pengenalan Struktur Folder dan File
```
Folder/
│
├── dags/
│   │
│   ├── __pycache__/
│   │
│   ├── scripts/
│   │   │
│   │   ├── fetch_orders.py
│   │   │
│   │   ├── process_orders_spark.py
│   │   │  
│   │   │
│   │   └── process_user_behavior.py
│   │
│   └── orders_pipeline.py
│
├── data_lake/
│   │
│   └── orders/
│       │
│       ├── orders_20260517_....
│       ├── orders_20260517_....
│       └── ...
│
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── sql-metabase.sql
└── .gitignore
```

Dalam pengerjaan ini, total akan ada 1 folder utama yang menaungi keseluruhan file dan folder lain, 5 sub folder (dags, data_lake, scripts, \_\_pycache__, dan orders), 9 file utama (meliputi 4 file .py, 1 Dockerfile, 1 file .yml, 1 file .txt, 1 file .sql dan 1 .gitignore(kosong pada kasus ini)), serta file dinamis pada folder dags/\_\_pycache__/ yang berisikan cache agar kode python bisa lebih efisien dan data_lake/orders/ yang berisikan raw data dalam bentuk parquet.

<br>
<br>

## Penjelasan Fungsi Umum Setiap Kode
1. __Dockerfile & docker-compose.yml__
   * Menyiapkan kebutuhan dan menjalankan compose apache spark dan airflow dengan docker
2. __requirements.txt__
   * Berisikan kebutuhan extension dan versinya
3. __fetch_order.py__
   * Mengambil JSON data dari URL dan menjadikannya dalam bentuk dataset dengan data type .parquet
4. __process_orders_spark.py__
   * Membuat tabel orders_top_product beserta isinya pada database analytics.
5. __process_user_behavior.py__
   * Membuat tabel customer_ranking beserta isinya pada database analytics.
6. __orders_pipeline.py__
   * Membentuk DAG pada airflow dan melakukan fetch dari dataset ke file process untuk dioleh menjadi tabel baru.
<br>
<br>

## Kode untuk DAG Apache Airflow
Proses ini berpusat pada fetch_order.py dan orders_pipeline.py.

### orders_pipeline.py
Source code: [orders_pipeline.py](https://github.com/Akahazu/MCI2026_Task2_Kelompok30/blob/main/dags/orders_pipeline.py) <br>
Langkah pada pengerjaan kode:
1. __Import Library__
   * Library yang digunakan yaitu Airflow dan datetime.
2. __Konfigurasi Awal__
   * Blok kode default_args mendefinisikan konfigurasi awal pada pipeline dari owner dan bagaimana kecenderungannya.
3. __Konfigurasi Lanjutan__
   * Memberi nama pada DAG, mengatur cron, dan konfigurasi lain
4. __Tasks__
   * Program membuat task untuk DAG yang mana di sini kami membuat 3 task yaitu menjalankan fetch_order.py, process_orders_spark.py, dan process_user_behavior.py. Setelah itu, diberikan pengaturan agar fetch_order.py harus berhasil dijalankan sebelum kedua tasks lainnya.
<br>

### fetch_order.py
Source code: [fetch_order.py](https://github.com/Akahazu/MCI2026_Task2_Kelompok30/blob/main/dags/scripts/fetch_orders.py) <br>
Langkah pengerjaan pada kode:
1. __Import Library__
   * Library yang digunakan yaitu request, pandas,os, dan datetime.
2. __Mendefinisikan path API_URL__
   * Memastikan bahwa program mengambil dari data yang benar.
3. __Fetch__
   * Fetch dimulai dengan melakukan request dan mengambil payload dari URL yang mana setelahnya setiap kolom didefinisikan satu per satu isinya agar sesuai antara dataset awal dengan tabel baru nantinya yang akan dianalisis. Lalu dilanjutkan dengan membuat setiap calon kolom tadi ke dalam data frame dan mengubahnya lagi ke parquet pada direktori data_lake/orders/.
<br>
<br>

## Kode untuk Database 
Proses ini meliputi 2 tahap yaitu DDL untuk membuat tabel baru dari dataset dan query untuk membuat visualisasi pada metabase. Untuk DDL, pada proses ini diwakili oleh __process_orders_spark.py__ untuk tabel orders_top_products dan __process_user_behavior.py__ untuk tabel customer_ranking. Sedangkan untuk query yang kami gunakan tersimpan dalam sql-metabase.sql.

### process_orders_spark.py
Source code: [process_orders_spark.py](https://github.com/Akahazu/MCI2026_Task2_Kelompok30/blob/main/dags/scripts/process_orders_spark.py) <br>
Langkah pengerjaan pada kode:
1. __Import Libary__
   * Library yang digunakan yaitu pyspark.sql, clickhouse_driver.
2. __Membuat SparkSession dan Data Frame__
   * Membuat sesi agar spark dikenali dan dapat dikonfigurasi untuk digunakan. Setelah itu dilanjutkan dengan membaca file parquet dengan spark agar bisa diubah ke dalam bentuk data frame.
3. __Agregasi Produk__
   * Membuat kolom kolom berisi data yang diinginkan untuk digunakan dan diagregasikan menjadi sebuah "prototype" tabel. Setelah selesai diagregasi, data-data produk diolah ke dalam Pandas.
4. __Koneksi ke Clickhouse dan Pembuatan Tabel__
   * Program mengoneksikan diri dengan ClickHouse yang telah dikonfigurasikan pada docker-compose.yml lalu membuat tabel baru berisikan data data pada data frame yang telah dibuat sebelumnya.
5. __Auto Update__
   * Dengan truncate - insert, metabase selalu menampilkan hasil terbaru dari URL dataset.
__Hasil__:

<br>

### __process_user_behavior.py__
Source Code: [process_user_behavior.py](https://github.com/Akahazu/MCI2026_Task2_Kelompok30/blob/main/dags/scripts/process_user_behavior.py) <br>
Langkah pengerjaan pada kode sama dengan process_orders_spark.py karena memiliki fungsi yang sama yaitu membuat tabel, namun __process_user_behavior.py__ mengimport os dan glob untuk melakukan parquet clenaing di akhir setelah auto update.
__Hasil__:
<br>

### sql-metabase.sql
Source Code: [sql-metabase.sql](https://github.com/Akahazu/MCI2026_Task2_Kelompok30/blob/main/sql-metabase.sql) <br>



