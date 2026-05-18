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
   * Dengan truncate - insert, metabase selalu menampilkan hasil terbaru dari URL dataset. <br> <br>


__Hasil__: <br>
<img alt="image" src="https://github.com/Akahazu/MCI2026_Task2_Kelompok30/blob/main/images/Metabase%20-%20Orders%20Top%20Products_page-0001.jpg" />

<br>
__Penjelasan Tabel:__ <br>
Tabel ini berisikan data per produk yang terkhusus untuk melihat produk dengan penjualan terbanyak. Total kolom pada tabel ini ada 5 yaitu product_name department, total_orders, reorder_count Int32, dan unique_users. Dengan kombinasi ini, diharapkan tabel ini bisa membantu visualisasi yang berfokus pada pengenalan produk.
<br>

### __process_user_behavior.py__
Source Code: [process_user_behavior.py](https://github.com/Akahazu/MCI2026_Task2_Kelompok30/blob/main/dags/scripts/process_user_behavior.py) <br>
Langkah pengerjaan pada kode sama dengan process_orders_spark.py karena memiliki fungsi yang sama yaitu membuat tabel, namun __process_user_behavior.py__ mengimport os dan glob untuk melakukan parquet clenaing di akhir setelah auto update. <br>
__Hasil__:

<br>
__Penjelasan Tabel:__ <br>
Tabel ini berisikan data setiap user dan perilakunya. Tabel ini memiliki 9 kolom yaitu user_id, basket_size (total produk yang dibeli), reorder_total (total barang yang user reorder dalam order yang terdata), days_since_prior_order, avg_order_hour, basket_score (hasil normalisasi basket_size dengan skala 0-1), reorder_rate (hasil normalisasi reorder_total dengan skala 0-1), recency_score (hasil normalisasi days_since_prior_order dengan skala 0-1), dan final_score yaitu perhitungan nilai yang diberikan pada setiap user dari perilaku membelinya yang didasari dengan model yang mengikuti RFM tapi dengan penyesuaian dengan data yang ada.
<br>

### sql-metabase.sql
Source Code: [sql-metabase.sql](https://github.com/Akahazu/MCI2026_Task2_Kelompok30/blob/main/sql-metabase.sql) <br>
Data yang telah tersimpan di ClickHouse kemudian divisualisasikan melalui Metabase dengan Dashboard berdasarkan query-query dibawah ini. Hal ini dirancang untuk mengubah data transaksional menjadi metrik yang terukur, membantu proses pengambilan keputusan melalui pendekatan berbasis data.

#### A. Analisis Perilaku & Loyalitas Pelanggan

- **Customer Segmentation:** Pengelompokan dilakukan dengan mengevaluasi final_score, reorder_rate, dan durasi pesanan terakhir. Kategori yang terbentuk meliputi:

    - **The Champions:** Pelanggan dengan skor > 80 (paling loyal).

    - **The Potential:** Pelanggan dengan skor cukup tinggi yang memiliki potensi besar untuk menjadi pelanggan paling loyal.

    - **Regular:** Pelanggan dengan skor biasa-biasa saja. 
    
    - **At Risk Customer:** Pelanggan dengan skor menengah yang memerlukan perhatian agar tidak berpindah.

    - **New Customer:** Pelanggan baru

    - **Lost Customer:** Pelanggan dengan performa skor terendah (sudah tidak pernah kembali lagi).

- **Top 10 Loyal Customer:** Tabel ini mengagregasikan skor loyalitas setiap pengguna. Fungsi utamanya adalah mengidentifikasi individu dengan final score tertinggi.

#### B. Analisis Produk & Kebutuhan Stok

- **Most Reordered Products:** Menampilkan produk yang memiliki tingkat pemesanan ulang (reorder count) tertinggi.

- **Top Department:** Agregasi jumlah pesanan berdasarkan departemen. 

- **Stocking Type:** Mengklasifikasikan perilaku belanja pelanggan berdasarkan basket size dan reorder rate:

    - **Stockpiler:** Pelanggan yang membeli dalam jumlah besar sekaligus namun jarang memesan ulang 

    - **Frequent Reorderer:** Pelanggan yang membeli dalam jumlah sedikit namun sangat sering memesan kembali.

    - **Balanced:** Pelanggan dengan pola belanja menengah yang stabil.

#### C. Analisis Tambahan

- **KPI Scorecards:** Menyajikan angka kumulatif yang bersifat esensial, seperti **Total Orders**, **Total Products Type**, hingga **Average User per Product**. 

- **Peak Hour Analysis:** Memetakan frekuensi transaksi berdasarkan waktu kejadian. Query ini bertujuan untuk mengidentifikasi jam-jam sibuk (peak hours) di mana kepadatan transaksi meningkat.


## Konklusi
Dataset yang diberikan dapat digunakan untuk pengolahan data yang beragam seperti mencari top product maupun clustering customer. Untuk kelebihan dari dataset ini, data mudah dibaca karena memiliki format yang jelas sehingga tidak ada kendala parsing. Selain itu dataset ini juga memiliki informasi yang cukup lengkap seperti departemen dari setiap produk dan apakah produk tersebut sudah pernah diorder oleh user yang sedang membelinya sekarang atau belum. Untuk kekurangannya sendiri terletak di bagian informasi krusial atau umum yang malah tidak tercantum seperi harga produk dan kuantitas produk yang dibeli per order sehingga visualisasi data harus melewati step tambahan untuk penyesuaian.
<br>
Dengan bantuan dari spark, docker, clickhouse, apache airflow, dan metabase, pengolahan data seperti ini dapat dilakukan dengan lebih mudah dengan fitur yang cukup beragam di setiap stepnya. Kelebihan dari semua tools yang kami sebutkan tadi adalah keberagaman dari cara penggunaan yang masih bisa dieksplor lebih jauh dari apa yang sudah digunakan sehingga memungkinkan penggunaan yang lebih versatile. Kekurangan dari semua tools yang kami sebutkan tadi adalah session yang selalu restart setiap dijalankan ulang sehingga tidak bisa menyimpan progress dan juga docker yang cukup berat sehingga terkadang membuat device overload.
